// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Open-loop load generator that talks to the metadata quorum and chunk groups directly through
//! the SDK client, with no HTTP gateway in between. Same command line and JSON output as the
//! HTTP load generator used for the gateway, so the two are comparable.
//!
//!   qps_direct populate|run --objects N --value V --conc C [--per-object M] [--rate R --secs S]
//!       [--read-frac F] [--dist hot:N:F] [--rsize BYTES|whole] [--warm S] [--prefix p] [--label L]
//! Environment: QPS_META (comma separated endpoints), QPS_GROUP (metadata group id, default 1),
//! QPS_BUCKET (default temporalstore).
use matrixobjectstore_rs::cluster::{
    MetaClientOptions, ObjectWriteOptions, RemoteMetaClient, ReplicaClientOptions,
};
use matrixobjectstore_rs::object_manifest::ObjectManifest;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545F4914F6CDD1D)
    }
    fn f(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
}

#[derive(Clone)]
struct Cfg {
    meta: Vec<String>,
    group: u64,
    bucket: String,
    prefix: String,
    objects: usize,
    per_object: usize,
    value: usize,
    conc: usize,
    rate: f64,
    secs: f64,
    warm: f64,
    read_frac: f64,
    hot_n: usize,
    hot_f: f64,
    rsize: Option<usize>,
    label: String,
    first: usize,
}

fn parse() -> (String, Cfg) {
    let a: Vec<String> = std::env::args().collect();
    let mode = a.get(1).cloned().unwrap_or_default();
    let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
    let mut c = Cfg {
        meta: env("QPS_META", "127.0.0.1:22004,127.0.0.1:22005,127.0.0.1:22006").split(',').map(str::to_owned).collect(),
        group: env("QPS_GROUP", "1").parse().unwrap(),
        bucket: env("QPS_BUCKET", "temporalstore"),
        prefix: "o".into(),
        objects: 100,
        per_object: 1,
        value: 1024,
        conc: 16,
        rate: 100.0,
        secs: 10.0,
        warm: 0.0,
        read_frac: 0.0,
        hot_n: 0,
        hot_f: 0.0,
        rsize: Some(1024),
        label: String::new(),
        first: 0,
    };
    let mut i = 2;
    while i < a.len() {
        let k = a[i].as_str();
        if k == "--poisson" {
            i += 1;
            continue;
        }
        let v = a.get(i + 1).cloned().unwrap_or_default();
        match k {
            "--addr" | "--bucket" | "--status-every" => {}
            "--prefix" => c.prefix = v,
            "--objects" => c.objects = v.parse().unwrap(),
            "--per-object" => c.per_object = v.parse().unwrap(),
            "--value" => c.value = v.parse().unwrap(),
            "--conc" => c.conc = v.parse().unwrap(),
            "--rate" => c.rate = v.parse().unwrap(),
            "--secs" => c.secs = v.parse().unwrap(),
            "--warm" => c.warm = v.parse().unwrap(),
            "--read-frac" => c.read_frac = v.parse().unwrap(),
            "--label" => c.label = v,
            "--first" => c.first = v.parse().unwrap(),
            "--rsize" => c.rsize = if v == "whole" { None } else { Some(v.parse().unwrap()) },
            "--dist" => {
                if let Some(rest) = v.strip_prefix("hot:") {
                    let mut p = rest.split(':');
                    c.hot_n = p.next().unwrap().parse().unwrap();
                    c.hot_f = p.next().unwrap().parse().unwrap();
                }
            }
            _ => {
                eprintln!("unknown arg {k}");
                std::process::exit(2);
            }
        }
        i += 2;
    }
    (mode, c)
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

fn client(c: &Cfg) -> RemoteMetaClient {
    let t = Duration::from_secs(30);
    let o = MetaClientOptions { connect_timeout: t, read_timeout: t, write_timeout: t, ..Default::default() };
    RemoteMetaClient::connect_raft_catalog(&c.meta, c.group, &o).expect("connect metadata quorum")
}

fn wopts(c: &Cfg) -> ObjectWriteOptions {
    let mut w = ObjectWriteOptions::default();
    w.page_bytes = 1024 * 1024;
    w.target_groups = 1;
    w.max_object_bytes = 64 * 1024 * 1024;
    w.update_time_ms = now_ms();
    w.replica = ropts();
    let _ = c;
    w
}

fn ropts() -> ReplicaClientOptions {
    let mut r = ReplicaClientOptions::default();
    r.deadline = Some(Instant::now() + Duration::from_secs(10));
    r
}

fn key(c: &Cfg, i: usize) -> String {
    format!("{}{:08}", c.prefix, i)
}

fn load_note() -> String {
    std::fs::read_to_string("/proc/loadavg").unwrap_or_default().split_whitespace().take(3).collect::<Vec<_>>().join(" ")
}

struct Slot {
    m: Option<ObjectManifest>,
}

fn populate(c: Cfg) {
    let slots: Arc<Vec<Mutex<Slot>>> = Arc::new((0..c.objects).map(|_| Mutex::new(Slot { m: None })).collect());
    let next = Arc::new(AtomicU64::new(0));
    let errs = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicU64::new(0));
    let t0 = Instant::now();
    let data = Arc::new(vec![b'x'; c.value]);
    let mut hs = vec![];
    for _ in 0..c.conc {
        let (c, next, errs, done, slots, data) = (c.clone(), next.clone(), errs.clone(), done.clone(), slots.clone(), data.clone());
        hs.push(std::thread::spawn(move || {
            let mut cl = client(&c);
            loop {
                let i = next.fetch_add(1, Ordering::Relaxed) as usize;
                if i >= c.objects {
                    break;
                }
                let k = key(&c, c.first + i);
                let mut m: Option<ObjectManifest> = None;
                for _ in 0..c.per_object {
                    let r = match m.take() {
                        None => cl.write_object(&c.bucket, &k, None, &data, &wopts(&c)),
                        Some(prev) => cl.append_object(prev, &data, &wopts(&c)),
                    };
                    match r {
                        Ok(nm) => {
                            m = Some(nm);
                            done.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(e) => {
                            eprintln!("populate {k}: {e}");
                            errs.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                    }
                }
                slots[i].lock().unwrap().m = m;
            }
        }));
    }
    for h in hs {
        h.join().unwrap();
    }
    let el = t0.elapsed().as_secs_f64();
    let n = done.load(Ordering::Relaxed);
    println!(
        "{{\"type\":\"populate\",\"label\":\"{}\",\"objects\":{},\"per_object\":{},\"value\":{},\"appends\":{},\"errors\":{},\"secs\":{:.2},\"appends_per_s\":{:.1},\"load\":\"{}\"}}",
        c.label, c.objects, c.per_object, c.value, n, errs.load(Ordering::Relaxed), el, n as f64 / el, load_note()
    );
}

struct Job {
    intended: Instant,
    obj: usize,
    read: bool,
    roff: u64,
}
struct Sample {
    sec: u32,
    lat_us: u32,
    svc_us: u32,
    read: bool,
}
fn pct(v: &[u32], p: f64) -> u32 {
    if v.is_empty() {
        0
    } else {
        v[(((v.len() as f64) * p / 100.0) as usize).min(v.len() - 1)]
    }
}

fn run(c: Cfg) {
    let slots: Arc<Vec<Mutex<Slot>>> = Arc::new((0..c.objects).map(|_| Mutex::new(Slot { m: None })).collect());
    {
        // learn each object's manifest once
        let next = Arc::new(AtomicU64::new(0));
        let mut hs = vec![];
        for _ in 0..c.conc.min(16).max(1) {
            let (c, next, slots) = (c.clone(), next.clone(), slots.clone());
            hs.push(std::thread::spawn(move || {
                let mut cl = client(&c);
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed) as usize;
                    if i >= c.objects {
                        break;
                    }
                    if let Ok(m) = cl.read_manifest(&c.bucket, &key(&c, c.first + i)) {
                        slots[i].lock().unwrap().m = Some(m);
                    }
                }
            }));
        }
        for h in hs {
            h.join().unwrap();
        }
    }
    let queue: Arc<(Mutex<VecDeque<Job>>, Condvar)> = Arc::new((Mutex::new(VecDeque::new()), Condvar::new()));
    let closed = Arc::new(AtomicBool::new(false));
    let qmax = Arc::new(AtomicU64::new(0));
    let errs = Arc::new(Mutex::new(std::collections::BTreeMap::<String, u64>::new()));
    let data = Arc::new(vec![b'y'; c.value]);
    let start = Instant::now() + Duration::from_millis(500);
    let warm_end = c.warm;
    let mut hs = vec![];
    for _ in 0..c.conc {
        let (c2, queue, closed, slots, errs, data) = (c.clone(), queue.clone(), closed.clone(), slots.clone(), errs.clone(), data.clone());
        hs.push(std::thread::spawn(move || {
            let mut cl = client(&c2);
            let mut out: Vec<Sample> = Vec::new();
            loop {
                let job = {
                    let (m, cv) = &*queue;
                    let mut g = m.lock().unwrap();
                    loop {
                        if let Some(j) = g.pop_front() {
                            break Some(j);
                        }
                        if closed.load(Ordering::Acquire) {
                            break None;
                        }
                        g = cv.wait(g).unwrap();
                    }
                };
                let Some(job) = job else { break };
                let t0 = Instant::now();
                let k = key(&c2, c2.first + job.obj);
                let mut ok = false;
                if job.read {
                    let len = slots[job.obj].lock().unwrap().m.as_ref().map(|m| m.length).unwrap_or(0);
                    if len == 0 {
                        continue;
                    }
                    let (off, n) = match c2.rsize {
                        None => (0, len),
                        Some(n) => {
                            let off = job.roff % len.saturating_sub(1).max(1);
                            (off, (n as u64).min(len - off))
                        }
                    };
                    match cl.read_object_range(&c2.bucket, &k, off, n, 64 * 1024 * 1024, &ropts()) {
                        Ok(_) => ok = true,
                        Err(e) => *errs.lock().unwrap().entry(format!("read {e}")).or_insert(0) += 1,
                    }
                } else {
                    let mut g = slots[job.obj].lock().unwrap();
                    let prev = match g.m.take() {
                        Some(m) => Some(m),
                        None => cl.read_manifest(&c2.bucket, &k).ok(),
                    };
                    let r = match prev {
                        Some(p) => cl.append_object(p, &data, &wopts(&c2)),
                        None => cl.write_object(&c2.bucket, &k, None, &data, &wopts(&c2)),
                    };
                    match r {
                        Ok(m) => {
                            g.m = Some(m);
                            ok = true;
                        }
                        Err(e) => {
                            *errs.lock().unwrap().entry(format!("append {e}").chars().take(120).collect()).or_insert(0) += 1;
                            g.m = cl.read_manifest(&c2.bucket, &k).ok();
                        }
                    }
                }
                if ok {
                    let done = Instant::now();
                    let since = job.intended.saturating_duration_since(start).as_secs_f64();
                    if since >= warm_end {
                        out.push(Sample {
                            sec: since as u32,
                            lat_us: done.saturating_duration_since(job.intended).as_micros().min(u32::MAX as u128) as u32,
                            svc_us: done.saturating_duration_since(t0).as_micros().min(u32::MAX as u128) as u32,
                            read: job.read,
                        });
                    }
                }
            }
            out
        }));
    }
    let mut rng = Rng(0x9E3779B97F4A7C15 ^ (c.rate as u64).wrapping_mul(7919) ^ c.conc as u64);
    let total = c.rate * (c.secs + c.warm);
    let mut sent = 0u64;
    let mut t_off = 0.0f64;
    while (sent as f64) < total {
        let intended = start + Duration::from_secs_f64(t_off);
        let now = Instant::now();
        if intended > now {
            let d = intended - now;
            if d > Duration::from_micros(300) {
                std::thread::sleep(d - Duration::from_micros(200));
            }
            while Instant::now() < intended {
                std::hint::spin_loop();
            }
        }
        let read = rng.f() < c.read_frac;
        let obj = if c.hot_n > 0 && rng.f() < c.hot_f { (rng.next() % c.hot_n as u64) as usize } else { (rng.next() % c.objects as u64) as usize };
        {
            let (m, cv) = &*queue;
            let mut g = m.lock().unwrap();
            g.push_back(Job { intended, obj, read, roff: rng.next() });
            let l = g.len() as u64;
            if l > qmax.load(Ordering::Relaxed) {
                qmax.store(l, Ordering::Relaxed);
            }
            cv.notify_one();
        }
        sent += 1;
        t_off += 1.0 / c.rate;
    }
    let deadline = Instant::now() + Duration::from_secs(120);
    while !queue.0.lock().unwrap().is_empty() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    closed.store(true, Ordering::Release);
    queue.1.notify_all();
    let mut samples: Vec<Sample> = vec![];
    for h in hs {
        samples.extend(h.join().unwrap());
    }
    let n = samples.len();
    let sorted = |f: &dyn Fn(&Sample) -> Option<u32>| {
        let mut v: Vec<u32> = samples.iter().filter_map(f).collect();
        v.sort_unstable();
        v
    };
    let lat = sorted(&|s| Some(s.lat_us));
    let svc = sorted(&|s| Some(s.svc_us));
    let rl = sorted(&|s| s.read.then_some(s.lat_us));
    let wl = sorted(&|s| (!s.read).then_some(s.lat_us));
    let mut worst: std::collections::BTreeMap<u32, u32> = Default::default();
    for s in &samples {
        let e = worst.entry(s.sec).or_insert(0);
        *e = (*e).max(s.lat_us);
    }
    let slow = worst.values().filter(|&&v| v > 1_000_000).count();
    let errj = errs.lock().unwrap().iter().map(|(k, v)| format!("\"{}\":{}", k.replace('"', "'"), v)).collect::<Vec<_>>().join(",");
    let errsum: u64 = errs.lock().unwrap().values().sum();
    let ms = |x: u32| x as f64 / 1000.0;
    println!(
        "{{\"type\":\"run\",\"label\":\"{}\",\"rate\":{},\"secs\":{},\"conc\":{},\"value\":{},\"objects\":{},\"read_frac\":{},\"hot\":\"{}:{}\",\"rsize\":\"{}\",\"completed\":{},\"goodput_ops\":{:.1},\"goodput_MiB\":{:.2},\"p50_ms\":{:.2},\"p90_ms\":{:.2},\"p99_ms\":{:.2},\"p999_ms\":{:.2},\"max_ms\":{:.1},\"w_p50_ms\":{:.2},\"w_p99_ms\":{:.2},\"r_p50_ms\":{:.2},\"r_p99_ms\":{:.2},\"svc_p50_ms\":{:.2},\"svc_p99_ms\":{:.2},\"queue_max\":{},\"slow_secs_gt1s\":{},\"errors\":{{{}}},\"err_total\":{},\"mode\":\"direct\",\"load\":\"{}\"}}",
        c.label, c.rate, c.secs, c.conc, c.value, c.objects, c.read_frac, c.hot_n, c.hot_f,
        c.rsize.map(|x| x.to_string()).unwrap_or("whole".into()), n, n as f64 / c.secs,
        samples.iter().map(|s| if s.read { c.rsize.unwrap_or(c.value) } else { c.value } as f64).sum::<f64>() / 1048576.0 / c.secs,
        ms(pct(&lat, 50.0)), ms(pct(&lat, 90.0)), ms(pct(&lat, 99.0)), ms(pct(&lat, 99.9)), ms(lat.last().copied().unwrap_or(0)),
        ms(pct(&wl, 50.0)), ms(pct(&wl, 99.0)), ms(pct(&rl, 50.0)), ms(pct(&rl, 99.0)), ms(pct(&svc, 50.0)), ms(pct(&svc, 99.0)),
        qmax.load(Ordering::Relaxed), slow, errj, errsum, load_note()
    );
}

fn main() {
    let (mode, c) = parse();
    match mode.as_str() {
        "populate" => populate(c),
        "run" => run(c),
        _ => {
            eprintln!("usage: qps_direct populate|run ...");
            std::process::exit(2);
        }
    }
}
