// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
// Open-loop HTTP load generator for the S3 gateway (std only).
//
// Latency is measured from the INTENDED send time, so a stall that delays later requests is
// charged to them (no coordinated omission). A scheduler thread emits jobs at a fixed offered
// rate; N worker threads (the maximum in flight) execute them over keep-alive connections.
//
//   qps_http populate --addr A --bucket B --objects N --per-object M --value V --conc C [--prefix p]
//   qps_http run      --addr A --bucket B --objects N --rate R --secs S --conc C --value V
//                    [--read-frac F] [--dist uniform|hot:N:F] [--rsize BYTES|whole] [--poisson]
//                    [--warm SECS] [--prefix p] [--label L]
//
// Object model: key = prefix + 8-digit index. `populate` creates objects with M appends of V
// bytes each (offset header), `run` appends V bytes (write) or reads a range (read).
use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

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
    addr: String,
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
    rsize: Option<usize>, // None => whole
    poisson: bool,
    label: String,
    first: usize,
    status_every: f64,
}

fn parse() -> (String, Cfg) {
    let a: Vec<String> = std::env::args().collect();
    let mode = a.get(1).cloned().unwrap_or_default();
    let mut c = Cfg {
        addr: "127.0.0.1:19410".into(),
        bucket: "temporalstore".into(),
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
        poisson: false,
        label: String::new(),
        first: 0,
        status_every: 0.0,
    };
    let mut i = 2;
    while i < a.len() {
        let k = a[i].as_str();
        if k == "--poisson" {
            c.poisson = true;
            i += 1;
            continue;
        }
        let v = a.get(i + 1).cloned().unwrap_or_default();
        match k {
            "--addr" => c.addr = v,
            "--bucket" => c.bucket = v,
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
            "--status-every" => c.status_every = v.parse().unwrap(),
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

struct Conn {
    addr: String,
    r: Option<BufReader<TcpStream>>,
}
impl Conn {
    fn new(addr: &str) -> Self {
        Conn { addr: addr.into(), r: None }
    }
    fn ensure(&mut self) -> std::io::Result<()> {
        if self.r.is_none() {
            let s = TcpStream::connect(&self.addr)?;
            s.set_nodelay(true)?;
            s.set_read_timeout(Some(Duration::from_secs(120)))?;
            s.set_write_timeout(Some(Duration::from_secs(120)))?;
            self.r = Some(BufReader::with_capacity(64 * 1024, s));
        }
        Ok(())
    }
    // returns (status, object-size header, body bytes read)
    fn req(&mut self, method: &str, path: &str, extra: &str, body: &[u8]) -> std::io::Result<(u16, i64, usize)> {
        let r = self.try_req(method, path, extra, body);
        if r.is_err() {
            self.r = None;
        }
        r
    }
    fn try_req(&mut self, method: &str, path: &str, extra: &str, body: &[u8]) -> std::io::Result<(u16, i64, usize)> {
        self.ensure()?;
        let rd = self.r.as_mut().unwrap();
        let head = format!(
            "{method} {path} HTTP/1.1\r\nHost: {}\r\nContent-Length: {}\r\n{extra}\r\n",
            self.addr,
            body.len()
        );
        {
            let s = rd.get_mut();
            // one write for small bodies to avoid two packets
            if body.len() <= 16 * 1024 {
                let mut buf = head.into_bytes();
                buf.extend_from_slice(body);
                s.write_all(&buf)?;
            } else {
                s.write_all(head.as_bytes())?;
                s.write_all(body)?;
            }
        }
        let mut line = String::new();
        rd.read_line(&mut line)?;
        let status: u16 = line.split_whitespace().nth(1).and_then(|x| x.parse().ok()).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, format!("bad status line {line:?}"))
        })?;
        let mut clen = 0usize;
        let mut osize = -1i64;
        let mut chunked = false;
        loop {
            line.clear();
            rd.read_line(&mut line)?;
            let l = line.trim_end();
            if l.is_empty() {
                break;
            }
            let lower = l.to_ascii_lowercase();
            if let Some(v) = lower.strip_prefix("content-length:") {
                clen = v.trim().parse().unwrap_or(0);
            } else if let Some(v) = lower.strip_prefix("x-amz-object-size:") {
                osize = v.trim().parse().unwrap_or(-1);
            } else if lower.starts_with("transfer-encoding:") && lower.contains("chunked") {
                chunked = true;
            }
        }
        if chunked {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "chunked response unsupported"));
        }
        let mut left = clen;
        let mut sink = [0u8; 65536];
        let is_head = method == "HEAD";
        if !is_head {
            while left > 0 {
                let n = rd.read(&mut sink[..left.min(65536)])?;
                if n == 0 {
                    return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "short body"));
                }
                left -= n;
            }
        }
        Ok((status, osize, if is_head { 0 } else { clen }))
    }
}

struct Obj {
    len: u64,
}

fn key(c: &Cfg, i: usize) -> String {
    format!("/{}/{}{:08}", c.bucket, c.prefix, i)
}

fn load_note() -> String {
    let l = std::fs::read_to_string("/proc/loadavg").unwrap_or_default();
    l.split_whitespace().take(3).collect::<Vec<_>>().join(" ")
}

fn populate(c: Cfg) {
    let objs = Arc::new((0..c.objects).map(|_| Mutex::new(Obj { len: 0 })).collect::<Vec<_>>());
    let next = Arc::new(AtomicU64::new(0));
    let errs = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicU64::new(0));
    let retries = Arc::new(AtomicU64::new(0));
    let t0 = Instant::now();
    let data = Arc::new(vec![b'x'; c.value]);
    let mut hs = vec![];
    for _ in 0..c.conc {
        let (c, next, errs, done, objs, data, retries) = (c.clone(), next.clone(), errs.clone(), done.clone(), objs.clone(), data.clone(), retries.clone());
        hs.push(std::thread::spawn(move || {
            let mut cn = Conn::new(&c.addr);
            loop {
                let i = next.fetch_add(1, Ordering::Relaxed) as usize;
                if i >= c.objects {
                    break;
                }
                let k = key(&c, c.first + i);
                for _ in 0..c.per_object {
                    let off = objs[i].lock().unwrap().len;
                    let mut ok = false;
                    for _try in 0..60 {
                        match cn.req("PUT", &k, &format!("x-amz-write-offset-bytes: {off}\r\n"), &data) {
                            Ok((200, _, _)) => {
                                ok = true;
                                break;
                            }
                            Ok((s, _, _)) => {
                                retries.fetch_add(1, Ordering::Relaxed);
                                if retries.load(Ordering::Relaxed) <= 5 {
                                    eprintln!("populate {k} off {off}: status {s} (retrying)");
                                }
                                std::thread::sleep(Duration::from_millis(200));
                                if let Ok((200, sz, _)) = cn.req("HEAD", &k, "", b"") {
                                    if sz as u64 == off + c.value as u64 {
                                        ok = true;
                                        break;
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("populate {k}: {e}");
                                std::thread::sleep(Duration::from_millis(200));
                            }
                        }
                    }
                    if !ok {
                        errs.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                    objs[i].lock().unwrap().len += c.value as u64;
                    let d = done.fetch_add(1, Ordering::Relaxed) + 1;
                    if c.status_every > 0.0 && d % 5000 == 0 {
                        eprintln!("populate {d} appends {:.0}s", t0.elapsed().as_secs_f64());
                    }
                }
            }
        }));
    }
    for h in hs {
        h.join().unwrap();
    }
    let el = t0.elapsed().as_secs_f64();
    let n = done.load(Ordering::Relaxed);
    println!(
        "{{\"type\":\"populate\",\"label\":\"{}\",\"objects\":{},\"per_object\":{},\"value\":{},\"appends\":{},\"errors\":{},\"retries\":{},\"secs\":{:.2},\"appends_per_s\":{:.1},\"MiB_per_s\":{:.2},\"load\":\"{}\"}}",
        c.label, c.objects, c.per_object, c.value, n, errs.load(Ordering::Relaxed), retries.load(Ordering::Relaxed), el, n as f64 / el,
        n as f64 * c.value as f64 / 1048576.0 / el, load_note()
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
        return 0;
    }
    v[(((v.len() as f64) * p / 100.0) as usize).min(v.len() - 1)]
}

fn run(c: Cfg) {
    // discover each object's length with HEAD (they were populated earlier); needed for appends/reads
    let t_setup = Instant::now();
    let lens: Arc<Vec<Mutex<Obj>>> = Arc::new((0..c.objects).map(|_| Mutex::new(Obj { len: 0 })).collect());
    {
        let next = Arc::new(AtomicU64::new(0));
        let mut hs = vec![];
        for _ in 0..c.conc.min(32).max(1) {
            let (c, next, lens) = (c.clone(), next.clone(), lens.clone());
            hs.push(std::thread::spawn(move || {
                let mut cn = Conn::new(&c.addr);
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed) as usize;
                    if i >= c.objects {
                        break;
                    }
                    if let Ok((200, sz, _)) = cn.req("HEAD", &key(&c, c.first + i), "", b"") {
                        lens[i].lock().unwrap().len = sz.max(0) as u64;
                    }
                }
            }));
        }
        for h in hs {
            h.join().unwrap();
        }
    }
    let setup_s = t_setup.elapsed().as_secs_f64();

    let queue: Arc<(Mutex<VecDeque<Job>>, Condvar)> = Arc::new((Mutex::new(VecDeque::new()), Condvar::new()));
    let closed = Arc::new(AtomicBool::new(false));
    let qmax = Arc::new(AtomicU64::new(0));
    let errs = Arc::new(Mutex::new(std::collections::BTreeMap::<String, u64>::new()));
    let data = Arc::new(vec![b'y'; c.value]);
    let start = Instant::now() + Duration::from_millis(300);
    let warm_end = c.warm;
    let mut hs = vec![];
    for _ in 0..c.conc {
        let (c2, queue, closed, lens, errs, data) = (c.clone(), queue.clone(), closed.clone(), lens.clone(), errs.clone(), data.clone());
        hs.push(std::thread::spawn(move || {
            let mut cn = Conn::new(&c2.addr);
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
                let mut status = 0u16;
                let mut note = String::new();
                if job.read {
                    let len = lens[job.obj].lock().unwrap().len;
                    if len == 0 {
                        continue;
                    }
                    let hdr = match c2.rsize {
                        None => String::new(),
                        Some(n) => {
                            let off = job.roff % len.saturating_sub(1).max(1);
                            let end = (off + n as u64 - 1).min(len - 1);
                            format!("Range: bytes={off}-{end}\r\n")
                        }
                    };
                    match cn.req("GET", &k, &hdr, b"") {
                        Ok((s, _, _)) => status = s,
                        Err(e) => note = format!("{e}"),
                    }
                    if status != 200 && status != 206 {
                        let key = if note.is_empty() { format!("read {status}") } else { format!("read io {note}") };
                        *errs.lock().unwrap().entry(key).or_insert(0) += 1;
                        status = 0;
                    }
                } else {
                    let mut g = lens[job.obj].lock().unwrap();
                    let off = g.len;
                    match cn.req("PUT", &k, &format!("x-amz-write-offset-bytes: {off}\r\n"), &data) {
                        Ok((200, _, _)) => {
                            g.len += c2.value as u64;
                            status = 200;
                        }
                        Ok((s, _, _)) => {
                            *errs.lock().unwrap().entry(format!("append {s}")).or_insert(0) += 1;
                            if let Ok((200, sz, _)) = cn.req("HEAD", &k, "", b"") {
                                g.len = sz.max(0) as u64;
                            }
                        }
                        Err(e) => {
                            *errs.lock().unwrap().entry(format!("append io {e}")).or_insert(0) += 1;
                            if let Ok((200, sz, _)) = cn.req("HEAD", &k, "", b"") {
                                g.len = sz.max(0) as u64;
                            }
                        }
                    }
                }
                if status != 0 {
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

    // scheduler
    let mut rng = Rng(0x9E3779B97F4A7C15 ^ (c.rate as u64).wrapping_mul(7919) ^ c.conc as u64);
    let mut t_off = 0.0f64;
    let total = c.rate * (c.secs + c.warm);
    let mut sent = 0u64;
    let mut last_status = 0.0;
    loop {
        if sent as f64 >= total {
            break;
        }
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
        let obj = if c.hot_n > 0 && rng.f() < c.hot_f {
            (rng.next() % c.hot_n as u64) as usize
        } else {
            (rng.next() % c.objects as u64) as usize
        };
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
        t_off += if c.poisson { -((1.0 - rng.f()).ln()) / c.rate } else { 1.0 / c.rate };
        if c.status_every > 0.0 && t_off - last_status >= c.status_every {
            last_status = t_off;
            let q = queue.0.lock().unwrap().len();
            eprintln!("t={t_off:.0}s sent={sent} queue={q}");
        }
    }
    let issue_end = Instant::now();
    // drain
    let drain_deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let q = queue.0.lock().unwrap().len();
        if q == 0 || Instant::now() > drain_deadline {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    closed.store(true, Ordering::Release);
    queue.1.notify_all();
    let mut samples: Vec<Sample> = vec![];
    for h in hs {
        samples.extend(h.join().unwrap());
    }
    let wall = (Instant::now() - start).as_secs_f64();
    let _ = issue_end;
    let n = samples.len();
    let mut lat: Vec<u32> = samples.iter().map(|s| s.lat_us).collect();
    lat.sort_unstable();
    let mut svc: Vec<u32> = samples.iter().map(|s| s.svc_us).collect();
    svc.sort_unstable();
    let mut rl: Vec<u32> = samples.iter().filter(|s| s.read).map(|s| s.lat_us).collect();
    rl.sort_unstable();
    let mut wl: Vec<u32> = samples.iter().filter(|s| !s.read).map(|s| s.lat_us).collect();
    wl.sort_unstable();
    // per-second max latency: count seconds above 1 s and the worst second
    let mut worst: std::collections::BTreeMap<u32, u32> = Default::default();
    for s in &samples {
        let e = worst.entry(s.sec).or_insert(0);
        if s.lat_us > *e {
            *e = s.lat_us;
        }
    }
    let slow_secs = worst.values().filter(|&&v| v > 1_000_000).count();
    // per intended-second timeline: second:completed:worst-latency-ms (a stall shows as seconds with huge latency, a backlog as a ramp)
    let mut counts: std::collections::BTreeMap<u32, u32> = Default::default();
    for s in &samples {
        *counts.entry(s.sec).or_insert(0) += 1;
    }
    let series = worst
        .iter()
        .map(|(sec, w)| format!("{}:{}:{}", sec, counts.get(sec).copied().unwrap_or(0), w / 1000))
        .collect::<Vec<_>>()
        .join(" ");
    let measured = c.secs;
    let errsum: u64 = errs.lock().unwrap().values().sum();
    let errj = errs
        .lock()
        .unwrap()
        .iter()
        .map(|(k, v)| format!("\"{}\":{}", k.replace('"', "'"), v))
        .collect::<Vec<_>>()
        .join(",");
    println!(
        "{{\"type\":\"run\",\"label\":\"{}\",\"rate\":{},\"secs\":{},\"conc\":{},\"value\":{},\"objects\":{},\"read_frac\":{},\"hot\":\"{}:{}\",\"rsize\":\"{}\",\"completed\":{},\"goodput_ops\":{:.1},\"goodput_MiB\":{:.2},\"p50_ms\":{:.2},\"p90_ms\":{:.2},\"p99_ms\":{:.2},\"p999_ms\":{:.2},\"max_ms\":{:.1},\"w_p50_ms\":{:.2},\"w_p99_ms\":{:.2},\"r_p50_ms\":{:.2},\"r_p99_ms\":{:.2},\"svc_p50_ms\":{:.2},\"svc_p99_ms\":{:.2},\"queue_max\":{},\"slow_secs_gt1s\":{},\"errors\":{{{}}},\"err_total\":{},\"wall_s\":{:.1},\"setup_s\":{:.1},\"load\":\"{}\",\"series\":\"{}\"}}",
        c.label, c.rate, c.secs, c.conc, c.value, c.objects, c.read_frac, c.hot_n, c.hot_f,
        c.rsize.map(|x| x.to_string()).unwrap_or("whole".into()),
        n, n as f64 / measured,
        samples.iter().map(|s| if s.read { c.rsize.unwrap_or(c.value) } else { c.value } as f64).sum::<f64>() / 1048576.0 / measured,
        pct(&lat, 50.0) as f64 / 1000.0, pct(&lat, 90.0) as f64 / 1000.0, pct(&lat, 99.0) as f64 / 1000.0,
        pct(&lat, 99.9) as f64 / 1000.0, lat.last().copied().unwrap_or(0) as f64 / 1000.0,
        pct(&wl, 50.0) as f64 / 1000.0, pct(&wl, 99.0) as f64 / 1000.0,
        pct(&rl, 50.0) as f64 / 1000.0, pct(&rl, 99.0) as f64 / 1000.0,
        pct(&svc, 50.0) as f64 / 1000.0, pct(&svc, 99.0) as f64 / 1000.0,
        qmax.load(Ordering::Relaxed), slow_secs, errj, errsum, wall, setup_s, load_note(), series
    );
}

fn main() {
    let (mode, c) = parse();
    match mode.as_str() {
        "populate" => populate(c),
        "run" => run(c),
        _ => {
            eprintln!("usage: qps_http populate|run ...");
            std::process::exit(2);
        }
    }
}
