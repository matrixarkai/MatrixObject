// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! End-to-end measurement of a log object grown to several GiB on a private cluster, next to the
//! same appender on a plain page-path object. Both tests are `#[ignore]`d: they need a quiet
//! machine and tens of GiB of disk, and they print machine-readable result lines
//! (`E2E_APPEND`, `E2E_META`, `E2E_RESTART`, `E2E_READ`, `E2E_VERIFY`, `E2E_TAIL`,
//! `E2E_BASELINE_END`) for `scripts/log_object_e2e/table.py`.
//!
//! The cluster is private to the test: three metadata voter processes and three chunk server
//! processes on kernel-chosen loopback ports, with their state under `TMPDIR`. The two cluster
//! features are raised on THIS cluster only, with the operator commands (`declare` for every voter,
//! then `raise`). Nothing here talks to any other cluster.
//!
//! Settings (environment): `E2E_BYTES` (object size to grow to), `E2E_APPEND_BYTES`,
//! `E2E_INTERVAL_BYTES` (one result window), `E2E_MODE` (`closed`, or `open` with
//! `E2E_RATE_PER_S`), `E2E_META_RESTART_AT` and `E2E_CHUNK_RESTART_AT` (fractions of the size at
//! which a voter and a chunk server are killed and restarted; 0 disables), `E2E_DOWN_APPENDS`,
//! `E2E_READ_BYTES`, `E2E_RESULTS` (the file the result lines are appended to).
#![allow(dead_code)]
use super::*;
use matrixobjectstore_rs::{
    cluster::{
        ChunkGroupCatalog, ChunkGroupConfig, ChunkGroupIdentity, LogObject, LogObjectOptions,
        LogObjectReader, MetaReply, ObjectWriteOptions, ReplicaClientOptions,
    },
    cluster_features::{FEATURE_CHUNK_MANIFEST, FEATURE_LOG_CHUNK_LEASES, SUPPORTED_FEATURES},
};
use prost::Message as _;
use std::path::Path;

const BUCKET: &str = "e2e";
const LOG_KEY: &str = "e2e-log";
const PAGE_KEY: &str = "e2e-pages";
/// One read slice when the whole object is read back and compared.
const READ_SLICE: u64 = 32 << 20;

struct Directory(PathBuf);
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|value| value.parse::<u64>().ok()).unwrap_or(default)
}
fn env_f64(name: &str, default: f64) -> f64 {
    std::env::var(name).ok().and_then(|value| value.parse::<f64>().ok()).unwrap_or(default)
}
fn env_string(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

struct Config {
    total_bytes: u64,
    append_bytes: usize,
    interval_bytes: u64,
    open_loop_rate: f64,
    meta_restart_at: f64,
    chunk_restart_at: f64,
    down_appends: u64,
    read_bytes: u64,
}
impl Config {
    fn from_env(append_variable: &str, default_append: u64) -> Self {
        Self {
            total_bytes: env_u64("E2E_BYTES", (4u64 << 30) + (64u64 << 20)),
            append_bytes: env_u64(append_variable, default_append).max(1) as usize,
            interval_bytes: env_u64("E2E_INTERVAL_BYTES", 256u64 << 20).max(1),
            open_loop_rate: if env_string("E2E_MODE", "closed") == "open" {
                env_f64("E2E_RATE_PER_S", 200.0)
            } else {
                0.0
            },
            meta_restart_at: env_f64("E2E_META_RESTART_AT", 0.4),
            chunk_restart_at: env_f64("E2E_CHUNK_RESTART_AT", 0.7),
            down_appends: env_u64("E2E_DOWN_APPENDS", 200),
            read_bytes: env_u64("E2E_READ_BYTES", 16 << 20),
        }
    }
}

/// Result lines go to standard output and, appended, to a file that outlives the run directory.
struct Out {
    file: std::fs::File,
    started: Instant,
}
impl Out {
    fn new(label: &str) -> Self {
        let path = std::env::var("E2E_RESULTS")
            .map(PathBuf::from)
            .unwrap_or_else(|_| std::env::temp_dir().join(format!("e2e_{label}.txt")));
        let file = std::fs::OpenOptions::new().create(true).append(true).open(&path).unwrap();
        Self { file, started: Instant::now() }
    }
    fn line(&mut self, text: String) {
        let text = format!("{text} t_s={:.1}", self.started.elapsed().as_secs_f64());
        println!("{text}");
        writeln!(self.file, "{text}").unwrap();
    }
}

fn percentile(sorted: &[u128], p: usize) -> u128 {
    sorted[(sorted.len() * p / 100).min(sorted.len() - 1)]
}

/// The byte the object holds at `offset`: a function of the position only, so what any prefix of
/// the object must contain is known without keeping the bytes the appender wrote.
fn stream_byte(offset: u64) -> u8 {
    let mixed = offset.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    ((mixed >> 56) as u8) ^ (offset as u8)
}
fn stream_piece(offset: u64, length: usize) -> Vec<u8> {
    (0..length as u64).map(|index| stream_byte(offset + index)).collect()
}

fn loadavg() -> f64 {
    std::fs::read_to_string("/proc/loadavg")
        .ok()
        .and_then(|text| text.split_whitespace().next().and_then(|word| word.parse::<f64>().ok()))
        .unwrap_or(-1.0)
}
fn kb_field(text: &str, key: &str) -> u64 {
    text.lines()
        .find(|line| line.starts_with(key))
        .and_then(|line| line.split_whitespace().nth(1).and_then(|word| word.parse::<u64>().ok()))
        .unwrap_or(0)
}
fn swap_used_kb() -> u64 {
    let text = std::fs::read_to_string("/proc/meminfo").unwrap_or_default();
    kb_field(&text, "SwapTotal:").saturating_sub(kb_field(&text, "SwapFree:"))
}
fn rss_kb(pid: u32) -> u64 {
    let text = std::fs::read_to_string(format!("/proc/{pid}/status")).unwrap_or_default();
    kb_field(&text, "VmRSS:")
}
fn pids_of(nodes: &[Option<NodeProcess>]) -> Vec<u32> {
    nodes.iter().flatten().map(|node| node.pid).collect()
}
fn meta_wal_bytes(group: &Group) -> u64 {
    (0..3)
        .map(|id| {
            std::fs::metadata(group.root.join(id.to_string()).join("metadata.peer.wal"))
                .map(|metadata| metadata.len())
                .unwrap_or(0)
        })
        .sum::<u64>()
}
fn commit_index(client: &mut RemoteMetaClient) -> Option<u64> {
    match client.probe().into_result() {
        Ok(MetaReply::Pong { commit_index, .. }) => Some(commit_index),
        _ => None,
    }
}
fn meta_client(group: &Group, index: usize) -> RemoteMetaClient {
    RemoteMetaClient::connect(
        &group.nodes[index].as_ref().unwrap().endpoint,
        &MetaClientOptions {
            connect_timeout: Duration::from_secs(3),
            read_timeout: Duration::from_secs(30),
            write_timeout: Duration::from_secs(30),
            ..Default::default()
        },
    )
    .unwrap()
}

/// Three chunk server processes, each with its own directory: a node keeps its log chunk files
/// beside its journal, so two nodes must never share one.
struct Chunks {
    nodes: Vec<Option<NodeProcess>>,
    addresses: Vec<String>,
    directories: Vec<PathBuf>,
}
impl Chunks {
    fn start_all(root: &Path) -> Self {
        let mut chunks = Self { nodes: Vec::new(), addresses: Vec::new(), directories: Vec::new() };
        for index in 0..3 {
            let directory = root.join(format!("n{index}"));
            std::fs::create_dir_all(&directory).unwrap();
            chunks.directories.push(directory);
            chunks.addresses.push("127.0.0.1:0".to_string());
            chunks.nodes.push(None);
            chunks.start(index);
        }
        chunks
    }
    fn start(&mut self, index: usize) {
        let journal = self.directories[index].join("journal");
        let node = processes::start(
            env!("CARGO_BIN_EXE_chunkserver"),
            &[
                ("MATRIXOBJECT_REPLICA_ADDR", self.addresses[index].as_str()),
                ("MATRIXOBJECT_REPLICA_JOURNAL", journal.to_str().unwrap()),
                ("MATRIXOBJECT_REPLICA_CAPACITY_BYTES", "17179869184"),
                ("MATRIXOBJECT_REPLICA_MAX_PAGES", "65536"),
            ],
        );
        self.addresses[index] = node.endpoint.clone();
        self.nodes[index] = Some(node);
    }
    fn kill(&mut self, index: usize) {
        let mut node = self.nodes[index].take().unwrap();
        node.child.kill().unwrap();
        let _ = processes::reap_within(&mut node.child, Duration::from_secs(5));
    }
}

fn catalog(endpoints: &[String], page_path: bool, members: &[String]) -> ChunkGroupCatalog {
    let identity = if page_path {
        ChunkGroupIdentity {
            version: 3,
            group_id: 1,
            node_id: 1,
            capacity_bytes: 1 << 36,
            max_pages: 65536,
            voters: vec![1, 2, 3],
            peer_endpoints: vec![],
            bootstrap_voters: vec![],
            bootstrap_id: 0,
        }
    } else {
        ChunkGroupIdentity {
            bootstrap_voters: Vec::new(),
            bootstrap_id: 0,
            version: 1,
            group_id: 1,
            node_id: 1,
            capacity_bytes: 1 << 40,
            max_pages: 1 << 20,
            voters: vec![1, 2, 3],
            peer_endpoints: vec!["127.0.0.1:9101".into(), "127.0.0.1:9102".into(), "127.0.0.1:9103".into()],
        }
    };
    ChunkGroupCatalog::new(vec![ChunkGroupConfig {
        placement_id: if page_path { "log-group".into() } else { "group-a".into() },
        failure_domain: if page_path { "local-domain".into() } else { "rack-a".into() },
        identity: Some(identity),
        api_endpoints: endpoints.to_vec(),
        member_ids: members.to_vec(),
    }])
    .unwrap()
}

fn log_options(holder: &str) -> LogObjectOptions {
    let mut options = LogObjectOptions::new(holder, "group-a");
    options.replica = ReplicaClientOptions {
        read_timeout: Duration::from_secs(30),
        connect_timeout: Duration::from_secs(3),
        ..Default::default()
    };
    options
}

/// What one result window collects.
struct Window {
    index: u64,
    appends: u64,
    grants: u64,
    switches: u64,
    renewals: u64,
    latencies: Vec<u128>,
    commit_start: Option<u64>,
    load_max: f64,
    swap_max_kb: u64,
}
impl Window {
    fn new(index: u64, commit_start: Option<u64>) -> Self {
        Self {
            index,
            appends: 0,
            grants: 0,
            switches: 0,
            renewals: 0,
            latencies: Vec::new(),
            commit_start,
            load_max: loadavg(),
            swap_max_kb: swap_used_kb(),
        }
    }
}

/// Print the two result lines of one window: latency, and what the metadata plane was charged.
fn emit_window(
    out: &mut Out,
    kind: &str,
    window: &Window,
    object_bytes: u64,
    append_bytes: usize,
    commit_end: Option<u64>,
    manifest: (i64, i64),
    group: &Group,
    chunk_pids: &[u32],
) {
    let mut sorted = window.latencies.clone();
    sorted.sort_unstable();
    let (p50, p99, max) = if sorted.is_empty() {
        (0u128, 0u128, 0u128)
    } else {
        (percentile(&sorted, 50), percentile(&sorted, 99), sorted[sorted.len() - 1])
    };
    let load_max = window.load_max.max(loadavg());
    let swap_max_kb = window.swap_max_kb.max(swap_used_kb());
    let meta_rss: Vec<u64> = pids_of(&group.nodes).into_iter().map(rss_kb).collect();
    let chunk_rss: Vec<u64> = chunk_pids.iter().map(|pid| rss_kb(*pid)).collect();
    let entries: i64 = match (window.commit_start, commit_end) {
        (Some(start), Some(end)) => end as i64 - start as i64,
        _ => -1,
    };
    let explained = (window.grants + window.switches + window.renewals) as i64;
    let unexplained: i64 = if entries < 0 { -1 } else { entries - explained };
    let wal = meta_wal_bytes(group);
    out.line(format!(
        "E2E_APPEND kind={kind} interval={} object_bytes={object_bytes} append_bytes={append_bytes} appends={} p50_us={p50} p99_us={p99} max_us={max} load1_max={load_max:.2} swap_used_kb_max={swap_max_kb}",
        window.index, window.appends
    ));
    out.line(format!(
        "E2E_META kind={kind} interval={} object_bytes={object_bytes} appends={} commit_entries={entries} grants={} switches={} renewals={} unexplained_entries={unexplained} manifest_bytes={} extents={} meta_wal_bytes={wal} meta_rss_kb={meta_rss:?} chunk_rss_kb={chunk_rss:?}",
        window.index, window.appends, window.grants, window.switches, window.renewals, manifest.0, manifest.1
    ));
}

fn manifest_facts(meta: &mut RemoteMetaClient, bucket: &str, key: &str) -> (i64, i64) {
    match meta.read_manifest(bucket, key) {
        Ok(manifest) => (manifest.encoded_len() as i64, manifest.extents.len() as i64),
        Err(_) => (-1, -1),
    }
}

/// A chunk server that is down while the appender keeps going on the other two.
struct ChunkDown {
    remaining: u64,
    killed: Instant,
    at_bytes: u64,
    max_us: u128,
    appends: u64,
}

#[test]
#[ignore = "explicit end-to-end measurement: needs a quiet machine and tens of GiB of disk; run scripts/log_object_e2e/run.sh"]
fn measure_log_object_growth_end_to_end() {
    let config = Config::from_env("E2E_APPEND_BYTES", 64 << 10);
    let mut out = Out::new("log_object");
    let base = std::env::temp_dir();
    let run = Directory(base.join(node_regime::run_token("e2e-log-object")));
    std::fs::create_dir(&run.0).unwrap();
    let mut chunks = Chunks::start_all(&run.0);
    let members: Vec<String> = (0..3).map(|id| format!("member-{id}")).collect();
    let catalog = catalog(&chunks.addresses, false, &members);
    let mut group = Group::configured_with_timeout(None, None, Some(catalog.to_bytes().unwrap()), 0, false, 5000);
    let mut meta = meta_client(&group, group.leader());

    // the operator procedure, on this private cluster only: every voter declares what it reads,
    // then the two features are raised for all of them
    for voter in 1..=3u64 {
        meta.declare_voter_features(voter, SUPPORTED_FEATURES).unwrap();
    }
    let both = FEATURE_CHUNK_MANIFEST | FEATURE_LOG_CHUNK_LEASES;
    assert!(meta.raise_cluster_features(both, vec![1, 2, 3]).unwrap().has(both));

    let options = log_options("gw-a");
    let chunk_bytes = options.chunk_bytes;
    let chunk_of = |total: u64| if total == 0 { 0 } else { (total - 1) / chunk_bytes };
    let mut object = LogObject::new(BUCKET, LOG_KEY, options);
    let mut acked = object.open_for_append(&mut meta).unwrap();
    assert_eq!(acked, 0, "a new log object starts empty");
    out.line(format!(
        "E2E_CONFIG kind=log total_bytes={} append_bytes={} interval_bytes={} chunk_bytes={chunk_bytes} mode={} meta_restart_at={} chunk_restart_at={}",
        config.total_bytes,
        config.append_bytes,
        config.interval_bytes,
        if config.open_loop_rate > 0.0 { "open" } else { "closed" },
        config.meta_restart_at,
        config.chunk_restart_at
    ));

    let run_start = Instant::now();
    let mut window = Window::new(0, commit_index(&mut meta));
    window.grants = 1;
    let mut next_interval = config.interval_bytes;
    let mut meta_killed: Option<(usize, Instant, u64)> = None;
    let mut meta_restarted = config.meta_restart_at <= 0.0;
    let mut chunk_down: Option<ChunkDown> = None;
    let mut chunk_restarted = config.chunk_restart_at <= 0.0;
    let mut lost_acknowledged = false;
    let mut sent = 0u64;

    while acked < config.total_bytes {
        let fraction = acked as f64 / config.total_bytes as f64;
        if !meta_restarted && meta_killed.is_none() && fraction >= config.meta_restart_at {
            let leader = group.leader();
            group.kill(leader);
            out.line(format!("E2E_RESTART kind=metaserver phase=killed voter={leader} at_bytes={acked}"));
            meta_killed = Some((leader, Instant::now(), 0));
        }
        if let Some((voter, killed, appends)) = meta_killed {
            // the killed voter comes back after a few appends, still mid-growth
            if appends >= 32 {
                let started = Instant::now();
                group.start(voter);
                out.line(format!(
                    "E2E_RESTART kind=metaserver phase=restarted voter={voter} down_ms={} start_ms={}",
                    killed.elapsed().as_millis(),
                    started.elapsed().as_millis()
                ));
                meta_killed = None;
                meta_restarted = true;
            } else {
                meta_killed = Some((voter, killed, appends + 1));
            }
        }
        if !chunk_restarted && chunk_down.is_none() && fraction >= config.chunk_restart_at {
            chunks.kill(0);
            out.line(format!("E2E_RESTART kind=chunkserver phase=killed node=0 at_bytes={acked}"));
            chunk_down = Some(ChunkDown {
                remaining: config.down_appends,
                killed: Instant::now(),
                at_bytes: acked,
                max_us: 0,
                appends: 0,
            });
        }

        let length = config.append_bytes;
        let piece = stream_piece(acked, length);
        let renewal = u64::from(object.renew_due());
        let switches = chunk_of(acked + length as u64) - chunk_of(acked);
        let started = if config.open_loop_rate > 0.0 {
            let intended = run_start + Duration::from_secs_f64(sent as f64 / config.open_loop_rate);
            let now = Instant::now();
            if intended > now {
                std::thread::sleep(intended - now);
            }
            intended
        } else {
            Instant::now()
        };
        sent += 1;
        let result = object.append(&mut meta, &piece);
        let elapsed = started.elapsed().as_micros();
        match result {
            Ok(total) => {
                assert_eq!(total, acked + length as u64, "the append is acknowledged at the length it reached");
                acked = total;
                window.appends += 1;
                window.switches += switches;
                window.renewals += renewal;
                window.latencies.push(elapsed);
                if let Some(down) = chunk_down.as_mut() {
                    down.appends += 1;
                    down.max_us = down.max_us.max(elapsed);
                }
            }
            Err(error) => {
                // The failed append may or may not have landed. The object is taken over again: the
                // length the nodes hold decides where the stream goes on, and it can never be shorter
                // than what was acknowledged.
                let error_text = format!("{error:?}");
                out.line(format!("E2E_RESTART kind=append_error at_bytes={acked} error={error_text:?}"));
                let recovery = Instant::now();
                let mut attempts = 0u32;
                loop {
                    attempts += 1;
                    assert!(attempts < 240, "the object never recovered after: {error_text}");
                    std::thread::sleep(Duration::from_millis(500));
                    meta = meta_client(&group, group.leader());
                    if let Ok(reopened) = object.open_for_append(&mut meta) {
                        if reopened < acked {
                            lost_acknowledged = true;
                            out.line(format!("E2E_LOSS acknowledged={acked} reopened_at={reopened}"));
                        }
                        out.line(format!(
                            "E2E_RESTART kind=recovered at_bytes={acked} reopened_at={reopened} extra_bytes={} recovery_ms={}",
                            reopened.saturating_sub(acked),
                            recovery.elapsed().as_millis()
                        ));
                        acked = reopened;
                        window.grants += 1;
                        break;
                    }
                }
            }
        }
        if let Some(down) = chunk_down.as_ref() {
            if down.remaining == 0 {
                let started = Instant::now();
                chunks.start(0);
                out.line(format!(
                    "E2E_RESTART kind=chunkserver phase=restarted node=0 down_ms={} appends_while_down={} max_append_us_while_down={} at_bytes={} restart_ms={}",
                    down.killed.elapsed().as_millis(),
                    down.appends,
                    down.max_us,
                    down.at_bytes,
                    started.elapsed().as_millis()
                ));
                chunk_down = None;
                chunk_restarted = true;
            }
        }
        if let Some(down) = chunk_down.as_mut() {
            down.remaining = down.remaining.saturating_sub(1);
        }

        if acked >= next_interval {
            let chunk_pids = pids_of(&chunks.nodes);
            let commit_end = commit_index(&mut meta);
            let facts = manifest_facts(&mut meta, BUCKET, LOG_KEY);
            emit_window(&mut out, "log", &window, acked, config.append_bytes, commit_end, facts, &group, &chunk_pids);
            window = Window::new(window.index + 1, commit_end);
            next_interval += config.interval_bytes;
        }
    }
    let chunk_pids = pids_of(&chunks.nodes);
    let commit_end = commit_index(&mut meta);
    let facts = manifest_facts(&mut meta, BUCKET, LOG_KEY);
    emit_window(&mut out, "log", &window, acked, config.append_bytes, commit_end, facts, &group, &chunk_pids);
    out.line(format!("E2E_GROWTH_DONE kind=log acknowledged_bytes={acked} run_s={:.1}", run_start.elapsed().as_secs_f64()));

    // a sealed read through the chunk servers' cache, cold and then warm; the chunk servers'
    // resident memory is read before and after it and after the whole object was read back
    let mut reader = LogObjectReader::new(BUCKET, LOG_KEY, log_options("reader").replica);
    let rss_before: Vec<u64> = chunk_pids.iter().map(|pid| rss_kb(*pid)).collect();
    let sealed_chunks = chunk_of(acked);
    let offset = if sealed_chunks >= 4 { chunk_bytes * 2 } else { 0 };
    let want = config.read_bytes.min(chunk_bytes - 1).max(1) as usize;
    let cold = Instant::now();
    let first = reader.read_range(&mut meta, offset, want).unwrap();
    let cold_us = cold.elapsed().as_micros();
    let warm = Instant::now();
    let second = reader.read_range(&mut meta, offset, want).unwrap();
    let warm_us = warm.elapsed().as_micros();
    let sample_ok = first.len() == want
        && second == first
        && first.iter().enumerate().all(|(index, byte)| *byte == stream_byte(offset + index as u64));
    let rss_sample: Vec<u64> = chunk_pids.iter().map(|pid| rss_kb(*pid)).collect();
    out.line(format!(
        "E2E_READ kind=sealed_range offset={offset} bytes={want} cold_us={cold_us} warm_us={warm_us} correct={sample_ok} chunk_rss_kb_before={rss_before:?} chunk_rss_kb_after={rss_sample:?}"
    ));

    // every byte, compared against the position function; nothing acknowledged may be missing
    let durable = reader.length(&mut meta).unwrap();
    let verify_start = Instant::now();
    let mut position = 0u64;
    let mut mismatches = 0u64;
    while position < durable {
        let slice = (durable - position).min(READ_SLICE) as usize;
        let data = reader.read_range(&mut meta, position, slice).unwrap();
        assert_eq!(data.len(), slice, "a read inside the object returns every byte");
        for (index, byte) in data.iter().enumerate() {
            if *byte != stream_byte(position + index as u64) {
                mismatches += 1;
            }
        }
        position += slice as u64;
    }
    let rss_full: Vec<u64> = chunk_pids.iter().map(|pid| rss_kb(*pid)).collect();
    out.line(format!(
        "E2E_VERIFY kind=log length={durable} acknowledged={acked} mismatches={mismatches} lost_acknowledged={lost_acknowledged} read_ms={} chunk_rss_kb_after_full_read={rss_full:?}",
        verify_start.elapsed().as_millis()
    ));

    // the tail-length rule: the first writer is abandoned, a second one takes the object over after
    // the lease has run out, and the length it starts at is what the nodes durably hold
    let mut second_writer = LogObject::new(BUCKET, LOG_KEY, log_options("gw-b"));
    let takeover = Instant::now();
    let mut attempts = 0u32;
    let reopened = loop {
        match second_writer.open_for_append(&mut meta) {
            Ok(length) => break length,
            Err(error) => {
                attempts += 1;
                assert!(attempts < 240, "the object was never released: {error:?}");
                std::thread::sleep(Duration::from_secs(1));
            }
        }
    };
    let probe = stream_piece(reopened, 4096);
    let probe_total = second_writer.append(&mut meta, &probe).unwrap();
    let fenced = object.append(&mut meta, b"late").is_err();
    out.line(format!(
        "E2E_TAIL second_open_len={reopened} durable_len_before={durable} acknowledged={acked} equal={} takeover_wait_ms={} probe_total={probe_total} first_writer_fenced={fenced}",
        reopened == durable && reopened == acked,
        takeover.elapsed().as_millis()
    ));

    assert!(!lost_acknowledged, "an acknowledged append was lost");
    assert_eq!(mismatches, 0, "the object holds bytes that were never appended");
    assert!(durable >= acked, "the nodes hold fewer bytes than were acknowledged");
    assert!(fenced, "the first writer was not fenced");
}

#[test]
#[ignore = "explicit end-to-end baseline: the same appender on a plain page-path object; needs a quiet machine; run scripts/log_object_e2e/run.sh"]
fn measure_page_path_growth_baseline() {
    let config = Config::from_env("E2E_BASELINE_APPEND_BYTES", 1 << 20);
    let mut out = Out::new("page_path");
    let base = std::env::temp_dir();
    let run = Directory(base.join(node_regime::run_token("e2e-page-path")));
    std::fs::create_dir(&run.0).unwrap();
    let mut chunks = Chunks::start_all(&run.0);
    let members: Vec<String> = (0..3).map(|id| format!("log-member-{id}")).collect();
    let catalog = catalog(&chunks.addresses, true, &members);
    let group = Group::configured_with_timeout(None, None, Some(catalog.to_bytes().unwrap()), 0, false, 5000);
    let group_id = meta_client(&group, group.leader()).metadata_group_identity().unwrap().group_id;
    let mut client = RemoteMetaClient::connect_raft_catalog(
        &group.api_addresses.iter().map(|address| address.to_string()).collect::<Vec<_>>(),
        group_id,
        &MetaClientOptions {
            connect_timeout: Duration::from_secs(3),
            read_timeout: Duration::from_secs(60),
            write_timeout: Duration::from_secs(60),
            ..Default::default()
        },
    )
    .unwrap();
    for member in &members {
        client.heartbeat_placement_node(member, 0, 1 << 36, false, true, 100).unwrap();
    }
    let mut probe = meta_client(&group, group.leader());
    // the object limit is raised to the target size so that the extent limit, not the default
    // byte limit, is what ends the page-path object
    let options = ObjectWriteOptions {
        max_object_bytes: usize::try_from(config.total_bytes + (64u64 << 20)).unwrap(),
        ..Default::default()
    };
    out.line(format!(
        "E2E_CONFIG kind=page total_bytes={} append_bytes={} interval_bytes={} page_bytes={}",
        config.total_bytes, config.append_bytes, config.interval_bytes, options.page_bytes
    ));

    let run_start = Instant::now();
    let mut window = Window::new(0, commit_index(&mut probe));
    let mut next_interval = config.interval_bytes;
    let mut manifest: Option<matrixobjectstore_rs::object_manifest::ObjectManifest> = None;
    let mut object_bytes = 0u64;
    let mut failure = String::new();
    while object_bytes < config.total_bytes {
        let data = stream_piece(object_bytes, config.append_bytes);
        let started = Instant::now();
        let result = match manifest.clone() {
            None => client.write_object(BUCKET, PAGE_KEY, None, &data, &options),
            Some(previous) => client.append_object(previous, &data, &options),
        };
        let elapsed = started.elapsed().as_micros();
        match result {
            Ok(next) => {
                object_bytes = next.length;
                manifest = Some(next);
                window.appends += 1;
                window.latencies.push(elapsed);
            }
            Err(error) => {
                failure = error.to_string();
                break;
            }
        }
        if object_bytes >= next_interval {
            let current = manifest.as_ref().unwrap();
            let facts = (current.encoded_len() as i64, current.extents.len() as i64);
            let commit_end = commit_index(&mut probe);
            let chunk_pids = pids_of(&chunks.nodes);
            emit_window(&mut out, "page", &window, object_bytes, config.append_bytes, commit_end, facts, &group, &chunk_pids);
            window = Window::new(window.index + 1, commit_end);
            next_interval += config.interval_bytes;
        }
    }
    let (manifest_bytes, extents) = match manifest.as_ref() {
        Some(current) => (current.encoded_len() as i64, current.extents.len() as i64),
        None => (-1, -1),
    };
    let chunk_pids = pids_of(&chunks.nodes);
    let commit_end = commit_index(&mut probe);
    emit_window(&mut out, "page", &window, object_bytes, config.append_bytes, commit_end, (manifest_bytes, extents), &group, &chunk_pids);
    out.line(format!(
        "E2E_BASELINE_END object_bytes={object_bytes} extents={extents} manifest_bytes={manifest_bytes} failure={failure:?} run_s={:.1}",
        run_start.elapsed().as_secs_f64()
    ));
    // what was accepted reads back: the last bytes of the object, compared with the position function
    if let Some(current) = manifest.as_ref() {
        let tail = (config.append_bytes as u64).min(current.length);
        let start = current.length - tail;
        let (_, bytes) = client
            .read_object_range(BUCKET, PAGE_KEY, start, tail, current.length as usize, &ReplicaClientOptions::default())
            .unwrap();
        let correct = bytes.iter().enumerate().all(|(index, byte)| *byte == stream_byte(start + index as u64));
        out.line(format!("E2E_BASELINE_READ offset={start} bytes={tail} correct={correct}"));
        assert!(correct, "the page-path object does not read back what was appended");
    }
}
