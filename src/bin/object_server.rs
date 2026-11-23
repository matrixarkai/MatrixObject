// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Serve the cluster's object plane over HTTP.
//!
//! # RUNNING IT
//!
//! ```text
//! cargo run --bin object_server
//! ```
//!
//! Configuration is environment variables, because this is a process an operator starts from a
//! unit file or a container spec rather than a command they type:
//!
//! | variable | default | meaning |
//! |---|---|---|
//! | `MATRIXOBJECT_HTTP_ADDR` | `127.0.0.1:17200` | address to bind |
//! | `MATRIXOBJECT_BUCKET` | `default` | bucket created at startup |
//! | `MATRIXOBJECT_LIFECYCLE_INTERVAL_MS` | `3600000` | time between lifecycle passes (expirations and abandoned multipart uploads of the buckets' rules), 1000 to 86400000; any other value is refused |
//! | `MATRIXOBJECT_MAX_BODY_BYTES` | `67108864` | largest object this door accepts |
//! | `MATRIXOBJECT_THREADS` | `4` | accept threads |
//! | `MATRIXOBJECT_CLUSTER_META` | *(unset)* | `host:port[,host:port...]`: serve the replicated object plane through these metadata endpoints instead of an in-process store (see below) |
//! | `MATRIXOBJECT_CLUSTER_META_GROUP` | *(unset)* | metadata Raft group id; set for a Raft metadata quorum (several endpoints imply `1`) |
//! | `MATRIXOBJECT_CLUSTER_TIMEOUT_MS` | `10000` | bound on one cluster operation |
//! | `MATRIXOBJECT_CLUSTER_PAGE_BYTES` | `1048576` | page size for written data, 1 to 1048576 |
//! | `MATRIXOBJECT_CLUSTER_CACHE_ENTRIES` | `4096` | manifests the door remembers so a repeated append can skip reading one (and `MATRIXOBJECT_CLUSTER_CACHE_MIB`, `64`, bounds their memory: a manifest of 4,000 extents is about 0.75 MiB); the least recently used leave first |
//! | `MATRIXOBJECT_CLUSTER_POOL_MAX` | `64` (at least the thread count) | idle metadata connections kept for reuse; a request that finds the pool empty opens one and drops it when the pool is full |
//! | `MATRIXOBJECT_CLUSTER_TARGET_GROUPS` | `1` | data groups one object is spread over |
//! | `MATRIXOBJECT_GATEWAY_NODE_ID` | *(unset)* | cluster mode: this door's id in the metaserver roster (declared there with `MATRIXOBJECT_METASERVER_GATEWAY_ROSTER`). When set, the door heartbeats that node online, withdraws it (draining) on SIGTERM, and clients find live doors with `RemoteMetaClient::list_gateways` |
//! | `MATRIXOBJECT_GATEWAY_HEARTBEAT_MS` | `3000` | roster heartbeat interval, 100..60000 |
//! | `MATRIXOBJECT_GATEWAY_DRAIN_GRACE_MS` | `5000` | after SIGTERM: how long to keep answering after withdrawing from the roster, so requests already routed here finish |
//! | `MATRIXOBJECT_RECLAIM_GRACE_MS` | `3600000` | a page must stay unnamed this long (60000..604800000) before it is freed; longer than any write takes from its first page to its publication |
//! | `MATRIXOBJECT_RECLAIM_INTERVAL_MS` | a quarter of the grace | time between rounds, 1000 up to the grace |
//! | `MATRIXOBJECT_CLIENT_TLS_CA_CERT`, `MATRIXOBJECT_CLIENT_TLS_CERT`, `MATRIXOBJECT_CLIENT_TLS_KEY` | *(unset)* | mutual TLS from this door to the metadata and chunk servers (cluster mode only); the door itself still listens in plaintext |
//!
//! # CLUSTER MODE
//!
//! `MATRIXOBJECT_CLUSTER_META` is required: this door serves a metadata quorum and its chunk
//! groups, running as separate processes. It answers PUT, append (`x-amz-write-offset-bytes`),
//! GET with `Range`, HEAD, DELETE and ListObjectsV2, and answers every other S3 verb
//! `NotImplemented`. Without it the process refuses to start. See `docs/cluster-gateway.md`.
//!
//! # BEFORE PUTTING DATA IN IT
//!
//! **It is unauthenticated.** No signing, no token, no TLS. Anything that can open a socket to the
//! port can read, overwrite and delete every object. That is why the default bind is loopback, and
//! moving it off loopback means putting the whole store behind whatever else protects that
//! network.


/// Set by the SIGTERM handler; polled by the thread that withdraws this door from the roster.
#[cfg(all(feature = "http-server", unix))]
static TERMINATE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

#[cfg(all(feature = "http-server", unix))]
extern "C" fn on_terminate(_signal: i32) {
    TERMINATE.store(true, std::sync::atomic::Ordering::SeqCst);
}

/// With `MATRIXOBJECT_GATEWAY_NODE_ID` set, heartbeat this door's roster node online, and on
/// SIGTERM mark it draining, keep serving for the grace period, then exit. Without it nothing
/// is registered and SIGTERM keeps its default meaning.
#[cfg(all(feature = "http-server", unix))]
fn start_roster_heartbeat<F>(
    open: std::sync::Arc<F>,
    drain_flag: std::sync::Arc<std::sync::atomic::AtomicBool>,
) where
    F: Fn() -> Result<matrixobjectstore_rs::cluster::RemoteMetaClient, matrixobjectstore_rs::ObjectError>
        + Send
        + Sync
        + 'static,
{
    use std::sync::atomic::Ordering;
    use std::time::Duration;
    let Ok(node_id) = std::env::var("MATRIXOBJECT_GATEWAY_NODE_ID") else { return };
    let number = |name: &str, default: u64, low: u64, high: u64| -> u64 {
        match std::env::var(name) {
            Err(_) => default,
            Ok(raw) => match raw.trim().parse::<u64>() {
                Ok(value) if (low..=high).contains(&value) => value,
                _ => {
                    eprintln!("{name} must be a number from {low} to {high}");
                    std::process::exit(2);
                }
            },
        }
    };
    let interval = number("MATRIXOBJECT_GATEWAY_HEARTBEAT_MS", 3_000, 100, 60_000);
    let grace = number("MATRIXOBJECT_GATEWAY_DRAIN_GRACE_MS", 5_000, 0, 600_000);
    let mut client = None;
    let id = node_id.clone();
    let advertiser = matrixobjectstore_rs::net::gateway_door::GatewayAdvertiser::start(
        Duration::from_millis(interval),
        move |draining| {
            if client.is_none() {
                client = Some((*open)()?);
            }
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|elapsed| elapsed.as_millis() as u64)
                .unwrap_or(0);
            let result = client.as_mut().unwrap().heartbeat_placement_node(&id, 0, 0, draining, true, now_ms);
            if let Err(error) = &result {
                // A connection that failed is dropped so the next beat opens a new one.
                client = None;
                eprintln!("gateway roster heartbeat for {id} failed: {error}");
            }
            result.map(|_| ())
        },
    );
    unsafe {
        libc::signal(libc::SIGTERM, on_terminate as extern "C" fn(i32) as usize);
    }
    std::thread::spawn(move || {
        while !TERMINATE.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(100));
        }
        drain_flag.store(true, Ordering::SeqCst);
        advertiser.drain();
        std::thread::sleep(Duration::from_millis(grace));
        std::process::exit(0);
    });
    println!("object_server roster node={node_id} heartbeat_ms={interval} drain_grace_ms={grace}");
}

#[cfg(not(feature = "http-server"))]
fn main() {
    eprintln!(
        "this binary needs the `http-server` feature, which is on by default -- so this \
         build turned it off: cargo run --features http-server --bin object_server"
    );
    std::process::exit(2);
}

#[cfg(feature = "http-server")]
#[cfg_attr(not(all(feature = "replica-server", feature = "meta-server")), allow(unused_variables))]
fn main() {
    #[cfg(all(feature = "replica-server", feature = "meta-server"))]
    use std::time::Duration;

    fn env_string(name: &str, default: &str) -> String {
        std::env::var(name)
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| default.to_string())
    }

    /// Read a numeric setting, REFUSING a value that does not parse.
    ///
    /// Falling back to the default on an unparseable value is how a server ends up running with a
    /// configuration nobody chose: the operator set the variable, the process ignored it, and
    /// nothing said so.
    fn env_usize(name: &str, default: usize) -> usize {
        match std::env::var(name) {
            Ok(raw) if !raw.trim().is_empty() => match raw.trim().parse::<usize>() {
                Ok(parsed) => parsed,
                Err(err) => {
                    eprintln!("{name}={raw:?} is not a number: {err}");
                    std::process::exit(2);
                }
            },
            _ => default,
        }
    }

    let addr = env_string("MATRIXOBJECT_HTTP_ADDR", "127.0.0.1:17200");
    let bucket = env_string("MATRIXOBJECT_BUCKET", "default");
    let max_body_bytes = env_usize("MATRIXOBJECT_MAX_BODY_BYTES", 64 * 1024 * 1024);
    let worker_threads = env_usize("MATRIXOBJECT_THREADS", 4).max(1);

    #[cfg(all(feature = "replica-server", feature = "meta-server"))]
    if let Some(meta) = std::env::var("MATRIXOBJECT_CLUSTER_META")
        .ok()
        .filter(|value| !value.trim().is_empty())
    {
        let timeout_ms = env_usize("MATRIXOBJECT_CLUSTER_TIMEOUT_MS", 10_000).max(1) as u64;
        let page_bytes = env_usize("MATRIXOBJECT_CLUSTER_PAGE_BYTES", 1024 * 1024);
        let target_groups = env_usize("MATRIXOBJECT_CLUSTER_TARGET_GROUPS", 1);
        let group = std::env::var("MATRIXOBJECT_CLUSTER_META_GROUP")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map(|_| env_usize("MATRIXOBJECT_CLUSTER_META_GROUP", 1) as u64);
        run_cluster_gateway(
            &addr,
            &bucket,
            &meta,
            group,
            max_body_bytes,
            worker_threads,
            Duration::from_millis(timeout_ms),
            page_bytes,
            target_groups as u32,
        );
    }

    eprintln!(
        "object_server serves the cluster: set MATRIXOBJECT_CLUSTER_META to the metadata endpoints \
         (and build with the replica-server and meta-server features)"
    );
    std::process::exit(2);
}

/// The time between lifecycle passes: 1000 to 86400000 ms, one hour when the variable is unset. Any other value,
/// `0` included, is refused.
#[cfg(all(feature = "http-server", feature = "replica-server", feature = "meta-server"))]
fn resolve_lifecycle_interval(raw: Option<&str>) -> Result<u64, String> {
    match raw {
        None => Ok(3_600_000),
        Some(raw) => match raw.trim().parse::<u64>() {
            Ok(ms) if (1_000..=86_400_000).contains(&ms) => Ok(ms),
            _ => Err(format!("MATRIXOBJECT_LIFECYCLE_INTERVAL_MS must be from 1000 to 86400000, and {raw:?} is not")),
        },
    }
}

#[cfg(all(feature = "http-server", feature = "replica-server", feature = "meta-server"))]
#[allow(clippy::too_many_arguments)]
fn run_cluster_gateway(
    addr: &str,
    bucket: &str,
    meta: &str,
    group: Option<u64>,
    max_body_bytes: usize,
    worker_threads: usize,
    timeout: std::time::Duration,
    page_bytes: usize,
    target_groups: u32,
) -> ! {
    use std::sync::Arc;

    use matrixobjectstore_rs::cluster::{
        ObjectWriteOptions, RemoteMetaClient, ReplicaClientOptions,
    };
    use matrixobjectstore_rs::net::cluster_gateway::{ClusterGateway, GatewayOptions};
    use matrixobjectstore_rs::net::{ObjectHttpServer, ServerOptions};

    let endpoints: Vec<String> = meta
        .split(',')
        .map(|endpoint| endpoint.trim().to_string())
        .filter(|endpoint| !endpoint.is_empty())
        .collect();
    if endpoints.is_empty() {
        eprintln!("MATRIXOBJECT_CLUSTER_META names no endpoint");
        std::process::exit(2);
    }
    let mut write = ObjectWriteOptions::default();
    write.page_bytes = page_bytes;
    write.target_groups = target_groups;
    write.max_object_bytes = max_body_bytes;
    // MUTUAL TLS TO THE CLUSTER, from the same MATRIXOBJECT_CLIENT_TLS_* variables the chunk
    // server uses for its heartbeats. Unset means plaintext, as before.
    let tls = match matrixobjectstore_rs::cluster::ApiTlsConfig::from_env("MATRIXOBJECT_CLIENT") {
        Ok(tls) => tls,
        Err(err) => {
            eprintln!("the cluster TLS configuration is invalid: {err}");
            std::process::exit(2);
        }
    };
    let replica = ReplicaClientOptions {
        tls: tls.clone(),
        connect_timeout: timeout,
        read_timeout: timeout,
        write_timeout: timeout,
        ..ReplicaClientOptions::default()
    };
    // Each open HTTP connection is served on its own thread and takes a metadata connection for
    // the length of a request: a pool smaller than the connections in flight opens a new metadata
    // connection for every request beyond it and drops it afterwards.
    let pool_max = matrixobjectstore_rs::net::cluster_gateway::resolve_pool_max(
        std::env::var("MATRIXOBJECT_CLUSTER_POOL_MAX").ok().as_deref(),
        worker_threads,
    );
    let reclaim_write = write.clone();
    let options = GatewayOptions {
        write,
        replica,
        operation_timeout: timeout,
        max_object_bytes: max_body_bytes,
        pool_max,
        cache_entries: matrixobjectstore_rs::net::cluster_gateway::resolve_positive(
            std::env::var("MATRIXOBJECT_CLUSTER_CACHE_ENTRIES").ok().as_deref(),
            GatewayOptions::default().cache_entries,
        ),
        cache_bytes: matrixobjectstore_rs::net::cluster_gateway::resolve_positive(
            std::env::var("MATRIXOBJECT_CLUSTER_CACHE_MIB").ok().as_deref(),
            GatewayOptions::default().cache_bytes / (1024 * 1024),
        ) * 1024 * 1024,
        ..GatewayOptions::default()
    };
    let mut meta_options = ClusterGateway::meta_options(timeout);
    meta_options.tls = tls;
    let open_endpoints = endpoints.clone();
    let connect = move |options: &matrixobjectstore_rs::cluster::MetaClientOptions| {
        if let Some(group) = group.or(if open_endpoints.len() > 1 { Some(1) } else { None }) {
            RemoteMetaClient::connect_raft_catalog(&open_endpoints, group, options)
        } else {
            RemoteMetaClient::connect(&open_endpoints[0], options)
        }
    };
    // Startup reads share the operation deadline so a listening voter may elect
    // a leader before the door serves. Runtime connections get fresh deadlines.
    let mut startup_options = meta_options.clone();
    startup_options.deadline = Some(std::time::Instant::now() + timeout);
    // THE SWEEP THAT FREES THE PAGES NO MANIFEST NAMES always runs in this process: objects are chunk manifests.
    let reclaim = match matrixobjectstore_rs::cluster::reclaim_loop::resolve_reclaim(
        std::env::var("MATRIXOBJECT_RECLAIM_GRACE_MS").ok().as_deref(),
        std::env::var("MATRIXOBJECT_RECLAIM_INTERVAL_MS").ok().as_deref(),
    ) {
        Ok(settings) => settings,
        Err(message) => {
            eprintln!("{message}");
            std::process::exit(2);
        }
    };
    // OBJECTS ARE CHUNK MANIFESTS AND NOTHING ELSE. A data directory written by an earlier build holds page-granular
    // objects this build cannot read or reclaim, and there is no migration: refuse to start over one.
    match connect(&startup_options) {
        Ok(mut client) => {
            if let Err(message) = matrixobjectstore_rs::cluster::reclaim_loop::refuse_older_data_directory(&mut client) {
                eprintln!("{message}");
                std::process::exit(1);
            }
        }
        Err(err) => {
            eprintln!("no metadata endpoint in {meta} answered: {err}");
            std::process::exit(1);
        }
    }
    let open = Arc::new(move || connect(&meta_options));
    let for_gateway = Arc::clone(&open);
    let gateway = Arc::new(ClusterGateway::over_chunk_objects(move || (*for_gateway)(), options, bucket.to_string()));
    // the bucket this door serves exists from the start
    if let Err(err) = gateway.ensure_bucket(&bucket) {
        eprintln!("the bucket {bucket} could not be created: {err}");
        std::process::exit(1);
    }
    // `/livez` and `/readyz` in front of the gateway, and the drain switch behind them.
    let for_lifecycle = Arc::clone(&gateway);
    let door = matrixobjectstore_rs::net::gateway_door::GatewayDoor::new(gateway);
    let drain_flag = door.drain_flag();
    // THE LIFECYCLE RULES OF THE BUCKETS are acted on by a pass every interval: expirations and abandoned multipart
    // uploads. A pass is safe to repeat and to run from several doors at once.
    let lifecycle_interval = match resolve_lifecycle_interval(std::env::var("MATRIXOBJECT_LIFECYCLE_INTERVAL_MS").ok().as_deref()) {
        Ok(ms) => ms,
        Err(message) => {
            eprintln!("{message}");
            std::process::exit(2);
        }
    };
    let stop = Arc::clone(&drain_flag);
    std::thread::spawn(move || {
        let mut waited = 0u64;
        while !stop.load(std::sync::atomic::Ordering::SeqCst) {
            std::thread::sleep(std::time::Duration::from_millis(500));
            waited += 500;
            if waited < lifecycle_interval {
                continue;
            }
            waited = 0;
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |elapsed| elapsed.as_millis() as u64);
            match for_lifecycle.run_lifecycle(now) {
                Ok(report) if report.expired + report.expired_versions + report.aborted_uploads + report.kept_locked > 0 => eprintln!(
                    "lifecycle: {} expired, {} old versions expired, {} kept by a lock, {} uploads aborted in {} buckets",
                    report.expired, report.expired_versions, report.kept_locked, report.aborted_uploads, report.buckets
                ),
                Ok(_) => {}
                Err(error) => eprintln!("lifecycle pass failed: {error}"),
            }
        }
    });
    let gateway = Arc::new(door);
    {
        let stop = Arc::clone(&drain_flag);
        let for_reclaim = Arc::clone(&open);
        std::thread::spawn(move || {
            matrixobjectstore_rs::cluster::reclaim_loop::ReclaimLoop::new(move || (*for_reclaim)(), reclaim_write, reclaim)
                .run(stop)
        });
    }
    #[cfg(unix)]
    start_roster_heartbeat(Arc::clone(&open), drain_flag);
    #[cfg(not(unix))]
    let _ = drain_flag;
    let server = match ObjectHttpServer::bind_handler(
        addr,
        gateway,
        ServerOptions {
            max_body_bytes,
            worker_threads,
            ..ServerOptions::default()
        },
    ) {
        Ok(server) => server,
        Err(err) => {
            eprintln!("binding {addr} failed: {err}");
            std::process::exit(1);
        }
    };
    let bound = match server.local_addr() {
        Ok(bound) => bound,
        Err(err) => {
            eprintln!("the bound address could not be read back: {err}");
            std::process::exit(1);
        }
    };
    println!(
        "object_server listening on {bound} pid={} bucket={bucket} backend=cluster meta={meta} max_body_bytes={max_body_bytes} threads={worker_threads} auth=none",
        std::process::id()
    );
    // Flushed: a supervisor reading this line from a pipe must not wait for a full buffer.
    let _ = std::io::Write::flush(&mut std::io::stdout());
    // The sweep's status follows the listening line: a supervisor reads the FIRST line for the address.
    println!("object_server reclaim_refused_page_objects=0 (a sweep refused because page objects exist is reported as a warning)");
    println!("object_server reclaim grace_ms={} interval_ms={}", reclaim.grace_ms, reclaim.interval_ms);
    let _ = std::io::Write::flush(&mut std::io::stdout());
    if !bound.ip().is_loopback() {
        println!(
            "WARNING: {bound} is not loopback and this server is unauthenticated, so anything              that can reach this port can read, overwrite and delete every object"
        );
    }
    if let Err(err) = server.serve() {
        eprintln!("the accept loop stopped: {err}");
        std::process::exit(1);
    }
    std::process::exit(0);
}

#[cfg(all(test, feature = "http-server", feature = "replica-server", feature = "meta-server"))]
mod tests {
    use super::resolve_lifecycle_interval;

    #[test]
    fn the_lifecycle_interval_is_one_hour_by_default_and_zero_or_out_of_range_is_refused() {
        assert_eq!(resolve_lifecycle_interval(None), Ok(3_600_000));
        assert_eq!(resolve_lifecycle_interval(Some(" 1000 ")), Ok(1_000));
        assert_eq!(resolve_lifecycle_interval(Some("86400000")), Ok(86_400_000));
        for refused in ["0", "999", "86400001", "x", "", "-1"] {
            assert!(resolve_lifecycle_interval(Some(refused)).is_err(), "{refused:?}");
        }
    }
}
