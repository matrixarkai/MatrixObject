// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Serve one object store over HTTP.
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
//! | `MATRIXOBJECT_SNAPSHOT_PATH` | *(unset)* | durable snapshot file; in memory when unset |
//! | `MATRIXOBJECT_REPLICAS` | *(unset: the store's default, 3)* | in-process replica count, 1 to 7; the write quorum is a majority |
//! | `MATRIXOBJECT_JOURNAL_PATH` | *(unset)* | durable append-only journal; exclusive with the snapshot |
//! | `MATRIXOBJECT_JOURNAL_REPAIR_TORN_TAIL` | *(unset)* | `1` drops a torn final journal frame at start, keeping it beside the journal |
//! | `MATRIXOBJECT_MAX_BODY_BYTES` | `67108864` | largest object this door accepts |
//! | `MATRIXOBJECT_THREADS` | `4` | accept threads |
//! | `MATRIXOBJECT_CLUSTER_META` | *(unset)* | `host:port[,host:port...]`: serve the replicated object plane through these metadata endpoints instead of an in-process store (see below) |
//! | `MATRIXOBJECT_CLUSTER_META_GROUP` | *(unset)* | metadata Raft group id; set for a Raft metadata quorum (several endpoints imply `1`) |
//! | `MATRIXOBJECT_CLUSTER_TIMEOUT_MS` | `10000` | bound on one cluster operation |
//! | `MATRIXOBJECT_CLUSTER_PAGE_BYTES` | `1048576` | page size for written data, 1 to 1048576 |
//! | `MATRIXOBJECT_CLUSTER_TARGET_GROUPS` | `1` | data groups one object is spread over |
//!
//! # CLUSTER MODE
//!
//! With `MATRIXOBJECT_CLUSTER_META` set this door serves a metadata quorum and its chunk
//! groups, running as separate processes, rather than a store in its own memory. It answers
//! PUT, append (`x-amz-write-offset-bytes`), GET with `Range`, HEAD, DELETE and ListObjectsV2,
//! and answers every other S3 verb `NotImplemented`. The snapshot, journal and in-process
//! replica variables do not apply and are refused when combined with it. See
//! `docs/cluster-gateway.md`.
//!
//! # TWO THINGS TO KNOW BEFORE PUTTING DATA IN IT
//!
//! **It is unauthenticated.** No signing, no token, no TLS. Anything that can open a socket to the
//! port can read, overwrite and delete every object. That is why the default bind is loopback, and
//! moving it off loopback means putting the whole store behind whatever else protects that
//! network.
//!
//! **Without `MATRIXOBJECT_SNAPSHOT_PATH` nothing survives the process.** The default store is
//! entirely in memory. The variable is reported at startup, including when it is unset, so an
//! operator reading the first line of the log can see which of the two they have rather than
//! finding out at the next restart.

#[cfg(not(feature = "http-server"))]
fn main() {
    eprintln!(
        "this binary needs the `http-server` feature, which is on by default -- so this \
         build turned it off: cargo run --features http-server --bin object_server"
    );
    std::process::exit(2);
}

#[cfg(feature = "http-server")]
fn main() {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use matrixobjectstore_rs::net::{ObjectHttpServer, ServerOptions};
    use matrixobjectstore_rs::{MatrixObjectStore, StoreOptions};

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
    let snapshot_path = std::env::var("MATRIXOBJECT_SNAPSHOT_PATH")
        .ok()
        .filter(|value| !value.trim().is_empty());
    let max_body_bytes = env_usize("MATRIXOBJECT_MAX_BODY_BYTES", 64 * 1024 * 1024);
    let worker_threads = env_usize("MATRIXOBJECT_THREADS", 4).max(1);

    let journal_path = std::env::var("MATRIXOBJECT_JOURNAL_PATH")
        .ok()
        .filter(|value| !value.trim().is_empty());
    if snapshot_path.is_some() && journal_path.is_some() {
        // The store refuses this pair itself; saying so here names the two variables instead
        // of surfacing a store-open error that mentions neither.
        eprintln!(
            "MATRIXOBJECT_SNAPSHOT_PATH and MATRIXOBJECT_JOURNAL_PATH are mutually exclusive"
        );
        std::process::exit(2);
    }

    #[cfg(all(feature = "replica-server", feature = "meta-server"))]
    if let Some(meta) = std::env::var("MATRIXOBJECT_CLUSTER_META")
        .ok()
        .filter(|value| !value.trim().is_empty())
    {
        if snapshot_path.is_some()
            || journal_path.is_some()
            || std::env::var("MATRIXOBJECT_REPLICAS").map_or(false, |v| !v.trim().is_empty())
        {
            eprintln!(
                "MATRIXOBJECT_CLUSTER_META serves the cluster; MATRIXOBJECT_SNAPSHOT_PATH,                  MATRIXOBJECT_JOURNAL_PATH and MATRIXOBJECT_REPLICAS describe an in-process store                  and cannot be combined with it"
            );
            std::process::exit(2);
        }
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

    let mut options = StoreOptions {
        persistent_snapshot_path: snapshot_path.clone().unwrap_or_default(),
        persistent_snapshot_flush_on_commit: snapshot_path.is_some(),
        persistent_journal_path: journal_path.clone().unwrap_or_default(),
        ..StoreOptions::default()
    };
    // IN-PROCESS REPLICAS. The store defaults to three, held in this one process: they multiply
    // resident memory and the copies written by the replica count, and survive no failure that
    // this process does not. Left unset, the default is unchanged.
    if std::env::var("MATRIXOBJECT_REPLICAS").map_or(false, |value| !value.trim().is_empty()) {
        let replicas = env_usize("MATRIXOBJECT_REPLICAS", 0);
        if !(1..=7).contains(&replicas) {
            eprintln!("MATRIXOBJECT_REPLICAS={replicas} is outside 1 to 7");
            std::process::exit(2);
        }
        options.replica_count = replicas;
        options.write_quorum = replicas / 2 + 1;
    }

    if let Some(path) = journal_path.as_deref() {
        let repair = std::env::var("MATRIXOBJECT_JOURNAL_REPAIR_TORN_TAIL")
            .map(|value| matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
            .unwrap_or(false);
        if repair {
            match MatrixObjectStore::repair_persistent_journal_tail(path) {
                Ok(None) => eprintln!("journal {path}: no torn tail, nothing repaired"),
                Ok(Some((kept, dropped, preserved))) => eprintln!(
                    "journal {path}: dropped a torn tail of {dropped} bytes after byte {kept}; \
                     the dropped bytes are preserved at {preserved}"
                ),
                Err(err) => {
                    eprintln!("journal repair refused: {err}");
                    std::process::exit(1);
                }
            }
        }
    }

    let mut store = match MatrixObjectStore::new(options) {
        Ok(store) => store,
        Err(err) => {
            eprintln!("the store could not be opened: {err}");
            std::process::exit(1);
        }
    };
    // IDEMPOTENT, so restarting against an existing snapshot is not an error.
    if let Err(err) = store.ensure_bucket(&bucket) {
        // A bucket that already exists is the normal case on a restart; anything else is fatal,
        // because every request addresses this bucket.
        let message = err.to_string();
        if !message.contains("exists") {
            eprintln!("the bucket {bucket:?} could not be created: {err}");
            std::process::exit(1);
        }
    }

    let store = Arc::new(Mutex::new(store));
    let server = match ObjectHttpServer::bind(
        &addr,
        store,
        ServerOptions {
            max_body_bytes,
            worker_threads,
            ..ServerOptions::default()
        },
    ) {
        Ok(server) => server.with_bucket_hint(bucket.clone()),
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

    // THE DURABILITY STATE IS IN THE FIRST LINE, named either way. "snapshot=none" is the one an
    // operator most needs to see and the one a log that only mentioned a configured path would
    // leave silent.
    println!(
        "object_server listening on {bound} bucket={bucket} snapshot={} \
         max_body_bytes={max_body_bytes} threads={worker_threads} auth=none",
        snapshot_path.as_deref().unwrap_or("none")
    );
    if snapshot_path.is_none() {
        println!(
            "WARNING: no MATRIXOBJECT_SNAPSHOT_PATH, so every object is held in memory only and \
             is lost when this process exits"
        );
    }
    if !bound.ip().is_loopback() {
        println!(
            "WARNING: {bound} is not loopback and this server is unauthenticated, so anything \
             that can reach this port can read, overwrite and delete every object"
        );
    }

    // `serve` blocks for the life of the process; the handle's shutdown path exists for callers
    // embedding the server, which a binary with no control socket cannot use.
    let _ = Duration::from_secs(0);
    if let Err(err) = server.serve() {
        eprintln!("the accept loop stopped: {err}");
        std::process::exit(1);
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
    let replica = ReplicaClientOptions {
        connect_timeout: timeout,
        read_timeout: timeout,
        write_timeout: timeout,
        ..ReplicaClientOptions::default()
    };
    let options = GatewayOptions {
        write,
        replica,
        operation_timeout: timeout,
        max_object_bytes: max_body_bytes,
        pool_max: worker_threads.max(1),
        ..GatewayOptions::default()
    };
    let meta_options = ClusterGateway::meta_options(timeout);
    let open_endpoints = endpoints.clone();
    let open = move || {
        if let Some(group) = group.or(if open_endpoints.len() > 1 { Some(1) } else { None }) {
            RemoteMetaClient::connect_raft_catalog(&open_endpoints, group, &meta_options)
        } else {
            RemoteMetaClient::connect(&open_endpoints[0], &meta_options)
        }
    };
    // THE CLUSTER MUST ANSWER BEFORE THIS BINDS: a door that listens while its metadata quorum
    // is unreachable reports ready and then fails every request.
    if let Err(err) = open() {
        eprintln!("no metadata endpoint in {meta} answered: {err}");
        std::process::exit(1);
    }
    let gateway = Arc::new(ClusterGateway::over_sdk(open, options, bucket.to_string()));
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
        "object_server listening on {bound} bucket={bucket} backend=cluster meta={meta}          max_body_bytes={max_body_bytes} threads={worker_threads} auth=none"
    );
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
