// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Serve this store's control plane over a socket, in its own process.
//!
//! # RUNNING IT
//!
//! ```text
//! cargo run --bin metaserver
//! ```
//!
//! One of these, plus some number of `replica_server` processes, is a cluster with a control
//! plane of its own: the roster of nodes and the placement of objects onto them live in this
//! process and nowhere else, and killing it stops new placement without stopping reads of
//! objects the store already knows. Why that asymmetry is the right one, and what it costs,
//! is in `crate::cluster::directory`.
//!
//! Configuration is environment variables, like `object_server` and `replica_server`, because
//! this is a process started from a unit file or a container spec:
//!
//! | variable | default | meaning |
//! |---|---|---|
//! | `MATRIXOBJECT_METASERVER_ADDR` | `127.0.0.1:17200` | address to bind; port `0` lets the kernel choose |
//! | `MATRIXOBJECT_METASERVER_HEARTBEAT_TTL_MS` | `0` | automatic expiry TTL, 1000..3600000 ms; zero disables |
//! | `MATRIXOBJECT_METASERVER_THREADS` | `2` | connections served at once |
//! | `MATRIXOBJECT_METASERVER_MAX_PAYLOAD_BYTES` | `8388608` | largest payload one frame may declare |
//! | `MATRIXOBJECT_METASERVER_STATE_DIR` | *(unset)* | existing directory for locked, atomic metadata snapshots |
//! | `MATRIXOBJECT_METASERVER_DATA_ROSTER` | *(unset)* | comma-separated `node-id@socket-address@failure-domain`; starts offline until heartbeat |
//! | `MATRIXOBJECT_METASERVER_SEED_DATA_NODES` | `0` | data nodes to put in the roster before serving |
//! | `MATRIXOBJECT_METASERVER_SEED_CAPACITY_BYTES` | `1073741824` | capacity each seeded node reports |
//!
//! `MATRIXOBJECT_METASERVER_CHUNK_GROUPS_FILE` selects a canonical binary MOGC
//! catalog instead of the individual-node roster. It cannot be combined with
//! legacy seed count/capacity or DATA_ROSTER. Each group contributes one logical
//! placement target; voter entries report health without adding placement capacity.
//! The catalog is persisted with metadata and restored on restart.
//!
//! The explicit roster supplies operator-selected chunk endpoints and starts them offline.
//! A controller must heartbeat verified chunk processes before placement can select them.
//! The legacy node-count bootstrap is for loopback development and cannot be combined with
//! an explicit roster. Persisted membership takes precedence over all bootstrap settings.
//!
//! # THE FIRST LINE OF OUTPUT IS A CONTRACT
//!
//! The bound address is printed on stdout, flushed, before the accept loop starts:
//!
//! ```text
//! metaserver listening on 127.0.0.1:41923 pid=81124 protocol_stamp=1 \
//!   threads=2 max_payload_bytes=8388608 auth=none storage=memory
//! ```
//!
//! Printed before serving and flushed, because a supervisor -- or a test standing this up on
//! port `0` alongside several replicas -- has no other way to learn which port the kernel
//! chose. A line printed after the first request would arrive too late to connect with.
//!
//! The format is the one `replica_server` prints, field for field, on purpose: a test or a
//! supervisor that can read one startup line can read both, and the alternative is two
//! parsers for two lines that say the same six things.
//!
//! # RESTART RECOVERY
//!
//! `MATRIXOBJECT_METASERVER_STATE_DIR` enables atomic, synced metadata snapshots in
//! an existing directory. A restored roster takes precedence over bootstrap settings.
//! Without this setting, restart loses the in-memory control plane. Durable snapshots
//! recover one server; they do not provide a replicated or highly available control plane.
//!
//! # IT IS UNAUTHENTICATED
//!
//! No signing, no token, no TLS. Anything that can open a socket to this port can register a
//! node, move a placement, or read the whole roster. The default bind is loopback for that
//! reason, and a non-loopback bind is reported as the choice it is.

#[cfg(not(feature = "meta-server"))]
fn main() {
    eprintln!(
        "this binary needs the `meta-server` feature, which is on by default -- so this \
         build turned it off: cargo run --features meta-server --bin metaserver"
    );
    std::process::exit(2);
}

#[cfg(feature = "meta-server")]
fn main() {
    use std::io::{Read, Write};

    use matrixobjectstore_rs::cluster::{
        ChunkGroupCatalog, DataNodeSeed, MetaNodeOptions, MetaNodeServer, MAX_PAYLOAD_BYTES,
        PROTOCOL_STAMP,
    };

    fn env_string(name: &str, default: &str) -> String {
        std::env::var(name)
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| default.to_string())
    }

    /// Read a numeric setting, REFUSING a value that does not parse.
    ///
    /// A metaserver that silently ignored the payload bound it was given would be running
    /// under a limit nobody chose, with nothing in the log to say which one.
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

    let addr = env_string("MATRIXOBJECT_METASERVER_ADDR", "127.0.0.1:17200");
    let worker_threads = env_usize("MATRIXOBJECT_METASERVER_THREADS", 2).max(1);
    let max_payload_bytes = env_usize(
        "MATRIXOBJECT_METASERVER_MAX_PAYLOAD_BYTES",
        MAX_PAYLOAD_BYTES,
    );

    let options = MetaNodeOptions {
        max_payload_bytes,
        worker_threads,
        ..Default::default()
    };
    let state_dir = std::env::var_os("MATRIXOBJECT_METASERVER_STATE_DIR");
    #[allow(unused_mut)]
    let mut storage = if state_dir.is_some() {
        "snapshot"
    } else {
        "memory"
    };
    let raft_peers = std::env::var_os("MATRIXOBJECT_METASERVER_RAFT_PEERS");
    if raft_peers.is_none()
        && [
            "MATRIXOBJECT_METASERVER_RAFT_NODE_ID",
            "MATRIXOBJECT_METASERVER_RAFT_STATE_DIR",
            "MATRIXOBJECT_METASERVER_RAFT_GROUP_ID",
            "MATRIXOBJECT_METASERVER_RAFT_TIMEOUT_MS",
        ]
        .iter()
        .any(|name| std::env::var_os(name).is_some())
    {
        eprintln!("partial Raft configuration: MATRIXOBJECT_METASERVER_RAFT_PEERS is required");
        std::process::exit(2);
    }
    if raft_peers.is_some() && state_dir.is_some() {
        eprintln!("local metadata state and Raft state cannot be configured together");
        std::process::exit(2);
    }
    let configured_groups =
        std::env::var_os("MATRIXOBJECT_METASERVER_CHUNK_GROUPS_FILE").map(|path| {
            if [
                "MATRIXOBJECT_METASERVER_DATA_ROSTER",
                "MATRIXOBJECT_METASERVER_SEED_DATA_NODES",
                "MATRIXOBJECT_METASERVER_SEED_CAPACITY_BYTES",
            ]
            .iter()
            .any(|name| std::env::var_os(name).is_some())
            {
                eprintln!(
                    "chunk group catalog cannot be combined with other bootstrap roster settings"
                );
                std::process::exit(2);
            }
            let read = || -> Result<ChunkGroupCatalog, Box<dyn std::error::Error>> {
                let mut bytes = Vec::new();
                std::fs::File::open(path)?
                    .take(2 * 1024 * 1024 + 5)
                    .read_to_end(&mut bytes)?;
                Ok(ChunkGroupCatalog::from_bytes(&bytes)?)
            };
            read().unwrap_or_else(|error| {
                eprintln!("invalid chunk group catalog: {error}");
                std::process::exit(2);
            })
        });
    let configured_roster = match std::env::var("MATRIXOBJECT_METASERVER_DATA_ROSTER") {
        Ok(raw) => {
            if std::env::var_os("MATRIXOBJECT_METASERVER_SEED_DATA_NODES").is_some() {
                eprintln!("data roster and legacy seed count cannot be configured together");
                std::process::exit(2);
            }
            match DataNodeSeed::parse_roster(&raw) {
                Ok(nodes) => Some(nodes),
                Err(error) => {
                    eprintln!("invalid data roster: {error}");
                    std::process::exit(2);
                }
            }
        }
        Err(std::env::VarError::NotPresent) => None,
        Err(error) => {
            eprintln!("invalid data roster: {error}");
            std::process::exit(2);
        }
    };

    let bound_server = match state_dir {
        Some(path) => {
            MetaNodeServer::bind_with_snapshot(&addr, options, std::path::Path::new(&path))
        }
        None => MetaNodeServer::bind(&addr, options),
    };
    #[allow(unused_mut)]
    let mut server = match bound_server {
        Ok(server) => server,
        Err(err) => {
            eprintln!("binding {addr} failed: {err}");
            std::process::exit(1);
        }
    };

    let heartbeat_ttl_ms = env_usize("MATRIXOBJECT_METASERVER_HEARTBEAT_TTL_MS", 0) as u64;
    if heartbeat_ttl_ms != 0 {
        if let Err(error) = server.enable_heartbeat_expiry(heartbeat_ttl_ms) {
            eprintln!("invalid heartbeat expiry: {error}");
            std::process::exit(2);
        }
    }

    let seed_nodes = env_usize("MATRIXOBJECT_METASERVER_SEED_DATA_NODES", 0);
    let seed_capacity = env_usize(
        "MATRIXOBJECT_METASERVER_SEED_CAPACITY_BYTES",
        1024 * 1024 * 1024,
    ) as u64;
    // SEEDED BEFORE THE STARTUP LINE IS PRINTED, so that a supervisor reading that line and
    // connecting immediately cannot see a roster that is still filling. The count is printed
    // on the line, so a test does not have to ask whether the seed happened.
    #[allow(unused_mut)]
    let mut seeded = match (configured_groups, configured_roster) {
        (Some(groups), _) => server.seed_chunk_groups(groups),
        (None, Some(nodes)) => server.seed_data_roster(&nodes, seed_capacity),
        (None, None) => server.seed_data_nodes(seed_nodes, seed_capacity),
    }
    .unwrap_or_else(|err| {
        eprintln!("seeding data roster failed: {err}");
        std::process::exit(1);
    });

    if let Some(raw) = raft_peers {
        #[cfg(not(feature = "metadata-consensus"))]
        {
            let _ = raw;
            eprintln!("Raft configuration requires the metadata-consensus feature");
            std::process::exit(2);
        }
        #[cfg(feature = "metadata-consensus")]
        {
            let configured = (|| -> Result<_, String> {
                let raw = raw.into_string().map_err(|_| "peer list is not UTF-8")?;
                let mut peers = std::collections::BTreeMap::new();
                for item in raw.split(',') {
                    let (id, address) = item.split_once('@').ok_or("expected id@address peers")?;
                    let id = id.parse::<u64>().map_err(|_| "invalid voter id")?;
                    let address = address
                        .parse::<std::net::SocketAddr>()
                        .map_err(|_| "invalid voter address")?;
                    if peers.insert(id, address).is_some() {
                        return Err("duplicate voter id".into());
                    }
                }
                let node_id = std::env::var("MATRIXOBJECT_METASERVER_RAFT_NODE_ID")
                    .map_err(|_| "missing Raft node id")?
                    .parse::<u64>()
                    .map_err(|_| "invalid Raft node id")?;
                let directory = std::env::var_os("MATRIXOBJECT_METASERVER_RAFT_STATE_DIR")
                    .ok_or("missing Raft state directory")?
                    .into();
                Ok(matrixobjectstore_rs::cluster::MetaRaftOptions {
                    node_id,
                    group_id: env_usize("MATRIXOBJECT_METASERVER_RAFT_GROUP_ID", 1) as u64,
                    peers,
                    directory,
                    request_timeout: std::time::Duration::from_millis(env_usize(
                        "MATRIXOBJECT_METASERVER_RAFT_TIMEOUT_MS",
                        3000,
                    ) as u64),
                })
            })();
            let options = match configured {
                Ok(options) => options,
                Err(error) => {
                    eprintln!("invalid Raft configuration: {error}");
                    std::process::exit(2);
                }
            };
            seeded = match server.enable_raft(options) {
                Ok(count) => count,
                Err(error) => {
                    eprintln!("opening metadata consensus failed: {error}");
                    std::process::exit(1);
                }
            };
            storage = "raft";
        }
    }

    let bound = match server.local_addr() {
        Ok(bound) => bound,
        Err(err) => {
            eprintln!("the bound address could not be read back: {err}");
            std::process::exit(1);
        }
    };

    println!(
        "metaserver listening on {bound} pid={} protocol_stamp={PROTOCOL_STAMP} \
         threads={worker_threads} max_payload_bytes={max_payload_bytes} auth=none \
         storage={storage} roster_nodes={seeded}",
        std::process::id()
    );
    if !bound.ip().is_loopback() {
        println!(
            "WARNING: {bound} is not loopback and this metaserver is unauthenticated, so \
             anything that can reach this port can register a node, move a placement, or \
             read the whole roster"
        );
    }
    // FLUSHED BEFORE SERVING. stdout to a pipe is block-buffered, so without this the line
    // above sits in a buffer until the buffer fills -- and a supervisor reading the port off
    // that line waits for a process that is already listening.
    if let Err(err) = std::io::stdout().flush() {
        eprintln!("the startup line could not be flushed: {err}");
        std::process::exit(1);
    }

    if let Err(err) = server.serve() {
        eprintln!("the accept loop stopped: {err}");
        std::process::exit(1);
    }
}
