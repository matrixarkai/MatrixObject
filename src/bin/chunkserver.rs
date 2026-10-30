// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Serve one replica of this store over a socket, in its own process.
//!
//! # RUNNING IT
//!
//! ```text
//! cargo run --bin replica_server
//! ```
//!
//! Three of these, on three ports, are three replicas in three address spaces. That is the
//! whole purpose of the binary: killing one of them leaves the other two holding the data,
//! which the in-process arrangement cannot do no matter how many replicas it is configured
//! with.
//!
//! Configuration is environment variables, like `object_server`, because this is a process
//! started from a unit file or a container spec:
//!
//! | variable | default | meaning |
//! |---|---|---|
//! | `MATRIXOBJECT_CHUNK_RAFT_CHECKPOINT_BYTES` | `0` | opt-in WAL checkpoint interval; 1024..134217728 bytes when enabled |
//! | `MATRIXOBJECT_REPLICA_ADDR` | `127.0.0.1:17300` | address to bind; port `0` lets the kernel choose |
//! | `MATRIXOBJECT_CHUNK_NODE_ID` | *(unset)* | configured metadata roster ID; enables reports with the catalog |
//! | `MATRIXOBJECT_CHUNK_METADATA_ENDPOINTS` | *(unset)* | comma-separated metaserver API IP sockets, 1..7 |
//! | `MATRIXOBJECT_CHUNK_ADVERTISE_ADDR` | bound address | socket address matching the metadata roster |
//! | `MATRIXOBJECT_CHUNK_HEARTBEAT_MS` | `1000` | report interval, 100..60000 ms |
//! | `MATRIXOBJECT_REPLICA_MAINTENANCE` | `false` | standalone repair mode; refuse ordinary writes and report offline |
//! | `MATRIXOBJECT_REPLICA_THREADS` | `4` | connections served at once |
//! | `MATRIXOBJECT_REPLICA_MAX_PAYLOAD_BYTES` | `8388608` | largest payload one frame may declare |
//! | `MATRIXOBJECT_REPLICA_CAPACITY_BYTES` | `268435456` | total page payload bytes; positive |
//! | `MATRIXOBJECT_REPLICA_MAX_PAGES` | `65536` | page identities, including empty pages; positive |
//!
//! # THE FIRST LINE OF OUTPUT IS A CONTRACT
//!
//! The bound address is printed on stdout, flushed, before the accept loop starts:
//!
//! ```text
//! chunkserver listening on 127.0.0.1:41923 pid=81124 protocol_stamp=1 \
//!   threads=4 max_payload_bytes=8388608 auth=none storage=memory
//! ```
//!
//! Printed before serving and flushed, because a supervisor -- or a test standing three of
//! these up on port `0` -- has no other way to learn which port the kernel chose. A line
//! printed after the first request would be a line that arrives too late to connect with.
//!
//! # RESTART RECOVERY
//!
//! On Unix, `MATRIXOBJECT_REPLICA_CHECKPOINT_BYTES` enables standalone journal
//! compaction after 1024..536870912 new outcome bytes; absent means disabled.
//! Set `MATRIXOBJECT_REPLICA_JOURNAL` to a file in an existing directory to retain
//! writes and discards across restarts. Each successful mutation is synced before
//! acknowledgment. Without this setting the replica remains in memory only.
//! The journal is exclusively locked, checksummed, and replayed before listening.
//! It grows with writes unless periodic checkpointing is enabled; staged installation
//! also persists a complete checkpoint before acknowledgement.
//! A recovered node still needs coordinated repair for writes missed while offline.
//!
//! The optional `chunk-consensus` feature enables independently durable Raft voters.
//! Configure `MATRIXOBJECT_CHUNK_RAFT_PEERS`, `MATRIXOBJECT_CHUNK_RAFT_NODE_ID`, and
//! `MATRIXOBJECT_CHUNK_RAFT_STATE_DIR` instead of a local journal. See
//! `docs/chunk-consensus.md` for group configuration, binary outcomes, and bounds.
//!
//! # IT IS UNAUTHENTICATED
//!
//! No signing, no token, no TLS. Anything that can open a socket to this port can read and
//! overwrite every page this replica holds. The default bind is loopback for that reason, and
//! a non-loopback bind is reported as the choice it is.

#[cfg(not(feature = "replica-server"))]
fn main() {
    eprintln!(
        "this binary needs the `replica-server` feature, which is on by default -- so this \
         build turned it off: cargo run --features replica-server --bin replica_server"
    );
    std::process::exit(2);
}

#[cfg(feature = "replica-server")]
fn main() {
    use std::io::Write;

    use matrixobjectstore_rs::cluster::{
        ReplicaNodeOptions, ReplicaNodeServer, MAX_PAYLOAD_BYTES, PROTOCOL_STAMP,
    };

    fn env_string(name: &str, default: &str) -> String {
        std::env::var(name)
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| default.to_string())
    }

    /// Read a numeric setting, REFUSING a value that does not parse.
    ///
    /// A replica that silently ignored the payload bound it was given would be running under
    /// a limit nobody chose, with nothing in the log to say which one.
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

    let addr = env_string("MATRIXOBJECT_REPLICA_ADDR", "127.0.0.1:17300");
    let worker_threads = env_usize("MATRIXOBJECT_REPLICA_THREADS", 4).max(1);
    let max_payload_bytes = env_usize("MATRIXOBJECT_REPLICA_MAX_PAYLOAD_BYTES", MAX_PAYLOAD_BYTES);

    let capacity_bytes = env_usize("MATRIXOBJECT_REPLICA_CAPACITY_BYTES", 256 * 1024 * 1024) as u64;
    let max_pages = env_usize("MATRIXOBJECT_REPLICA_MAX_PAGES", 65_536);
    let maintenance = match std::env::var("MATRIXOBJECT_REPLICA_MAINTENANCE") {
        Err(std::env::VarError::NotPresent) => false,
        Ok(raw) => raw.trim().parse::<bool>().unwrap_or_else(|_| {
            eprintln!("MATRIXOBJECT_REPLICA_MAINTENANCE must be true or false");
            std::process::exit(2);
        }),
        Err(_) => {
            eprintln!("MATRIXOBJECT_REPLICA_MAINTENANCE must be valid text");
            std::process::exit(2);
        }
    };
    let options = ReplicaNodeOptions {
        tls: matrixobjectstore_rs::cluster::ApiTlsConfig::from_env("MATRIXOBJECT_API").unwrap_or_else(|error| {
            eprintln!("invalid API TLS configuration: {error}");
            std::process::exit(2);
        }),
        maintenance,
        max_payload_bytes,
        worker_threads,
        capacity_bytes,
        max_pages,
        ..Default::default()
    };
    let journal = std::env::var_os("MATRIXOBJECT_REPLICA_JOURNAL");
    let raft_peers = std::env::var_os("MATRIXOBJECT_CHUNK_RAFT_PEERS");
    if raft_peers.is_none()
        && [
            "MATRIXOBJECT_CHUNK_RAFT_BOOTSTRAP_VOTERS",
            "MATRIXOBJECT_CHUNK_RAFT_NODE_ID",
            "MATRIXOBJECT_CHUNK_RAFT_STATE_DIR",
            "MATRIXOBJECT_CHUNK_RAFT_GROUP_ID",
            "MATRIXOBJECT_CHUNK_RAFT_TIMEOUT_MS",
            "MATRIXOBJECT_CHUNK_RAFT_CHECKPOINT_BYTES",
            "MATRIXOBJECT_CHUNK_RAFT_TLS_CA_CERT",
            "MATRIXOBJECT_CHUNK_RAFT_TLS_CERT",
            "MATRIXOBJECT_CHUNK_RAFT_TLS_KEY",
        ]
        .iter()
        .any(|name| std::env::var_os(name).is_some())
    {
        eprintln!("partial Raft configuration: MATRIXOBJECT_CHUNK_RAFT_PEERS is required");
        std::process::exit(2);
    }
    if raft_peers.is_some() && journal.is_some() {
        eprintln!("local chunk journal and Raft state cannot be configured together");
        std::process::exit(2);
    }
    #[allow(unused_mut)]
    let mut storage = if journal.is_some() {
        "journal"
    } else {
        "memory"
    };
    let bound_server = match journal {
        Some(path) => {
            ReplicaNodeServer::bind_with_journal(&addr, options, std::path::Path::new(&path))
        }
        None => ReplicaNodeServer::bind(&addr, options),
    };
    #[allow(unused_mut)]
    let mut server = match bound_server {
        Ok(server) => server,
        Err(err) => {
            eprintln!("binding {addr} failed: {err}");
            std::process::exit(1);
        }
    };

    if std::env::var_os("MATRIXOBJECT_REPLICA_CHECKPOINT_BYTES").is_some() {
        let interval = env_usize("MATRIXOBJECT_REPLICA_CHECKPOINT_BYTES", 0) as u64;
        if let Err(error) = server.enable_journal_checkpoints(interval) {
            eprintln!("invalid journal checkpoint configuration: {error}");
            std::process::exit(2);
        }
    }

    if let Some(raw) = raft_peers {
        #[cfg(not(feature = "chunk-consensus"))]
        {
            let _ = raw;
            eprintln!("Raft configuration requires the chunk-consensus feature");
            std::process::exit(2);
        }
        #[cfg(feature = "chunk-consensus")]
        {
            let configured = (|| -> Result<_, String> {
                let raw = raw.into_string().map_err(|_| "peer list is not UTF-8")?;
                if raw.len() > 4096 {
                    return Err("peer list exceeds size bound".into());
                }
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
                let node_id = std::env::var("MATRIXOBJECT_CHUNK_RAFT_NODE_ID")
                    .map_err(|_| "missing Raft node id")?
                    .parse::<u64>()
                    .map_err(|_| "invalid Raft node id")?;
                let directory = std::env::var_os("MATRIXOBJECT_CHUNK_RAFT_STATE_DIR")
                    .ok_or("missing Raft state directory")?
                    .into();
                Ok(matrixobjectstore_rs::cluster::ChunkRaftOptions {
                    bootstrap_voters: match std::env::var(
                        "MATRIXOBJECT_CHUNK_RAFT_BOOTSTRAP_VOTERS",
                    ) {
                        Ok(raw) => Some(
                            raw.split(',')
                                .map(|id| id.parse::<u64>().map_err(|_| "invalid bootstrap voter"))
                                .collect::<Result<Vec<_>, _>>()?,
                        ),
                        Err(std::env::VarError::NotPresent) => None,
                        Err(_) => return Err("bootstrap voters are not UTF-8".into()),
                    },
                    peer_tls: matrixobjectstore_rs::cluster::PeerTlsOptions::from_env(
                        "MATRIXOBJECT_CHUNK_RAFT",
                    )
                    .map_err(|e| e.to_string())?,
                    checkpoint_after_bytes: env_usize("MATRIXOBJECT_CHUNK_RAFT_CHECKPOINT_BYTES", 0)
                        as u64,
                    node_id,
                    group_id: env_usize("MATRIXOBJECT_CHUNK_RAFT_GROUP_ID", 1) as u64,
                    peers,
                    directory,
                    request_timeout: std::time::Duration::from_millis(env_usize(
                        "MATRIXOBJECT_CHUNK_RAFT_TIMEOUT_MS",
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
            if let Err(error) = server.enable_consensus(options) {
                eprintln!("opening chunk consensus failed: {error}");
                std::process::exit(1);
            }
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

    let heartbeat_names = [
        "MATRIXOBJECT_CHUNK_NODE_ID",
        "MATRIXOBJECT_CHUNK_METADATA_ENDPOINTS",
        "MATRIXOBJECT_CHUNK_ADVERTISE_ADDR",
        "MATRIXOBJECT_CHUNK_HEARTBEAT_MS",
        "MATRIXOBJECT_CHUNK_METADATA_IDENTITY_FILE",
    ];
    if heartbeat_names
        .iter()
        .any(|name| std::env::var_os(name).is_some())
    {
        #[cfg(not(feature = "meta-server"))]
        {
            eprintln!("chunk heartbeats require the meta-server feature");
            std::process::exit(2);
        }
        #[cfg(feature = "meta-server")]
        {
            let configured = (|| -> Result<_, String> {
                let node_id = std::env::var("MATRIXOBJECT_CHUNK_NODE_ID")
                    .map_err(|_| "missing chunk node ID")?;
                let endpoints = std::env::var("MATRIXOBJECT_CHUNK_METADATA_ENDPOINTS")
                    .map_err(|_| "missing metadata endpoints")?;
                if endpoints.len() > 4096 {
                    return Err("metadata endpoint catalog exceeds size bound".into());
                }
                let metadata_endpoints = endpoints
                    .split(',')
                    .map(|s| {
                        s.trim()
                            .parse::<std::net::SocketAddr>()
                            .map_err(|_| "invalid metadata socket address")
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let advertised_endpoint = match std::env::var("MATRIXOBJECT_CHUNK_ADVERTISE_ADDR") {
                    Ok(value) => value
                        .parse()
                        .map_err(|_| "invalid advertised socket address")?,
                    Err(std::env::VarError::NotPresent) => bound,
                    Err(_) => return Err("advertised address is not UTF-8".into()),
                };
                let expected_metadata_identity =
                    match std::env::var("MATRIXOBJECT_CHUNK_METADATA_IDENTITY_FILE") {
                        Ok(path) => {
                            use std::io::Read;
                            if path.trim().is_empty() {
                                return Err("metadata identity path is empty".into());
                            }
                            let file = std::fs::File::open(&path)
                                .map_err(|e| format!("opening metadata identity: {e}"))?;
                            let mut bytes = Vec::new();
                            file.take(4097)
                                .read_to_end(&mut bytes)
                                .map_err(|e| format!("reading metadata identity: {e}"))?;
                            match matrixobjectstore_rs::cluster::MetaReply::decode(
                                matrixobjectstore_rs::cluster::MetaRequestCode::GroupIdentity,
                                &bytes,
                            )
                            .map_err(|e| e.to_string())?
                            {
                                matrixobjectstore_rs::cluster::MetaReply::GroupIdentity(
                                    identity,
                                ) => Some(identity),
                                _ => return Err("invalid metadata identity reply".into()),
                            }
                        }
                        Err(std::env::VarError::NotPresent) => None,
                        Err(_) => return Err("metadata identity path is not UTF-8".into()),
                    };
                Ok(matrixobjectstore_rs::cluster::ReplicaHeartbeatOptions {
                    tls: matrixobjectstore_rs::cluster::ApiTlsConfig::from_env("MATRIXOBJECT_CLIENT").map_err(|error| error.to_string())?,
                    expected_metadata_identity,
                    node_id,
                    advertised_endpoint,
                    metadata_endpoints,
                    interval: std::time::Duration::from_millis(env_usize(
                        "MATRIXOBJECT_CHUNK_HEARTBEAT_MS",
                        1000,
                    ) as u64),
                })
            })();
            let options = match configured {
                Ok(options) => options,
                Err(error) => {
                    eprintln!("invalid chunk heartbeat configuration: {error}");
                    std::process::exit(2);
                }
            };
            if let Err(error) = server.enable_heartbeats(options) {
                eprintln!("invalid chunk heartbeat configuration: {error}");
                std::process::exit(2);
            }
        }
    }

    println!(
        "chunkserver listening on {bound} pid={} protocol_stamp={PROTOCOL_STAMP} \
         threads={worker_threads} max_payload_bytes={max_payload_bytes} auth=none \
         storage={storage} capacity_bytes={capacity_bytes} max_pages={max_pages} maintenance={maintenance}",
        std::process::id()
    );
    if !bound.ip().is_loopback() {
        println!(
            "WARNING: {bound} is not loopback and this replica is unauthenticated, so \
             anything that can reach this port can read and overwrite every page it holds"
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
