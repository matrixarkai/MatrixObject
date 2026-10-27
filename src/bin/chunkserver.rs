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
//! | `MATRIXOBJECT_REPLICA_ADDR` | `127.0.0.1:17300` | address to bind; port `0` lets the kernel choose |
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
//! Set `MATRIXOBJECT_REPLICA_JOURNAL` to a file in an existing directory to retain
//! writes and discards across restarts. Each successful mutation is synced before
//! acknowledgment. Without this setting the replica remains in memory only.
//! The journal is exclusively locked, checksummed, and replayed before listening.
//! It grows with writes; checkpointing and log compaction are not implemented.
//! A recovered node still needs coordinated repair for writes missed while offline.
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
    let max_payload_bytes =
        env_usize("MATRIXOBJECT_REPLICA_MAX_PAYLOAD_BYTES", MAX_PAYLOAD_BYTES);

    let capacity_bytes = env_usize("MATRIXOBJECT_REPLICA_CAPACITY_BYTES", 256 * 1024 * 1024) as u64;
    let max_pages = env_usize("MATRIXOBJECT_REPLICA_MAX_PAGES", 65_536);
    let options = ReplicaNodeOptions { max_payload_bytes, worker_threads, capacity_bytes, max_pages, ..Default::default() };
    let journal = std::env::var_os("MATRIXOBJECT_REPLICA_JOURNAL");
    let storage = if journal.is_some() { "journal" } else { "memory" };
    let bound_server = match journal {
        Some(path) => ReplicaNodeServer::bind_with_journal(&addr, options, std::path::Path::new(&path)),
        None => ReplicaNodeServer::bind(&addr, options),
    };
    let server = match bound_server {
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
        "chunkserver listening on {bound} pid={} protocol_stamp={PROTOCOL_STAMP} \
         threads={worker_threads} max_payload_bytes={max_payload_bytes} auth=none \
         storage={storage} capacity_bytes={capacity_bytes} max_pages={max_pages}",
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
