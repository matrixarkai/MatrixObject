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
//! | `MATRIXOBJECT_METASERVER_THREADS` | `2` | connections served at once |
//! | `MATRIXOBJECT_METASERVER_MAX_PAYLOAD_BYTES` | `8388608` | largest payload one frame may declare |
//! | `MATRIXOBJECT_METASERVER_SEED_DATA_NODES` | `0` | data nodes to put in the roster before serving |
//! | `MATRIXOBJECT_METASERVER_SEED_CAPACITY_BYTES` | `1073741824` | capacity each seeded node reports |
//!
//! The two seed settings are a bootstrap, and named so. Placement needs a roster, and
//! registration is deliberately not one of this service's verbs -- see
//! `crate::cluster::meta_wire` for why a `PlacementNode` is not on this wire. They stand in for
//! the membership a later change will agree between metaserver nodes rather than read from
//! one node's environment.
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
//! # NOTHING SURVIVES THE PROCESS
//!
//! This control plane is held entirely in memory. There is no snapshot path and no write to
//! disk, so a restarted metaserver has an empty roster and no placements. `MatrixMetaService`
//! has a durable form already -- a `MetaSnapshot` of twenty-eight fields with a checksum --
//! and this binary does not write it. **This process is a single point of failure for
//! placement**, and that is stated rather than implied.
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
    use std::io::Write;

    use matrixobjectstore_rs::cluster::{
        MetaNodeOptions, MetaNodeServer, MAX_PAYLOAD_BYTES, PROTOCOL_STAMP,
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
    let max_payload_bytes =
        env_usize("MATRIXOBJECT_METASERVER_MAX_PAYLOAD_BYTES", MAX_PAYLOAD_BYTES);

    let server = match MetaNodeServer::bind(
        &addr,
        MetaNodeOptions {
            max_payload_bytes,
            worker_threads,
            ..MetaNodeOptions::default()
        },
    ) {
        Ok(server) => server,
        Err(err) => {
            eprintln!("binding {addr} failed: {err}");
            std::process::exit(1);
        }
    };

    let seed_nodes = env_usize("MATRIXOBJECT_METASERVER_SEED_DATA_NODES", 0);
    let seed_capacity = env_usize(
        "MATRIXOBJECT_METASERVER_SEED_CAPACITY_BYTES",
        1024 * 1024 * 1024,
    ) as u64;
    // SEEDED BEFORE THE STARTUP LINE IS PRINTED, so that a supervisor reading that line and
    // connecting immediately cannot see a roster that is still filling. The count is printed
    // on the line, so a test does not have to ask whether the seed happened.
    let seeded = match server.seed_data_nodes(seed_nodes, seed_capacity) {
        Ok(seeded) => seeded,
        Err(err) => {
            eprintln!("seeding {seed_nodes} data node(s) failed: {err}");
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
        "metaserver listening on {bound} pid={} protocol_stamp={PROTOCOL_STAMP} \
         threads={worker_threads} max_payload_bytes={max_payload_bytes} auth=none \
         storage=memory roster_nodes={seeded}",
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
