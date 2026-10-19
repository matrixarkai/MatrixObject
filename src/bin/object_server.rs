// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Serve one object store over HTTP.
//!
//! # RUNNING IT
//!
//! ```text
//! cargo run --features http-server --bin object_server
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
//! | `MATRIXOBJECT_MAX_BODY_BYTES` | `67108864` | largest object this door accepts |
//! | `MATRIXOBJECT_THREADS` | `4` | accept threads |
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
        "this binary needs the `http-server` feature: \
         cargo run --features http-server --bin object_server"
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

    let options = StoreOptions {
        persistent_snapshot_path: snapshot_path.clone().unwrap_or_default(),
        persistent_snapshot_flush_on_commit: snapshot_path.is_some(),
        ..StoreOptions::default()
    };

    let mut store = match MatrixObjectStore::new(options) {
        Ok(store) => store,
        Err(err) => {
            eprintln!("the store could not be opened: {err}");
            std::process::exit(1);
        }
    };
    // IDEMPOTENT, so restarting against an existing snapshot is not an error.
    if let Err(err) = store.create_bucket(&bucket) {
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
