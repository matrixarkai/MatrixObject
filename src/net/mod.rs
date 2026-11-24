// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! An HTTP front door for the object plane, so it can be reached from another machine.
//!
//! # WHAT IT IS BUILT ON
//!
//! The standard library alone. See [`crate::net::http`] for why that is a deliberate constraint
//! rather than an omission, [`crate::net::s3`] for the wire format, and the cluster gateway module
//! for exactly which verbs and headers are implemented -- including the ones that are not, each of
//! which answers a status rather than a plausible wrong answer.
//!
//! # IT IS UNAUTHENTICATED, AND THE DEFAULT BIND REFLECTS THAT
//!
//! There is no signing, no token and no TLS. Anything that can open a socket to this port can
//! read, overwrite and delete every object in the store. The shipped binary therefore binds
//! loopback unless told otherwise, and anyone who moves it off loopback is choosing to put the
//! whole store behind whatever else protects that network.

pub mod http;
mod in_process_door;
pub mod shard_router;
pub mod s3;
pub mod s3_batch;
pub mod s3_config;
pub mod s3_limits;
pub mod s3_multipart;
pub mod s3_versions;

#[cfg(all(feature = "http-server", feature = "replica-server", feature = "meta-server"))]
pub mod cluster_gateway;
#[cfg(all(feature = "http-server", feature = "replica-server", feature = "meta-server"))]
pub mod chunk_plane;
#[cfg(all(feature = "http-server", feature = "meta-server"))]
pub mod gateway_door;

pub use http::{Request, Response};
pub use in_process_door::{request_from_target, InProcessDoor};

/// Anything that can answer one HTTP request, so the same listener can front the cluster gateway
/// or any other backend.
pub trait RequestHandler: Send + Sync {
    fn handle(&self, request: &Request) -> Response;
}

use std::collections::HashMap;
use std::io::BufReader;
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// How the server is sized and bounded.
#[derive(Debug, Clone)]
pub struct ServerOptions {
    /// Largest request body accepted, which is the largest object this door can write.
    pub max_body_bytes: usize,
    /// Threads blocked in `accept`. Each accepted connection is served on a thread of its own, so
    /// this bounds how fast connections are taken, not how many are served at once: that is
    /// `max_connections`.
    pub worker_threads: usize,
    /// How long a connection may go without sending, before it is dropped. It is what frees the
    /// slot of a client that vanished without closing.
    pub read_timeout: Duration,
    /// How long a write to a client may block.
    pub write_timeout: Duration,
    /// Ignored: accept now blocks, so there is no idle poll. Kept so existing option literals
    /// still compile.
    pub accept_poll_interval: Duration,
    /// Most connections open at once. A connection past it is answered `503` and closed.
    pub max_connections: usize,
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            max_body_bytes: 64 * 1024 * 1024,
            worker_threads: 4,
            read_timeout: Duration::from_secs(30),
            write_timeout: Duration::from_secs(30),
            accept_poll_interval: Duration::from_millis(20),
            max_connections: 1024,
        }
    }
}

/// A bound listener and the backend behind it.
pub struct ObjectHttpServer {
    listener: TcpListener,
    handler: Arc<dyn RequestHandler>,
    bucket_hint: Option<String>,
    options: ServerOptions,
}

/// One open connection, as the shutdown path sees it.
struct Conn {
    stream: TcpStream,
    /// True from the moment a request has been read until its response is written.
    busy: Arc<AtomicBool>,
}

/// What the accept threads and the connection threads share.
struct Shared {
    stop: AtomicBool,
    active: AtomicUsize,
    next_id: AtomicU64,
    conns: Mutex<HashMap<u64, Conn>>,
}

/// A running server: the address it is on, and the flag that stops it.
pub struct ServerHandle {
    /// The address accepted on, resolved -- so a caller that bound port 0 can learn the port.
    pub local_addr: SocketAddr,
    shared: Arc<Shared>,
    threads: Vec<std::thread::JoinHandle<()>>,
}

impl ServerHandle {
    /// Stop accepting, close idle connections, and wait up to a minute for requests in flight.
    pub fn shutdown(self) {
        self.shutdown_within(Duration::from_secs(60));
    }

    /// Stop accepting and close every connection that is between requests at once; wait up to
    /// `timeout` for the ones answering a request, then close those too.
    ///
    /// Returns when the accept threads have ended, so the port is free.
    pub fn shutdown_within(self, timeout: Duration) {
        self.shared.stop.store(true, Ordering::SeqCst);
        // A thread blocked in `accept` notices the flag only when a connection arrives, so one
        // is made for each: a woken thread sees the flag and ends, and cannot take a second.
        for _ in &self.threads {
            let _ = TcpStream::connect_timeout(&wake_addr(self.local_addr), Duration::from_secs(1));
        }
        let deadline = Instant::now() + timeout;
        loop {
            let force = Instant::now() >= deadline;
            {
                let conns = self.shared.conns.lock().unwrap_or_else(|e| e.into_inner());
                for conn in conns.values() {
                    if force || !conn.busy.load(Ordering::SeqCst) {
                        let _ = conn.stream.shutdown(Shutdown::Both);
                    }
                }
            }
            if self.shared.active.load(Ordering::SeqCst) == 0 || force {
                break;
            }
            std::thread::sleep(Duration::from_millis(2));
        }
        for thread in self.threads {
            // A panicking worker has already been reported by the thread itself; joining is to
            // wait for the port to be free, so a failed join is not worth propagating here.
            let _ = thread.join();
        }
    }
}

/// The address to connect to in order to wake an accept thread: a wildcard bind is reached
/// through loopback.
fn wake_addr(bound: SocketAddr) -> SocketAddr {
    let mut addr = bound;
    if addr.ip().is_unspecified() {
        addr.set_ip(if addr.is_ipv4() {
            std::net::Ipv4Addr::LOCALHOST.into()
        } else {
            std::net::Ipv6Addr::LOCALHOST.into()
        });
    }
    addr
}

impl ObjectHttpServer {
    /// Bind `addr` and serve every request with `handler`.
    pub fn bind_handler(
        addr: impl ToSocketAddrs,
        handler: Arc<dyn RequestHandler>,
        options: ServerOptions,
    ) -> std::io::Result<Self> {
        let listener = TcpListener::bind(addr)?;
        Ok(Self {
            listener,
            handler,
            bucket_hint: None,
            options,
        })
    }

    /// Report which bucket this process was configured for, via `/healthz`.
    pub fn with_bucket_hint(mut self, bucket: impl Into<String>) -> Self {
        self.bucket_hint = Some(bucket.into());
        self
    }

    /// The address bound, which is how a caller that asked for port 0 learns the port.
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Serve on this thread until [`ServerHandle::shutdown`] would have been called.
    ///
    /// Returns only if the listener fails; otherwise it runs for as long as the process does.
    pub fn serve(self) -> std::io::Result<()> {
        let local_addr = self.listener.local_addr()?;
        let handle = self.spawn()?;
        // One thread is already accepting inside `spawn`; this one just waits, so a caller that
        // wanted a blocking server gets one without a second accept path to keep in step.
        let _ = local_addr;
        for thread in handle.threads {
            let _ = thread.join();
        }
        Ok(())
    }

    /// Start accepting on background threads and return at once.
    pub fn spawn(self) -> std::io::Result<ServerHandle> {
        let local_addr = self.listener.local_addr()?;
        let service: Arc<dyn RequestHandler> = Arc::clone(&self.handler);
        let shared = Arc::new(Shared {
            stop: AtomicBool::new(false),
            active: AtomicUsize::new(0),
            next_id: AtomicU64::new(0),
            conns: Mutex::new(HashMap::new()),
        });
        let mut threads = Vec::with_capacity(self.options.worker_threads.max(1));
        for index in 0..self.options.worker_threads.max(1) {
            let listener = self.listener.try_clone()?;
            let service = Arc::clone(&service);
            let options = self.options.clone();
            let shared = Arc::clone(&shared);
            let thread = std::thread::Builder::new()
                .name(format!("object-http-{index}"))
                .spawn(move || accept_loop(listener, service, options, shared))?;
            threads.push(thread);
        }
        Ok(ServerHandle {
            local_addr,
            shared,
            threads,
        })
    }
}

fn accept_loop(
    listener: TcpListener,
    service: Arc<dyn RequestHandler>,
    options: ServerOptions,
    shared: Arc<Shared>,
) {
    loop {
        let accepted = listener.accept();
        if shared.stop.load(Ordering::SeqCst) {
            return;
        }
        let stream = match accepted {
            Ok((stream, _peer)) => stream,
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => {
                // A listener that has failed will keep failing (or a descriptor limit is hit);
                // sleeping rather than spinning keeps it from burning a core.
                std::thread::sleep(Duration::from_millis(50));
                continue;
            }
        };
        // THE BOUND IS TAKEN BEFORE THE THREAD IS SPAWNED, so a flood of connections cannot spawn
        // past it while the count catches up.
        if shared.active.fetch_add(1, Ordering::SeqCst) >= options.max_connections {
            shared.active.fetch_sub(1, Ordering::SeqCst);
            refuse(stream);
            continue;
        }
        let _ = stream.set_read_timeout(Some(options.read_timeout));
        let _ = stream.set_write_timeout(Some(options.write_timeout));
        let _ = stream.set_nodelay(true);
        let id = shared.next_id.fetch_add(1, Ordering::Relaxed);
        let busy = Arc::new(AtomicBool::new(false));
        if let Ok(clone) = stream.try_clone() {
            shared.conns.lock().unwrap_or_else(|e| e.into_inner()).insert(
                id,
                Conn {
                    stream: clone,
                    busy: Arc::clone(&busy),
                },
            );
        }
        let service = Arc::clone(&service);
        let conn_options = options.clone();
        let conn_shared = Arc::clone(&shared);
        let spawned = std::thread::Builder::new()
            .name("object-http-conn".to_string())
            .spawn(move || {
                serve_connection(stream, &*service, &conn_options, &busy, &conn_shared.stop);
                conn_shared.conns.lock().unwrap_or_else(|e| e.into_inner()).remove(&id);
                conn_shared.active.fetch_sub(1, Ordering::SeqCst);
            });
        if spawned.is_err() {
            shared.conns.lock().unwrap_or_else(|e| e.into_inner()).remove(&id);
            shared.active.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

/// Tell a connection over the bound so, and close it. Bounded by a short write timeout so a
/// client that does not read cannot hold the accept thread.
fn refuse(mut stream: TcpStream) {
    let _ = stream.set_write_timeout(Some(Duration::from_secs(1)));
    let response = Response::text(503, "too many connections
").with_header("retry-after", "1");
    let _ = http::write_response(&mut stream, &response, false, false);
}

fn serve_connection(
    stream: TcpStream,
    service: &dyn RequestHandler,
    options: &ServerOptions,
    busy: &AtomicBool,
    stop: &AtomicBool,
) {
    let Ok(write_half) = stream.try_clone() else {
        return;
    };
    let mut reader = BufReader::new(stream);
    let mut writer = write_half;
    loop {
        match http::read_request(&mut reader, options.max_body_bytes) {
            Ok(None) => return,
            Ok(Some(request)) => {
                busy.store(true, Ordering::SeqCst);
                let head_only = request.method == "HEAD";
                let keep_alive = request.keep_alive && !stop.load(Ordering::SeqCst);
                let response = service.handle(&request);
                let wrote = http::write_response(&mut writer, &response, keep_alive, head_only);
                busy.store(false, Ordering::SeqCst);
                if wrote.is_err() || !keep_alive {
                    return;
                }
            }
            Err(err) => {
                // THE REASON IS SENT, AND THEN THE CONNECTION CLOSES. A framing error means this
                // reader no longer knows where the next request starts, so continuing to read
                // would parse body bytes as a request line.
                let response = Response::text(err.status, format!("{}\n", err.reason));
                let _ = http::write_response(&mut writer, &response, false, false);
                return;
            }
        }
    }
}
