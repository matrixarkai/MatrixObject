// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! A [`Transport`] over HTTP/1.1 keep-alive sockets.

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::transport::{Reply, Transport, TransportError, WireStats};

/// Idle sockets kept per transport. Small on purpose: a client issues requests from one task at a
/// time per object, so a deep pool would hold descriptors open to no benefit.
const MAX_IDLE_CONNECTIONS: usize = 8;

/// Requests to one `host:port`, on pooled keep-alive sockets.
///
/// Cloning shares the pool and the counters.
#[derive(Clone, Debug)]
pub struct TcpTransport {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    addr: String,
    idle: Mutex<Vec<TcpStream>>,
    sent: AtomicU64,
    received: AtomicU64,
    opened: AtomicU64,
}

/// A reply that was read off a socket, with what it cost on the wire.
struct Exchange {
    reply: Reply,
    raw_len: usize,
    request_len: usize,
    reusable: bool,
}

impl TcpTransport {
    pub fn new(addr: impl Into<String>) -> Self {
        TcpTransport {
            inner: Arc::new(Inner {
                addr: addr.into(),
                idle: Mutex::new(Vec::new()),
                sent: AtomicU64::new(0),
                received: AtomicU64::new(0),
                opened: AtomicU64::new(0),
            }),
        }
    }

    /// The `host:port` this transport connects to.
    pub fn addr(&self) -> &str {
        &self.inner.addr
    }

    /// Open a socket and count it, so a handshake cannot happen without being counted.
    fn open(&self) -> Result<TcpStream, TransportError> {
        self.inner.opened.fetch_add(1, Ordering::Relaxed);
        connect(&self.inner.addr)
    }

    fn take(&self) -> Option<TcpStream> {
        self.inner.idle.lock().expect("idle sockets poisoned").pop()
    }

    fn give_back(&self, stream: TcpStream) {
        let mut pool = self.inner.idle.lock().expect("idle sockets poisoned");
        if pool.len() < MAX_IDLE_CONNECTIONS {
            pool.push(stream);
        }
    }

    fn exchange_on_fresh(
        &self,
        method: &str,
        target: &str,
        headers: &[(&str, String)],
        body: &[u8],
    ) -> Result<Exchange, TransportError> {
        let mut fresh = self.open()?;
        let exchange = http_exchange(&mut fresh, &self.inner.addr, method, target, headers, body)?;
        if exchange.reusable {
            self.give_back(fresh);
        }
        Ok(exchange)
    }
}

impl Transport for TcpTransport {
    fn send(
        &self,
        method: &str,
        target: &str,
        headers: &[(&str, String)],
        body: &[u8],
    ) -> Result<Reply, TransportError> {
        // A POOLED SOCKET MAY HAVE DIED WHILE IT SAT IDLE, and nothing tells the holder: a server
        // may close an idle keep-alive connection whenever it likes. So a failure on a REUSED
        // socket is retried once on a fresh one, and only a failure on a connection this call
        // opened is real. Without that distinction a healthy client reports io errors in
        // proportion to how long it was idle.
        let exchange = match self.take() {
            Some(mut pooled) => {
                match http_exchange(&mut pooled, &self.inner.addr, method, target, headers, body) {
                    Ok(exchange) => {
                        if exchange.reusable {
                            self.give_back(pooled);
                        }
                        exchange
                    }
                    Err(_) => self.exchange_on_fresh(method, target, headers, body)?,
                }
            }
            None => self.exchange_on_fresh(method, target, headers, body)?,
        };
        // COUNTED FROM WHAT THE TRANSPORT ACTUALLY MOVED, not from the payload: the head is most
        // of a small request and all of a HEAD reply.
        self.inner.sent.fetch_add(exchange.request_len as u64, Ordering::Relaxed);
        self.inner.received.fetch_add(exchange.raw_len as u64, Ordering::Relaxed);
        Ok(exchange.reply)
    }

    fn wire_stats(&self) -> WireStats {
        WireStats {
            bytes_sent: self.inner.sent.load(Ordering::Relaxed),
            bytes_received: self.inner.received.load(Ordering::Relaxed),
            connections_opened: self.inner.opened.load(Ordering::Relaxed),
        }
    }
}

fn connect(addr: &str) -> Result<TcpStream, TransportError> {
    let socket_addr = addr
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| TransportError(format!("cannot resolve {addr}")))?;
    let stream = TcpStream::connect_timeout(&socket_addr, Duration::from_millis(500))?;
    stream.set_read_timeout(Some(Duration::from_secs(30)))?;
    stream.set_write_timeout(Some(Duration::from_secs(30)))?;
    stream.set_nodelay(true)?;
    Ok(stream)
}

/// One request and its reply on a socket that may be used again.
///
/// # WHY THIS CAN READ THE BODY BY LENGTH AND NOTHING ELSE
///
/// The door frames every reply with `Content-Length` and refuses `Transfer-Encoding` of any kind
/// with a 501, so there is no chunked decoder to get right. Reading to end-of-stream is what is
/// not possible: the socket stays open, so a read-to-end would block until the server gave up.
///
/// A HEAD REPLY CARRIES `Content-Length` AND NO BODY. The method decides, not the header.
fn http_exchange(
    stream: &mut TcpStream,
    addr: &str,
    method: &str,
    target: &str,
    extra: &[(&str, String)],
    body: &[u8],
) -> Result<Exchange, TransportError> {
    let mut head = format!("{method} {target} HTTP/1.1\r\nHost: {addr}\r\n");
    for (name, value) in extra {
        head.push_str(name);
        head.push_str(": ");
        head.push_str(value);
        head.push_str("\r\n");
    }
    head.push_str(&format!("Content-Length: {}\r\n\r\n", body.len()));
    stream.write_all(head.as_bytes())?;
    if !body.is_empty() {
        stream.write_all(body)?;
    }
    stream.flush()?;

    // Read until the head is complete, keeping whatever body bytes arrived with it.
    let marker = b"\r\n\r\n";
    let mut raw: Vec<u8> = Vec::with_capacity(512);
    let mut chunk = [0u8; 4096];
    let header_end = loop {
        if let Some(at) = raw.windows(marker.len()).position(|window| window == marker) {
            break at;
        }
        let read = stream.read(&mut chunk)?;
        if read == 0 {
            return Err(TransportError(
                "the store closed the connection before finishing its response head".to_string(),
            ));
        }
        raw.extend_from_slice(&chunk[..read]);
    };

    let head_text = String::from_utf8_lossy(&raw[..header_end]).into_owned();
    let mut lines = head_text.lines();
    let status = lines
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .ok_or_else(|| TransportError("unparseable status line".to_string()))?;
    let headers: Vec<(String, String)> = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_string(), value.trim().to_string()))
        .collect();
    let header_of = |want: &str| {
        headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(want))
            .map(|(_, value)| value.as_str())
    };

    // A HEAD has no body however long it says it is; 204 and 304 never carry one either.
    let expected = if method.eq_ignore_ascii_case("HEAD") || status == 204 || status == 304 {
        0
    } else {
        header_of("content-length")
            .and_then(|value| value.trim().parse::<usize>().ok())
            .ok_or_else(|| {
                TransportError(format!(
                    "the store gave no Content-Length on a {status} reply, and this client reads \
                     bodies by length because the door frames them that way"
                ))
            })?
    };

    let body_start = header_end + marker.len();
    while raw.len() - body_start < expected {
        let read = stream.read(&mut chunk)?;
        if read == 0 {
            return Err(TransportError(format!(
                "the store closed the connection with {} of {expected} body bytes delivered",
                raw.len() - body_start
            )));
        }
        raw.extend_from_slice(&chunk[..read]);
    }

    // KEEP THE SOCKET ONLY IF THE SERVER SAID SO, and only if the body was read to the byte: a
    // socket with unread bytes on it would hand them to the next request as its reply head.
    let reusable = !header_of("connection").is_some_and(|value| value.eq_ignore_ascii_case("close"))
        && raw.len() - body_start == expected;

    Ok(Exchange {
        reply: Reply { status, body: raw[body_start..body_start + expected].to_vec(), headers },
        raw_len: raw.len(),
        request_len: head.len() + body.len(),
        reusable,
    })
}
