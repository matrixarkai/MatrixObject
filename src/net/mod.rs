// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! An HTTP front door for the object store, so it can be reached from another machine.
//!
//! # WHY THIS EXISTS
//!
//! Every way into this store was an in-process call, so a store was only ever as shared as the
//! process holding it. A caller on another host had nothing to connect to, and a caller on the
//! same host in a different process had nothing either. This module is the smallest honest answer
//! to that: a listener, a request reader, and a mapping onto the store's own object API.
//!
//! # WHAT IT IS BUILT ON
//!
//! The standard library alone. See [`http`] for why that is a deliberate constraint rather than
//! an omission, and [`service`] for exactly which verbs and headers are implemented -- including
//! the ones that are not, each of which answers a status rather than a plausible wrong answer.
//!
//! # IT IS UNAUTHENTICATED, AND THE DEFAULT BIND REFLECTS THAT
//!
//! There is no signing, no token and no TLS. Anything that can open a socket to this port can
//! read, overwrite and delete every object in the store. The shipped binary therefore binds
//! loopback unless told otherwise, and anyone who moves it off loopback is choosing to put the
//! whole store behind whatever else protects that network.

pub mod http;
pub mod service;

pub use http::{Request, Response};
pub use service::ObjectService;

use std::io::BufReader;
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::MatrixObjectStore;

/// How the server is sized and bounded.
#[derive(Debug, Clone)]
pub struct ServerOptions {
    /// Largest request body accepted, which is the largest object this door can write.
    pub max_body_bytes: usize,
    /// Threads accepting connections.
    ///
    /// More than a couple buys little: the store is behind one lock (see [`ObjectService`]), so
    /// extra threads wait on it rather than working. What they do buy is that a slow client
    /// reading its response cannot stall every other request.
    pub worker_threads: usize,
    /// How long a connection may go without sending, before it is dropped.
    ///
    /// Set, not optional, because a fixed pool of accept threads and no read timeout is a
    /// denial-of-service by one idle connection per thread.
    pub read_timeout: Duration,
    /// How long a write to a client may block.
    pub write_timeout: Duration,
    /// How long an accept loop sleeps between polls when idle.
    pub accept_poll_interval: Duration,
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            max_body_bytes: 64 * 1024 * 1024,
            worker_threads: 4,
            read_timeout: Duration::from_secs(30),
            write_timeout: Duration::from_secs(30),
            accept_poll_interval: Duration::from_millis(20),
        }
    }
}

/// A bound listener and the store behind it.
pub struct ObjectHttpServer {
    listener: TcpListener,
    store: Arc<Mutex<MatrixObjectStore>>,
    bucket_hint: Option<String>,
    options: ServerOptions,
    stop: Arc<AtomicBool>,
}

/// A running server: the address it is on, and the flag that stops it.
pub struct ServerHandle {
    /// The address accepted on, resolved -- so a caller that bound port 0 can learn the port.
    pub local_addr: SocketAddr,
    stop: Arc<AtomicBool>,
    threads: Vec<std::thread::JoinHandle<()>>,
}

impl ServerHandle {
    /// Ask the accept loops to stop, and wait for them.
    ///
    /// The loops notice between polls, so this returns within roughly one
    /// `accept_poll_interval` plus however long an in-flight request takes.
    pub fn shutdown(self) {
        self.stop.store(true, Ordering::SeqCst);
        for thread in self.threads {
            // A panicking worker has already been reported by the thread itself; joining is to
            // wait for the port to be free, so a failed join is not worth propagating here.
            let _ = thread.join();
        }
    }
}

impl ObjectHttpServer {
    /// Bind `addr` and serve `store`.
    pub fn bind(
        addr: impl ToSocketAddrs,
        store: Arc<Mutex<MatrixObjectStore>>,
        options: ServerOptions,
    ) -> std::io::Result<Self> {
        let listener = TcpListener::bind(addr)?;
        // NONBLOCKING, SO THE LOOP CAN BE STOPPED. `accept` has no timeout in the standard
        // library, so a blocking listener gives a server that can only be ended by ending the
        // process -- which makes it untestable in-process and unusable as a component. The cost
        // is a poll every `accept_poll_interval` while idle, which is the right trade for a door
        // whose work is bounded by a single store lock anyway.
        listener.set_nonblocking(true)?;
        Ok(Self {
            listener,
            store,
            bucket_hint: None,
            options,
            stop: Arc::new(AtomicBool::new(false)),
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
        // BUILT ONCE, HERE, so the bucket hint is in place before any thread can answer. Building
        // it in `bind` and then rebuilding it in `with_bucket_hint` would have meant unwrapping an
        // `Arc` that the threads may already hold.
        let mut service = ObjectService::new(Arc::clone(&self.store));
        if let Some(bucket) = self.bucket_hint.clone() {
            service = service.with_bucket_hint(bucket);
        }
        let service = Arc::new(service);
        let mut threads = Vec::with_capacity(self.options.worker_threads.max(1));
        for index in 0..self.options.worker_threads.max(1) {
            let listener = self.listener.try_clone()?;
            let service = Arc::clone(&service);
            let options = self.options.clone();
            let stop = Arc::clone(&self.stop);
            let thread = std::thread::Builder::new()
                .name(format!("object-http-{index}"))
                .spawn(move || accept_loop(listener, service, options, stop))?;
            threads.push(thread);
        }
        Ok(ServerHandle {
            local_addr,
            stop: self.stop,
            threads,
        })
    }
}

fn accept_loop(
    listener: TcpListener,
    service: Arc<ObjectService>,
    options: ServerOptions,
    stop: Arc<AtomicBool>,
) {
    while !stop.load(Ordering::SeqCst) {
        match listener.accept() {
            Ok((stream, _peer)) => {
                // BACK TO BLOCKING FOR THE CONNECTION. The listener is nonblocking so the loop can
                // be stopped; a nonblocking *stream* would make every read return `WouldBlock` and
                // turn the request reader into a spin. The timeouts below are what bound a
                // connection instead.
                if stream.set_nonblocking(false).is_err() {
                    continue;
                }
                let _ = stream.set_read_timeout(Some(options.read_timeout));
                let _ = stream.set_write_timeout(Some(options.write_timeout));
                serve_connection(stream, &service, &options);
            }
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(options.accept_poll_interval);
            }
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => {}
            Err(_) => {
                // A listener that has stopped accepting will keep doing so; sleeping rather than
                // spinning keeps a broken socket from burning a core.
                std::thread::sleep(options.accept_poll_interval);
            }
        }
    }
}

fn serve_connection(stream: TcpStream, service: &ObjectService, options: &ServerOptions) {
    let Ok(write_half) = stream.try_clone() else {
        return;
    };
    let mut reader = BufReader::new(stream);
    let mut writer = write_half;
    loop {
        match http::read_request(&mut reader, options.max_body_bytes) {
            Ok(None) => return,
            Ok(Some(request)) => {
                let head_only = request.method == "HEAD";
                let keep_alive = request.keep_alive;
                let response = service.handle(&request);
                if http::write_response(&mut writer, &response, keep_alive, head_only).is_err() {
                    return;
                }
                if !keep_alive {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    fn store_with_bucket(bucket: &str) -> Arc<Mutex<MatrixObjectStore>> {
        let mut store = MatrixObjectStore::new(crate::StoreOptions::default())
            .expect("a default store is constructible");
        store.create_bucket(bucket).expect("the bucket is created");
        Arc::new(Mutex::new(store))
    }

    fn serve() -> (ServerHandle, String) {
        let store = store_with_bucket("b");
        let server = ObjectHttpServer::bind(
            "127.0.0.1:0",
            store,
            ServerOptions {
                worker_threads: 2,
                read_timeout: Duration::from_secs(5),
                write_timeout: Duration::from_secs(5),
                accept_poll_interval: Duration::from_millis(5),
                ..ServerOptions::default()
            },
        )
        .expect("binding loopback on an ephemeral port");
        let handle = server.spawn().expect("the accept threads start");
        let addr = handle.local_addr.to_string();
        (handle, addr)
    }

    /// Send one raw request and read the whole response.
    ///
    /// Raw rather than through a client, because what is being tested includes the framing: a
    /// client library would paper over exactly the mistakes this server has to get right.
    fn roundtrip(addr: &str, request: &[u8]) -> String {
        let mut stream = TcpStream::connect(addr).expect("connecting to the test server");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("setting a read timeout");
        stream.write_all(request).expect("writing the request");
        stream.flush().expect("flushing the request");
        let mut out = Vec::new();
        // The server answers `connection: close` for these, so read-to-end terminates.
        let _ = stream.read_to_end(&mut out);
        String::from_utf8_lossy(&out).into_owned()
    }

    fn request(method: &str, target: &str, extra: &str, body: &[u8]) -> Vec<u8> {
        let mut out = format!(
            "{method} {target} HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n{extra}\
             content-length: {}\r\n\r\n",
            body.len()
        )
        .into_bytes();
        out.extend_from_slice(body);
        out
    }

    fn status_of(response: &str) -> u16 {
        response
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|code| code.parse().ok())
            .unwrap_or(0)
    }

    fn body_of(response: &str) -> &str {
        response.split_once("\r\n\r\n").map_or("", |(_, body)| body)
    }

    #[test]
    fn an_object_written_over_http_reads_back_over_http() {
        let (handle, addr) = serve();
        let put = roundtrip(&addr, &request("PUT", "/b/k1", "", b"hello-over-http"));
        assert_eq!(status_of(&put), 200, "PUT answered: {put}");
        assert!(
            put.to_ascii_lowercase().contains("etag:"),
            "a write must report the object's etag, which is how a conditional write addresses \
             it next time; got: {put}"
        );

        let get = roundtrip(&addr, &request("GET", "/b/k1", "", b""));
        assert_eq!(status_of(&get), 200, "GET answered: {get}");
        assert_eq!(
            body_of(&get),
            "hello-over-http",
            "the bytes that came back are not the bytes written"
        );
        handle.shutdown();
    }

    #[test]
    fn a_head_reports_the_length_without_the_body() {
        let (handle, addr) = serve();
        let _ = roundtrip(&addr, &request("PUT", "/b/k2", "", b"0123456789"));
        let head = roundtrip(&addr, &request("HEAD", "/b/k2", "", b""));
        assert_eq!(status_of(&head), 200, "HEAD answered: {head}");
        assert!(
            head.to_ascii_lowercase().contains("content-length: 10"),
            "a HEAD has to report the length the matching GET would send, so a client can size a \
             read before making it; got: {head}"
        );
        assert_eq!(
            body_of(&head),
            "",
            "a HEAD answered with a body, which means the writer is suppressing the wrong thing"
        );
        handle.shutdown();
    }

    #[test]
    fn a_range_request_gets_those_bytes_and_says_which_they_were() {
        let (handle, addr) = serve();
        let _ = roundtrip(&addr, &request("PUT", "/b/k3", "", b"abcdefghij"));
        let ranged = roundtrip(&addr, &request("GET", "/b/k3", "range: bytes=2-5\r\n", b""));
        assert_eq!(status_of(&ranged), 206, "range GET answered: {ranged}");
        assert_eq!(body_of(&ranged), "cdef", "the wrong bytes came back");
        assert!(
            ranged.to_ascii_lowercase().contains("content-range: bytes 2-5/10"),
            "a 206 that does not say which bytes it is, of how many, cannot be assembled by a \
             client; got: {ranged}"
        );

        let past_end = roundtrip(&addr, &request("GET", "/b/k3", "range: bytes=99-\r\n", b""));
        assert_eq!(
            status_of(&past_end),
            416,
            "a range entirely past the end must be refused, not silently clamped: {past_end}"
        );
        handle.shutdown();
    }

    #[test]
    fn a_conditional_write_is_refused_by_the_store_rather_than_by_this_server() {
        let (handle, addr) = serve();
        let first = roundtrip(
            &addr,
            &request("PUT", "/b/k4", "if-none-match: *\r\n", b"first"),
        );
        assert_eq!(status_of(&first), 200, "the first write answered: {first}");
        let second = roundtrip(
            &addr,
            &request("PUT", "/b/k4", "if-none-match: *\r\n", b"second"),
        );
        assert_eq!(
            status_of(&second),
            412,
            "a second if-none-match write must fail its precondition: {second}"
        );
        let get = roundtrip(&addr, &request("GET", "/b/k4", "", b""));
        assert_eq!(
            body_of(&get),
            "first",
            "the refused write changed the object anyway, so the condition was checked after the \
             mutation rather than inside it"
        );
        handle.shutdown();
    }

    #[test]
    fn a_conditional_read_answers_not_modified_without_the_payload() {
        let (handle, addr) = serve();
        let put = roundtrip(&addr, &request("PUT", "/b/k5", "", b"payload"));
        let etag = put
            .lines()
            .find_map(|line| {
                let lowered = line.to_ascii_lowercase();
                lowered
                    .starts_with("etag:")
                    .then(|| line.split_once(':').map(|(_, value)| value.trim().to_string()))
                    .flatten()
            })
            .expect("the write reported an etag");
        let conditional = roundtrip(
            &addr,
            &request("GET", "/b/k5", &format!("if-none-match: {etag}\r\n"), b""),
        );
        assert_eq!(
            status_of(&conditional),
            304,
            "a matching if-none-match must answer 304: {conditional}"
        );
        assert_eq!(
            body_of(&conditional),
            "",
            "a 304 carried a body, which no client will read"
        );
        handle.shutdown();
    }

    #[test]
    fn a_delete_is_idempotent_and_a_missing_object_is_not_found() {
        let (handle, addr) = serve();
        let _ = roundtrip(&addr, &request("PUT", "/b/k6", "", b"x"));
        assert_eq!(status_of(&roundtrip(&addr, &request("DELETE", "/b/k6", "", b""))), 204);
        assert_eq!(
            status_of(&roundtrip(&addr, &request("DELETE", "/b/k6", "", b""))),
            204,
            "a repeated delete must not report failure: a client retrying one it already \
             completed wants to learn the object is gone"
        );
        assert_eq!(
            status_of(&roundtrip(&addr, &request("GET", "/b/k6", "", b""))),
            404,
            "the object is still readable after being deleted"
        );
        handle.shutdown();
    }

    #[test]
    fn a_framing_this_server_does_not_implement_is_refused_rather_than_guessed_at() {
        let (handle, addr) = serve();
        // No content-length, and a chunked body. Read as `content-length: 0` this would store an
        // empty object and answer success, which is the failure this refusal exists to prevent.
        let chunked = roundtrip(
            &addr,
            b"PUT /b/k7 HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\
              transfer-encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n",
        );
        assert_eq!(
            status_of(&chunked),
            501,
            "chunked framing must be refused: {chunked}"
        );
        assert_eq!(
            status_of(&roundtrip(&addr, &request("GET", "/b/k7", "", b""))),
            404,
            "the refused chunked write stored an object anyway"
        );

        // A body-carrying method with no length at all.
        let no_length = roundtrip(
            &addr,
            b"PUT /b/k8 HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n",
        );
        assert_eq!(
            status_of(&no_length),
            411,
            "a PUT with no content-length must be refused rather than read as empty: {no_length}"
        );
        handle.shutdown();
    }

    #[test]
    fn a_bucket_listing_names_its_keys_and_says_when_there_are_more() {
        let (handle, addr) = serve();
        for key in ["a/1", "a/2", "b/1"] {
            let _ = roundtrip(&addr, &request("PUT", &format!("/b/{key}"), "", b"v"));
        }
        let listed = roundtrip(&addr, &request("GET", "/b?prefix=a/", "", b""));
        assert_eq!(status_of(&listed), 200, "listing answered: {listed}");
        let keys: Vec<&str> = body_of(&listed).lines().collect();
        assert_eq!(keys, vec!["a/1", "a/2"], "the prefix did not narrow the listing");

        let page = roundtrip(&addr, &request("GET", "/b?prefix=a/&max-keys=1", "", b""));
        assert_eq!(body_of(&page).lines().count(), 1, "max-keys was not applied");
        assert!(
            page.to_ascii_lowercase().contains("x-matrixobject-truncated: true"),
            "a full page must say whether there is more, because a client cannot tell a complete \
             page from a truncated one by counting; got: {page}"
        );

        let bad = roundtrip(&addr, &request("GET", "/b?max-keys=lots", "", b""));
        assert_eq!(
            status_of(&bad),
            400,
            "an unparseable max-keys must be refused, not silently defaulted -- a caller that \
             asked for a page size and got another would page wrongly and never know: {bad}"
        );
        handle.shutdown();
    }

    #[test]
    fn two_requests_share_one_connection_when_it_is_kept_alive() {
        let (handle, addr) = serve();
        let mut stream = TcpStream::connect(&addr).expect("connecting");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("read timeout");
        // Both requests written up front, so the server has to find the second one's request line
        // exactly where the first one's body ended.
        let mut out = Vec::new();
        out.extend_from_slice(
            b"PUT /b/ka HTTP/1.1\r\nhost: l\r\ncontent-length: 3\r\n\r\nabc",
        );
        out.extend_from_slice(
            b"GET /b/ka HTTP/1.1\r\nhost: l\r\nconnection: close\r\ncontent-length: 0\r\n\r\n",
        );
        stream.write_all(&out).expect("writing both requests");
        stream.flush().expect("flush");
        let mut response = Vec::new();
        let _ = stream.read_to_end(&mut response);
        let text = String::from_utf8_lossy(&response);
        assert_eq!(
            text.matches("HTTP/1.1").count(),
            2,
            "two requests on one connection produced {} responses: {text}",
            text.matches("HTTP/1.1").count()
        );
        assert!(
            text.ends_with("abc"),
            "the second response did not return the object the first one wrote: {text}"
        );
        handle.shutdown();
    }

    #[test]
    fn the_http_date_is_the_date_http_asks_for() {
        // Fixed points rather than a round-trip through the same arithmetic, which would agree
        // with itself whatever it computed.
        assert_eq!(
            http::http_date(0),
            "Thu, 01 Jan 1970 00:00:00 GMT",
            "the epoch itself is wrong, so the civil-date arithmetic is wrong everywhere"
        );
        assert_eq!(
            http::http_date(1_000_000_000_000),
            "Sun, 09 Sep 2001 01:46:40 GMT"
        );
        // BOTH SIDES OF A LEAP DAY, which is where a shift-to-March algorithm earns its keep.
        //
        // The first constant here was wrong when this test was written -- 1_583_020_800_000 was
        // asserted to be 29 February and is in fact 1 March, because 2020-02-29 00:00 UTC is
        // 1_582_934_400. The implementation said so and the test was corrected, which is the
        // reason these are fixed points rather than a round-trip through the same arithmetic: a
        // round-trip agrees with whatever it computes, and could not have told which of the two
        // was wrong.
        assert_eq!(
            http::http_date(1_582_934_400_000),
            "Sat, 29 Feb 2020 00:00:00 GMT"
        );
        assert_eq!(
            http::http_date(1_583_020_800_000),
            "Sun, 01 Mar 2020 00:00:00 GMT"
        );
    }

    #[test]
    fn a_range_header_this_server_cannot_answer_falls_back_to_the_whole_object() {
        use http::ResolvedRange;
        // A multi-range request wants a multipart body this server does not build. Serving the
        // FIRST range under a 206 would be read as the whole of what was asked for, so the
        // fallback is the whole object, which is a legal answer to any range request.
        assert_eq!(
            http::resolve_range(Some("bytes=0-1,4-5"), 10),
            ResolvedRange::Whole
        );
        assert_eq!(http::resolve_range(Some("items=0-1"), 10), ResolvedRange::Whole);
        assert_eq!(http::resolve_range(None, 10), ResolvedRange::Whole);
        // Inclusive end, clamped to the object.
        assert_eq!(
            http::resolve_range(Some("bytes=8-99"), 10),
            ResolvedRange::Partial { offset: 8, length: 2 }
        );
        // Suffix form.
        assert_eq!(
            http::resolve_range(Some("bytes=-3"), 10),
            ResolvedRange::Partial { offset: 7, length: 3 }
        );
        assert_eq!(
            http::resolve_range(Some("bytes=10-"), 10),
            ResolvedRange::Unsatisfiable
        );
    }
}
