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
//! The standard library alone. See [`crate::net::http`] for why that is a deliberate constraint
//! rather than an omission, [`crate::net::s3`] for the wire format, and [`crate::net::service`] for
//! exactly which verbs and headers are implemented -- including
//! the ones that are not, each of which answers a status rather than a plausible wrong answer.
//!
//! # IT IS UNAUTHENTICATED, AND THE DEFAULT BIND REFLECTS THAT
//!
//! There is no signing, no token and no TLS. Anything that can open a socket to this port can
//! read, overwrite and delete every object in the store. The shipped binary therefore binds
//! loopback unless told otherwise, and anyone who moves it off loopback is choosing to put the
//! whole store behind whatever else protects that network.

pub mod http;
pub mod s3;
pub mod s3_multipart;
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
    fn a_bucket_listing_is_the_xml_an_s3_client_parses() {
        let (handle, addr) = serve();
        for key in ["a/1", "a/2", "b/1"] {
            let _ = roundtrip(&addr, &request("PUT", &format!("/b/{key}"), "", b"v"));
        }
        let listed = roundtrip(&addr, &request("GET", "/b?prefix=a/", "", b""));
        assert_eq!(status_of(&listed), 200, "listing answered: {listed}");
        assert!(
            listed
                .to_ascii_lowercase()
                .contains("content-type: application/xml"),
            "a listing must be announced as XML, or a client will not try to parse it: {listed}"
        );
        let body = body_of(&listed);
        assert!(
            body.contains("<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"),
            "the root element and its namespace are what a client matches on: {body}"
        );
        assert!(body.contains("<Name>b</Name>"), "{body}");
        assert!(body.contains("<Prefix>a/</Prefix>"), "{body}");
        assert!(body.contains("<Key>a/1</Key>"), "{body}");
        assert!(body.contains("<Key>a/2</Key>"), "{body}");
        assert!(
            !body.contains("<Key>b/1</Key>"),
            "the prefix did not narrow the listing: {body}"
        );
        assert!(
            body.contains("<KeyCount>2</KeyCount>") && body.contains("<IsTruncated>false</IsTruncated>"),
            "{body}"
        );
        assert!(
            body.contains("<StorageClass>STANDARD</StorageClass>"),
            "an empty storage class reads as neither of the answers a client knows: {body}"
        );
        // THE QUOTES ARE PART OF AN ETAG AND IN XML THEY ARE ESCAPED. A client strips `&quot;`
        // and compares; a raw `"` here would end the attribute-free text early in a strict parser.
        assert!(
            body.contains("<ETag>&quot;fnv1a64:"),
            "the etag must be quoted and the quotes escaped: {body}"
        );
        // ISO 8601 WITH MILLIS, NOT THE HTTP DATE. The two live in different places and a client
        // parses each with its own format; swapping them is a listing a client cannot read.
        let last_modified = body
            .split("<LastModified>")
            .nth(1)
            .and_then(|rest| rest.split("</LastModified>").next())
            .expect("a LastModified in the listing");
        assert!(
            Regex::matches_iso8601(last_modified),
            "a listing dates an object in ISO 8601 with millis and a literal Z, not in the HTTP \
             date format `Last-Modified` uses; got {last_modified}"
        );

        let bad = roundtrip(&addr, &request("GET", "/b?max-keys=lots", "", b""));
        assert_eq!(
            status_of(&bad),
            400,
            "an unparseable max-keys must be refused, not silently defaulted -- a caller that \
             asked for a page size and got another would page wrongly and never know: {bad}"
        );
        assert!(
            body_of(&bad).contains("<Code>InvalidArgument</Code>"),
            "the code is what a client branches on, not the status alone: {bad}"
        );
        handle.shutdown();
    }

    #[test]
    fn a_truncated_listing_hands_back_a_token_that_resumes_it() {
        let (handle, addr) = serve();
        for key in ["k1", "k2", "k3"] {
            let _ = roundtrip(&addr, &request("PUT", &format!("/b/{key}"), "", b"v"));
        }
        let first = roundtrip(&addr, &request("GET", "/b?max-keys=2", "", b""));
        let body = body_of(&first).to_string();
        assert!(body.contains("<IsTruncated>true</IsTruncated>"), "{body}");
        let token = body
            .split("<NextContinuationToken>")
            .nth(1)
            .and_then(|rest| rest.split("</NextContinuationToken>").next())
            .expect("a full page must hand back a token: a client cannot tell a complete page \
                     from a truncated one by counting")
            .to_string();
        assert_eq!(token, "k2", "the token resumes AFTER the last key answered: {body}");

        let second = roundtrip(
            &addr,
            &request("GET", &format!("/b?max-keys=2&continuation-token={token}"), "", b""),
        );
        let body = body_of(&second);
        assert!(body.contains("<Key>k3</Key>"), "the token did not resume: {body}");
        assert!(
            !body.contains("<Key>k1</Key>") && !body.contains("<Key>k2</Key>"),
            "a resumed page re-answered keys the first page already gave: {body}"
        );
        assert!(
            body.contains("<IsTruncated>false</IsTruncated>")
                && !body.contains("<NextContinuationToken>"),
            "a final page must not carry a token: a client that trusts the token over \
             IsTruncated would ask again for nothing, forever: {body}"
        );
        handle.shutdown();
    }

    #[test]
    fn a_delimiter_rolls_the_keys_under_it_into_one_prefix() {
        let (handle, addr) = serve();
        for key in ["d/1", "d/2", "d/3", "top"] {
            let _ = roundtrip(&addr, &request("PUT", &format!("/b/{key}"), "", b"v"));
        }
        let listed = roundtrip(&addr, &request("GET", "/b?delimiter=/", "", b""));
        let body = body_of(&listed);
        assert!(
            body.contains("<CommonPrefixes><Prefix>d/</Prefix></CommonPrefixes>"),
            "three keys under one delimiter must roll up to one prefix, which is what lets a \
             client walk a tree without listing every leaf: {body}"
        );
        assert_eq!(
            body.matches("<CommonPrefixes>").count(),
            1,
            "a prefix is reported once however many keys sit under it: {body}"
        );
        assert!(body.contains("<Key>top</Key>"), "{body}");
        assert!(
            !body.contains("<Key>d/1</Key>"),
            "a rolled-up key must not also be answered as a result: {body}"
        );
        // KeyCount COUNTS THE PREFIXES TOO. A listing fully rolled up answers prefixes and no
        // keys at all, and a client paging on the entry count alone would stop early.
        assert!(
            body.contains("<KeyCount>2</KeyCount>"),
            "KeyCount is the entries plus the common prefixes: {body}"
        );
        assert!(body.contains("<Delimiter>/</Delimiter>"), "{body}");
        handle.shutdown();
    }

    #[test]
    fn every_failure_is_an_error_document_with_the_code_a_client_branches_on() {
        let (handle, addr) = serve();
        let _ = roundtrip(&addr, &request("PUT", "/b/present", "", b"v"));

        // A MISSING KEY AND A MISSING BUCKET ARE DIFFERENT ANSWERS. Both are 404 and the store
        // reports both as NotFound, but one tells a client to create the bucket and retry and the
        // other tells it to stop. Reported the same way they would be indistinguishable.
        let missing_key = roundtrip(&addr, &request("GET", "/b/absent", "", b""));
        assert_eq!(status_of(&missing_key), 404);
        assert!(
            body_of(&missing_key).contains("<Code>NoSuchKey</Code>"),
            "{missing_key}"
        );
        assert!(
            body_of(&missing_key).contains("<Resource>/b/absent</Resource>"),
            "the resource is what a client logs: {missing_key}"
        );

        let missing_bucket = roundtrip(&addr, &request("GET", "/nope", "", b""));
        assert_eq!(status_of(&missing_bucket), 404, "{missing_bucket}");
        assert!(
            body_of(&missing_bucket).contains("<Code>NoSuchBucket</Code>"),
            "a bucket that was never created must not answer an empty listing: a client told its \
             bucket is empty keeps writing into a name that will never hold anything: \
             {missing_bucket}"
        );

        // PATCH, not POST: POST on an object is CreateMultipartUpload or
        // CompleteMultipartUpload depending on its sub-resource, so it is a method this surface
        // allows and a bare one is a different complaint -- asserted just below.
        let wrong_method = roundtrip(&addr, &request("PATCH", "/b/present", "", b""));
        assert_eq!(status_of(&wrong_method), 405, "{wrong_method}");
        assert!(
            body_of(&wrong_method).contains("<Code>MethodNotAllowed</Code>"),
            "{wrong_method}"
        );
        assert!(
            wrong_method.to_ascii_lowercase().contains("allow: "),
            "the Allow header is what an HTTP client reads and the body is what an S3 client \
             reads; answering only one leaves the other guessing: {wrong_method}"
        );
        assert!(
            wrong_method.to_ascii_lowercase().contains("post"),
            "the Allow header must name POST now that the multipart verbs use it: {wrong_method}"
        );

        // A POST naming NEITHER sub-resource is a malformed request, not a disallowed method, and
        // the two send a client to different places: one fixes its URL, the other stops using the
        // verb.
        let bare_post = roundtrip(&addr, &request("POST", "/b/present", "", b""));
        assert_eq!(status_of(&bare_post), 400, "{bare_post}");
        assert!(
            body_of(&bare_post).contains("<Code>InvalidArgument</Code>"),
            "{bare_post}"
        );

        let bad_range = roundtrip(
            &addr,
            &request("GET", "/b/present", "range: bytes=99-200\r\n", b""),
        );
        assert_eq!(status_of(&bad_range), 416, "{bad_range}");
        assert!(
            body_of(&bad_range).contains("<Code>InvalidRange</Code>"),
            "{bad_range}"
        );
        assert!(
            bad_range.to_ascii_lowercase().contains("content-range: bytes */1"),
            "the size a client should have asked within must still be reported: {bad_range}"
        );

        let refused = roundtrip(
            &addr,
            &request("PUT", "/b/present", "if-none-match: *\r\n", b"again"),
        );
        assert_eq!(status_of(&refused), 412, "{refused}");
        assert!(
            body_of(&refused).contains("<Code>PreconditionFailed</Code>"),
            "{refused}"
        );
        handle.shutdown();
    }

    #[test]
    fn a_key_holding_xml_syntax_is_escaped_rather_than_breaking_the_document() {
        let (handle, addr) = serve();
        // An ampersand alone is enough to make a listing unparseable, and object keys are
        // caller-supplied, so this is not a hypothetical input.
        let key = "a&b<c>d";
        let put = roundtrip(&addr, &request("PUT", &format!("/b/{key}"), "", b"v"));
        assert_eq!(status_of(&put), 200, "the key was refused: {put}");
        let listed = roundtrip(&addr, &request("GET", "/b", "", b""));
        let body = body_of(&listed);
        assert!(
            body.contains("<Key>a&amp;b&lt;c&gt;d</Key>"),
            "every one of the five XML metacharacters must be escaped in a key: {body}"
        );
        assert!(
            !body.contains("<Key>a&b"),
            "a raw ampersand in a key makes the whole listing unparseable: {body}"
        );
        handle.shutdown();
    }

    #[test]
    fn the_service_root_lists_the_buckets() {
        let (handle, addr) = serve();
        let listed = roundtrip(&addr, &request("GET", "/", "", b""));
        assert_eq!(status_of(&listed), 200, "{listed}");
        let body = body_of(&listed);
        assert!(
            body.contains("<ListAllMyBucketsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"),
            "{body}"
        );
        assert!(body.contains("<Bucket><Name>b</Name></Bucket>"), "{body}");
        // NO CreationDate. This store records no creation time for a bucket, and a fabricated one
        // would be an answer it never knew; the element is omitted instead.
        assert!(!body.contains("<CreationDate>"), "{body}");

        let wrong_method = roundtrip(&addr, &request("DELETE", "/", "", b""));
        assert_eq!(status_of(&wrong_method), 405, "{wrong_method}");
        handle.shutdown();
    }

    #[test]
    fn a_bucket_can_be_created_checked_and_deleted_over_http() {
        let (handle, addr) = serve();

        let absent = roundtrip(&addr, &request("HEAD", "/fresh", "", b""));
        assert_eq!(
            status_of(&absent),
            404,
            "an existence check on a bucket that is not there must not answer 200: {absent}"
        );

        let created = roundtrip(&addr, &request("PUT", "/fresh", "", b""));
        assert_eq!(status_of(&created), 200, "{created}");
        assert!(
            created.to_ascii_lowercase().contains("location: /fresh"),
            "CreateBucket answers a Location and no body: {created}"
        );
        assert_eq!(
            status_of(&roundtrip(&addr, &request("PUT", "/fresh", "", b""))),
            200,
            "a repeated create must succeed: a client that cannot tell whether its first PUT \
             landed will send it again, and a 409 would make the retry look like a failure"
        );
        assert_eq!(status_of(&roundtrip(&addr, &request("HEAD", "/fresh", "", b""))), 200);

        let _ = roundtrip(&addr, &request("PUT", "/fresh/held", "", b"v"));
        let not_empty = roundtrip(&addr, &request("DELETE", "/fresh", "", b""));
        assert_eq!(status_of(&not_empty), 409, "{not_empty}");
        assert!(
            body_of(&not_empty).contains("<Code>BucketNotEmpty</Code>"),
            "the store refuses a non-empty bucket in the same vocabulary it uses for a malformed \
             name, so without asking first both would be reported as 400 and a client could not \
             tell it needs to empty the bucket: {not_empty}"
        );

        assert_eq!(status_of(&roundtrip(&addr, &request("DELETE", "/fresh/held", "", b""))), 204);
        let deleted = roundtrip(&addr, &request("DELETE", "/fresh", "", b""));
        assert_eq!(status_of(&deleted), 204, "{deleted}");
        assert_eq!(
            status_of(&roundtrip(&addr, &request("HEAD", "/fresh", "", b""))),
            404,
            "the bucket is still there after being deleted"
        );
        handle.shutdown();
    }

    #[test]
    fn the_readiness_route_stays_plain_text_because_it_is_not_an_s3_route() {
        let (handle, addr) = serve();
        let health = roundtrip(&addr, &request("GET", "/healthz", "", b""));
        assert_eq!(status_of(&health), 200, "{health}");
        assert!(
            !body_of(&health).contains("<?xml"),
            "a container probe and an operator read this, not an S3 client; dressing a readiness \
             answer in XML would help neither: {health}"
        );
        assert!(body_of(&health).starts_with("ok bucket="), "{health}");
        handle.shutdown();
    }

    /// Whether a listing timestamp is ISO 8601 with milliseconds and a literal Z.
    ///
    /// Spelled out rather than pattern-matched with a dependency: the point is the SHAPE a client
    /// parses, and a hand-written check names which character is wrong when it is.
    struct Regex;

    impl Regex {
        fn matches_iso8601(text: &str) -> bool {
            let bytes = text.as_bytes();
            if bytes.len() != 24 {
                return false;
            }
            let digit = |at: usize| bytes[at].is_ascii_digit();
            (0..4).all(digit)
                && bytes[4] == b'-'
                && (5..7).all(digit)
                && bytes[7] == b'-'
                && (8..10).all(digit)
                && bytes[10] == b'T'
                && (11..13).all(digit)
                && bytes[13] == b':'
                && (14..16).all(digit)
                && bytes[16] == b':'
                && (17..19).all(digit)
                && bytes[19] == b'.'
                && (20..23).all(digit)
                && bytes[23] == b'Z'
        }
    }

    #[test]
    fn a_multipart_upload_runs_end_to_end_over_http() {
        let (handle, addr) = serve();

        // CreateMultipartUpload: POST ?uploads.
        let created = roundtrip(
            &addr,
            &request(
                "POST",
                "/b/big?uploads",
                "content-type: text/plain\r\nx-amz-meta-origin: the-create\r\n",
                b"",
            ),
        );
        assert_eq!(status_of(&created), 200, "{created}");
        let body = body_of(&created).to_string();
        assert!(
            body.contains("<InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"),
            "{body}"
        );
        assert!(body.contains("<Bucket>b</Bucket>") && body.contains("<Key>big</Key>"), "{body}");
        let upload_id = body
            .split("<UploadId>")
            .nth(1)
            .and_then(|rest| rest.split("</UploadId>").next())
            .expect("an upload id")
            .to_string();
        assert!(!upload_id.is_empty(), "{body}");

        // UploadPart: PUT ?partNumber=N&uploadId=X, answering an ETag and no body.
        let mut etags: Vec<String> = Vec::new();
        for (number, payload) in [(1u32, vec![b'a'; 64]), (2, vec![b'b'; 32])] {
            let put = roundtrip(
                &addr,
                &request(
                    "PUT",
                    &format!("/b/big?partNumber={number}&uploadId={upload_id}"),
                    "",
                    &payload,
                ),
            );
            assert_eq!(status_of(&put), 200, "part {number}: {put}");
            assert!(
                body_of(&put).is_empty(),
                "UploadPart answers no body, and a client reads the header: {put}"
            );
            let etag = put
                .split("etag: ")
                .nth(1)
                .and_then(|rest| rest.split("\r\n").next())
                .expect("an etag header -- without it a client cannot complete the upload")
                .trim()
                .trim_matches('"')
                .to_string();
            assert!(!etag.is_empty(), "{put}");
            etags.push(etag);
        }

        // ListParts: GET ?uploadId=X.
        let listed = roundtrip(&addr, &request("GET", &format!("/b/big?uploadId={upload_id}"), "", b""));
        assert_eq!(status_of(&listed), 200, "{listed}");
        let body = body_of(&listed);
        assert!(body.contains("<ListPartsResult"), "{body}");
        assert!(body.contains("<PartNumber>1</PartNumber>"), "{body}");
        assert!(body.contains("<PartNumber>2</PartNumber>"), "{body}");
        assert!(body.contains("<Size>64</Size>") && body.contains("<Size>32</Size>"), "{body}");
        assert!(body.contains("<IsTruncated>false</IsTruncated>"), "{body}");

        // ListMultipartUploads: GET /{bucket}?uploads.
        let uploads = roundtrip(&addr, &request("GET", "/b?uploads", "", b""));
        assert_eq!(status_of(&uploads), 200, "{uploads}");
        assert!(body_of(&uploads).contains("<ListMultipartUploadsResult"), "{uploads}");
        assert!(
            body_of(&uploads).contains(&format!("<UploadId>{upload_id}</UploadId>")),
            "the upload in flight was not listed: {uploads}"
        );

        // CompleteMultipartUpload: POST ?uploadId=X with the part list.
        let complete_body = format!(
            "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>&quot;{}&quot;</ETag>\
             </Part><Part><PartNumber>2</PartNumber><ETag>&quot;{}&quot;</ETag></Part>\
             </CompleteMultipartUpload>",
            etags[0], etags[1]
        );
        let completed = roundtrip(
            &addr,
            &request(
                "POST",
                &format!("/b/big?uploadId={upload_id}"),
                "content-type: application/xml\r\n",
                complete_body.as_bytes(),
            ),
        );
        assert_eq!(status_of(&completed), 200, "{completed}");
        let body = body_of(&completed);
        assert!(body.contains("<CompleteMultipartUploadResult"), "{body}");
        assert!(body.contains("<Location>/b/big</Location>"), "{body}");
        assert!(body.contains("<ETag>&quot;fnv1a64:"), "{body}");

        // AND THE OBJECT IS THERE, which is the only assertion that proves the upload did
        // anything: every step above could pass against a surface that assembled nothing.
        let read = roundtrip(&addr, &request("GET", "/b/big", "", b""));
        assert_eq!(status_of(&read), 200, "{read}");
        // THE METADATA WAS TAKEN AT THE CREATE, which is where S3 takes it -- a client sends
        // `x-amz-meta-*` with the create and not with the parts, so a surface reading it anywhere
        // else drops it silently and the object comes back without it.
        assert!(
            read.to_ascii_lowercase().contains("x-amz-meta-origin: the-create"),
            "the metadata given at CreateMultipartUpload did not reach the assembled object: \
             {read}"
        );
        assert!(
            read.to_ascii_lowercase().contains("content-type: text/plain"),
            "the content type given at the create did not reach the object: {read}"
        );
        assert_eq!(
            body_of(&read).len(),
            96,
            "the assembled object is not the two parts: {}",
            body_of(&read).len()
        );
        assert!(body_of(&read).starts_with(&"a".repeat(64)), "the parts are out of order");
        assert!(body_of(&read).ends_with(&"b".repeat(32)), "the parts are out of order");

        // The upload is gone from the listing now that it completed.
        let after = roundtrip(&addr, &request("GET", "/b?uploads", "", b""));
        assert!(
            !body_of(&after).contains(&format!("<UploadId>{upload_id}</UploadId>")),
            "a completed upload is still listed as in flight: {after}"
        );
        handle.shutdown();
    }

    #[test]
    fn a_part_list_that_does_not_match_what_was_uploaded_is_refused() {
        let (handle, addr) = serve();
        let created = roundtrip(&addr, &request("POST", "/b/mismatch?uploads", "", b""));
        let upload_id = body_of(&created)
            .split("<UploadId>")
            .nth(1)
            .and_then(|rest| rest.split("</UploadId>").next())
            .expect("an upload id")
            .to_string();
        let put = roundtrip(
            &addr,
            &request("PUT", &format!("/b/mismatch?partNumber=1&uploadId={upload_id}"), "", b"xyz"),
        );
        let etag = put
            .split("etag: ")
            .nth(1)
            .and_then(|rest| rest.split("\r\n").next())
            .unwrap()
            .trim()
            .trim_matches('"')
            .to_string();

        // THE STORE WOULD HAVE COMPLETED EVERY ONE OF THESE. `complete_multipart_upload` takes no
        // part list and completes whatever it holds, so a mismatch not caught at this surface is
        // not caught at all -- the object is assembled and the client told it succeeded.
        let cases: Vec<(&str, String, &str)> = vec![
            (
                "a part that was never uploaded",
                format!(
                    "<CompleteMultipartUpload><Part><PartNumber>7</PartNumber>\
                     <ETag>&quot;{etag}&quot;</ETag></Part></CompleteMultipartUpload>"
                ),
                "InvalidPart",
            ),
            (
                "a part with the wrong etag",
                "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber>\
                 <ETag>&quot;fnv1a64:0000000000000000&quot;</ETag></Part>\
                 </CompleteMultipartUpload>"
                    .to_string(),
                "InvalidPart",
            ),
            (
                "parts that do not ascend",
                format!(
                    "<CompleteMultipartUpload><Part><PartNumber>2</PartNumber>\
                     <ETag>&quot;{etag}&quot;</ETag></Part><Part><PartNumber>1</PartNumber>\
                     <ETag>&quot;{etag}&quot;</ETag></Part></CompleteMultipartUpload>"
                ),
                "InvalidPartOrder",
            ),
            (
                "an empty part list",
                "<CompleteMultipartUpload></CompleteMultipartUpload>".to_string(),
                "MalformedXML",
            ),
            (
                "a body that is not the document at all",
                "nothing like xml".to_string(),
                "MalformedXML",
            ),
            (
                // THE ROOT ELEMENT IS CHECKED, and this is the case that shows why. The previous
                // entry is refused for carrying no parts at all, so it passes whether or not the
                // root is checked. This one carries a perfectly well formed part list under a
                // root this endpoint never asked for -- without the check it would be accepted
                // as a CompleteMultipartUpload the client did not send.
                "a part list under the wrong root element",
                format!(
                    "<PutSomethingElse><Part><PartNumber>1</PartNumber>\
                     <ETag>&quot;{etag}&quot;</ETag></Part></PutSomethingElse>"
                ),
                "MalformedXML",
            ),
            (
                // Part numbers start at 1. Refused by the PARSER, which is what makes the answer
                // MalformedXML: a zero is a malformed document, where an unknown part number is
                // an InvalidPart, and a client reading the wrong one of those looks in the wrong
                // place.
                "part number zero",
                format!(
                    "<CompleteMultipartUpload><Part><PartNumber>0</PartNumber>\
                     <ETag>&quot;{etag}&quot;</ETag></Part></CompleteMultipartUpload>"
                ),
                "MalformedXML",
            ),
            (
                "a document type declaration",
                "<!DOCTYPE x><CompleteMultipartUpload><Part><PartNumber>1</PartNumber>\
                 <ETag>&quot;x&quot;</ETag></Part></CompleteMultipartUpload>"
                    .to_string(),
                "MalformedXML",
            ),
        ];
        for (name, body, code) in cases {
            let answered = roundtrip(
                &addr,
                &request(
                    "POST",
                    &format!("/b/mismatch?uploadId={upload_id}"),
                    "",
                    body.as_bytes(),
                ),
            );
            assert_eq!(status_of(&answered), 400, "{name}: {answered}");
            assert!(
                body_of(&answered).contains(&format!("<Code>{code}</Code>")),
                "{name} must answer {code}: {answered}"
            );
        }

        // And the object was not created by any of them.
        assert_eq!(
            status_of(&roundtrip(&addr, &request("GET", "/b/mismatch", "", b""))),
            404,
            "a refused completion assembled the object anyway"
        );
        handle.shutdown();
    }

    #[test]
    fn an_unknown_upload_id_is_no_such_upload_and_not_no_such_key() {
        let (handle, addr) = serve();
        // A THIRD CODE, AND IT EARNS ITS KEEP. The store reports a missing upload the same way it
        // reports a missing object. A client that reads NoSuchKey goes looking for an object that
        // was never supposed to exist yet; one that reads NoSuchUpload knows its upload id is
        // wrong or the upload was aborted.
        for (method, target, body) in [
            ("PUT", "/b/k?partNumber=1&uploadId=nope", &b"x"[..]),
            ("GET", "/b/k?uploadId=nope", b""),
            ("DELETE", "/b/k?uploadId=nope", b""),
        ] {
            let answered = roundtrip(&addr, &request(method, target, "", body));
            assert_eq!(status_of(&answered), 404, "{method} {target}: {answered}");
            assert!(
                body_of(&answered).contains("<Code>NoSuchUpload</Code>"),
                "{method} {target}: {answered}"
            );
        }

        let completed = roundtrip(
            &addr,
            &request(
                "POST",
                "/b/k?uploadId=nope",
                "",
                b"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>&quot;x&quot;</ETag></Part></CompleteMultipartUpload>",
            ),
        );
        assert_eq!(status_of(&completed), 404, "{completed}");
        assert!(body_of(&completed).contains("<Code>NoSuchUpload</Code>"), "{completed}");
        handle.shutdown();
    }

    #[test]
    fn an_upload_can_be_aborted_and_then_it_is_gone() {
        let (handle, addr) = serve();
        let created = roundtrip(&addr, &request("POST", "/b/dropped?uploads", "", b""));
        let upload_id = body_of(&created)
            .split("<UploadId>")
            .nth(1)
            .and_then(|rest| rest.split("</UploadId>").next())
            .expect("an upload id")
            .to_string();
        let _ = roundtrip(
            &addr,
            &request("PUT", &format!("/b/dropped?partNumber=1&uploadId={upload_id}"), "", b"abc"),
        );

        let aborted = roundtrip(
            &addr,
            &request("DELETE", &format!("/b/dropped?uploadId={upload_id}"), "", b""),
        );
        assert_eq!(status_of(&aborted), 204, "{aborted}");

        // NOT IDEMPOTENT, unlike a DELETE of an object, and that is S3's behaviour: an abort that
        // reported success for an upload id the server never had would hide a client sending the
        // wrong id.
        let again = roundtrip(
            &addr,
            &request("DELETE", &format!("/b/dropped?uploadId={upload_id}"), "", b""),
        );
        assert_eq!(status_of(&again), 404, "{again}");
        assert!(body_of(&again).contains("<Code>NoSuchUpload</Code>"), "{again}");
        assert_eq!(
            status_of(&roundtrip(&addr, &request("GET", "/b/dropped", "", b""))),
            404,
            "an aborted upload left an object behind"
        );
        handle.shutdown();
    }

    #[test]
    fn a_part_number_that_is_not_a_positive_number_is_refused() {
        let (handle, addr) = serve();
        let created = roundtrip(&addr, &request("POST", "/b/nums?uploads", "", b""));
        let upload_id = body_of(&created)
            .split("<UploadId>")
            .nth(1)
            .and_then(|rest| rest.split("</UploadId>").next())
            .expect("an upload id")
            .to_string();
        // REFUSED, NOT COERCED. A part stored under a number the client did not ask for is a part
        // the client will not name when it completes, and the upload then fails with a message
        // about the wrong thing.
        for raw in ["0", "-1", "lots", ""] {
            let answered = roundtrip(
                &addr,
                &request(
                    "PUT",
                    &format!("/b/nums?partNumber={raw}&uploadId={upload_id}"),
                    "",
                    b"x",
                ),
            );
            assert_eq!(status_of(&answered), 400, "partNumber={raw:?}: {answered}");
            assert!(
                body_of(&answered).contains("<Code>InvalidArgument</Code>"),
                "partNumber={raw:?}: {answered}"
            );
        }
        // A partNumber with no uploadId names a part of nothing.
        let orphan = roundtrip(&addr, &request("PUT", "/b/nums?partNumber=1", "", b"x"));
        assert_eq!(status_of(&orphan), 400, "{orphan}");
        handle.shutdown();
    }

    #[test]
    fn an_escaped_ampersand_in_a_part_etag_survives_the_round_trip() {
        // The unescape has to replace `&amp;` LAST. Replacing it first turns `&amp;quot;` -- an
        // escaped literal `&quot;` -- into `&quot;` and then into a quote character, inventing a
        // quote the client never sent. Checked directly, because no etag this store produces
        // contains an ampersand and the path would otherwise never be exercised.
        assert_eq!(
            super::s3_multipart::xml_unescape("&amp;quot;x&amp;quot;"),
            "&quot;x&quot;",
            "an escaped literal entity was decoded twice"
        );
        assert_eq!(
            super::s3_multipart::xml_unescape("&quot;a&amp;b&quot;"),
            "\"a&b\"",
            "the five escapes must round-trip"
        );
        assert_eq!(
            super::s3_multipart::xml_unescape("&lt;&gt;&apos;"),
            "<>'"
        );
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
