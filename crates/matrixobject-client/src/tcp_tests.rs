// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! The TCP transport and the client against peers that are slow, truncated or wrong. A client
//! must come back with an error, not hang, not buffer without bound and not misread a frame.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::time::{Duration, Instant};

use crate::{ClientError, S3Client, TcpOptions, TcpTransport, Transport};

/// Serve each of `replies` to one request on one connection each, in order, then stop. A reply
/// is sent as raw bytes after the request head has been read. With `hold` the connection is then
/// kept open for that long instead of being closed.
fn serve(replies: Vec<Vec<u8>>, hold: Duration) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    std::thread::spawn(move || {
        for reply in replies {
            let Ok((mut stream, _)) = listener.accept() else { return };
            let mut buf = [0u8; 4096];
            let _ = stream.read(&mut buf);
            let _ = stream.write_all(&reply);
            let _ = stream.flush();
            std::thread::sleep(hold);
        }
    });
    addr
}

fn transport(addr: String, options: TcpOptions) -> TcpTransport {
    TcpTransport::with_options(addr, options)
}

fn quick() -> TcpOptions {
    TcpOptions { io_timeout: Duration::from_millis(300), ..TcpOptions::default() }
}

fn send_get(transport: &TcpTransport) -> Result<crate::Reply, crate::TransportError> {
    transport.send("GET", "/b/k", &[], &[])
}

#[test]
fn a_well_formed_reply_is_read_by_its_length() {
    let addr = serve(vec![b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\nx-a: b\r\n\r\nabc".to_vec()], Duration::ZERO);
    let reply = send_get(&transport(addr, quick())).unwrap();
    assert_eq!((reply.status, reply.body.as_slice(), reply.header("X-A")), (200, b"abc".as_slice(), Some("b")));
}

#[test]
fn a_reply_with_no_content_length_is_refused_not_read_to_the_end() {
    let addr = serve(vec![b"HTTP/1.1 200 OK\r\n\r\nabc".to_vec()], Duration::from_secs(2));
    let started = Instant::now();
    let error = send_get(&transport(addr, quick())).unwrap_err();
    assert!(error.0.contains("Content-Length"), "{error}");
    assert!(started.elapsed() < Duration::from_secs(1), "it must not wait for the peer to close");
}

#[test]
fn a_chunked_reply_is_refused() {
    let addr = serve(
        vec![b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Length: 3\r\n\r\n3\r\nabc\r\n0\r\n\r\n".to_vec()],
        Duration::ZERO,
    );
    let error = send_get(&transport(addr, quick())).unwrap_err();
    assert!(error.0.contains("Transfer-Encoding"), "{error}");
}

#[test]
fn conflicting_content_lengths_are_refused() {
    let addr = serve(vec![b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\nContent-Length: 5\r\n\r\nabcde".to_vec()], Duration::ZERO);
    let error = send_get(&transport(addr, quick())).unwrap_err();
    assert!(error.0.contains("conflicting"), "{error}");
}

#[test]
fn a_garbage_status_line_is_refused() {
    for line in ["SMTP/1.1 200 OK", "HTTP/1.1", "HTTP/1.1 abc OK", "HTTP/2 200 OK", ""] {
        let addr = serve(vec![format!("{line}\r\nContent-Length: 0\r\n\r\n").into_bytes()], Duration::ZERO);
        let error = send_get(&transport(addr, quick())).unwrap_err();
        assert!(error.0.contains("status line"), "{line:?}: {error}");
    }
}

#[test]
fn a_truncated_body_is_an_error() {
    let addr = serve(vec![b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nabc".to_vec()], Duration::ZERO);
    let error = send_get(&transport(addr, quick())).unwrap_err();
    assert!(error.0.contains("3 of 10"), "{error}");
}

#[test]
fn a_body_longer_than_the_cap_is_refused_before_it_is_read() {
    let addr = serve(vec![b"HTTP/1.1 200 OK\r\nContent-Length: 4000000000\r\n\r\n".to_vec()], Duration::from_secs(2));
    let options = TcpOptions { max_body_bytes: 1024, ..quick() };
    let started = Instant::now();
    let error = send_get(&transport(addr, options)).unwrap_err();
    assert!(error.0.contains("accepts"), "{error}");
    assert!(started.elapsed() < Duration::from_secs(1));
}

#[test]
fn a_reply_head_longer_than_the_cap_is_refused() {
    let mut reply = b"HTTP/1.1 200 OK\r\n".to_vec();
    for i in 0..4000 {
        reply.extend_from_slice(format!("x-pad-{i}: yyyyyyyyyyyyyyyy\r\n").as_bytes());
    }
    reply.extend_from_slice(b"Content-Length: 0\r\n\r\n");
    let addr = serve(vec![reply], Duration::ZERO);
    let options = TcpOptions { max_head_bytes: 8 * 1024, ..quick() };
    let error = send_get(&transport(addr, options)).unwrap_err();
    assert!(error.0.contains("head is longer"), "{error}");
}

#[test]
fn a_peer_that_never_answers_times_out_and_does_not_hang() {
    let addr = serve(vec![Vec::new()], Duration::from_secs(3));
    let started = Instant::now();
    let error = send_get(&transport(addr, quick())).unwrap_err();
    assert!(!error.0.is_empty());
    assert!(started.elapsed() < Duration::from_secs(2), "the read timeout bounds the wait: {:?}", started.elapsed());
}

#[test]
fn a_reply_that_says_close_is_not_reused() {
    let one = b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\nConnection: close\r\n\r\na".to_vec();
    let addr = serve(vec![one.clone(), one], Duration::ZERO);
    let transport = transport(addr, quick());
    send_get(&transport).unwrap();
    send_get(&transport).unwrap();
    assert_eq!(transport.wire_stats().connections_opened, 2, "each request needed its own socket");
}

#[test]
fn a_head_reply_carries_no_body_whatever_its_length_says() {
    let addr = serve(vec![b"HTTP/1.1 200 OK\r\nContent-Length: 999\r\nx-amz-object-size: 999\r\n\r\n".to_vec()], Duration::from_secs(2));
    let reply = transport(addr, quick()).send("HEAD", "/b/k", &[], &[]).unwrap();
    assert_eq!((reply.status, reply.body.len(), reply.header("x-amz-object-size")), (200, 0, Some("999")));
}

#[test]
fn a_connection_that_cannot_be_made_is_an_error_within_the_connect_timeout() {
    // Nothing listens on a port this process just released.
    let addr = {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().to_string()
    };
    let started = Instant::now();
    assert!(send_get(&transport(addr, quick())).is_err());
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[test]
fn a_key_the_door_cannot_address_is_refused_before_anything_is_sent() {
    // The address is never connected to: refusing the key sends nothing.
    let client = S3Client::new(TcpTransport::new("127.0.0.1:1"), "b");
    for result in [
        client.put("", b"x").map(|_| ()),
        client.get("").map(|_| ()),
        client.delete("").map(|_| ()),
        client.append("", b"x").map(|_| ()),
        client.get_range("", 0, 1).map(|_| ()),
        client.length_or_zero("").map(|_| ()),
    ] {
        assert!(matches!(result, Err(ClientError::InvalidKey(_))), "{result:?}");
    }
    let long = "k".repeat(crate::MAX_KEY_BYTES + 1);
    assert!(matches!(client.get(&long), Err(ClientError::InvalidKey(_))));
    assert_eq!(client.requests_issued(), 0, "nothing was sent");
}

#[test]
fn a_bucket_that_would_change_the_path_is_refused() {
    for bad in ["", "a/b", "a b", "a?b", "a#b", "a%2Fb", "ünï", ".", "..", &"x".repeat(256)] {
        let result = S3Client::try_new(TcpTransport::new("127.0.0.1:1"), bad);
        assert!(matches!(result, Err(ClientError::InvalidBucket(_))), "{bad:?}");
    }
    for good in ["logs", "a-b_c.d", "B2", &"x".repeat(255)] {
        assert!(S3Client::try_new(TcpTransport::new("127.0.0.1:1"), good).is_ok(), "{good:?}");
    }
}
