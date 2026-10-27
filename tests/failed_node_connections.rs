// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

#![cfg(all(feature = "replica-server", feature = "meta-server"))]

use matrixobjectstore_rs::cluster::message::{encode_failure, encode_reply, ReplicaReply};
use matrixobjectstore_rs::cluster::meta_message::{encode_meta_reply, MetaReply};
use matrixobjectstore_rs::cluster::{
    MetaClientOptions, RemoteMetaClient, RemoteReplicaClient, ReplicaClientOptions, StatusCode,
    PROTOCOL_STAMP,
};
use matrixobjectstore_rs::ObjectError;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::thread;
use std::time::Duration;

enum Client {
    Replica(RemoteReplicaClient),
    Meta(RemoteMetaClient),
}
impl Client {
    fn connect(endpoint: &str, meta: bool) -> Self {
        if meta {
            Self::Meta(
                RemoteMetaClient::connect(
                    endpoint,
                    &MetaClientOptions {
                        read_timeout: Duration::from_secs(2),
                        max_payload_bytes: 64,
                        ..MetaClientOptions::default()
                    },
                )
                .unwrap(),
            )
        } else {
            Self::Replica(
                RemoteReplicaClient::connect(
                    endpoint,
                    &ReplicaClientOptions {
                        read_timeout: Duration::from_secs(2),
                        max_payload_bytes: 64,
                        ..ReplicaClientOptions::default()
                    },
                )
                .unwrap(),
            )
        }
    }
    fn ping(&mut self) -> Result<(), ObjectError> {
        match self {
            Self::Replica(c) => c.ping().map(|_| ()),
            Self::Meta(c) => c.probe().into_result().map(|_| ()),
        }
    }
}
fn pong(meta: bool) -> Vec<u8> {
    if meta {
        encode_meta_reply(&MetaReply::Pong {
            node_count: 7,
            placement_count: 8,
            commit_index: 9,
        })
    } else {
        encode_reply(&ReplicaReply::Pong { page_count: 7 })
    }
}
fn request(socket: &mut TcpStream) {
    let mut header = [0u8; 6];
    socket.read_exact(&mut header).unwrap();
    assert_eq!(&header[2..], &[0, 0, 0, 0], "ping has no payload");
}
fn damaged_connections_are_closed(meta: bool) {
    let cases = [
        ("unknown stamp", vec![99, 0, 0, 0, 0, 0], false),
        (
            "unknown status",
            vec![PROTOCOL_STAMP, 99, 0, 0, 0, 0],
            false,
        ),
        (
            "oversized payload",
            vec![PROTOCOL_STAMP, 0, 0, 0, 0, 65],
            false,
        ),
        ("invalid reply", vec![PROTOCOL_STAMP, 0, 0, 0, 0, 0], false),
        (
            "invalid refusal",
            vec![PROTOCOL_STAMP, 1, 0, 0, 0, 0],
            false,
        ),
        ("torn header", vec![PROTOCOL_STAMP, 0, 0], true),
        (
            "torn payload",
            vec![PROTOCOL_STAMP, 0, 0, 0, 0, 8, 0, 0],
            true,
        ),
    ];
    for (label, mut bytes, torn) in cases {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        if !torn {
            // A plausible reply left behind must never be attributed to the next request.
            bytes.extend(pong(meta));
        }
        let peer = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            request(&mut socket);
            socket.write_all(&bytes).unwrap();
            if torn {
                socket.shutdown(Shutdown::Write).unwrap();
            }
            let mut next = [0u8; 6];
            match socket.read(&mut next) {
                Ok(0) => true,
                Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => true,
                _ => false,
            }
        });
        let mut client = Client::connect(&endpoint, meta);
        let first = client.ping();
        let second = client.ping();
        drop(client);
        let closed = peer.join().unwrap();
        assert!(first.is_err(), "{label}: first response must fail");
        assert!(second.is_err(), "{label}: old bytes answered a new call");
        assert!(
            closed,
            "{label}: another request reached the damaged connection"
        );
    }
}
fn valid_refusals_keep_the_connection(meta: bool) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = listener.local_addr().unwrap().to_string();
    let peer = thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        request(&mut socket);
        socket
            .write_all(&encode_failure(
                StatusCode::Corruption,
                "stored data is damaged",
            ))
            .unwrap();
        request(&mut socket);
        socket.write_all(&pong(meta)).unwrap();
    });
    let mut client = Client::connect(&endpoint, meta);
    let refusal = client.ping();
    let next = client.ping();
    drop(client);
    peer.join().unwrap();
    assert!(matches!(refusal, Err(ObjectError::Corruption(_))));
    assert!(
        next.is_ok(),
        "a fully decoded refusal must not poison the connection"
    );
}
#[test]
fn replica_connections_close_after_damaged_replies() {
    damaged_connections_are_closed(false);
}
#[test]
fn meta_connections_close_after_damaged_replies() {
    damaged_connections_are_closed(true);
}
#[test]
fn replica_connections_survive_valid_refusals() {
    valid_refusals_keep_the_connection(false);
}
#[test]
fn meta_connections_survive_valid_refusals() {
    valid_refusals_keep_the_connection(true);
}

#[test]
fn replica_response_deadline_covers_the_whole_fragmented_frame() {
    use std::time::Instant;
    for split_header in [true, false] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let peer = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            request(&mut socket);
            let bytes = pong(false);
            let start = if split_header {
                0
            } else {
                socket.write_all(&bytes[..6]).unwrap();
                6
            };
            for byte in &bytes[start..] {
                if socket.write_all(&[*byte]).is_err() {
                    break;
                }
                thread::sleep(Duration::from_millis(25));
            }
            let mut next = [0; 1];
            match socket.read(&mut next) {
                Ok(0) => true,
                Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => true,
                _ => false,
            }
        });
        let mut client = RemoteReplicaClient::connect(
            &endpoint,
            &ReplicaClientOptions {
                read_timeout: Duration::from_millis(80),
                ..Default::default()
            },
        )
        .unwrap();
        let started = Instant::now();
        let first = client.ping();
        let elapsed = started.elapsed();
        let second = client.ping();
        drop(client);
        let closed = peer.join().unwrap();
        assert!(
            first.is_err(),
            "fragmented frame escaped its total deadline"
        );
        assert!(elapsed < Duration::from_millis(600));
        assert!(second.is_err());
        assert!(closed, "a second request reached the expired connection");
    }
}
#[test]
fn a_timed_out_replica_write_cannot_supply_a_later_acknowledgment() {
    use matrixobjectstore_rs::{ChunkId, PageId};
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = listener.local_addr().unwrap().to_string();
    let peer = thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut header = [0; 6];
        socket.read_exact(&mut header).unwrap();
        let length = u32::from_be_bytes(header[2..].try_into().unwrap()) as usize;
        assert!(length < 1024);
        let mut payload = vec![0; length];
        socket.read_exact(&mut payload).unwrap();
        let reply = encode_reply(&ReplicaReply::Applied);
        // No header byte arrives before the first call times out. A later complete
        // acknowledgment must not be mistaken for the next write's acknowledgment.
        thread::sleep(Duration::from_millis(120));
        let _ = socket.write_all(&reply);
        let _ = socket.write_all(&reply);
        let mut next = [0; 1];
        match socket.read(&mut next) {
            Ok(0) => true,
            Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => true,
            _ => false,
        }
    });
    let mut client = RemoteReplicaClient::connect(
        &endpoint,
        &ReplicaClientOptions {
            read_timeout: Duration::from_millis(80),
            ..Default::default()
        },
    )
    .unwrap();
    let page = PageId {
        chunk_id: ChunkId::new(1, 1).unwrap(),
        page_index: 0,
    };
    assert!(client.write_chunk(page.clone(), 0, b"first").is_err());
    let later = client.write_chunk(page, 0, b"second");
    drop(client);
    let closed = peer.join().unwrap();
    assert!(
        later.is_err(),
        "an old acknowledgment answered the next write"
    );
    assert!(closed, "uncertain write reused its connection");
}
