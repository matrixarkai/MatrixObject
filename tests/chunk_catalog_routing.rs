// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#![cfg(feature = "replica-server")]
use matrixobjectstore_rs::cluster::{
    message::{encode_failure, encode_reply},
    RemoteReplicaClient, ReplicaClientOptions, ReplicaReply, StatusCode, PROTOCOL_STAMP,
};
use matrixobjectstore_rs::{ChunkId, ObjectError, PageId};
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    thread::{self, JoinHandle},
    time::Duration,
};
fn options() -> ReplicaClientOptions {
    ReplicaClientOptions {
        connect_timeout: Duration::from_millis(200),
        read_timeout: Duration::from_millis(200),
        write_timeout: Duration::from_millis(200),
        ..Default::default()
    }
}
fn page() -> PageId {
    PageId {
        chunk_id: ChunkId::new(1, 1).unwrap(),
        page_index: 0,
    }
}
fn receive(stream: &mut TcpStream) -> u8 {
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let mut header = [0; 6];
    stream.read_exact(&mut header).unwrap();
    let len = u32::from_be_bytes(header[2..].try_into().unwrap()) as usize;
    assert!(len < 1024);
    let mut payload = vec![0; len];
    stream.read_exact(&mut payload).unwrap();
    header[1]
}
fn server(response: Option<Vec<u8>>) -> (String, JoinHandle<u8>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = listener.local_addr().unwrap().to_string();
    let handle = thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        let code = receive(&mut socket);
        if let Some(bytes) = response {
            socket.write_all(&bytes).unwrap();
        }
        code
    });
    (endpoint, handle)
}
#[test]
fn chunk_catalog_routes_only_typed_unadmitted_mutations() {
    for discard in [false, true] {
        let unavailable = TcpListener::bind("127.0.0.1:0").unwrap();
        let absent = unavailable.local_addr().unwrap().to_string();
        drop(unavailable);
        let (follower, one) = server(Some(encode_reply(&ReplicaReply::NotLeader)));
        let (leader, two) = server(Some(encode_reply(&ReplicaReply::Applied)));
        let mut client =
            RemoteReplicaClient::connect_catalog(&[absent, follower, leader.clone()], &options())
                .unwrap();
        if discard {
            client.discard_chunk(&page(), 0, 1).unwrap();
        } else {
            client.write_chunk(page(), 0, b"data").unwrap();
        }
        assert_eq!(client.endpoint(), leader);
        assert_eq!(one.join().unwrap(), two.join().unwrap());
    }
}
#[test]
fn chunk_catalog_never_replays_uncertain_or_refused_mutations() {
    let mut malformed = vec![PROTOCOL_STAMP, StatusCode::NotLeader as u8, 0, 0, 0, 1, 0];
    malformed.extend(encode_reply(&ReplicaReply::Applied));
    for response in [
        None,
        Some(encode_failure(
            StatusCode::InvalidArgument,
            "leadership changed; outcome may be committed",
        )),
        Some(encode_failure(
            StatusCode::PreconditionFailed,
            "chunk request was not admitted by a leader",
        )),
        Some(malformed),
    ] {
        for discard in [false, true] {
            let (follower, redirect) = server(Some(encode_reply(&ReplicaReply::NotLeader)));
            let (first, handle) = server(response.clone());
            let unused = TcpListener::bind("127.0.0.1:0").unwrap();
            unused.set_nonblocking(true).unwrap();
            let mut client = RemoteReplicaClient::connect_catalog(
                &[follower, first, unused.local_addr().unwrap().to_string()],
                &options(),
            )
            .unwrap();
            let result = if discard {
                client.discard_chunk(&page(), 0, 1)
            } else {
                client.write_chunk(page(), 0, b"data")
            };
            assert!(result.is_err());
            assert_eq!(redirect.join().unwrap(), handle.join().unwrap());
            assert!(
                matches!(unused.accept(),Err(e) if e.kind()==std::io::ErrorKind::WouldBlock),
                "mutation was replayed after uncertainty or an ordinary refusal"
            );
        }
    }
}
#[test]
fn chunk_reads_fail_over_and_each_catalog_pass_is_bounded() {
    let (first, one) = server(None);
    let (second, two) = server(Some(encode_reply(&ReplicaReply::Pong { page_count: 7 })));
    let mut client =
        RemoteReplicaClient::connect_catalog(&[first, second.clone()], &options()).unwrap();
    assert_eq!(client.ping().unwrap(), 7);
    assert_eq!(client.endpoint(), second);
    one.join().unwrap();
    two.join().unwrap();
    let (first, one) = server(Some(encode_reply(&ReplicaReply::NotLeader)));
    let (second, two) = server(Some(encode_reply(&ReplicaReply::NotLeader)));
    let mut client = RemoteReplicaClient::connect_catalog(&[first, second], &options()).unwrap();
    assert!(client.ping().is_err());
    one.join().unwrap();
    two.join().unwrap();
}
#[test]
fn malformed_chunk_routing_response_cannot_answer_a_later_call() {
    let mut bytes = vec![PROTOCOL_STAMP, StatusCode::NotLeader as u8, 0, 0, 0, 1, 0];
    bytes.extend(encode_reply(&ReplicaReply::Pong { page_count: 7 }));
    let (endpoint, handle) = server(Some(bytes));
    let mut client = RemoteReplicaClient::connect(&endpoint, &options()).unwrap();
    assert!(client.ping().is_err());
    assert!(client.ping().is_err());
    handle.join().unwrap();
}
#[test]
fn chunk_catalog_validates_all_endpoints_before_connecting() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap();
    let valid = address.to_string();
    let mapped = format!("[::ffff:127.0.0.1]:{}", address.port());
    for endpoints in [
        vec![],
        vec![valid.clone(); 8],
        vec![valid.clone(), valid.clone()],
        vec![valid.clone(), mapped],
        vec![valid.clone(), "0.0.0.0:1".into()],
        vec![valid.clone(), "224.0.0.1:1".into()],
        vec![valid.clone(), "[::ffff:224.0.0.1]:1".into()],
        vec![valid.clone(), "127.0.0.1:0".into()],
        vec![valid.clone(), "localhost:1".into()],
        vec![valid.clone(), "x".repeat(129)],
    ] {
        assert!(matches!(
            RemoteReplicaClient::connect_catalog(&endpoints, &options()),
            Err(ObjectError::InvalidArgument(_))
        ));
        assert!(matches!(listener.accept(),Err(e) if e.kind()==std::io::ErrorKind::WouldBlock));
    }
    listener.set_nonblocking(false).unwrap();
    let handle = thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        receive(&mut socket);
        socket
            .write_all(&encode_reply(&ReplicaReply::Pong { page_count: 1 }))
            .unwrap();
    });
    let mut client = RemoteReplicaClient::connect_catalog(
        &[format!("[::ffff:127.0.0.1]:{}", address.port())],
        &options(),
    )
    .unwrap();
    assert_eq!(client.endpoint(), valid);
    assert_eq!(client.ping().unwrap(), 1);
    handle.join().unwrap();
}
#[test]
fn a_valid_chunk_not_leader_reply_preserves_the_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = listener.local_addr().unwrap().to_string();
    let handle = thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        receive(&mut socket);
        socket
            .write_all(&encode_reply(&ReplicaReply::NotLeader))
            .unwrap();
        receive(&mut socket);
        socket
            .write_all(&encode_reply(&ReplicaReply::Pong { page_count: 9 }))
            .unwrap();
    });
    let mut client = RemoteReplicaClient::connect(&endpoint, &options()).unwrap();
    assert!(matches!(
        client.ping(),
        Err(ObjectError::PreconditionFailed(_))
    ));
    assert_eq!(client.ping().unwrap(), 9);
    handle.join().unwrap();
}
