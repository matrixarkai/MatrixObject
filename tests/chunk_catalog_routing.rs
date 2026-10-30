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
    for operation in 0..4 {
        let unavailable = TcpListener::bind("127.0.0.1:0").unwrap();
        let absent = unavailable.local_addr().unwrap().to_string();
        drop(unavailable);
        let (follower, one) = server(Some(encode_reply(&ReplicaReply::NotLeader)));
        let (leader, two) = server(Some(encode_reply(&ReplicaReply::Applied)));
        let mut client =
            RemoteReplicaClient::connect_catalog(&[absent, follower, leader.clone()], &options())
                .unwrap();
        if operation == 0 {
            client.discard_chunk(&page(), 0, 1).unwrap();
        } else if operation == 1 {
            client
                .write_chunk_if_sequence(page(), 0, b"data", 1)
                .unwrap();
        } else if operation == 2 {
            client.discard_chunk_if_sequence(&page(), 0, 1, 1).unwrap();
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
        for operation in 0..4 {
            let (follower, redirect) = server(Some(encode_reply(&ReplicaReply::NotLeader)));
            let (first, handle) = server(response.clone());
            let unused = TcpListener::bind("127.0.0.1:0").unwrap();
            unused.set_nonblocking(true).unwrap();
            let mut client = RemoteReplicaClient::connect_catalog(
                &[follower, first, unused.local_addr().unwrap().to_string()],
                &options(),
            )
            .unwrap();
            let result = if operation == 0 {
                client.discard_chunk(&page(), 0, 1)
            } else if operation == 1 {
                client.write_chunk_if_sequence(page(), 0, b"data", 1)
            } else if operation == 2 {
                client.discard_chunk_if_sequence(&page(), 0, 1, 1)
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

fn identity(node: u64) -> matrixobjectstore_rs::cluster::ChunkGroupIdentity {
    matrixobjectstore_rs::cluster::ChunkGroupIdentity {
        bootstrap_voters: Vec::new(),
        bootstrap_id: 0,
        version: 1,
        group_id: 42,
        node_id: node,
        capacity_bytes: 4096,
        max_pages: 16,
        voters: vec![1, 2],
        peer_endpoints: vec!["127.0.0.1:18001".into(), "127.0.0.1:18002".into()],
    }
}
fn identity_server(
    group: matrixobjectstore_rs::cluster::ChunkGroupIdentity,
    answer: Option<ReplicaReply>,
) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = listener.local_addr().unwrap().to_string();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        assert_eq!(receive(&mut stream), 7);
        stream
            .write_all(&encode_reply(&ReplicaReply::GroupIdentity(group)))
            .unwrap();
        if let Some(answer) = answer {
            assert_eq!(receive(&mut stream), 1);
            stream.write_all(&encode_reply(&answer)).unwrap();
        } else {
            let mut byte = [0];
            assert_eq!(
                stream.read(&mut byte).unwrap(),
                0,
                "data reached a rejected voter"
            );
        }
    });
    (endpoint, handle)
}
#[test]
fn verified_chunk_catalog_checks_identity_before_redirected_write() {
    let (first, one) = identity_server(identity(1), Some(ReplicaReply::NotLeader));
    let (second, two) = identity_server(identity(2), Some(ReplicaReply::Applied));
    let mut client =
        RemoteReplicaClient::connect_raft_catalog(&[first, second.clone()], 42, &options())
            .unwrap();
    client.write_chunk(page(), 0, b"data").unwrap();
    assert_eq!(client.endpoint(), second);
    one.join().unwrap();
    two.join().unwrap();
}
#[test]
fn verified_chunk_catalog_refuses_changed_configuration_before_data() {
    for field in 0..5 {
        let mut changed = identity(2);
        match field {
            0 => changed.group_id = 43,
            1 => changed.capacity_bytes += 1,
            2 => changed.max_pages += 1,
            3 => {
                changed.voters = vec![2, 3];
            }
            _ => changed.peer_endpoints[0] = "127.0.0.1:19001".into(),
        }
        let (first, one) = identity_server(identity(1), Some(ReplicaReply::NotLeader));
        let (second, two) = identity_server(changed, None);
        let mut client =
            RemoteReplicaClient::connect_raft_catalog(&[first, second], 42, &options()).unwrap();
        assert!(client.write_chunk(page(), 0, b"data").is_err());
        one.join().unwrap();
        two.join().unwrap();
    }
}
#[test]
fn verified_chunk_catalog_refuses_wrong_group_and_older_servers() {
    let mut wrong = identity(1);
    wrong.peer_endpoints[0] = "127.0.0.1:19001".into();
    let (endpoint, handle) = identity_server(wrong, None);
    assert!(RemoteReplicaClient::connect_verified_raft_catalog(
        &[endpoint],
        &identity(2),
        &options()
    )
    .is_err());
    handle.join().unwrap();
    let (endpoint, handle) = identity_server(identity(1), None);
    assert!(RemoteReplicaClient::connect_raft_catalog(&[endpoint], 43, &options()).is_err());
    handle.join().unwrap();
    let (endpoint, handle) = server(Some(encode_failure(
        StatusCode::UnknownRequest,
        "unsupported",
    )));
    assert!(RemoteReplicaClient::connect_raft_catalog(&[endpoint], 42, &options()).is_err());
    assert_eq!(handle.join().unwrap(), 7);
    assert!(RemoteReplicaClient::connect_raft_catalog(&[], 0, &options()).is_err());
}
#[test]
fn chunk_group_protobuf_rejects_invalid_or_noncanonical_configuration() {
    use matrixobjectstore_rs::cluster::RequestCode;
    use prost::Message;
    let valid = identity(1);
    let bytes = valid.encode_to_vec();
    assert_eq!(
        ReplicaReply::decode(RequestCode::GroupIdentity, &bytes).unwrap(),
        ReplicaReply::GroupIdentity(valid.clone())
    );
    for field in 0..9 {
        let mut bad = valid.clone();
        match field {
            0 => bad.version = 2,
            1 => bad.group_id = 0,
            2 => bad.node_id = 3,
            3 => bad.voters = vec![1, 1],
            4 => bad.voters = vec![],
            5 => bad.peer_endpoints.pop().map(|_| ()).unwrap(),
            6 => bad.peer_endpoints[0] = "0.0.0.0:1".into(),
            7 => bad.peer_endpoints[0] = bad.peer_endpoints[1].clone(),
            _ => bad.peer_endpoints[0] = "[::ffff:127.0.0.1]:18001".into(),
        }
        assert!(ReplicaReply::decode(RequestCode::GroupIdentity, &bad.encode_to_vec()).is_err());
    }
    let mut unknown = bytes.clone();
    unknown.extend([0x40, 1]);
    for malformed in [
        vec![],
        vec![0; 2049],
        bytes[..bytes.len() - 1].to_vec(),
        unknown,
    ] {
        assert!(ReplicaReply::decode(RequestCode::GroupIdentity, &malformed).is_err());
    }
}

#[test]
fn failed_chunk_identity_exchange_can_try_another_voter_before_writing() {
    for redirected in [false, true] {
        let (failed, failed_worker) = server(None);
        let (healthy, healthy_worker) = identity_server(identity(2), Some(ReplicaReply::Applied));
        let mut endpoints = vec![failed, healthy.clone()];
        let first = if redirected {
            let (endpoint, worker) = identity_server(identity(1), Some(ReplicaReply::NotLeader));
            endpoints.insert(0, endpoint);
            Some(worker)
        } else {
            None
        };
        let mut client = RemoteReplicaClient::connect_verified_raft_catalog(
            &endpoints,
            &identity(1),
            &options(),
        )
        .unwrap();
        client.write_chunk(page(), 0, b"data").unwrap();
        assert_eq!(client.endpoint(), healthy);
        assert_eq!(failed_worker.join().unwrap(), 7);
        healthy_worker.join().unwrap();
        if let Some(worker) = first {
            worker.join().unwrap();
        }
    }
    let mut wrong = identity(1);
    wrong.capacity_bytes += 1;
    for response in [
        encode_reply(&ReplicaReply::GroupIdentity(wrong)),
        encode_failure(StatusCode::UnknownRequest, "unsupported identity"),
    ] {
        let (first, worker) = server(Some(response));
        let unused = TcpListener::bind("127.0.0.1:0").unwrap();
        unused.set_nonblocking(true).unwrap();
        assert!(RemoteReplicaClient::connect_verified_raft_catalog(
            &[first, unused.local_addr().unwrap().to_string()],
            &identity(1),
            &options()
        )
        .is_err());
        worker.join().unwrap();
        assert!(matches!(unused.accept(), Err(e) if e.kind() == std::io::ErrorKind::WouldBlock));
    }
    let (first, one) = server(None);
    let (second, two) = server(None);
    assert!(RemoteReplicaClient::connect_raft_catalog(&[first, second], 42, &options()).is_err());
    assert_eq!(one.join().unwrap(), 7);
    assert_eq!(two.join().unwrap(), 7);
}

#[test]
fn star_set_reconnects_only_for_a_later_operation_after_uncertain_write() {
    use matrixobjectstore_rs::cluster::RemoteReplicaSet;
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = listener.local_addr().unwrap().to_string();
    listener.set_nonblocking(true).unwrap();
    let worker = std::thread::spawn(move || {
        let accept = || {
            let until = std::time::Instant::now() + Duration::from_secs(5);
            loop {
                match listener.accept() {
                    Ok((stream, _)) => {
                        stream
                            .set_read_timeout(Some(Duration::from_secs(5)))
                            .unwrap();
                        stream
                            .set_write_timeout(Some(Duration::from_secs(5)))
                            .unwrap();
                        break stream;
                    }
                    Err(e)
                        if e.kind() == std::io::ErrorKind::WouldBlock
                            && std::time::Instant::now() < until =>
                    {
                        std::thread::sleep(Duration::from_millis(2))
                    }
                    other => panic!("missing connection: {other:?}"),
                }
            }
        };
        let mut first = accept();
        assert_eq!(receive(&mut first), 3);
        first
            .write_all(&encode_failure(StatusCode::NotFound, "absent"))
            .unwrap();
        assert_eq!(receive(&mut first), 8);
        // The write may have committed, but its response is lost.
        drop(first);
        let mut later = accept();
        assert_eq!(receive(&mut later), 5, "the failed mutation was replayed");
        later
            .write_all(&encode_reply(&ReplicaReply::Pong { page_count: 1 }))
            .unwrap();
    });
    let mut set = RemoteReplicaSet::connect(&[endpoint], 1, 1, &options()).unwrap();
    assert!(set.write_chunk(&page(), 0, b"once").is_err());
    assert_eq!(set.ping_all(), vec![Some(1)]);
    worker.join().unwrap();
}
