// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#![cfg(feature = "meta-server")]
use matrixobjectstore_rs::cluster::{message::encode_failure, meta_message::encode_meta_reply};
use matrixobjectstore_rs::cluster::{
    MetaCallOutcome, MetaClientOptions, MetaReply, MetaRequest, RemoteMetaClient, StatusCode,
    PROTOCOL_STAMP,
};
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
fn options() -> MetaClientOptions {
    MetaClientOptions {
        connect_timeout: Duration::from_millis(200),
        read_timeout: Duration::from_millis(200),
        write_timeout: Duration::from_millis(200),
        ..Default::default()
    }
}
fn receive(stream: &mut TcpStream) -> u8 {
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let mut header = [0; 6];
    stream.read_exact(&mut header).unwrap();
    let len = u32::from_be_bytes(header[2..6].try_into().unwrap()) as usize;
    assert!(len < 1024);
    let mut payload = vec![0; len];
    stream.read_exact(&mut payload).unwrap();
    header[1]
}
fn server(response: Option<Vec<u8>>) -> (String, JoinHandle<u8>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let code = receive(&mut stream);
        if let Some(bytes) = response {
            stream.write_all(&bytes).unwrap();
        }
        code
    });
    (address, handle)
}
fn mutation() -> MetaRequest {
    MetaRequest::ExpireRootNodeHeartbeats {
        ttl_ms: 10,
        now_ms: 20,
    }
}
#[test]
fn typed_not_leader_can_route_an_unadmitted_mutation() {
    let unavailable = TcpListener::bind("127.0.0.1:0").unwrap();
    let absent = unavailable.local_addr().unwrap().to_string();
    drop(unavailable);
    let (follower, one) = server(Some(encode_meta_reply(&MetaReply::NotLeader)));
    let (leader, two) = server(Some(encode_meta_reply(&MetaReply::Expired {
        offline_count: 7,
    })));
    let mut client =
        RemoteMetaClient::connect_catalog(&[absent, follower, leader.clone()], &options()).unwrap();
    assert_eq!(
        client.try_call(mutation()).into_result().unwrap(),
        MetaReply::Expired { offline_count: 7 }
    );
    assert_eq!(client.endpoint(), leader);
    assert_eq!(one.join().unwrap(), mutation().code() as u8);
    assert_eq!(two.join().unwrap(), mutation().code() as u8);
}
#[test]
fn uncertain_mutations_are_never_sent_to_another_node() {
    let placement = matrixobjectstore_rs::cluster::CommittedPlacement {
        bucket: "bucket".into(),
        key: "key".into(),
        version: 1,
        length: 4,
        node_ids: vec!["node".into()],
        update_time_ms: 1,
    };
    for request in [
        mutation(),
        MetaRequest::CommitObjectPlacementIfCurrent {
            placement,
            expected: None,
            expected_plan_checksum: "checksum".into(),
        },
    ] {
        for response in [
            None,
            Some(encode_failure(
                StatusCode::InvalidArgument,
                "leadership changed; outcome may be committed",
            )),
        ] {
            let (first, handle) = server(response);
            let (follower, redirect) = server(Some(encode_meta_reply(&MetaReply::NotLeader)));
            let unused = TcpListener::bind("127.0.0.1:0").unwrap();
            unused.set_nonblocking(true).unwrap();
            let second = unused.local_addr().unwrap().to_string();
            let mut client =
                RemoteMetaClient::connect_catalog(&[follower, first, second], &options()).unwrap();
            assert!(client.try_call(request.clone()).into_result().is_err());
            assert_eq!(handle.join().unwrap(), request.code() as u8);
            assert_eq!(redirect.join().unwrap(), request.code() as u8);
            assert!(
                matches!(unused.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock),
                "uncertain mutation was retried"
            );
        }
    }
}
#[test]
fn reads_can_move_after_disconnect_and_catalog_attempts_are_bounded() {
    let (first, one) = server(None);
    let answer = MetaReply::Pong {
        node_count: 3,
        placement_count: 5,
        commit_index: 9,
    };
    let (second, two) = server(Some(encode_meta_reply(&answer)));
    let mut client = RemoteMetaClient::connect_catalog(&[first, second], &options()).unwrap();
    assert_eq!(client.probe().into_result().unwrap(), answer);
    one.join().unwrap();
    two.join().unwrap();
    let (first, one) = server(Some(encode_meta_reply(&MetaReply::NotLeader)));
    let (second, two) = server(Some(encode_meta_reply(&MetaReply::NotLeader)));
    let mut client = RemoteMetaClient::connect_catalog(&[first, second], &options()).unwrap();
    assert!(matches!(client.probe(), MetaCallOutcome::Refused(_)));
    one.join().unwrap();
    two.join().unwrap();
}
#[test]
fn malformed_routing_response_closes_the_stream_before_buffered_bytes_can_reply() {
    let mut bytes = vec![PROTOCOL_STAMP, StatusCode::NotLeader as u8, 0, 0, 0, 1, 0];
    bytes.extend(encode_meta_reply(&MetaReply::Pong {
        node_count: 9,
        placement_count: 9,
        commit_index: 9,
    }));
    let (address, handle) = server(Some(bytes));
    let mut client = RemoteMetaClient::connect(&address, &options()).unwrap();
    assert!(matches!(client.probe(), MetaCallOutcome::Refused(_)));
    assert!(matches!(client.probe(), MetaCallOutcome::Unreachable(_)));
    handle.join().unwrap();
}
#[test]
fn response_timeout_covers_the_entire_frame_not_each_byte() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        receive(&mut stream);
        for byte in [PROTOCOL_STAMP, 0, 0, 0, 0, 24] {
            if stream.write_all(&[byte]).is_err() {
                break;
            }
            thread::sleep(Duration::from_millis(80));
        }
    });
    let mut config = options();
    config.read_timeout = Duration::from_millis(120);
    let mut client = RemoteMetaClient::connect(&address, &config).unwrap();
    let start = Instant::now();
    assert!(client.probe().is_unreachable());
    assert!(start.elapsed() < Duration::from_millis(350));
    handle.join().unwrap();
}

fn identity() -> matrixobjectstore_rs::cluster::MetaGroupIdentity {
    matrixobjectstore_rs::cluster::MetaGroupIdentity {
        bootstrap_voters: Vec::new(),
        version: 1,
        group_id: 42,
        node_id: 1,
        voters: vec![1, 2],
        peer_endpoints: vec!["127.0.0.1:18001".into(), "127.0.0.1:18002".into()],
        bootstrap_id: 99,
    }
}
fn identity_server(answer: MetaReply) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let worker = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        assert_eq!(
            receive(&mut stream),
            MetaRequest::GroupIdentity.code() as u8
        );
        stream
            .write_all(&encode_meta_reply(&MetaReply::GroupIdentity(identity())))
            .unwrap();
        assert_eq!(receive(&mut stream), mutation().code() as u8);
        stream.write_all(&encode_meta_reply(&answer)).unwrap();
    });
    (address, worker)
}
#[test]
fn failed_metadata_identity_exchange_can_try_another_voter_before_mutation() {
    for redirected in [false, true] {
        let (failed, failed_worker) = server(None);
        let (healthy, healthy_worker) = identity_server(MetaReply::Expired { offline_count: 7 });
        let mut endpoints = vec![failed, healthy.clone()];
        let first = if redirected {
            let (endpoint, worker) = identity_server(MetaReply::NotLeader);
            endpoints.insert(0, endpoint);
            Some(worker)
        } else {
            None
        };
        let mut client =
            RemoteMetaClient::connect_verified_raft_catalog(&endpoints, &identity(), &options())
                .unwrap();
        assert_eq!(
            client.try_call(mutation()).into_result().unwrap(),
            MetaReply::Expired { offline_count: 7 }
        );
        assert_eq!(client.endpoint(), healthy);
        assert_eq!(
            failed_worker.join().unwrap(),
            MetaRequest::GroupIdentity.code() as u8
        );
        healthy_worker.join().unwrap();
        if let Some(worker) = first {
            worker.join().unwrap();
        }
    }
    let mut wrong = identity();
    wrong.bootstrap_id += 1;
    for response in [
        encode_meta_reply(&MetaReply::GroupIdentity(wrong)),
        encode_failure(StatusCode::UnknownRequest, "unsupported identity"),
    ] {
        let (first, worker) = server(Some(response));
        let unused = TcpListener::bind("127.0.0.1:0").unwrap();
        unused.set_nonblocking(true).unwrap();
        assert!(RemoteMetaClient::connect_verified_raft_catalog(
            &[first, unused.local_addr().unwrap().to_string()],
            &identity(),
            &options()
        )
        .is_err());
        worker.join().unwrap();
        assert!(matches!(unused.accept(), Err(e) if e.kind() == std::io::ErrorKind::WouldBlock));
    }
    let (first, one) = server(None);
    let (second, two) = server(None);
    assert!(RemoteMetaClient::connect_raft_catalog(&[first, second], 42, &options()).is_err());
    assert_eq!(one.join().unwrap(), MetaRequest::GroupIdentity.code() as u8);
    assert_eq!(two.join().unwrap(), MetaRequest::GroupIdentity.code() as u8);
}
