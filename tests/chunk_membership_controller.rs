// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#![cfg(all(feature = "meta-server", feature = "replica-server"))]
use matrixobjectstore_rs::cluster::*;
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    thread,
    time::Duration,
};

fn receive(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    let mut header = [0; 6];
    stream.read_exact(&mut header).unwrap();
    assert_eq!(header[0], PROTOCOL_STAMP);
    let size = u32::from_be_bytes(header[2..].try_into().unwrap()) as usize;
    assert!(size < 16384);
    let mut body = vec![0; size];
    stream.read_exact(&mut body).unwrap();
    (header[1], body)
}
fn closed(stream: &mut TcpStream) {
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    assert_eq!(
        stream.read(&mut [0]).unwrap(),
        0,
        "unexpected additional request"
    );
}
#[derive(Clone, Copy, Debug, PartialEq)]
enum Case {
    Success,
    Fixed,
    WrongIdentity,
    Stale,
    OutsideInventory,
    Unchanged,
    Conflict,
    LostReply,
    WrongReceipt,
    WrongObserver,
    NoQuorum,
}
fn exercise(case: Case) {
    let chunk = TcpListener::bind("127.0.0.1:0").unwrap();
    let chunk_address = chunk.local_addr().unwrap().to_string();
    let spare = TcpListener::bind("127.0.0.1:0").unwrap();
    spare.set_nonblocking(true).unwrap();
    let mut identity = ChunkGroupIdentity {
        version: 2,
        group_id: 42,
        node_id: 1,
        capacity_bytes: 4096,
        max_pages: 16,
        voters: vec![1, 2],
        peer_endpoints: vec!["127.0.0.1:18001".into(), "127.0.0.1:18002".into()],
        bootstrap_id: 91,
        bootstrap_voters: vec![1],
    };
    let initial = MembershipStatus {
        version: 1,
        node_id: 1,
        term: 2,
        applied_index: 10,
        voters: vec![1],
        outgoing_voters: vec![],
        learners: vec![2],
    };
    let previous = ChunkMembershipFence::from_status(&identity, &initial).unwrap();
    let mut status = MembershipStatus {
        applied_index: 20,
        voters: vec![1, 2],
        outgoing_voters: vec![1],
        learners: vec![],
        ..initial.clone()
    };
    match case {
        Case::Stale => status.applied_index = 9,
        Case::OutsideInventory => status.voters = vec![1, 3],
        Case::WrongObserver => status.node_id = 2,
        Case::Unchanged => status = initial.clone(),
        Case::Fixed => {
            identity.version = 1;
            identity.bootstrap_id = 0;
            identity.bootstrap_voters.clear();
        }
        _ => {}
    }
    let route = ChunkGroupRoute {
        version: 1,
        group: Some(ChunkGroupConfig {
            placement_id: "group-a".into(),
            failure_domain: "zone-a".into(),
            identity: Some(identity.clone()),
            api_endpoints: vec![chunk_address, spare.local_addr().unwrap().to_string()],
            member_ids: vec!["member-1".into(), "member-2".into()],
        }),
        fence: if case == Case::Fixed {
            vec![]
        } else {
            previous.to_bytes()
        },
    };
    let expected = if matches!(
        case,
        Case::Success | Case::Conflict | Case::LostReply | Case::WrongReceipt
    ) {
        Some(previous.advance(&identity, &status).unwrap())
    } else {
        None
    };
    let observation = status.clone();
    let chunk_worker = thread::spawn(move || {
        if case == Case::Fixed {
            chunk.set_nonblocking(true).unwrap();
            return chunk;
        }
        let (mut stream, _) = chunk.accept().unwrap();
        assert_eq!(receive(&mut stream).0, 7);
        if case == Case::WrongIdentity {
            identity.group_id += 1;
        }
        stream
            .write_all(&message::encode_reply(&ReplicaReply::GroupIdentity(
                identity,
            )))
            .unwrap();
        if case == Case::WrongIdentity {
            closed(&mut stream);
            return chunk;
        }
        let (code, bytes) = receive(&mut stream);
        let ReplicaRequest::Membership(request) =
            ReplicaRequest::decode(RequestCode::from_u8(code).unwrap(), &bytes).unwrap()
        else {
            panic!("membership read missing")
        };
        assert_eq!(request.operation, MembershipOperation::Read as i32);
        assert_eq!(request.bootstrap_id, 91);
        let response = if case == Case::NoQuorum {
            message::encode_failure(
                StatusCode::PreconditionFailed,
                "membership quorum unavailable",
            )
        } else {
            message::encode_reply(&ReplicaReply::Membership(observation))
        };
        stream.write_all(&response).unwrap();
        closed(&mut stream);
        chunk
    });
    let metadata = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = metadata.local_addr().unwrap().to_string();
    let old_bytes = previous.to_bytes();
    let meta_worker = thread::spawn(move || {
        let (mut stream, _) = metadata.accept().unwrap();
        let (code, bytes) = receive(&mut stream);
        assert!(
            matches!(MetaRequest::decode(MetaRequestCode::from_u8(code).unwrap(),&bytes).unwrap(),MetaRequest::ReadChunkRoute {placement_id} if placement_id=="group-a")
        );
        stream
            .write_all(&meta_message::encode_meta_reply(&MetaReply::ChunkRoute(
                route,
            )))
            .unwrap();
        if !matches!(
            case,
            Case::Success | Case::Conflict | Case::LostReply | Case::WrongReceipt
        ) {
            closed(&mut stream);
            return;
        }
        let (code, bytes) = receive(&mut stream);
        let MetaRequest::ChunkCatalog(command) =
            MetaRequest::decode(MetaRequestCode::from_u8(code).unwrap(), &bytes).unwrap()
        else {
            panic!("catalog commit missing")
        };
        assert_eq!(command.operation, 1);
        assert_eq!(command.expected_fence, old_bytes);
        assert_eq!(command.status, Some(status));
        assert_eq!(command.placement_id, "group-a");
        assert_eq!(command.bootstrap_id, 91);
        if case == Case::LostReply {
            return;
        }
        let response = if case == Case::Conflict {
            message::encode_failure(StatusCode::PreconditionFailed, "catalog fence changed")
        } else {
            meta_message::encode_meta_reply(&MetaReply::PlacementNode(PlacementNodeSummary {
                node_id: if case == Case::WrongReceipt {
                    "group-b"
                } else {
                    "group-a"
                }
                .into(),
                ..Default::default()
            }))
        };
        stream.write_all(&response).unwrap();
        closed(&mut stream);
    });
    let unused_meta = TcpListener::bind("127.0.0.1:0").unwrap();
    unused_meta.set_nonblocking(true).unwrap();
    let mut meta = RemoteMetaClient::connect_catalog(
        &[address, unused_meta.local_addr().unwrap().to_string()],
        &MetaClientOptions::default(),
    ).unwrap();
    let result = meta.refresh_chunk_membership("group-a", &ReplicaClientOptions::default());
    match case {
        Case::Success => assert_eq!(result.unwrap(), expected.unwrap()),
        Case::Unchanged => assert_eq!(result.unwrap(), previous),
        _ => assert!(result.is_err(), "case {case:?}"),
    }
    drop(meta);
    assert!(matches!(unused_meta.accept(), Err(e) if e.kind() == std::io::ErrorKind::WouldBlock), "catalog mutation retried on another metadata server");
    meta_worker.join().unwrap();
    let chunk = chunk_worker.join().unwrap();
    if case == Case::Fixed {
        assert!(matches!(chunk.accept(),Err(e) if e.kind()==std::io::ErrorKind::WouldBlock));
    }
    assert!(matches!(spare.accept(),Err(e) if e.kind()==std::io::ErrorKind::WouldBlock));
}
#[test]
fn controller_commits_only_verified_new_observations() {
    exercise(Case::Success);
    exercise(Case::Unchanged);
}
#[test]
fn controller_rejects_invalid_or_unavailable_observations_before_commit() {
    for case in [
        Case::Fixed,
        Case::WrongIdentity,
        Case::Stale,
        Case::OutsideInventory,
        Case::WrongObserver,
        Case::NoQuorum,
    ] {
        exercise(case);
    }
}
#[test]
fn controller_never_replays_conflicting_or_uncertain_commits() {
    for case in [Case::Conflict, Case::LostReply, Case::WrongReceipt] {
        exercise(case);
    }
}
