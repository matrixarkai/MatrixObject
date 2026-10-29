// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

#![cfg(all(target_os = "linux", feature = "replica-server"))]

#[allow(dead_code)]
mod node_regime;

use matrixobjectstore_rs::cluster::{RemoteReplicaSet, ReplicaClientOptions};
use matrixobjectstore_rs::{ChunkId, ObjectError, PageId};
use node_regime::processes::start_replica;

#[test]
fn one_endpoint_cannot_supply_multiple_votes() {
    let token = node_regime::run_token("duplicate-peer");
    let node = start_replica(env!("CARGO_BIN_EXE_chunkserver"), &token);
    let result = RemoteReplicaSet::connect(
        &[node.endpoint.clone(), node.endpoint.clone()],
        2,
        2,
        &ReplicaClientOptions::default(),
    );
    assert!(
        matches!(result, Err(ObjectError::InvalidArgument(_))),
        "duplicate membership was accepted: {result:?}"
    );
}

#[test]
fn two_spellings_of_one_peer_cannot_supply_multiple_votes() {
    let token = node_regime::run_token("aliased-peer");
    let node = start_replica(env!("CARGO_BIN_EXE_chunkserver"), &token);
    let address: std::net::SocketAddr = node.endpoint.parse().unwrap();
    let alias = format!("{}:0{}", address.ip(), address.port());
    assert_ne!(alias, node.endpoint);
    assert_eq!(alias.parse::<std::net::SocketAddr>().unwrap(), address);
    let result = RemoteReplicaSet::connect(
        &[node.endpoint.clone(), alias],
        2,
        2,
        &ReplicaClientOptions::default(),
    );
    assert!(
        matches!(result, Err(ObjectError::InvalidArgument(_))),
        "aliased membership was accepted: {result:?}"
    );
}

#[test]
fn distinct_processes_can_still_form_a_quorum() {
    let token = node_regime::run_token("distinct-peers");
    let a = start_replica(env!("CARGO_BIN_EXE_chunkserver"), &token);
    let b = start_replica(env!("CARGO_BIN_EXE_chunkserver"), &token);
    assert_ne!(a.pid, b.pid);
    assert_ne!(a.pid, std::process::id());
    assert_ne!(b.pid, std::process::id());
    let mut set = RemoteReplicaSet::connect(
        &[a.endpoint.clone(), b.endpoint.clone()],
        2,
        2,
        &ReplicaClientOptions::default(),
    )
    .unwrap();
    let page = PageId {
        chunk_id: ChunkId::new(17, 3).unwrap(),
        page_index: 0,
    };
    assert_eq!(
        set.write_chunk(&page, 0, b"two independent peers").unwrap(),
        2
    );
    assert_eq!(
        set.read_chunk(&page, 0, 21).unwrap().data,
        b"two independent peers"
    );
}

#[test]
fn mapped_ipv6_and_ipv4_cannot_supply_multiple_votes() {
    let token = node_regime::run_token("mapped-peer");
    let node = start_replica(env!("CARGO_BIN_EXE_chunkserver"), &token);
    let address: std::net::SocketAddrV4 = node.endpoint.parse().unwrap();
    let alias = format!("[{}]:{}", address.ip().to_ipv6_mapped(), address.port());
    let result = RemoteReplicaSet::connect(
        &[node.endpoint.clone(), alias],
        2,
        2,
        &ReplicaClientOptions::default(),
    );
    assert!(
        matches!(result, Err(ObjectError::InvalidArgument(_))),
        "mapped address supplied a second vote: {result:?}"
    );
}

#[test]
fn head_quorum_requires_every_returned_field_to_agree() {
    use matrixobjectstore_rs::cluster::{message::encode_reply, ReplicaReply, PROTOCOL_STAMP};
    use matrixobjectstore_rs::{DataRange, ReplicaHead};
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::time::{Duration, Instant};
    let expected = ReplicaHead {
        logical_length: 16,
        write_sequence: 7,
        checksum: "data-checksum".into(),
        state_checksum: "state-checksum".into(),
        discarded_ranges: vec![DataRange {
            offset: 2,
            length: 3,
        }],
    };
    for field in 0..7 {
        let mut changed = expected.clone();
        match field {
            0 => changed.logical_length += 1,
            1 => changed.write_sequence += 1,
            2 => changed.checksum.push('x'),
            3 => changed.state_checksum.push('x'),
            4 => changed.discarded_ranges[0].offset += 1,
            5 => changed.discarded_ranges[0].length += 1,
            _ => changed.discarded_ranges.push(DataRange {
                offset: 9,
                length: 1,
            }),
        }
        for agreeing in [false, true] {
            // Put the inconsistent reply first: a checksum-only vote used to return it.
            let replies = if agreeing {
                vec![changed.clone(), expected.clone(), expected.clone()]
            } else {
                vec![changed.clone(), expected.clone()]
            };
            let listeners: Vec<_> = replies
                .iter()
                .map(|_| {
                    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
                    listener.set_nonblocking(true).unwrap();
                    listener
                })
                .collect();
            let endpoints: Vec<_> = listeners
                .iter()
                .map(|l| l.local_addr().unwrap().to_string())
                .collect();
            let workers: Vec<_> = listeners
                .into_iter()
                .zip(replies)
                .map(|(listener, reply)| {
                    std::thread::spawn(move || {
                        let until = Instant::now() + Duration::from_secs(5);
                        let mut stream = loop {
                            match listener.accept() {
                                Ok((stream, _)) => break stream,
                                Err(err)
                                    if err.kind() == std::io::ErrorKind::WouldBlock
                                        && Instant::now() < until =>
                                {
                                    std::thread::sleep(Duration::from_millis(2))
                                }
                                result => panic!("missing client: {result:?}"),
                            }
                        };
                        stream
                            .set_read_timeout(Some(Duration::from_secs(5)))
                            .unwrap();
                        stream
                            .set_write_timeout(Some(Duration::from_secs(5)))
                            .unwrap();
                        let mut header = [0; 6];
                        stream.read_exact(&mut header).unwrap();
                        assert_eq!(header[0], PROTOCOL_STAMP);
                        let length = u32::from_be_bytes(header[2..].try_into().unwrap()) as usize;
                        assert!(length <= 128);
                        let mut body = vec![0; length];
                        stream.read_exact(&mut body).unwrap();
                        use matrixobjectstore_rs::cluster::{ReplicaRequest, RequestCode};
                        assert!(matches!(
                            ReplicaRequest::decode(RequestCode::from_u8(header[1]).unwrap(), &body)
                                .unwrap(),
                            ReplicaRequest::HeadChunk { .. }
                        ));
                        stream
                            .write_all(&encode_reply(&ReplicaReply::Head(reply)))
                            .unwrap();
                    })
                })
                .collect();
            let mut set =
                RemoteReplicaSet::connect(&endpoints, 2, 2, &ReplicaClientOptions::default())
                    .unwrap();
            let page = PageId {
                chunk_id: ChunkId::new(17, 3).unwrap(),
                page_index: 0,
            };
            let result = set.head_chunk(&page);
            for worker in workers {
                worker.join().unwrap();
            }
            if agreeing {
                assert_eq!(result.unwrap(), expected, "field {field}");
            } else {
                assert!(
                    matches!(result, Err(ObjectError::Corruption(_))),
                    "field {field}: {result:?}"
                );
            }
        }
    }
}
