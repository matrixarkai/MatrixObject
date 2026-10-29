// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#![cfg(all(feature = "meta-server", feature = "replica-server"))]
use matrixobjectstore_rs::{cluster::*, object_manifest::ObjectPageAllocation, ReplicaReadResult};
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    thread,
    time::{Duration, Instant},
};
fn listener() -> TcpListener {
    let socket = TcpListener::bind("127.0.0.1:0").unwrap();
    socket.set_nonblocking(true).unwrap();
    socket
}
fn accept(socket: &TcpListener) -> TcpStream {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match socket.accept() {
            Ok((stream, _)) => {
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                stream
                    .set_write_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                return stream;
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock && Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(2))
            }
            other => panic!("missing connection: {other:?}"),
        }
    }
}
fn frame(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    let mut header = [0; 6];
    stream.read_exact(&mut header).unwrap();
    assert_eq!(header[0], PROTOCOL_STAMP);
    let length = u32::from_be_bytes(header[2..].try_into().unwrap()) as usize;
    assert!(length <= 4096);
    let mut body = vec![0; length];
    stream.read_exact(&mut body).unwrap();
    (header[1], body)
}
fn meta(stream: &mut TcpStream) -> MetaRequest {
    let (code, body) = frame(stream);
    MetaRequest::decode(MetaRequestCode::from_u8(code).unwrap(), &body).unwrap()
}
fn replica(stream: &mut TcpStream) -> ReplicaRequest {
    let (code, body) = frame(stream);
    ReplicaRequest::decode(RequestCode::from_u8(code).unwrap(), &body).unwrap()
}
fn reply(stream: &mut TcpStream, value: MetaReply) {
    stream
        .write_all(&meta_message::encode_meta_reply(&value))
        .unwrap();
}
fn config(socket: &TcpListener, id: u64) -> ChunkGroupConfig {
    ChunkGroupConfig {
        placement_id: format!("group-{id}"),
        failure_domain: format!("zone-{id}"),
        identity: Some(ChunkGroupIdentity {
            version: 1,
            group_id: id,
            node_id: 1,
            capacity_bytes: 4096,
            max_pages: 16,
            voters: vec![1],
            peer_endpoints: vec![format!("127.0.0.1:{}", 12000 + id)],
        }),
        api_endpoints: vec![socket.local_addr().unwrap().to_string()],
        member_ids: vec![format!("member-{id}")],
    }
}
#[test]
fn chunk_group_reply_must_name_the_requested_placement() {
    for matched in [true, false] {
        let socket = listener();
        let address = socket.local_addr().unwrap().to_string();
        let group = config(&socket, 1);
        let worker = thread::spawn(move || {
            let mut stream = accept(&socket);
            assert!(matches!(
                meta(&mut stream),
                MetaRequest::ReadChunkGroup { .. }
            ));
            reply(&mut stream, MetaReply::ChunkGroup(group));
        });
        let mut client =
            RemoteMetaClient::connect(&address, &MetaClientOptions::default()).unwrap();
        let result = client.read_chunk_group(if matched { "group-1" } else { "group-2" });
        if matched {
            assert_eq!(result.unwrap().placement_id, "group-1");
        } else {
            assert!(matches!(
                result,
                Err(matrixobjectstore_rs::ObjectError::Corruption(_))
            ));
        }
        worker.join().unwrap();
    }
}
#[test]
fn writer_reuses_preflight_connection_and_only_looks_up_each_group_once() {
    let sockets = [listener(), listener()];
    let groups = [config(&sockets[0], 1), config(&sockets[1], 2)];
    let workers: Vec<_> = sockets
        .into_iter()
        .zip(groups.clone())
        .enumerate()
        .map(|(index, (socket, group))| {
            thread::spawn(move || {
                // Group 2's first connection verifies preflight only. Group 1's first
                // connection is retained for page 0; later connections recheck identity.
                for turn in 0..2 {
                    let mut stream = accept(&socket);
                    assert!(matches!(
                        replica(&mut stream),
                        ReplicaRequest::GroupIdentity
                    ));
                    stream
                        .write_all(&message::encode_reply(&ReplicaReply::GroupIdentity(
                            group.identity.clone().unwrap(),
                        )))
                        .unwrap();
                    if index == 1 && turn == 0 {
                        assert_eq!(stream.read(&mut [0]).unwrap(), 0);
                        continue;
                    }
                    let page_index = if index == 0 { turn * 2 } else { 1 };
                    let ReplicaRequest::WriteChunkIfSequence {
                        page_id,
                        offset,
                        data,
                        expected_sequence,
                    } = replica(&mut stream)
                    else {
                        panic!("conditional write missing");
                    };
                    assert_eq!(page_id.page_index, page_index);
                    assert_eq!(offset, 0);
                    assert_eq!(expected_sequence, 0);
                    assert_eq!(
                        data,
                        b"abcdefghijkl"[(page_index as usize) * 4..(page_index as usize + 1) * 4]
                    );
                    stream
                        .write_all(&message::encode_reply(&ReplicaReply::Applied))
                        .unwrap();
                    let ReplicaRequest::ReadChunk {
                        page_id: read_page,
                        offset,
                        length,
                    } = replica(&mut stream)
                    else {
                        panic!("readback missing");
                    };
                    assert_eq!(read_page, page_id);
                    assert_eq!(offset, 0);
                    assert_eq!(length, 4);
                    stream
                        .write_all(&message::encode_reply(&ReplicaReply::Read(
                            ReplicaReadResult {
                                data,
                                write_sequence: 1,
                                checksum: String::new(),
                            },
                        )))
                        .unwrap();
                    assert_eq!(stream.read(&mut [0]).unwrap(), 0);
                }
            })
        })
        .collect();
    let socket = listener();
    let address = socket.local_addr().unwrap().to_string();
    let metadata = thread::spawn(move || {
        let mut stream = accept(&socket);
        assert!(matches!(
            meta(&mut stream),
            MetaRequest::ReadManifest { .. }
        ));
        stream
            .write_all(&message::encode_failure(
                StatusCode::NotFound,
                "missing object",
            ))
            .unwrap();
        assert!(matches!(
            meta(&mut stream),
            MetaRequest::PlanObjectPlacement {
                length: 12,
                replica_count: 2,
                ..
            }
        ));
        reply(
            &mut stream,
            MetaReply::Plan(PlacementAssignment {
                bucket: "bucket".into(),
                key: "key".into(),
                length: 12,
                node_ids: groups.iter().map(|g| g.placement_id.clone()).collect(),
                skipped_draining: 0,
                skipped_offline: 0,
                skipped_capacity: 0,
                plan_checksum: "plan".into(),
            }),
        );
        for group in groups.into_iter().rev() {
            let MetaRequest::ReadChunkGroup { placement_id } = meta(&mut stream) else {
                panic!("group lookup missing");
            };
            assert_eq!(placement_id, group.placement_id);
            reply(&mut stream, MetaReply::ChunkGroup(group));
        }
        assert!(matches!(
            meta(&mut stream),
            MetaRequest::AllocateObjectPages { page_count: 3 }
        ));
        reply(
            &mut stream,
            MetaReply::ObjectPages(ObjectPageAllocation {
                format_version: 1,
                allocation_id: 1,
                page_count: 3,
            }),
        );
        let MetaRequest::PublishManifest { outcome, .. } = meta(&mut stream) else {
            panic!("unexpected additional metadata lookup");
        };
        let manifest = outcome.after.unwrap();
        assert_eq!(manifest.extents.len(), 3);
        reply(&mut stream, MetaReply::Manifest(manifest.clone()));
        manifest
    });
    let mut client = RemoteMetaClient::connect(&address, &MetaClientOptions::default()).unwrap();
    let result = client.write_object(
        "bucket",
        "key",
        None,
        b"abcdefghijkl",
        &ObjectWriteOptions {
            page_bytes: 4,
            target_groups: 2,
            ..Default::default()
        },
    );
    assert_eq!(result.unwrap(), metadata.join().unwrap());
    for worker in workers {
        worker.join().unwrap();
    }
}
