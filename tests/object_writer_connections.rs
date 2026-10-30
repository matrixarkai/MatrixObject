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
            bootstrap_voters: Vec::new(),
            bootstrap_id: 0,
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
fn chunk_route_reply_must_name_the_requested_placement() {
    for matched in [true, false] {
        let socket = listener();
        let address = socket.local_addr().unwrap().to_string();
        let group = config(&socket, 1);
        let worker = thread::spawn(move || {
            let mut stream = accept(&socket);
            assert!(matches!(
                meta(&mut stream),
                MetaRequest::ReadChunkRoute { .. }
            ));
            reply(&mut stream, MetaReply::ChunkRoute(ChunkGroupRoute {version:1,group:Some(group),fence:vec![]}));
        });
        let mut client =
            RemoteMetaClient::connect(&address, &MetaClientOptions::default()).unwrap();
        let result = client.read_chunk_route(if matched { "group-1" } else { "group-2" });
        if matched {
            assert_eq!(result.unwrap().group.unwrap().placement_id, "group-1");
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
    let object: Vec<u8> = (0..125).collect();
    let sockets = [listener(), listener()];
    let groups = [config(&sockets[0], 1), config(&sockets[1], 2)];
    let workers: Vec<_> = sockets
        .into_iter()
        .zip(groups.clone())
        .enumerate()
        .map(|(index, (socket, group))| {
            let object = object.clone();
            thread::spawn(move || {
                // One retained preflight connection for group 1; group 2 verifies
                // preflight then reconnects once for all writes. Each read visits once.
                let turns = if index == 0 { 2 } else { 3 };
                for turn in 0..turns {
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
                    let writing = turn == if index == 0 { 0 } else { 1 };
                    let pages = (index as u64..32).step_by(2);
                    for page_index in pages {
                        let start = page_index as usize * 4;
                        let data = object[start..object.len().min(start + 4)].to_vec();
                        if writing {
                            let ReplicaRequest::WriteChunkIfSequence {
                                page_id,
                                offset,
                                data: actual,
                                expected_sequence,
                            } = replica(&mut stream)
                            else {
                                panic!("conditional write missing");
                            };
                            assert_eq!(page_id.page_index, page_index);
                            assert_eq!(offset, 0);
                            assert_eq!(expected_sequence, 0);
                            assert_eq!(actual, data);
                            stream
                                .write_all(&message::encode_reply(&ReplicaReply::Applied))
                                .unwrap();
                        }
                        let ReplicaRequest::ReadChunk {
                            page_id,
                            offset,
                            length,
                        } = replica(&mut stream)
                        else {
                            panic!("readback missing");
                        };
                        assert_eq!(page_id.page_index, page_index);
                        assert_eq!(offset, 0);
                        assert_eq!(length, data.len() as u64);
                        stream
                            .write_all(&message::encode_reply(&ReplicaReply::Read(
                                ReplicaReadResult {
                                    data,
                                    write_sequence: 1,
                                    checksum: String::new(),
                                },
                            )))
                            .unwrap();
                    }
                    assert_eq!(stream.read(&mut [0]).unwrap(), 0);
                }
                assert!(
                    matches!(socket.accept(), Err(e) if e.kind() == std::io::ErrorKind::WouldBlock)
                );
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
                length: 125,
                replica_count: 2,
                ..
            }
        ));
        reply(
            &mut stream,
            MetaReply::Plan(PlacementAssignment {
                bucket: "bucket".into(),
                key: "key".into(),
                length: 125,
                node_ids: groups.iter().map(|g| g.placement_id.clone()).collect(),
                skipped_draining: 0,
                skipped_offline: 0,
                skipped_capacity: 0,
                plan_checksum: "plan".into(),
            }),
        );
        for group in groups.clone().into_iter().rev() {
            let MetaRequest::ReadChunkRoute { placement_id } = meta(&mut stream) else {
                panic!("group lookup missing");
            };
            assert_eq!(placement_id, group.placement_id);
            reply(&mut stream, MetaReply::ChunkRoute(ChunkGroupRoute {version:1,group:Some(group),fence:vec![]}));
        }
        assert!(matches!(
            meta(&mut stream),
            MetaRequest::AllocateObjectPages { page_count: 32 }
        ));
        reply(
            &mut stream,
            MetaReply::ObjectPages(ObjectPageAllocation {
                format_version: 1,
                allocation_id: 1,
                page_count: 32,
            }),
        );
        let MetaRequest::PublishManifest { outcome, .. } = meta(&mut stream) else {
            panic!("unexpected additional metadata lookup");
        };
        let manifest = outcome.after.unwrap();
        assert_eq!(manifest.extents.len(), 32);
        reply(&mut stream, MetaReply::Manifest(manifest.clone()));
        assert!(matches!(
            meta(&mut stream),
            MetaRequest::ReadManifest { .. }
        ));
        reply(&mut stream, MetaReply::Manifest(manifest.clone()));
        for group in groups {
            let MetaRequest::ReadChunkRoute { placement_id } = meta(&mut stream) else {
                panic!("read group lookup missing");
            };
            assert_eq!(placement_id, group.placement_id);
            reply(&mut stream, MetaReply::ChunkRoute(ChunkGroupRoute {version:1,group:Some(group),fence:vec![]}));
        }
        assert_eq!(stream.read(&mut [0]).unwrap(), 0);
        manifest
    });
    let mut client = RemoteMetaClient::connect(&address, &MetaClientOptions::default()).unwrap();
    let result = client.write_object(
        "bucket",
        "key",
        None,
        &object,
        &ObjectWriteOptions {
            page_bytes: 4,
            target_groups: 2,
            ..Default::default()
        },
    );
    let written = result.unwrap();
    let (read_manifest, bytes) = client
        .read_object("bucket", "key", 125, &ReplicaClientOptions::default())
        .unwrap();
    assert_eq!(bytes, object);
    assert_eq!(read_manifest, written);
    assert_eq!(
        written
            .extents
            .iter()
            .map(|e| e.group.as_str())
            .collect::<Vec<_>>(),
        (0..32)
            .map(|i| if i % 2 == 0 { "group-1" } else { "group-2" })
            .collect::<Vec<_>>()
    );
    drop(client);
    assert_eq!(written, metadata.join().unwrap());
    for worker in workers {
        worker.join().unwrap();
    }
}
