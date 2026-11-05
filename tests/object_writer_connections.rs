// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#![cfg(all(feature = "meta-server", feature = "replica-server"))]
use matrixobjectstore_rs::{cluster::*, object_manifest::ObjectPageAllocation, ChunkId, PageId, ReplicaReadResult};
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, Condvar, Mutex},
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
    assert!(length <= 2 * 1024 * 1024);
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
fn failed_pipelined_write_retires_socket_without_replaying() {
    let socket = listener();
    let address = socket.local_addr().unwrap().to_string();
    let worker = thread::spawn(move || {
        let mut stream = accept(&socket);
        assert!(matches!(replica(&mut stream), ReplicaRequest::WriteChunkIfSequence { .. }));
        stream.write_all(&message::encode_reply(&ReplicaReply::Applied)).unwrap();
        assert!(matches!(replica(&mut stream), ReplicaRequest::WriteChunkBatchIfSequence { writes } if writes.is_empty()));
        stream.write_all(&message::encode_failure(StatusCode::UnknownRequest, "older peer")).unwrap();
        // Hold the second response until the third request arrives. This
        // distinguishes a pipeline from a sequence of round trips.
        assert!(matches!(replica(&mut stream), ReplicaRequest::WriteChunkIfSequence { .. }));
        assert!(matches!(replica(&mut stream), ReplicaRequest::WriteChunkIfSequence { .. }));
        stream.write_all(&message::encode_failure(
            StatusCode::PreconditionFailed,
            "write condition changed",
        )).unwrap();
        assert_eq!(stream.read(&mut [0]).unwrap(), 0);
        assert!(matches!(socket.accept(), Err(e) if e.kind() == std::io::ErrorKind::WouldBlock));
    });
    let mut client = RemoteReplicaClient::connect(&address, &ReplicaClientOptions::default()).unwrap();
    let chunk_id = ChunkId::new(1, 1).unwrap();
    let writes: Vec<_> = (0..3)
        .map(|index| (PageId { chunk_id: chunk_id.clone(), page_index: index }, 0, b"page".as_slice(), 0))
        .collect();
    assert!(matches!(
        client.write_chunks_if_sequence_pipelined(&writes),
        Err(matrixobjectstore_rs::ObjectError::PreconditionFailed(_))
    ));
    assert!(client.ping().is_err());
    worker.join().unwrap();
}
#[test]
fn large_pages_keep_bounded_individual_writes() {
    let socket = listener();
    let address = socket.local_addr().unwrap().to_string();
    let worker = thread::spawn(move || {
        let mut stream = accept(&socket);
        for index in 0..3 {
            let ReplicaRequest::WriteChunkIfSequence { page_id, data, .. } = replica(&mut stream) else {
                panic!("large page must use a conditional write");
            };
            assert_eq!(page_id.page_index, index);
            assert_eq!(data.len(), 65 * 1024);
            if index == 0 {
                stream.write_all(&message::encode_reply(&ReplicaReply::Applied)).unwrap();
            }
        }
        for _ in 1..3 {
            stream.write_all(&message::encode_reply(&ReplicaReply::Applied)).unwrap();
        }
    });
    let mut client = RemoteReplicaClient::connect(&address, &ReplicaClientOptions::default()).unwrap();
    let data = vec![7u8; 65 * 1024];
    let chunk_id = ChunkId::new(1, 2).unwrap();
    let writes: Vec<_> = (0..3).map(|index| (
        PageId { chunk_id: chunk_id.clone(), page_index: index }, 0, data.as_slice(), 0,
    )).collect();
    client.write_chunks_if_sequence_pipelined(&writes).unwrap();
    worker.join().unwrap();
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
    let simultaneous_reads = Arc::new((Mutex::new(0usize), Condvar::new()));
    let workers: Vec<_> = sockets
        .into_iter()
        .zip(groups.clone())
        .enumerate()
        .map(|(index, (socket, group))| {
            let object = object.clone();
            let simultaneous_reads = Arc::clone(&simultaneous_reads);
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
                    let pages: Vec<_> = (index as u64..32).step_by(2).collect();
                    for (batch_index, batch) in pages.chunks(8).enumerate() {
                        if writing {
                            for (position, &page_index) in batch.iter().enumerate() {
                                if position > 0 { break; }
                                let start = page_index as usize * 4;
                                let data = &object[start..object.len().min(start + 4)];
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
                                let response = message::encode_reply(&ReplicaReply::Applied);
                                if position == 0 { stream.write_all(&response).unwrap(); }
                            }
                            if batch_index == 0 {
                                assert!(matches!(replica(&mut stream), ReplicaRequest::WriteChunkBatchIfSequence { writes } if writes.is_empty()));
                                if index == 0 {
                                    stream.write_all(&message::encode_reply(&ReplicaReply::Applied)).unwrap();
                                } else {
                                    stream.write_all(&message::encode_failure(StatusCode::UnknownRequest, "older peer")).unwrap();
                                }
                            }
                            if index == 0 {
                                for part in batch[1..].chunks(3) {
                                    if part.len() == 1 {
                                        assert!(matches!(replica(&mut stream), ReplicaRequest::WriteChunkIfSequence { page_id, .. } if page_id.page_index == part[0]));
                                    } else {
                                        let ReplicaRequest::WriteChunkBatchIfSequence { writes } = replica(&mut stream) else { panic!("page batch missing"); };
                                        assert_eq!(writes.len(), part.len());
                                        for (write, &page_index) in writes.iter().zip(part) {
                                            let start = page_index as usize * 4;
                                            assert_eq!(write.page_id.page_index, page_index);
                                            assert_eq!(write.offset, 0);
                                            assert_eq!(write.expected_sequence, 0);
                                            assert_eq!(write.data, object[start..object.len().min(start + 4)]);
                                        }
                                    }
                                    stream.write_all(&message::encode_reply(&ReplicaReply::Applied)).unwrap();
                                }
                            } else {
                                let mut responses = Vec::new();
                                for &page_index in &batch[1..] {
                                    let ReplicaRequest::WriteChunkIfSequence { page_id, .. } = replica(&mut stream) else { panic!("fallback write missing"); };
                                    assert_eq!(page_id.page_index, page_index);
                                    responses.push(message::encode_reply(&ReplicaReply::Applied));
                                }
                                for response in responses { stream.write_all(&response).unwrap(); }
                            }
                            if index == 0 {
                                let mut responses = Vec::new();
                                for (position, &page_index) in batch.iter().enumerate() {
                                    let start = page_index as usize * 4;
                                    let data = &object[start..object.len().min(start + 4)];
                                    let ReplicaRequest::DigestChunk { page_id, offset, length } = replica(&mut stream) else {
                                        panic!("page digest missing");
                                    };
                                    assert_eq!(page_id.page_index, page_index);
                                    assert_eq!(offset, 0);
                                    assert_eq!(length, data.len() as u64);
                                    let mut sha256 = [0; 32];
                                    sha256.copy_from_slice(ring::digest::digest(&ring::digest::SHA256, data).as_ref());
                                    let response = message::encode_reply(&ReplicaReply::Digest {
                                        write_sequence: 1,
                                        length,
                                        sha256,
                                    });
                                    if position == 0 {
                                        stream.write_all(&response).unwrap();
                                    } else {
                                        responses.push(response);
                                    }
                                }
                                for response in responses {
                                    stream.write_all(&response).unwrap();
                                }
                                continue;
                            }
                            assert!(matches!(replica(&mut stream), ReplicaRequest::DigestChunk { .. }));
                            stream.write_all(&message::encode_failure(
                                StatusCode::UnknownRequest,
                                "older peer",
                            )).unwrap();
                        }
                        let mut replies = Vec::new();
                        for &page_index in batch {
                            let start = page_index as usize * 4;
                            let data = object[start..object.len().min(start + 4)].to_vec();
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
                            replies.push(message::encode_reply(&ReplicaReply::Read(
                                ReplicaReadResult {
                                    data,
                                    write_sequence: 1,
                                    checksum: String::new(),
                                },
                            )));
                        }
                        if !writing && batch_index == 0 {
                            let (count, ready) = &*simultaneous_reads;
                            let mut arrived = count.lock().unwrap();
                            *arrived += 1;
                            ready.notify_all();
                            let (arrived, timeout) = ready
                                .wait_timeout_while(arrived, Duration::from_secs(5), |count| {
                                    *count < 2
                                })
                                .unwrap();
                            assert!(
                                !timeout.timed_out() && *arrived == 2,
                                "group reads were serialized"
                            );
                        }
                        for response in replies {
                            stream.write_all(&response).unwrap();
                        }
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
                length: 64,
                replica_count: 2,
                ..
            }
        ));
        reply(
            &mut stream,
            MetaReply::Plan(PlacementAssignment {
                bucket: "bucket".into(),
                key: "key".into(),
                length: 64,
                node_ids: groups.iter().map(|g| g.placement_id.clone()).collect(),
                skipped_draining: 0,
                skipped_offline: 0,
                skipped_capacity: 0,
                plan_checksum: "plan".into(),
            }),
        );
        let MetaRequest::ReadChunkRoutes { placement_ids } = meta(&mut stream) else {
            panic!("write route batch missing");
        };
        assert_eq!(placement_ids, ["group-2", "group-1"]);
        reply(&mut stream, MetaReply::ChunkRoutes(ChunkRouteBatch { version: 1, routes: groups.clone().into_iter().rev()
            .map(|group| ChunkGroupRoute { version: 1, group: Some(group), fence: vec![] })
            .collect() }));
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
            MetaRequest::ReadManifestRoutes { .. }
        ));
        reply(&mut stream, MetaReply::ManifestRoutes {
            manifest: manifest.clone(),
            routes: groups.into_iter()
                .map(|group| ChunkGroupRoute { version: 1, group: Some(group), fence: vec![] })
                .collect(),
        });
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

#[test]
fn partial_pipelined_read_retries_only_unread_pages() {
    let socket = listener();
    let endpoint = socket.local_addr().unwrap().to_string();
    let worker = thread::spawn(move || {
        let mut first = accept(&socket);
        for index in 0..3 {
            let ReplicaRequest::ReadChunk { page_id, .. } = replica(&mut first) else {
                panic!("pipelined read missing");
            };
            assert_eq!(page_id.page_index, index);
        }
        first.write_all(&message::encode_reply(&ReplicaReply::Read(ReplicaReadResult {
            data: vec![10], write_sequence: 1, checksum: String::new(),
        }))).unwrap();
        drop(first);
        let mut second = accept(&socket);
        for index in 1..3 {
            let ReplicaRequest::ReadChunk { page_id, .. } = replica(&mut second) else {
                panic!("unread page retry missing");
            };
            assert_eq!(page_id.page_index, index);
            second.write_all(&message::encode_reply(&ReplicaReply::Read(ReplicaReadResult {
                data: vec![10 + index as u8], write_sequence: 1, checksum: String::new(),
            }))).unwrap();
        }
        assert!(matches!(socket.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock));
    });
    let mut client = RemoteReplicaClient::connect_catalog(&[endpoint], &ReplicaClientOptions::default()).unwrap();
    let chunk_id = ChunkId::new(1, 1).unwrap();
    let pages: Vec<_> = (0..3).map(|index| (
        PageId { chunk_id: chunk_id.clone(), page_index: index }, 0, 1,
    )).collect();
    let reads = client.read_chunks_pipelined(&pages).unwrap();
    assert_eq!(reads.into_iter().map(|read| read.data).collect::<Vec<_>>(),
        [vec![10], vec![11], vec![12]]);
    worker.join().unwrap();
}
