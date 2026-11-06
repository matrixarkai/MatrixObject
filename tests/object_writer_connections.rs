// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#![cfg(all(feature = "meta-server", feature = "replica-server"))]
use matrixobjectstore_rs::{cluster::*, object_manifest::{ObjectManifest, ObjectPageAllocation}, ChunkId, PageId, ReplicaReadResult};
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
fn combined_placement_reply_rejects_a_route_for_another_group() {
    let socket = listener();
    let plan = PlacementAssignment {
        bucket: "bucket".into(), key: "key".into(), length: 64,
        node_ids: vec!["group-1".into()], skipped_draining: 0,
        skipped_offline: 0, skipped_capacity: 0, plan_checksum: "plan".into(),
    };
    let route = ChunkGroupRoute {
        version: 1, group: Some(config(&socket, 2)), fence: vec![],
    };
    let payload = MetaReply::PlanRoutes { plan, routes: vec![route] }.encode_payload();
    assert!(MetaReply::decode(MetaRequestCode::PlanObjectPlacementRoutes, &payload).is_err());
}

#[test]
fn write_plan_reply_keeps_manifest_and_routes_for_one_object() {
    let socket = listener();
    let request = MetaRequest::PlanObjectWriteRoutes {
        bucket: "bucket".into(), key: "key".into(), length: 64, replica_count: 1,
    };
    assert_eq!(MetaRequest::decode(request.code(), &request.encode_payload()).unwrap(), request);
    let plan = PlacementAssignment {
        bucket: "bucket".into(), key: "key".into(), length: 64,
        node_ids: vec!["group-1".into()], skipped_draining: 0,
        skipped_offline: 0, skipped_capacity: 0, plan_checksum: "plan".into(),
    };
    let current = ObjectManifest {
        format_version: 1, bucket: "bucket".into(), key: "key".into(),
        generation: 1, length: 0, extents: vec![], deleted: false,
    };
    let route = ChunkGroupRoute { version: 1, group: Some(config(&socket, 1)), fence: vec![] };
    let reply = MetaReply::WritePlanRoutes {
        current: Some(current.clone()), plan: plan.clone(), routes: vec![route.clone()],
    };
    assert_eq!(MetaReply::decode(request.code(), &reply.encode_payload()).unwrap(), reply);
    let wrong_route = MetaReply::WritePlanRoutes {
        current: None, plan: plan.clone(),
        routes: vec![ChunkGroupRoute {
            version: 1, group: Some(config(&socket, 2)), fence: vec![],
        }],
    };
    assert!(MetaReply::decode(request.code(), &wrong_route.encode_payload()).is_err());
    let wrong = MetaReply::WritePlanRoutes {
        current: Some(ObjectManifest {
            key: "another".into(), ..current
        }),
        plan, routes: vec![route],
    };
    assert!(MetaReply::decode(request.code(), &wrong.encode_payload()).is_err());
}

#[test]
fn changed_manifest_stops_combined_write_before_data_connections() {
    let metadata_socket = listener();
    let data_socket = listener();
    let endpoint = metadata_socket.local_addr().unwrap().to_string();
    let route = ChunkGroupRoute {
        version: 1, group: Some(config(&data_socket, 1)), fence: vec![],
    };
    let worker = thread::spawn(move || {
        let mut stream = accept(&metadata_socket);
        assert!(matches!(meta(&mut stream), MetaRequest::PlanObjectWriteRoutes { .. }));
        reply(&mut stream, MetaReply::WritePlanRoutes {
            current: Some(ObjectManifest {
                format_version: 1, bucket: "bucket".into(), key: "key".into(),
                generation: 1, length: 0, extents: vec![], deleted: false,
            }),
            plan: PlacementAssignment {
                bucket: "bucket".into(), key: "key".into(), length: 4,
                node_ids: vec!["group-1".into()], skipped_draining: 0,
                skipped_offline: 0, skipped_capacity: 0, plan_checksum: "plan".into(),
            },
            routes: vec![route],
        });
        assert_eq!(stream.read(&mut [0]).unwrap(), 0);
    });
    let mut client = RemoteMetaClient::connect(&endpoint, &MetaClientOptions::default()).unwrap();
    assert!(matches!(
        client.write_object("bucket", "key", None, b"page", &ObjectWriteOptions::default()),
        Err(ObjectWriteFailure::BeforePublication { staged: None, .. })
    ));
    drop(client);
    worker.join().unwrap();
    assert!(matches!(data_socket.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock));
}

#[test]
fn failed_pipelined_write_retires_socket_without_replaying() {
    let socket = listener();
    let address = socket.local_addr().unwrap().to_string();
    let worker = thread::spawn(move || {
        let mut stream = accept(&socket);
        assert!(matches!(
            replica(&mut stream),
            ReplicaRequest::WriteChunkBatchIfSequence { writes } if writes.len() == 3
        ));
        stream.write_all(&message::encode_failure(StatusCode::UnknownRequest, "older peer")).unwrap();
        assert!(matches!(
            replica(&mut stream),
            ReplicaRequest::WriteChunkIfSequence { page_id, .. } if page_id.page_index == 0
        ));
        stream.write_all(&message::encode_reply(&ReplicaReply::Applied)).unwrap();
        // Hold the next response until the final request arrives. This
        // distinguishes a pipeline from a sequence of round trips.
        assert!(matches!(
            replica(&mut stream),
            ReplicaRequest::WriteChunkIfSequence { page_id, .. } if page_id.page_index == 1
        ));
        assert!(matches!(
            replica(&mut stream),
            ReplicaRequest::WriteChunkIfSequence { page_id, .. } if page_id.page_index == 2
        ));
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
fn small_pages_start_with_full_nonempty_batches() {
    let socket = listener();
    let address = socket.local_addr().unwrap().to_string();
    let worker = thread::spawn(move || {
        let mut stream = accept(&socket);
        let ReplicaRequest::WriteChunkBatchIfSequence { writes } = replica(&mut stream) else {
            panic!("small pages did not use a conditional write batch");
        };
        assert_eq!(writes.len(), 8);
        assert_eq!(
            writes
                .iter()
                .map(|write| write.page_id.page_index)
                .collect::<Vec<_>>(),
            (0..8).collect::<Vec<_>>()
        );
        stream
            .write_all(&message::encode_reply(&ReplicaReply::Applied))
            .unwrap();
    });
    let mut client =
        RemoteReplicaClient::connect(&address, &ReplicaClientOptions::default()).unwrap();
    let chunk_id = ChunkId::new(1, 3).unwrap();
    let writes: Vec<_> = (0..8)
        .map(|index| {
            (
                PageId {
                    chunk_id: chunk_id.clone(),
                    page_index: index,
                },
                0,
                b"page".as_slice(),
                0,
            )
        })
        .collect();
    client.write_chunks_if_sequence_pipelined(&writes).unwrap();
    worker.join().unwrap();
}

#[test]
fn eight_small_page_writes_return_digests_in_one_batch_exchange() {
    let socket = listener();
    let address = socket.local_addr().unwrap().to_string();
    let worker = thread::spawn(move || {
        let mut stream = accept(&socket);
        let ReplicaRequest::WriteChunkBatchIfSequenceDigest { writes } = replica(&mut stream)
        else {
            panic!("small pages did not use a conditional write digest batch");
        };
        assert_eq!(writes.len(), 8);
        let digests = writes
            .iter()
            .enumerate()
            .map(|(position, write)| {
                assert_eq!(write.page_id.page_index, position as u64);
                let mut sha256 = [0; 32];
                sha256.copy_from_slice(
                    ring::digest::digest(&ring::digest::SHA256, &write.data).as_ref(),
                );
                (1, write.data.len() as u64, sha256)
            })
            .collect();
        stream
            .write_all(&message::encode_reply(&ReplicaReply::Digests(digests)))
            .unwrap();
    });
    let mut client =
        RemoteReplicaClient::connect(&address, &ReplicaClientOptions::default()).unwrap();
    let chunk_id = ChunkId::new(1, 4).unwrap();
    let writes: Vec<_> = (0..8)
        .map(|index| {
            (
                PageId {
                    chunk_id: chunk_id.clone(),
                    page_index: index,
                },
                0,
                b"page".as_slice(),
                0,
            )
        })
        .collect();
    let digests = client
        .write_chunks_if_sequence_with_digests(&writes)
        .unwrap();
    assert_eq!(digests.len(), writes.len());
    assert!(digests.iter().all(|(sequence, length, _)| *sequence == 1 && *length == 4));
    worker.join().unwrap();
}

#[test]
fn older_four_write_bound_is_discovered_before_admission() {
    let socket = listener();
    let address = socket.local_addr().unwrap().to_string();
    let worker = thread::spawn(move || {
        let mut stream = accept(&socket);
        assert!(matches!(
            replica(&mut stream),
            ReplicaRequest::WriteChunkBatchIfSequenceDigest { writes }
                if writes.len() == 8
        ));
        stream
            .write_all(&message::encode_failure(
                StatusCode::InvalidArgument,
                "invalid conditional write batch",
            ))
            .unwrap();
        for start in [0, 4] {
            let ReplicaRequest::WriteChunkBatchIfSequenceDigest { writes } =
                replica(&mut stream)
            else {
                panic!("four-write digest batch missing");
            };
            assert_eq!(writes.len(), 4);
            assert_eq!(writes[0].page_id.page_index, start);
            let digests = writes
                .iter()
                .map(|write| {
                    let mut sha256 = [0; 32];
                    sha256.copy_from_slice(
                        ring::digest::digest(&ring::digest::SHA256, &write.data).as_ref(),
                    );
                    (1, write.data.len() as u64, sha256)
                })
                .collect();
            stream
                .write_all(&message::encode_reply(&ReplicaReply::Digests(digests)))
                .unwrap();
        }
        for start in [0, 4] {
            let ReplicaRequest::WriteChunkBatchIfSequence { writes } = replica(&mut stream)
            else {
                panic!("four-write batch missing");
            };
            assert_eq!(writes.len(), 4);
            assert_eq!(writes[0].page_id.page_index, start);
            stream
                .write_all(&message::encode_reply(&ReplicaReply::Applied))
                .unwrap();
        }
    });
    let mut client =
        RemoteReplicaClient::connect(&address, &ReplicaClientOptions::default()).unwrap();
    let chunk_id = ChunkId::new(1, 6).unwrap();
    let writes: Vec<_> = (0..8)
        .map(|index| {
            (
                PageId {
                    chunk_id: chunk_id.clone(),
                    page_index: index,
                },
                0,
                b"page".as_slice(),
                0,
            )
        })
        .collect();
    assert_eq!(
        client
            .write_chunks_if_sequence_with_digests(&writes)
            .unwrap()
            .len(),
        8
    );
    client.write_chunks_if_sequence_pipelined(&writes).unwrap();
    worker.join().unwrap();
}

#[test]
fn uncertain_digest_batch_is_not_replayed() {
    let socket = listener();
    let address = socket.local_addr().unwrap().to_string();
    let worker = thread::spawn(move || {
        let mut stream = accept(&socket);
        assert!(matches!(
            replica(&mut stream),
            ReplicaRequest::WriteChunkBatchIfSequenceDigest { writes }
                if writes.len() == 4
        ));
        drop(stream);
        thread::sleep(Duration::from_millis(50));
        assert!(matches!(
            socket.accept(),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock
        ));
    });
    let mut client =
        RemoteReplicaClient::connect(&address, &ReplicaClientOptions::default()).unwrap();
    let chunk_id = ChunkId::new(1, 5).unwrap();
    let writes: Vec<_> = (0..4)
        .map(|index| {
            (
                PageId {
                    chunk_id: chunk_id.clone(),
                    page_index: index,
                },
                0,
                b"page".as_slice(),
                0,
            )
        })
        .collect();
    assert!(client
        .write_chunks_if_sequence_with_digests(&writes)
        .is_err());
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
                // Both groups write on their verified preflight connections.
                // Each read opens one later connection.
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
                    let writing = turn == 0;
                    let pages: Vec<_> = (index as u64..32).step_by(2).collect();
                    for (batch_index, batch) in pages.chunks(8).enumerate() {
                        if writing {
                            if index == 0 {
                                let ReplicaRequest::WriteChunkBatchIfSequenceDigest { writes } =
                                    replica(&mut stream)
                                else {
                                    panic!("page digest batch missing");
                                };
                                assert_eq!(writes.len(), batch.len());
                                let mut digests = Vec::with_capacity(writes.len());
                                for (write, &page_index) in writes.iter().zip(batch) {
                                    let start = page_index as usize * 4;
                                    assert_eq!(write.page_id.page_index, page_index);
                                    assert_eq!(write.offset, 0);
                                    assert_eq!(write.expected_sequence, 0);
                                    assert_eq!(
                                        write.data,
                                        object[start..object.len().min(start + 4)]
                                    );
                                    let mut sha256 = [0; 32];
                                    sha256.copy_from_slice(
                                        ring::digest::digest(
                                            &ring::digest::SHA256,
                                            &write.data,
                                        )
                                        .as_ref(),
                                    );
                                    digests.push((1, write.data.len() as u64, sha256));
                                }
                                stream
                                    .write_all(&message::encode_reply(&ReplicaReply::Digests(
                                        digests,
                                    )))
                                    .unwrap();
                            } else {
                                if batch_index == 0 {
                                    assert!(matches!(
                                        replica(&mut stream),
                                        ReplicaRequest::WriteChunkBatchIfSequenceDigest { writes }
                                            if writes.len() == 8
                                    ));
                                    stream
                                        .write_all(&message::encode_failure(
                                            StatusCode::UnknownRequest,
                                            "older peer",
                                        ))
                                        .unwrap();
                                    assert!(matches!(
                                        replica(&mut stream),
                                        ReplicaRequest::WriteChunkBatchIfSequence { writes }
                                            if writes.len() == 8
                                    ));
                                    stream
                                        .write_all(&message::encode_failure(
                                            StatusCode::UnknownRequest,
                                            "older peer",
                                        ))
                                        .unwrap();
                                }
                                let ReplicaRequest::WriteChunkIfSequence { page_id, .. } =
                                    replica(&mut stream)
                                else {
                                    panic!("fallback write missing");
                                };
                                assert_eq!(page_id.page_index, batch[0]);
                                stream
                                    .write_all(&message::encode_reply(&ReplicaReply::Applied))
                                    .unwrap();
                                let mut responses = Vec::new();
                                for &page_index in &batch[1..] {
                                    let ReplicaRequest::WriteChunkIfSequence { page_id, .. } = replica(&mut stream) else { panic!("fallback write missing"); };
                                    assert_eq!(page_id.page_index, page_index);
                                    responses.push(message::encode_reply(&ReplicaReply::Applied));
                                }
                                for response in responses { stream.write_all(&response).unwrap(); }
                            }
                            if index == 0 {
                                continue;
                            }
                            assert!(matches!(replica(&mut stream), ReplicaRequest::DigestChunk { .. }));
                            stream.write_all(&message::encode_failure(
                                StatusCode::UnknownRequest,
                                "older peer",
                            )).unwrap();
                        }
                        let ReplicaRequest::ReadChunkBatch { reads } = replica(&mut stream) else {
                            panic!("read batch missing");
                        };
                        assert_eq!(reads.len(), batch.len());
                        let mut results = Vec::new();
                        for ((page_id, offset, length), &page_index) in reads.iter().zip(batch) {
                            let start = page_index as usize * 4;
                            let data = object[start..object.len().min(start + 4)].to_vec();
                            assert_eq!(page_id.page_index, page_index);
                            assert_eq!(*offset, 0);
                            assert_eq!(*length, data.len() as u64);
                            results.push(ReplicaReadResult {
                                data,
                                write_sequence: 1,
                                checksum: String::new(),
                            });
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
                        stream.write_all(&message::encode_reply(&ReplicaReply::Reads(results))).unwrap();
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
            MetaRequest::PlanObjectWriteRoutes {
                length: 64,
                replica_count: 2,
                ..
            }
        ));
        reply(
            &mut stream,
            MetaReply::WritePlanRoutes {
                current: None,
                plan: PlacementAssignment {
                    bucket: "bucket".into(),
                    key: "key".into(),
                    length: 64,
                    node_ids: groups.iter().map(|g| g.placement_id.clone()).collect(),
                    skipped_draining: 0,
                    skipped_offline: 0,
                    skipped_capacity: 0,
                    plan_checksum: "plan".into(),
                },
                routes: groups.iter().cloned().map(|group| ChunkGroupRoute {
                    version: 1, group: Some(group), fence: vec![],
                }).collect(),
            },
        );
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
        assert!(matches!(
            replica(&mut first),
            ReplicaRequest::ReadChunkBatch { reads } if reads.len() == 3
        ));
        first.write_all(&message::encode_failure(StatusCode::UnknownRequest, "older peer")).unwrap();
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
