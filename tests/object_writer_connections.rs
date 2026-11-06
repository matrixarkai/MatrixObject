// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#![cfg(all(feature = "meta-server", feature = "replica-server"))]
use matrixobjectstore_rs::{
    cluster::*,
    object_manifest::{ObjectExtent, ObjectManifest, ObjectPageAllocation},
    ChunkId, PageId, ReplicaReadResult,
};
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
fn eight_group_write_plan_uses_one_metadata_exchange() {
    let socket = listener();
    let endpoint = socket.local_addr().unwrap().to_string();
    let reply_socket = socket.try_clone().unwrap();
    let worker = thread::spawn(move || {
        let mut stream = accept(&reply_socket);
        assert!(matches!(
            meta(&mut stream),
            MetaRequest::PlanObjectWriteRoutesWide {
                length: 8192,
                replica_count: 8,
                ..
            }
        ));
        let node_ids: Vec<String> = (1..=8).map(|id| format!("group-{id}")).collect();
        let routes = (1..=8)
            .map(|id| ChunkGroupRoute {
                version: 1,
                group: Some(config(&reply_socket, id)),
                fence: Vec::new(),
            })
            .collect();
        reply(
            &mut stream,
            MetaReply::WritePlanRoutes {
                current: None,
                plan: PlacementAssignment {
                    bucket: "bucket".into(),
                    key: "key".into(),
                    length: 8192,
                    node_ids,
                    skipped_draining: 0,
                    skipped_offline: 0,
                    skipped_capacity: 0,
                    plan_checksum: "plan".into(),
                },
                routes,
            },
        );
        assert_eq!(stream.read(&mut [0]).unwrap(), 0);
    });
    let mut client = RemoteMetaClient::connect(&endpoint, &MetaClientOptions::default()).unwrap();
    let (current, plan, routes) = client
        .plan_object_write_routes("bucket", "key", 8192, 8)
        .unwrap();
    assert!(current.is_none());
    assert_eq!(plan.node_ids.len(), 8);
    assert_eq!(routes.len(), 8);
    drop(client);
    worker.join().unwrap();
}

#[test]
fn four_group_decoder_refuses_an_eight_group_write_plan() {
    let socket = listener();
    let node_ids: Vec<String> = (1..=8).map(|id| format!("group-{id}")).collect();
    let routes = (1..=8)
        .map(|id| ChunkGroupRoute {
            version: 1,
            group: Some(config(&socket, id)),
            fence: Vec::new(),
        })
        .collect();
    let reply = MetaReply::WritePlanRoutes {
        current: None,
        plan: PlacementAssignment {
            bucket: "bucket".into(),
            key: "key".into(),
            length: 8192,
            node_ids,
            skipped_draining: 0,
            skipped_offline: 0,
            skipped_capacity: 0,
            plan_checksum: "plan".into(),
        },
        routes,
    };
    let payload = reply.encode_payload();
    assert_eq!(
        MetaReply::decode(MetaRequestCode::PlanObjectWriteRoutesWide, &payload).unwrap(),
        reply
    );
    assert!(MetaReply::decode(MetaRequestCode::PlanObjectWriteRoutes, &payload).is_err());
}

#[test]
fn older_server_wide_write_plan_fallback_is_remembered() {
    let socket = listener();
    let endpoint = socket.local_addr().unwrap().to_string();
    let reply_socket = socket.try_clone().unwrap();
    let worker = thread::spawn(move || {
        let mut stream = accept(&reply_socket);
        let node_ids: Vec<String> = (1..=8).map(|id| format!("group-{id}")).collect();
        let mut wide_plans = 0;
        let mut scalar_plans = 0;
        for request_index in 0..26 {
            match meta(&mut stream) {
                MetaRequest::PlanObjectWriteRoutesWide { replica_count: 8, .. } => {
                    wide_plans += 1;
                    stream
                        .write_all(&message::encode_failure(
                            StatusCode::UnknownRequest,
                            "unknown eight-group write plan",
                        ))
                        .unwrap();
                }
                MetaRequest::ReadManifest { .. } => {
                    stream
                        .write_all(&message::encode_failure(
                            StatusCode::NotFound,
                            "object missing",
                        ))
                        .unwrap();
                }
                MetaRequest::PlanObjectPlacement { replica_count: 8, .. } => {
                    scalar_plans += 1;
                    reply(
                        &mut stream,
                        MetaReply::Plan(PlacementAssignment {
                            bucket: "bucket".into(),
                            key: "key".into(),
                            length: 8192,
                            node_ids: node_ids.clone(),
                            skipped_draining: 0,
                            skipped_offline: 0,
                            skipped_capacity: 0,
                            plan_checksum: "plan".into(),
                        }),
                    );
                }
                MetaRequest::ReadChunkRoutesWide { placement_ids }
                | MetaRequest::ReadChunkRoutes { placement_ids } => {
                    assert!(!placement_ids.is_empty());
                    assert!(placement_ids.iter().all(|id| node_ids.contains(id)));
                    stream
                        .write_all(&message::encode_failure(
                            StatusCode::UnknownRequest,
                            "unknown route batch",
                        ))
                        .unwrap();
                }
                MetaRequest::ReadChunkRoute { placement_id } => {
                    let id = placement_id
                        .strip_prefix("group-")
                        .unwrap()
                        .parse::<u64>()
                        .unwrap();
                    assert!(node_ids.contains(&placement_id));
                    reply(
                        &mut stream,
                        MetaReply::ChunkRoute(ChunkGroupRoute {
                            version: 1,
                            group: Some(config(&reply_socket, id)),
                            fence: Vec::new(),
                        }),
                    );
                }
                request => panic!("unexpected fallback request {request_index}: {request:?}"),
            }
        }
        assert_eq!(wide_plans, 1);
        assert_eq!(scalar_plans, 2);
        assert_eq!(stream.read(&mut [0]).unwrap(), 0);
    });
    let mut client = RemoteMetaClient::connect(&endpoint, &MetaClientOptions::default()).unwrap();
    for _ in 0..2 {
        let (current, plan, routes) = client
            .plan_object_write_routes("bucket", "key", 8192, 8)
            .unwrap();
        assert!(current.is_none());
        assert_eq!(plan.node_ids.len(), 8);
        assert_eq!(routes.len(), 8);
    }
    drop(client);
    worker.join().unwrap();
}

#[test]
fn rejected_wide_write_plan_is_not_replayed_as_separate_reads() {
    let socket = listener();
    let endpoint = socket.local_addr().unwrap().to_string();
    let worker = thread::spawn(move || {
        let mut stream = accept(&socket);
        assert!(matches!(
            meta(&mut stream),
            MetaRequest::PlanObjectWriteRoutesWide { replica_count: 8, .. }
        ));
        stream
            .write_all(&message::encode_failure(
                StatusCode::InvalidArgument,
                "write plan refused",
            ))
            .unwrap();
        assert_eq!(stream.read(&mut [0]).unwrap(), 0);
    });
    let mut client = RemoteMetaClient::connect(&endpoint, &MetaClientOptions::default()).unwrap();
    assert!(client
        .plan_object_write_routes("bucket", "key", 8192, 8)
        .is_err());
    drop(client);
    worker.join().unwrap();
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
fn sequential_writes_reuse_a_route_matched_group_connection() {
    let data_socket = listener();
    let group = config(&data_socket, 1);
    let data_group = group.clone();
    let data_worker = thread::spawn(move || {
        let mut stream = accept(&data_socket);
        assert!(matches!(
            replica(&mut stream),
            ReplicaRequest::GroupIdentity
        ));
        stream
            .write_all(&message::encode_reply(&ReplicaReply::GroupIdentity(
                data_group.identity.clone().unwrap(),
            )))
            .unwrap();
        for value in [1u8, 2] {
            let ReplicaRequest::WriteChunkIfSequenceDigest {
                data,
                expected_sequence,
                ..
            } = replica(&mut stream)
            else {
                panic!("conditional write digest missing");
            };
            assert_eq!(data, vec![value; 4]);
            assert_eq!(expected_sequence, 0);
            let mut sha256 = [0; 32];
            sha256.copy_from_slice(
                ring::digest::digest(&ring::digest::SHA256, &data).as_ref(),
            );
            stream
                .write_all(&message::encode_reply(&ReplicaReply::Digest {
                    write_sequence: 1,
                    length: 4,
                    sha256,
                }))
                .unwrap();
        }
        assert_eq!(stream.read(&mut [0]).unwrap(), 0);
        assert!(matches!(
            data_socket.accept(),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock
        ));
    });

    let meta_socket = listener();
    let endpoint = meta_socket.local_addr().unwrap().to_string();
    let metadata_group = group.clone();
    let metadata_worker = thread::spawn(move || {
        let mut stream = accept(&meta_socket);
        for (turn, key) in ["first", "second"].into_iter().enumerate() {
            assert!(matches!(
                meta(&mut stream),
                MetaRequest::PlanObjectWriteRoutes {
                    bucket,
                    key: requested,
                    length: 4,
                    replica_count: 1,
                } if bucket == "bucket" && requested == key
            ));
            reply(
                &mut stream,
                MetaReply::WritePlanRoutes {
                    current: None,
                    plan: PlacementAssignment {
                        bucket: "bucket".into(),
                        key: key.into(),
                        length: 4,
                        node_ids: vec![metadata_group.placement_id.clone()],
                        skipped_draining: 0,
                        skipped_offline: 0,
                        skipped_capacity: 0,
                        plan_checksum: format!("plan-{turn}"),
                    },
                    routes: vec![ChunkGroupRoute {
                        version: 1,
                        group: Some(metadata_group.clone()),
                        fence: vec![],
                    }],
                },
            );
            assert!(matches!(
                meta(&mut stream),
                MetaRequest::AllocateObjectPages { page_count: 1 }
            ));
            reply(
                &mut stream,
                MetaReply::ObjectPages(ObjectPageAllocation {
                    format_version: 1,
                    allocation_id: turn as u64 + 1,
                    page_count: 1,
                }),
            );
            let MetaRequest::PublishManifest { outcome, .. } = meta(&mut stream) else {
                panic!("manifest publication missing");
            };
            reply(
                &mut stream,
                MetaReply::Manifest(outcome.after.unwrap()),
            );
        }
        assert_eq!(stream.read(&mut [0]).unwrap(), 0);
    });

    let mut client = RemoteMetaClient::connect(&endpoint, &MetaClientOptions::default()).unwrap();
    let options = ObjectWriteOptions {
        page_bytes: 4,
        target_groups: 1,
        ..Default::default()
    };
    for (key, value) in [("first", 1u8), ("second", 2)] {
        client
            .write_object("bucket", key, None, &[value; 4], &options)
            .unwrap();
    }
    drop(client);
    metadata_worker.join().unwrap();
    data_worker.join().unwrap();
}

#[test]
fn changed_route_replaces_the_cached_group_connection() {
    let old_socket = listener();
    let new_socket = listener();
    let old_group = config(&old_socket, 1);
    let new_group = config(&new_socket, 1);
    let old_identity = old_group.identity.clone().unwrap();
    let new_identity = new_group.identity.clone().unwrap();
    let old_worker = thread::spawn(move || {
        let mut stream = accept(&old_socket);
        assert!(matches!(replica(&mut stream), ReplicaRequest::GroupIdentity));
        stream
            .write_all(&message::encode_reply(&ReplicaReply::GroupIdentity(old_identity)))
            .unwrap();
        assert!(matches!(
            replica(&mut stream),
            ReplicaRequest::WriteChunkIfSequenceDigest { data, .. } if data == vec![1; 4]
        ));
        stream
            .write_all(&message::encode_reply(&ReplicaReply::Digest {
                write_sequence: 1,
                length: 4,
                sha256: ring::digest::digest(&ring::digest::SHA256, &[1; 4])
                    .as_ref()
                    .try_into()
                    .unwrap(),
            }))
            .unwrap();
        assert_eq!(stream.read(&mut [0]).unwrap(), 0);
    });
    let new_worker = thread::spawn(move || {
        let mut stream = accept(&new_socket);
        assert!(matches!(replica(&mut stream), ReplicaRequest::GroupIdentity));
        stream
            .write_all(&message::encode_reply(&ReplicaReply::GroupIdentity(new_identity)))
            .unwrap();
        assert!(matches!(
            replica(&mut stream),
            ReplicaRequest::WriteChunkIfSequenceDigest { data, .. } if data == vec![2; 4]
        ));
        stream
            .write_all(&message::encode_reply(&ReplicaReply::Digest {
                write_sequence: 1,
                length: 4,
                sha256: ring::digest::digest(&ring::digest::SHA256, &[2; 4])
                    .as_ref()
                    .try_into()
                    .unwrap(),
            }))
            .unwrap();
        assert_eq!(stream.read(&mut [0]).unwrap(), 0);
    });

    let meta_socket = listener();
    let endpoint = meta_socket.local_addr().unwrap().to_string();
    let metadata_worker = thread::spawn(move || {
        let mut stream = accept(&meta_socket);
        for (turn, (key, group)) in [
            ("old", old_group),
            ("new", new_group),
        ]
        .into_iter()
        .enumerate()
        {
            assert!(matches!(meta(&mut stream), MetaRequest::PlanObjectWriteRoutes { .. }));
            reply(
                &mut stream,
                MetaReply::WritePlanRoutes {
                    current: None,
                    plan: PlacementAssignment {
                        bucket: "bucket".into(),
                        key: key.into(),
                        length: 4,
                        node_ids: vec![group.placement_id.clone()],
                        skipped_draining: 0,
                        skipped_offline: 0,
                        skipped_capacity: 0,
                        plan_checksum: format!("plan-{turn}"),
                    },
                    routes: vec![ChunkGroupRoute {
                        version: 1,
                        group: Some(group),
                        fence: vec![],
                    }],
                },
            );
            assert!(matches!(
                meta(&mut stream),
                MetaRequest::AllocateObjectPages { page_count: 1 }
            ));
            reply(
                &mut stream,
                MetaReply::ObjectPages(ObjectPageAllocation {
                    format_version: 1,
                    allocation_id: turn as u64 + 1,
                    page_count: 1,
                }),
            );
            let MetaRequest::PublishManifest { outcome, .. } = meta(&mut stream) else {
                panic!("manifest publication missing");
            };
            reply(&mut stream, MetaReply::Manifest(outcome.after.unwrap()));
        }
        assert_eq!(stream.read(&mut [0]).unwrap(), 0);
    });

    let mut client = RemoteMetaClient::connect(&endpoint, &MetaClientOptions::default()).unwrap();
    let options = ObjectWriteOptions {
        page_bytes: 4,
        target_groups: 1,
        ..Default::default()
    };
    client
        .write_object("bucket", "old", None, &[1; 4], &options)
        .unwrap();
    client
        .write_object("bucket", "new", None, &[2; 4], &options)
        .unwrap();
    drop(client);
    metadata_worker.join().unwrap();
    old_worker.join().unwrap();
    new_worker.join().unwrap();
}

#[test]
fn sequential_object_reads_reuse_one_verified_group_connection() {
    let data = b"abcdefgh";
    let data_socket = listener();
    let group = config(&data_socket, 1);
    let route = ChunkGroupRoute {
        version: 1,
        group: Some(group.clone()),
        fence: vec![],
    };
    let checksum = |bytes: &[u8]| {
        bytes.iter().fold(0xcbf29ce484222325u64, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
        })
    };
    let manifest = ObjectManifest {
        format_version: 1,
        bucket: "bucket".into(),
        key: "key".into(),
        generation: 1,
        length: data.len() as u64,
        extents: vec![
            ObjectExtent {
                group: group.placement_id.clone(),
                volume_id: 1,
                chunk_id: 1,
                page_index: 0,
                object_offset: 0,
                page_offset: 0,
                length: 4,
                write_sequence: 1,
                data_checksum: checksum(&data[..4]),
            },
            ObjectExtent {
                group: group.placement_id.clone(),
                volume_id: 1,
                chunk_id: 1,
                page_index: 1,
                object_offset: 4,
                page_offset: 0,
                length: 4,
                write_sequence: 1,
                data_checksum: checksum(&data[4..]),
            },
        ],
        deleted: false,
    };

    let data_manifest = manifest.clone();
    let data_worker = thread::spawn(move || {
        let mut stream = accept(&data_socket);
        assert!(matches!(replica(&mut stream), ReplicaRequest::GroupIdentity));
        stream
            .write_all(&message::encode_reply(&ReplicaReply::GroupIdentity(
                group.identity.unwrap(),
            )))
            .unwrap();
        for _ in 0..2 {
            let ReplicaRequest::ReadChunkBatch { reads } = replica(&mut stream) else {
                panic!("object read batch missing");
            };
            assert_eq!(reads.len(), 2);
            let results = reads
                .into_iter()
                .map(|(page, offset, length)| {
                    assert_eq!(offset, 0);
                    assert_eq!(length, 4);
                    let start = page.page_index as usize * 4;
                    ReplicaReadResult {
                        data: data[start..start + 4].to_vec(),
                        write_sequence: 1,
                        checksum: String::new(),
                    }
                })
                .collect();
            stream
                .write_all(&message::encode_reply(&ReplicaReply::Reads(results)))
                .unwrap();
        }
        assert_eq!(stream.read(&mut [0]).unwrap(), 0);
        assert!(
            matches!(data_socket.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock),
            "sequential reads opened another data connection"
        );
        data_manifest
    });

    let metadata_socket = listener();
    let endpoint = metadata_socket.local_addr().unwrap().to_string();
    let metadata_manifest = manifest.clone();
    let metadata_worker = thread::spawn(move || {
        let mut stream = accept(&metadata_socket);
        for _ in 0..2 {
            assert!(matches!(
                meta(&mut stream),
                MetaRequest::ReadManifestRoutesWide { .. }
            ));
            reply(
                &mut stream,
                MetaReply::ManifestRoutes {
                    manifest: metadata_manifest.clone(),
                    routes: vec![route.clone()],
                },
            );
        }
        assert_eq!(stream.read(&mut [0]).unwrap(), 0);
    });

    let mut client = RemoteMetaClient::connect(&endpoint, &MetaClientOptions::default()).unwrap();
    for _ in 0..2 {
        let (read_manifest, bytes) = client
            .read_object("bucket", "key", data.len(), &ReplicaClientOptions::default())
            .unwrap();
        assert_eq!(read_manifest, manifest);
        assert_eq!(bytes, data);
    }
    drop(client);
    metadata_worker.join().unwrap();
    assert_eq!(data_worker.join().unwrap(), manifest);
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
                // Both groups write and read on their verified preflight connections.
                let mut retained_write = None;
                for turn in 0..2 {
                    let mut stream = if turn == 1 {
                        retained_write.take().unwrap()
                    } else {
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
                        stream
                    };
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
                    if writing {
                        retained_write = Some(stream);
                    } else {
                        assert_eq!(stream.read(&mut [0]).unwrap(), 0);
                    }
                }
                if let Some(mut write_stream) = retained_write {
                    assert_eq!(write_stream.read(&mut [0]).unwrap(), 0);
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
            MetaRequest::ReadManifestRoutesWide { .. }
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
