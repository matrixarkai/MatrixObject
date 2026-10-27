// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

#![cfg(all(feature = "meta-server", feature = "replica-server"))]

use matrixobjectstore_rs::cluster::{
    message::{decode_failure, encode_request},
    meta_message::encode_meta_request,
    MetaNodeOptions, MetaNodeServer, MetaReply, MetaRequest, ReplicaNodeOptions, ReplicaNodeServer,
    ReplicaReply, ReplicaRequest, RequestCode, StatusCode,
};
use matrixobjectstore_rs::{ChunkId, PageId};
use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    time::Duration,
};

struct Node {
    address: SocketAddr,
    stop: Box<dyn Fn()>,
    worker: Option<std::thread::JoinHandle<std::io::Result<()>>>,
}
impl Node {
    fn replica(limit: usize) -> Self {
        let server = ReplicaNodeServer::bind(
            "127.0.0.1:0",
            ReplicaNodeOptions {
                max_payload_bytes: limit,
                worker_threads: 1,
                read_timeout: Duration::from_millis(300),
                ..Default::default()
            },
        )
        .unwrap();
        let address = server.local_addr().unwrap();
        let stop = server.shutdown_handle();
        Self {
            address,
            stop: Box::new(move || stop.stop()),
            worker: Some(std::thread::spawn(move || server.serve())),
        }
    }
    fn meta(limit: usize, count: usize) -> Self {
        let server = MetaNodeServer::bind(
            "127.0.0.1:0",
            MetaNodeOptions {
                max_payload_bytes: limit,
                worker_threads: 1,
                read_timeout: Duration::from_millis(300),
                ..Default::default()
            },
        )
        .unwrap();
        server.seed_data_nodes(count, 1024 * 1024).unwrap();
        let address = server.local_addr().unwrap();
        let stop = server.shutdown_handle();
        Self {
            address,
            stop: Box::new(move || stop.stop()),
            worker: Some(std::thread::spawn(move || server.serve())),
        }
    }
    fn connect(&self) -> TcpStream {
        let stream = TcpStream::connect(self.address).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        stream
    }
}
impl Drop for Node {
    fn drop(&mut self) {
        (self.stop)();
        // Wake the blocking accept loop on builds without interruptible shutdown.
        let _ = TcpStream::connect_timeout(&self.address, Duration::from_secs(1));
        self.worker.take().unwrap().join().unwrap().unwrap();
    }
}
fn page(index: u64) -> PageId {
    PageId {
        chunk_id: ChunkId::new(1, 1).unwrap(),
        page_index: index,
    }
}
fn exchange(stream: &mut TcpStream, request: Vec<u8>, limit: usize) -> (u8, Vec<u8>) {
    stream.write_all(&request).unwrap();
    let mut header = [0; 6];
    stream.read_exact(&mut header).unwrap();
    assert_eq!(header[0], 1);
    let size = u32::from_be_bytes(header[2..6].try_into().unwrap()) as usize;
    assert!(size <= limit, "reply payload {size} exceeds {limit}");
    let mut payload = vec![0; size];
    stream.read_exact(&mut payload).unwrap();
    (header[1], payload)
}
fn refused(reply: (u8, Vec<u8>)) {
    assert_eq!(reply.0, StatusCode::PreconditionFailed as u8);
    assert!(!decode_failure(&reply.1).unwrap().is_empty());
}
fn ping(stream: &mut TcpStream, limit: usize, count: u64) {
    let reply = exchange(stream, encode_request(&ReplicaRequest::Ping), limit);
    assert_eq!(reply.0, 0);
    assert_eq!(
        ReplicaReply::decode(RequestCode::Ping, &reply.1).unwrap(),
        ReplicaReply::Pong { page_count: count }
    );
}

#[test]
fn replica_read_budget_includes_overhead_and_keeps_connection_usable() {
    let node = Node::replica(128);
    let mut stream = node.connect();
    assert_eq!(
        exchange(
            &mut stream,
            encode_request(&ReplicaRequest::WriteChunk {
                page_id: page(0),
                offset: 0,
                data: vec![7; 92]
            }),
            128
        )
        .0,
        0
    );
    let reply = exchange(
        &mut stream,
        encode_request(&ReplicaRequest::ReadChunk {
            page_id: page(0),
            offset: 0,
            length: 88,
        }),
        128,
    );
    assert_eq!(reply.0, 0);
    assert_eq!(reply.1.len(), 128);
    match ReplicaReply::decode(RequestCode::ReadChunk, &reply.1).unwrap() {
        ReplicaReply::Read(read) => assert_eq!(read.data, vec![7; 88]),
        _ => panic!("read reply expected"),
    }
    for length in [89, 92, u64::MAX] {
        refused(exchange(
            &mut stream,
            encode_request(&ReplicaRequest::ReadChunk {
                page_id: page(0),
                offset: 0,
                length,
            }),
            128,
        ));
        ping(&mut stream, 128, 1);
    }
}

#[test]
fn replica_head_and_inventory_obey_reply_budget() {
    let node = Node::replica(128);
    let mut stream = node.connect();
    for index in 0..4 {
        assert_eq!(
            exchange(
                &mut stream,
                encode_request(&ReplicaRequest::WriteChunk {
                    page_id: page(index),
                    offset: 0,
                    data: vec![9; 16]
                }),
                128
            )
            .0,
            0
        );
    }
    for offset in [0, 2, 4, 6] {
        assert_eq!(
            exchange(
                &mut stream,
                encode_request(&ReplicaRequest::DiscardChunk {
                    page_id: page(0),
                    offset,
                    length: 1
                }),
                128
            )
            .0,
            0
        );
    }
    refused(exchange(
        &mut stream,
        encode_request(&ReplicaRequest::HeadChunk { page_id: page(0) }),
        128,
    ));
    refused(exchange(
        &mut stream,
        encode_request(&ReplicaRequest::PageInventory {
            after: None,
            limit: 4,
        }),
        128,
    ));
    let reply = exchange(
        &mut stream,
        encode_request(&ReplicaRequest::PageInventory {
            after: None,
            limit: 3,
        }),
        128,
    );
    assert_eq!(reply.0, 0);
    match ReplicaReply::decode(RequestCode::PageInventory, &reply.1).unwrap() {
        ReplicaReply::PageInventory(batch) => {
            assert_eq!(batch.entries.len(), 3);
            assert!(batch.has_more);
        }
        _ => panic!("inventory reply expected"),
    }
    ping(&mut stream, 128, 4);
}

#[test]
fn metadata_roster_budget_keeps_connection_usable() {
    let node = Node::meta(256, 3);
    let mut stream = node.connect();
    refused(exchange(
        &mut stream,
        encode_meta_request(&MetaRequest::ListRootNodes),
        256,
    ));
    let request = MetaRequest::HeartbeatRootNode {
        node_id: "data-0".to_string(),
        used_bytes: 0,
        capacity_bytes: 1024 * 1024,
        draining: false,
        online: true,
        now_ms: 1,
    };
    let reply = exchange(&mut stream, encode_meta_request(&request), 256);
    assert_eq!(reply.0, 0);
    assert!(matches!(
        MetaReply::decode(request.code(), &reply.1).unwrap(),
        MetaReply::PlacementNode(_)
    ));
    let reply = exchange(&mut stream, encode_meta_request(&MetaRequest::Ping), 256);
    assert_eq!(reply.0, 0);
    assert!(matches!(
        MetaReply::decode(MetaRequest::Ping.code(), &reply.1).unwrap(),
        MetaReply::Pong { node_count: 3, .. }
    ));
}

#[test]
fn metadata_failure_text_is_bounded_utf8() {
    let node = Node::meta(64, 1);
    let mut stream = node.connect();
    let request = MetaRequest::ReadObjectRootPlacement {
        bucket: "界".repeat(8),
        key: "界".repeat(10),
    };
    let reply = exchange(&mut stream, encode_meta_request(&request), 64);
    assert_eq!(reply.0, StatusCode::NotFound as u8);
    let text = decode_failure(&reply.1).unwrap();
    assert!(text.contains("no placement"));
    let reply = exchange(&mut stream, encode_meta_request(&MetaRequest::Ping), 64);
    assert_eq!(reply.0, 0);
}

#[test]
fn server_reply_budgets_must_fit_status_and_frame_lengths() {
    let mut limits = vec![0, 4, 23];
    if usize::BITS > 32 {
        limits.push(usize::MAX);
    }
    let directory = std::env::temp_dir().join(format!(
        "matrixobject-budget-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&directory).unwrap();
    for limit in limits {
        let journal = directory.join("replica.journal");
        assert!(ReplicaNodeServer::bind_with_journal(
            "127.0.0.1:0",
            ReplicaNodeOptions {
                max_payload_bytes: limit,
                ..Default::default()
            },
            &journal
        )
        .is_err());
        assert!(
            !journal.exists(),
            "invalid budget must not create a journal"
        );
        assert!(MetaNodeServer::bind_with_snapshot(
            "127.0.0.1:0",
            MetaNodeOptions {
                max_payload_bytes: limit,
                ..Default::default()
            },
            &directory
        )
        .is_err());
        assert!(
            !directory.join("metadata.lock").exists(),
            "invalid budget must not open metadata storage"
        );

        assert!(ReplicaNodeServer::bind(
            "127.0.0.1:0",
            ReplicaNodeOptions {
                max_payload_bytes: limit,
                ..Default::default()
            }
        )
        .is_err());
        assert!(MetaNodeServer::bind(
            "127.0.0.1:0",
            MetaNodeOptions {
                max_payload_bytes: limit,
                ..Default::default()
            }
        )
        .is_err());
    }
    std::fs::remove_dir(&directory).unwrap();
}

#[test]
fn metadata_rejects_large_heartbeat_reply_before_mutation() {
    let node = Node::meta(64, 1);
    let mut stream = node.connect();
    let heartbeat = MetaRequest::HeartbeatRootNode {
        node_id: "data-0".to_string(),
        used_bytes: 0,
        capacity_bytes: 1024 * 1024,
        draining: false,
        online: true,
        now_ms: 100,
    };
    refused(exchange(&mut stream, encode_meta_request(&heartbeat), 64));
    // A refused heartbeat must not refresh the old timestamp and hide expiry.
    let expire = MetaRequest::ExpireRootNodeHeartbeats {
        ttl_ms: 50,
        now_ms: 100,
    };
    let reply = exchange(&mut stream, encode_meta_request(&expire), 64);
    assert_eq!(reply.0, 0);
    assert_eq!(
        MetaReply::decode(expire.code(), &reply.1).unwrap(),
        MetaReply::Expired { offline_count: 1 }
    );
}
