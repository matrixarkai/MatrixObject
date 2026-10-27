// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

#![cfg(all(target_os = "linux", feature = "replica-server"))]

mod node_regime;
use matrixobjectstore_rs::cluster::{RemoteReplicaClient, ReplicaClientOptions};
use matrixobjectstore_rs::{ChunkId, ObjectError, PageId};
use node_regime::processes;

fn page(index: u64) -> PageId {
    PageId {
        chunk_id: ChunkId::new(1, 1).unwrap(),
        page_index: index,
    }
}

fn node() -> processes::NodeProcess {
    processes::start(
        env!("CARGO_BIN_EXE_chunkserver"),
        &[
            ("MATRIXOBJECT_REPLICA_ADDR", "127.0.0.1:0"),
            ("MATRIXOBJECT_REPLICA_CAPACITY_BYTES", "4096"),
            ("MATRIXOBJECT_REPLICA_MAX_PAGES", "2"),
        ],
    )
}

fn client(node: &processes::NodeProcess) -> RemoteReplicaClient {
    assert_ne!(node.pid, std::process::id());
    assert!(node.startup_line.contains("capacity_bytes=4096"));
    assert!(node.startup_line.contains("max_pages=2"));
    RemoteReplicaClient::connect(&node.endpoint, &ReplicaClientOptions::default()).unwrap()
}

#[test]
fn sparse_writes_are_charged_for_the_whole_allocation() {
    let node = node();
    let mut c = client(&node);
    c.write_chunk(page(1), 4095, b"a").unwrap();
    let before = c.head_chunk(&page(1)).unwrap();
    assert!(matches!(
        c.write_chunk(page(1), 4096, b"b"),
        Err(ObjectError::PreconditionFailed(_))
    ));
    assert_eq!(c.head_chunk(&page(1)).unwrap(), before);
    assert_eq!(c.read_chunk(&page(1), 4095, 1).unwrap().data, b"a");
    // An empty payload can also extend the underlying allocation.
    assert!(matches!(
        c.write_chunk(page(2), 4097, b""),
        Err(ObjectError::PreconditionFailed(_))
    ));
    assert!(matches!(
        c.write_chunk(page(2), i64::MAX as u64 - 1, b"x"),
        Err(ObjectError::PreconditionFailed(_))
    ));
    assert_eq!(c.ping().unwrap(), 1);
}

#[test]
fn byte_budget_is_shared_by_all_pages() {
    let node = node();
    let mut c = client(&node);
    c.write_chunk(page(1), 0, &[1; 2048]).unwrap();
    c.write_chunk(page(2), 0, &[2; 2048]).unwrap();
    assert!(matches!(
        c.write_chunk(page(2), 2048, &[3]),
        Err(ObjectError::PreconditionFailed(_))
    ));
    // An overwrite does not need more payload space and remains available at capacity.
    c.write_chunk(page(1), 0, b"ok").unwrap();
    assert_eq!(c.read_chunk(&page(1), 0, 2).unwrap().data, b"ok");
    assert_eq!(c.read_chunk(&page(2), 0, 2048).unwrap().data, vec![2; 2048]);
}

#[test]
fn zero_length_pages_cannot_bypass_the_page_budget() {
    let node = node();
    let mut c = client(&node);
    c.write_chunk(page(1), 0, b"").unwrap();
    c.write_chunk(page(2), 0, b"").unwrap();
    assert!(matches!(
        c.write_chunk(page(3), 0, b""),
        Err(ObjectError::PreconditionFailed(_))
    ));
    assert_eq!(c.ping().unwrap(), 2);
    c.write_chunk(page(1), 0, b"still writable").unwrap();
    assert_eq!(
        c.read_chunk(&page(1), 0, 14).unwrap().data,
        b"still writable"
    );
}

#[test]
fn zero_storage_limits_are_rejected_at_startup() {
    use matrixobjectstore_rs::cluster::{ReplicaNodeOptions, ReplicaNodeServer};
    for (capacity_bytes, max_pages) in [(0, 2), (4096, 0)] {
        let options = ReplicaNodeOptions {
            capacity_bytes,
            max_pages,
            ..Default::default()
        };
        match ReplicaNodeServer::bind("127.0.0.1:0", options) {
            Err(err) => assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput),
            Ok(_) => panic!("zero storage limits were accepted"),
        }
    }
}
