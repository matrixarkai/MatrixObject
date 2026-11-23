// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use crate::cluster::chunk_log::LOG_RECORD_BYTES;
use crate::cluster::log_op::{OP_APPEND, OP_IDENTITY, OP_READ};
use crate::cluster::AppendIdentity;
fn identity(n: u8, offset: u64, length: usize) -> AppendIdentity {
    AppendIdentity {
        object_incarnation: [1; 16],
        attempt_id: [n; 16],
        request_offset: offset,
        request_length: length as u64,
    }
}
fn identified(eps: &[String], epoch: u64) -> ChunkLogWriter {
    ChunkLogWriter::open_identified(eps, V, C, 1, epoch, options()).unwrap()
}
fn check(nodes: &[Node], physical: u64, logical: u64, length: usize, id: AppendIdentity) {
    let tag = id
        .record_tag(V, C, 1, physical, logical, length as u32)
        .unwrap();
    for node in nodes {
        let mut client = RemoteReplicaClient::connect(&node.addr, &options()).unwrap();
        let record = client
            .log_identity(V, C, 1, physical)
            .unwrap()
            .record_identity()
            .unwrap();
        assert_eq!(
            (record.offset, record.length, record.attempt_tag),
            (physical, length as u32, Some(tag))
        );
    }
}
#[test]
fn record_tags_pin_the_context_encoding_and_bind_every_range_dimension() {
    let id = identity(2, 100, 10);
    let tag = id.record_tag(V, C, 1, 0, 100, 10).unwrap();
    assert_eq!(
        tag,
        [172, 82, 166, 151, 65, 216, 151, 242, 116, 149, 230, 161, 65, 89, 254, 30]
    );
    let changed = AppendIdentity {
        object_incarnation: [3; 16],
        ..id
    };
    assert_ne!(tag, changed.record_tag(V, C, 1, 0, 100, 10).unwrap());
    let changed = AppendIdentity {
        attempt_id: [3; 16],
        ..id
    };
    assert_ne!(tag, changed.record_tag(V, C, 1, 0, 100, 10).unwrap());
    let changed = AppendIdentity {
        request_offset: 90,
        request_length: 20,
        ..id
    };
    assert_ne!(tag, changed.record_tag(V, C, 1, 0, 100, 10).unwrap());
    assert_ne!(tag, id.record_tag(V + 1, C, 1, 0, 100, 10).unwrap());
    assert_ne!(tag, id.record_tag(V, C + 1, 1, 0, 100, 10).unwrap());
    assert_ne!(tag, id.record_tag(V, C, 2, 0, 100, 10).unwrap());
    assert_ne!(tag, id.record_tag(V, C, 1, 1, 100, 10).unwrap());
    assert_ne!(tag, id.record_tag(V, C, 1, 0, 101, 9).unwrap());
    assert_ne!(tag, id.record_tag(V, C, 1, 0, 100, 9).unwrap());
}
#[test]
fn invalid_request_ranges_and_offsets_admit_no_payload() {
    let (_dir, nodes) = cluster();
    let eps = endpoints(&nodes);
    let mut w = identified(&eps, 7);
    for id in [identity(1, 0, 0), identity(1, u64::MAX, 2)] {
        assert!(w.append_identified(0, 0, b"x", id).is_err());
    }
    assert!(w
        .append_identified(1, 100, b"x", identity(1, 100, 1))
        .is_err());
    assert!(w
        .append_identified(0, 99, b"x", identity(1, 100, 1))
        .is_err());
    assert!(w
        .append_identified(0, 100, b"xx", identity(1, 100, 1))
        .is_err());
    assert_eq!(w.length(), 0);
    assert!(nodes.iter().all(|n| n.tail().length == 0));
}
#[test]
fn correlated_success_needs_no_identity_queries_after_capability_probes() {
    let (_dir, nodes) = cluster();
    let proxies: Vec<FaultProxy> = nodes
        .iter()
        .map(|n| FaultProxy::new(n.addr.clone(), u32::MAX, false))
        .collect();
    let eps: Vec<String> = proxies.iter().map(|p| p.addr.clone()).collect();
    let mut w = identified(&eps, 7);
    for proxy in &proxies {
        assert_eq!(
            proxy
                .operations
                .lock()
                .unwrap()
                .iter()
                .filter(|op| **op == OP_IDENTITY)
                .count(),
            1
        );
    }
    let a = identity(1, 100, 4);
    let b = identity(2, 104, 4);
    assert_eq!(w.append_identified(0, 100, b"same", a).unwrap(), 4);
    assert_eq!(w.append_identified(4, 104, b"same", b).unwrap(), 8);
    for proxy in &proxies {
        let ops = proxy.operations.lock().unwrap();
        assert_eq!(ops.iter().filter(|op| **op == OP_IDENTITY).count(), 1);
        assert_eq!(ops.iter().filter(|op| **op == OP_APPEND).count(), 2);
        assert_eq!(ops.iter().filter(|op| **op == OP_READ).count(), 0);
    }
    check(&nodes, 0, 100, 4, a);
    check(&nodes, 4, 104, 4, b);
}
#[test]
fn large_appends_keep_identical_record_boundaries_on_every_replica() {
    let (_dir, nodes) = cluster();
    let eps = endpoints(&nodes);
    let mut w = identified(&eps, 7);
    let data = bytes(9, 3 * LOG_RECORD_BYTES + 17);
    let id = identity(1, 1000, data.len());
    assert_eq!(
        w.append_identified(0, 1000, &data, id).unwrap(),
        data.len() as u64
    );
    let mut offset = 0;
    for piece in data.chunks(LOG_RECORD_BYTES) {
        check(&nodes, offset, 1000 + offset, piece.len(), id);
        offset += piece.len() as u64;
    }
    assert_eq!(reader(&eps).read(0, data.len()).unwrap(), data);
}
#[test]
fn one_unsettled_attempt_prevents_unbounded_new_admission() {
    let (_dir, mut nodes) = cluster();
    let eps = endpoints(&nodes);
    let mut w = identified(&eps, 7);
    nodes[2].halt();
    let data = b"pending";
    let id = identity(1, 100, data.len());
    assert!(matches!(
        w.append_identified(0, 100, data, id),
        Err(ChunkLogError::NoQuorum { .. })
    ));
    for _ in 0..1000 {
        assert!(matches!(
            w.append_identified(data.len() as u64, 107, b"other", identity(2, 107, 5)),
            Err(ChunkLogError::Refused(_))
        ));
    }
    assert!(w.append_identified(0, 100, b"changed", id).is_err());
    assert_eq!(w.length(), data.len() as u64);
    assert_eq!(w.durable_length(), 0);
    nodes[2].restart();
    assert_eq!(w.settle().unwrap(), data.len() as u64);
    check(&nodes, 0, 100, data.len(), id);
}
#[test]
fn an_exact_pending_retry_settles_without_adding_another_record() {
    let (_dir, mut nodes) = cluster();
    let eps = endpoints(&nodes);
    let mut w = identified(&eps, 7);
    nodes[2].halt();
    let id = identity(1, 100, 4);
    assert!(matches!(
        w.append_identified(0, 100, b"same", id),
        Err(ChunkLogError::NoQuorum { .. })
    ));
    nodes[2].restart();
    assert_eq!(w.append_identified(0, 100, b"same", id).unwrap(), 4);
    assert!(nodes.iter().all(|n| n.tail().length == 4));
    check(&nodes, 0, 100, 4, id);
    assert!(w.append_identified(0, 100, b"same", id).is_err());
    assert_eq!(w.length(), 4);
}
#[test]
fn a_lost_reply_requires_its_own_durable_tag_before_crediting_remote_length() {
    let (_dir, nodes) = cluster();
    let proxy = FaultProxy::new(nodes[2].addr.clone(), OP_APPEND, true);
    let mut eps = endpoints(&nodes);
    eps[2] = proxy.addr.clone();
    let mut w = identified(&eps, 7);
    let id = identity(1, 100, 4);
    assert!(matches!(
        w.append_identified(0, 100, b"same", id),
        Err(ChunkLogError::NoQuorum { .. })
    ));
    assert_eq!(w.durable_length(), 0);
    assert_eq!(nodes[2].tail().length, 4);
    assert_eq!(w.settle().unwrap(), 4);
    check(&nodes, 0, 100, 4, id);
    let ops = proxy.operations.lock().unwrap();
    assert_eq!(ops.iter().filter(|op| **op == OP_APPEND).count(), 1);
    assert_eq!(ops.iter().filter(|op| **op == OP_IDENTITY).count(), 2);
}
#[test]
fn a_lost_fragment_reply_resumes_at_the_original_next_boundary() {
    let (_dir, nodes) = cluster();
    let proxy = FaultProxy::new(nodes[2].addr.clone(), OP_APPEND, true);
    let mut eps = endpoints(&nodes);
    eps[2] = proxy.addr.clone();
    let mut w = identified(&eps, 7);
    let data = bytes(3, 2 * LOG_RECORD_BYTES + 17);
    let id = identity(1, 1000, data.len());
    assert!(matches!(
        w.append_identified(0, 1000, &data, id),
        Err(ChunkLogError::NoQuorum { .. })
    ));
    assert_eq!(nodes[2].tail().length, LOG_RECORD_BYTES as u64);
    assert_eq!(w.settle().unwrap(), data.len() as u64);
    let mut offset = 0;
    for piece in data.chunks(LOG_RECORD_BYTES) {
        check(&nodes, offset, 1000 + offset, piece.len(), id);
        offset += piece.len() as u64;
    }
    assert_eq!(
        proxy
            .operations
            .lock()
            .unwrap()
            .iter()
            .filter(|op| **op == OP_APPEND)
            .count(),
        3
    );
}
#[test]
fn a_foreign_or_legacy_record_with_equal_bytes_cannot_acknowledge_the_pending_attempt() {
    for foreign_id in [None, Some([99; 16])] {
        let (_dir, mut nodes) = cluster();
        let eps = endpoints(&nodes);
        let mut w = identified(&eps, 7);
        nodes[2].halt();
        let id = identity(1, 100, 4);
        assert!(matches!(
            w.append_identified(0, 100, b"same", id),
            Err(ChunkLogError::NoQuorum { .. })
        ));
        nodes[2].restart();
        let mut client = RemoteReplicaClient::connect(&nodes[2].addr, &options()).unwrap();
        let request = match foreign_id {
            Some(tag) => LogOpRequest::append_identified(V, C, 1, 7, 0, b"same".to_vec(), tag),
            None => LogOpRequest::append(V, C, 1, 7, 0, b"same".to_vec()),
        };
        client.log_call(request).unwrap();
        assert_eq!(
            w.settle().unwrap_err(),
            ChunkLogError::AttemptMismatch { offset: 0 }
        );
        assert_eq!(w.durable_length(), 0);
        assert_eq!(
            w.settle().unwrap_err(),
            ChunkLogError::AttemptMismatch { offset: 0 }
        );
        assert_eq!(
            client
                .log_identity(V, C, 1, 0)
                .unwrap()
                .record_identity()
                .unwrap()
                .attempt_tag,
            foreign_id
        );
    }
}
#[test]
fn an_identity_probe_failure_admits_no_data_and_never_downgrades() {
    let (_dir, nodes) = cluster();
    let proxy = FaultProxy::new(nodes[2].addr.clone(), OP_IDENTITY, false);
    let mut eps = endpoints(&nodes);
    eps[2] = proxy.addr.clone();
    assert!(ChunkLogWriter::open_identified(&eps, V, C, 1, 7, options()).is_err());
    for node in &nodes {
        assert!(!node
            .dir
            .join("logchunks")
            .join(format!("{V:016x}-{C:016x}.log"))
            .exists());
    }
    assert_eq!(
        proxy
            .operations
            .lock()
            .unwrap()
            .iter()
            .filter(|op| **op == OP_APPEND)
            .count(),
        0
    );
    let mut legacy = writer(&eps, 7);
    assert_eq!(legacy.append(b"legacy").unwrap(), 6);
}
#[test]
fn writer_modes_and_duplicate_replica_addresses_are_refused() {
    let (_dir, nodes) = cluster();
    let eps = endpoints(&nodes);
    assert!(
        ChunkLogWriter::open_identified(&vec![eps[0].clone(); 3], V, C, 1, 7, options()).is_err()
    );
    let mut legacy = writer(&eps, 7);
    assert!(legacy
        .append_identified(0, 100, b"same", identity(1, 100, 4))
        .is_err());
    drop(legacy);
    let mut w = identified(&eps, 8);
    assert!(w.append(b"legacy").is_err());
    assert_eq!(w.length(), 0);
}
#[test]
fn identified_records_and_sealed_footer_survive_all_replica_restarts() {
    let (_dir, mut nodes) = cluster();
    let eps = endpoints(&nodes);
    let mut w = identified(&eps, 7);
    let id = identity(1, 100, 4);
    w.append_identified(0, 100, b"same", id).unwrap();
    let footer = w.seal_footer(true).unwrap();
    drop(w);
    for node in &mut nodes {
        node.restart();
    }
    check(&nodes, 0, 100, 4, id);
    assert_eq!(reader(&eps).read_sealed(0, 4, &footer).unwrap(), b"same");
    assert!(matches!(
        ChunkLogWriter::open_identified(&eps, V, C, 1, 8, options()),
        Err(ChunkLogError::Sealed)
    ));
}

#[test]
fn a_reused_identity_with_different_bytes_cannot_confirm_the_pending_payload() {
    let (_dir, mut nodes) = cluster();
    let eps = endpoints(&nodes);
    let mut w = identified(&eps, 7);
    nodes[2].halt();
    let id = identity(1, 100, 4);
    assert!(matches!(
        w.append_identified(0, 100, b"same", id),
        Err(ChunkLogError::NoQuorum { .. })
    ));
    nodes[2].restart();
    let mut client = RemoteReplicaClient::connect(&nodes[2].addr, &options()).unwrap();
    client
        .log_call(LogOpRequest::append_identified(
            V,
            C,
            1,
            7,
            0,
            b"else".to_vec(),
            id.record_tag(V, C, 1, 0, 100, 4).unwrap(),
        ))
        .unwrap();
    assert_eq!(
        w.settle().unwrap_err(),
        ChunkLogError::AttemptMismatch { offset: 0 }
    );
    assert_eq!(w.durable_length(), 0);
    assert_eq!(client.log_read(V, C, 1, 0, 4).unwrap().data, b"else");
}
