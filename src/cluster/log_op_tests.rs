// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use crate::cluster::log_chunk_store::LogChunkStore;
use crate::cluster::log_op::*;
use std::path::{Path, PathBuf};

struct Dir(PathBuf);
impl Dir {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!("logop-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn node(dir: Option<&Path>) -> NodeReplica {
    NodeReplica {
        used_bytes: 0,
        service: MatrixReplicaService::new(),
        journal: None,
        journal_checkpoint_after_bytes: 0,
        staged: None,
        maintenance: false,
        max_pages: 65_536,
        #[cfg(all(feature = "chunk-consensus", feature = "meta-server"))]
        applied_membership: None,
        failed: false,
        log_chunks: dir.map(|dir| LogChunkStore::open(dir).unwrap()),
    }
}
fn call(node: &mut NodeReplica, op: LogOpRequest) -> Result<LogOpReply, ObjectError> {
    match apply_locked(node, ReplicaRequest::LogOp(op), MAX_PAYLOAD_BYTES, true)? {
        ReplicaReply::LogOp(reply) => Ok(reply),
        other => panic!("unexpected reply {other:?}"),
    }
}
fn bytes(seed: u8, n: usize) -> Vec<u8> {
    (0..n).map(|i| seed.wrapping_add((i * 5) as u8)).collect()
}
const V: u64 = 3;
const C: u64 = 11;

#[test]
fn the_verb_round_trips_on_the_wire_and_an_older_replica_would_not_know_it() {
    assert_eq!(RequestCode::from_u8(27), Some(RequestCode::LogOp));
    assert_eq!(RequestCode::from_u8(28), None);
    for op in [
        LogOpRequest::append(V, C, 1, 5, 100, bytes(1, 300)),
        LogOpRequest::seal(V, C, 1, 5, true),
        LogOpRequest::seal(V, C, 1, 5, false),
        LogOpRequest::truncate(V, C, 1, 6, 40),
        LogOpRequest::tail(V, C, 1),
        LogOpRequest::read(V, C, 1, 10, 50),
    ] {
        let request = ReplicaRequest::LogOp(op.clone());
        assert_eq!(request.code(), RequestCode::LogOp);
        let payload = request.encode_payload();
        match ReplicaRequest::decode(RequestCode::LogOp, &payload).unwrap() {
            ReplicaRequest::LogOp(back) => assert_eq!(back, op),
            other => panic!("{other:?}"),
        }
        // a byte more is refused
        let mut longer = payload.clone();
        longer.push(0);
        assert!(ReplicaRequest::decode(RequestCode::LogOp, &longer).is_err());
    }
    let reply = LogOpReply {
        length: 9,
        durable_length: 9,
        epoch: 2,
        sealed: true,
        crc: 77,
        sha256: vec![4; 32],
        data: bytes(2, 9),
    };
    let payload = ReplicaReply::LogOp(reply.clone()).encode_payload();
    match ReplicaReply::decode(RequestCode::LogOp, &payload).unwrap() {
        ReplicaReply::LogOp(back) => assert_eq!(back, reply),
        other => panic!("{other:?}"),
    }
}

#[test]
fn malformed_operations_are_refused_before_they_reach_a_chunk() {
    let bad = [
        LogOpRequest { version: 2, ..LogOpRequest::tail(V, C, 1) },
        LogOpRequest { op: 9, ..LogOpRequest::tail(V, C, 1) },
        LogOpRequest::append(V, C, 1, 0, 0, bytes(1, 4)),
        LogOpRequest::append(V, C, 1, 1, 0, Vec::new()),
        LogOpRequest::append(V, C, 1, 1, 0, vec![0; MAX_OP_DATA + 1]),
        LogOpRequest { length: 1, ..LogOpRequest::append(V, C, 1, 1, 0, bytes(1, 4)) },
        LogOpRequest { epoch: 1, ..LogOpRequest::tail(V, C, 1) },
        LogOpRequest { data: vec![1], ..LogOpRequest::read(V, C, 1, 0, 4) },
        LogOpRequest::read(V, C, 1, 0, (MAX_OP_DATA + 1) as u32),
        LogOpRequest { flags: 2, ..LogOpRequest::seal(V, C, 1, 1, false) },
        LogOpRequest::seal(V, 0, 1, 1, false),
    ];
    for op in bad {
        assert!(op.validate().is_err(), "{op:?}");
        assert!(LogOpRequest::from_payload(&prost::Message::encode_to_vec(&op)).is_err());
    }
}

#[test]
fn pipelined_appends_in_any_order_are_acknowledged_by_contiguous_length_and_survive_a_restart() {
    let dir = Dir::new("pipeline");
    let data = bytes(7, 600);
    let mut a = node(Some(&dir.0));
    let r = call(&mut a, LogOpRequest::append(V, C, 1, 4, 400, data[400..600].to_vec())).unwrap();
    assert_eq!((r.length, r.durable_length), (0, 0), "nothing below the first record yet");
    let r = call(&mut a, LogOpRequest::append(V, C, 1, 4, 200, data[200..400].to_vec())).unwrap();
    assert_eq!(r.length, 0);
    let r = call(&mut a, LogOpRequest::append(V, C, 1, 4, 0, data[..200].to_vec())).unwrap();
    assert_eq!((r.length, r.durable_length, r.epoch), (600, 600, 4));
    let read = call(&mut a, LogOpRequest::read(V, C, 1, 100, 300)).unwrap();
    assert_eq!(read.data, &data[100..400]);
    assert_eq!(read.length, 600);
    // a retry of the middle record is acknowledged again and writes nothing
    let r = call(&mut a, LogOpRequest::append(V, C, 1, 4, 200, data[200..400].to_vec())).unwrap();
    assert_eq!(r.length, 600);
    // restart: a new node over the same directory derives the length from the file
    drop(a);
    let mut b = node(Some(&dir.0));
    let tail = call(&mut b, LogOpRequest::tail(V, C, 1)).unwrap();
    assert_eq!((tail.length, tail.epoch, tail.sealed), (600, 4, false));
    assert_eq!(tail.crc, crate::log_chunk::crc32c(&data));
    assert_eq!(call(&mut b, LogOpRequest::read(V, C, 1, 0, 600)).unwrap().data, data);
}

#[test]
fn a_stale_writer_is_refused_the_new_owner_cuts_and_a_seal_closes_the_chunk() {
    let dir = Dir::new("epoch");
    let data = bytes(9, 500);
    let mut a = node(Some(&dir.0));
    call(&mut a, LogOpRequest::append(V, C, 1, 1, 0, data[..300].to_vec())).unwrap();
    call(&mut a, LogOpRequest::append(V, C, 1, 1, 300, data[300..500].to_vec())).unwrap();
    // a new owner at epoch 2 decides the tail is 400 and cuts
    let r = call(&mut a, LogOpRequest::truncate(V, C, 1, 2, 400)).unwrap();
    assert_eq!((r.length, r.epoch), (400, 2));
    // the old writer's next record is refused
    let refused = call(&mut a, LogOpRequest::append(V, C, 1, 1, 400, data[400..].to_vec())).unwrap_err();
    assert!(matches!(refused, ObjectError::PreconditionFailed(_)), "{refused:?}");
    call(&mut a, LogOpRequest::append(V, C, 1, 2, 400, data[400..450].to_vec())).unwrap();
    let sealed = call(&mut a, LogOpRequest::seal(V, C, 1, 2, true)).unwrap();
    assert!(sealed.sealed);
    assert_eq!(sealed.length, 450);
    assert_eq!(sealed.sha256.len(), 32);
    assert!(call(&mut a, LogOpRequest::append(V, C, 1, 2, 450, bytes(1, 5))).is_err());
    // the seal survives a restart
    drop(a);
    let mut b = node(Some(&dir.0));
    let tail = call(&mut b, LogOpRequest::tail(V, C, 1)).unwrap();
    assert!(tail.sealed);
    assert_eq!((tail.length, tail.sha256), (450, sealed.sha256));
}

#[test]
fn a_tail_or_read_of_a_chunk_never_held_is_empty_and_creates_nothing() {
    let dir = Dir::new("absent");
    let mut a = node(Some(&dir.0));
    let tail = call(&mut a, LogOpRequest::tail(V, 99, 1)).unwrap();
    assert_eq!((tail.length, tail.epoch, tail.sealed), (0, 0, false));
    assert!(call(&mut a, LogOpRequest::read(V, 99, 1, 0, 10)).unwrap().data.is_empty());
    assert_eq!(std::fs::read_dir(&dir.0).unwrap().count(), 0);
}

#[test]
fn log_chunks_need_durable_storage_and_maintenance_mode_allows_only_reads() {
    let mut none = node(None);
    let error = call(&mut none, LogOpRequest::tail(V, C, 1)).unwrap_err();
    assert!(matches!(error, ObjectError::PreconditionFailed(_)));
    let dir = Dir::new("maintenance");
    let mut held = node(Some(&dir.0));
    call(&mut held, LogOpRequest::append(V, C, 1, 1, 0, bytes(1, 10))).unwrap();
    held.maintenance = true;
    assert!(call(&mut held, LogOpRequest::append(V, C, 1, 1, 10, bytes(1, 10))).is_err());
    assert!(call(&mut held, LogOpRequest::truncate(V, C, 1, 2, 5)).is_err());
    assert!(call(&mut held, LogOpRequest::seal(V, C, 1, 1, false)).is_err());
    assert_eq!(call(&mut held, LogOpRequest::tail(V, C, 1)).unwrap().length, 10);
    assert_eq!(call(&mut held, LogOpRequest::read(V, C, 1, 0, 10)).unwrap().data.len(), 10);
}

#[test]
fn many_chunks_are_independent_and_the_open_set_is_bounded() {
    let dir = Dir::new("many");
    let mut a = node(Some(&dir.0));
    for chunk in 1..=5u64 {
        call(&mut a, LogOpRequest::append(V, chunk, 1, 1, 0, bytes(chunk as u8, 10 * chunk as usize))).unwrap();
    }
    for chunk in 1..=5u64 {
        let tail = call(&mut a, LogOpRequest::tail(V, chunk, 1)).unwrap();
        assert_eq!(tail.length, 10 * chunk);
    }
    // the replica error paths do not poison the node
    assert!(call(&mut a, LogOpRequest::append(V, 1, 1, 1, 5, bytes(0, 10))).is_err(), "differing bytes over held bytes");
    assert_eq!(call(&mut a, LogOpRequest::tail(V, 1, 1)).unwrap().length, 10);
    assert!(!a.failed);
}
