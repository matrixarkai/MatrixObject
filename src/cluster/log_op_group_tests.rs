// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Log chunk appends joining the star group commit: one sync per dirty chunk per group, and that sync
//! happens with the replica lock released.
use super::*;
use crate::cluster::log_chunk_store::{sync_pending, LogChunkStore};
use crate::cluster::log_op::*;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

struct Dir(PathBuf);
impl Dir {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!("logopgroup-{}-{name}", std::process::id()));
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
fn node(dir: &Path) -> NodeReplica {
    let (used_bytes, health) = super::replica_health::test_parts();
    NodeReplica {
        used_bytes,
        service: MatrixReplicaService::new(),
        journal: None,
        journal_checkpoint_after_bytes: 0,
        staged: None,
        max_pages: 65_536,
        health,
        log_chunks: Some(LogChunkStore::open(dir).unwrap()),
    }
}
fn bytes(seed: u8, n: usize) -> Vec<u8> {
    (0..n).map(|i| seed.wrapping_add((i * 3) as u8)).collect()
}
fn call(node: &mut NodeReplica, op: LogOpRequest, sync: bool) -> LogOpReply {
    match apply_locked(node, ReplicaRequest::LogOp(op), MAX_PAYLOAD_BYTES, sync).unwrap() {
        ReplicaReply::LogOp(reply) => reply,
        other => panic!("{other:?}"),
    }
}

#[test]
fn a_deferred_group_syncs_each_dirty_chunk_once_however_many_records_it_holds() {
    let dir = Dir::new("once");
    let mut n = node(&dir.0);
    for round in 0..10u64 {
        for chunk in 1..=2u64 {
            let reply = call(&mut n, LogOpRequest::append(1, chunk, 1, 1, round * 100, bytes(round as u8, 100)), false);
            assert_eq!(reply.length, (round + 1) * 100);
        }
    }
    let store = n.log_chunks.as_mut().unwrap();
    assert!(store.has_dirty());
    assert_eq!(store.commits(), 0, "twenty records and no sync yet");
    let pending = store.take_pending_syncs().unwrap();
    assert_eq!(pending.len(), 2, "one handle per dirty chunk, not per record");
    assert!(!store.has_dirty());
    // the sync itself needs no store and no lock
    sync_pending(&pending).unwrap();
    store.note_synced(&pending);
    assert_eq!(store.commits(), 2);
    assert!(store.take_pending_syncs().unwrap().is_empty(), "nothing dirty, nothing to sync");
    for chunk in 1..=2 {
        let t = call(&mut n, LogOpRequest::tail(1, chunk, 1), true);
        assert_eq!((t.length, t.durable_length), (1000, 1000));
    }
}

#[test]
fn a_read_never_returns_a_byte_that_no_sync_has_covered() {
    let dir = Dir::new("readcap");
    let mut n = node(&dir.0);
    call(&mut n, LogOpRequest::append(1, 1, 1, 1, 0, bytes(5, 100)), false);
    let tail = call(&mut n, LogOpRequest::tail(1, 1, 1), true);
    assert_eq!((tail.length, tail.durable_length), (100, 0), "applied but not durable");
    assert!(call(&mut n, LogOpRequest::read(1, 1, 1, 0, 100), true).data.is_empty());
    let store = n.log_chunks.as_mut().unwrap();
    let pending = store.take_pending_syncs().unwrap();
    sync_pending(&pending).unwrap();
    store.note_synced(&pending);
    let read = call(&mut n, LogOpRequest::read(1, 1, 1, 10, 50), true);
    assert_eq!(read.data, &bytes(5, 100)[10..60]);
}

#[test]
fn the_group_commit_accepts_log_mutations_and_refuses_reads_and_unsupported_requests() {
    use super::star_group_commit::eligible;
    assert!(eligible(&ReplicaRequest::LogOp(LogOpRequest::append(1, 1, 1, 1, 0, bytes(1, 4)))));
    assert!(eligible(&ReplicaRequest::LogOp(LogOpRequest::seal(1, 1, 1, 1, false))));
    assert!(eligible(&ReplicaRequest::LogOp(LogOpRequest::truncate(1, 1, 1, 2, 0))));
    assert!(!eligible(&ReplicaRequest::LogOp(LogOpRequest::tail(1, 1, 1))));
    assert!(!eligible(&ReplicaRequest::LogOp(LogOpRequest::read(1, 1, 1, 0, 4))));
}

#[test]
fn concurrent_writers_through_the_committer_all_reach_durable_and_exact_lengths() {
    let dir = Dir::new("committer");
    let replica = Arc::new(Mutex::new(node(&dir.0)));
    let committer = star_group_commit::Committer::start(Arc::clone(&replica), MAX_PAYLOAD_BYTES).unwrap();
    let writers: Vec<_> = (1..=8u64)
        .map(|chunk| {
            let committer = Arc::clone(&committer);
            std::thread::spawn(move || {
                for round in 0..25u64 {
                    let op = LogOpRequest::append(1, chunk, 1, 1, round * 64, bytes(chunk as u8, 64));
                    match committer.request(ReplicaRequest::LogOp(op), 64 + 64).unwrap() {
                        ReplicaReply::LogOp(r) => {
                            assert_eq!(r.length, (round + 1) * 64);
                            assert_eq!(r.durable_length, r.length);
                        }
                        other => panic!("{other:?}"),
                    }
                }
            })
        })
        .collect();
    for writer in writers {
        writer.join().unwrap();
    }
    let mut guard = replica.lock().unwrap();
    for chunk in 1..=8 {
        let t = call(&mut guard, LogOpRequest::tail(1, chunk, 1), true);
        assert_eq!((t.length, t.durable_length), (25 * 64, 25 * 64));
    }
    // fewer syncs than appends once writers overlap is allowed; never more than one per append
    assert!(guard.log_chunks.as_ref().unwrap().commits() <= 8 * 25);
    assert!(!guard.log_chunks.as_ref().unwrap().has_dirty());
    drop(guard);
    drop(committer);
    // everything is on disk: a new node over the directory sees the same lengths
    let mut restarted = node(&dir.0);
    for chunk in 1..=8 {
        assert_eq!(call(&mut restarted, LogOpRequest::tail(1, chunk, 1), true).length, 25 * 64);
    }
}

static IN_SLOW_SYNC: AtomicBool = AtomicBool::new(false);
fn slow_sync(file: &File) -> std::io::Result<()> {
    IN_SLOW_SYNC.store(true, AtomicOrdering::SeqCst);
    std::thread::sleep(Duration::from_millis(300));
    let result = file.sync_data();
    IN_SLOW_SYNC.store(false, AtomicOrdering::SeqCst);
    result
}

#[test]
fn a_slow_sync_does_not_hold_the_replica_lock_so_the_heartbeat_can_still_take_it() {
    let dir = Dir::new("slow");
    let mut n = node(&dir.0);
    n.log_chunks.as_mut().unwrap().set_syncer(slow_sync);
    let replica = Arc::new(Mutex::new(n));
    let committer = star_group_commit::Committer::start(Arc::clone(&replica), MAX_PAYLOAD_BYTES).unwrap();
    let writer = {
        let committer = Arc::clone(&committer);
        std::thread::spawn(move || {
            committer.request(ReplicaRequest::LogOp(LogOpRequest::append(1, 1, 1, 1, 0, bytes(3, 64))), 128)
        })
    };
    let started = Instant::now();
    while !IN_SLOW_SYNC.load(AtomicOrdering::SeqCst) {
        assert!(started.elapsed() < Duration::from_secs(10), "the sync never began");
        std::thread::sleep(Duration::from_millis(1));
    }
    // the heartbeat's probe: it must find the lock free for the whole time the sync is held.
    // Only probes made while the sync is actually held count, so a slow runner that stalls
    // this thread past the end of the sync can neither fail the test (a probe that lands on the
    // committer's brief re-lock afterwards) nor pass it (too few probes).
    let (mut probes, mut free) = (0, 0);
    while IN_SLOW_SYNC.load(AtomicOrdering::SeqCst) && probes < 60 {
        probes += 1;
        if replica.try_lock().is_ok() {
            free += 1;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(probes >= 5, "only {probes} probes fit inside the held sync");
    assert_eq!(free, probes, "a held sync blocked the replica lock");
    let reply = match writer.join().unwrap().unwrap() {
        ReplicaReply::LogOp(reply) => reply,
        other => panic!("{other:?}"),
    };
    assert!(started.elapsed() >= Duration::from_millis(100), "the answer came before the sync finished");
    assert_eq!((reply.length, reply.durable_length), (64, 64));
    assert_eq!(replica.lock().unwrap().log_chunks.as_ref().unwrap().commits(), 1);
}

fn failing_sync(_: &File) -> std::io::Result<()> {
    Err(std::io::Error::other("injected sync failure"))
}

#[test]
fn a_failed_sync_fails_the_node_and_no_acknowledgement_leaves_it() {
    let dir = Dir::new("fail");
    let mut n = node(&dir.0);
    n.log_chunks.as_mut().unwrap().set_syncer(failing_sync);
    let replica = Arc::new(Mutex::new(n));
    let committer = star_group_commit::Committer::start(Arc::clone(&replica), MAX_PAYLOAD_BYTES).unwrap();
    let outcome = committer.request(ReplicaRequest::LogOp(LogOpRequest::append(1, 1, 1, 1, 0, bytes(1, 10))), 128);
    assert!(outcome.is_err(), "an append whose sync failed was acknowledged");
    assert!(replica.lock().unwrap().health.failed());
    // and the node now refuses everything
    let again = committer.request(ReplicaRequest::LogOp(LogOpRequest::append(1, 1, 1, 1, 10, bytes(1, 10))), 128);
    assert!(again.is_err());
}
