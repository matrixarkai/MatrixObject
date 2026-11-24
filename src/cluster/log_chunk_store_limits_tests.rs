// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! What bounds a replica's log chunk memory: sealed chunks are closed and reopened as reads need
//! them (never refused), tails are never closed, and only a separate tail budget refuses.
use super::*;
use std::path::PathBuf;

struct Dir(PathBuf);
impl Dir {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!("logstorelimits-{}-{name}", std::process::id()));
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
fn payload(chunk: u64, n: usize) -> Vec<u8> {
    (0..n).map(|i| (chunk as u8).wrapping_add((i * 3) as u8)).collect()
}
fn append(store: &mut LogChunkStore, chunk: u64, offset: u64, data: Vec<u8>) -> Result<LogOpReply, ObjectError> {
    store.apply(&LogOpRequest::append(1, chunk, 1, 1, offset, data))
}
fn seal(store: &mut LogChunkStore, chunk: u64) {
    store.apply(&LogOpRequest::seal(1, chunk, 1, 1, false)).unwrap();
}
fn read(store: &mut LogChunkStore, chunk: u64, offset: u64, length: usize) -> Result<Vec<u8>, ObjectError> {
    store.apply(&LogOpRequest::read(1, chunk, 1, offset, length as u32)).map(|reply| reply.data)
}
fn refused_for_the_tail_budget(error: &ObjectError) -> bool {
    matches!(error, ObjectError::PreconditionFailed(message) if message == TAIL_BUDGET_MESSAGE)
}

#[test]
fn more_sealed_chunks_than_the_old_open_limit_are_read_in_rotation_with_no_refusal() {
    let dir = Dir::new("rotation");
    let mut store = LogChunkStore::with_limits(&dir.0, MAX_OPEN_LOG_CHUNKS, DEFAULT_TAIL_BUDGET, 256 * 1024).unwrap();
    let count = MAX_OPEN_LOG_CHUNKS as u64 + 44;
    for chunk in 1..=count {
        append(&mut store, chunk, 0, payload(chunk, 3000 + chunk as usize)).unwrap();
        seal(&mut store, chunk);
        assert!(store.open_chunks() <= MAX_OPEN_LOG_CHUNKS);
    }
    assert!(count as usize > MAX_OPEN_LOG_CHUNKS);
    for round in 0..2 {
        for chunk in 1..=count {
            let got = read(&mut store, chunk, 0, 10_000).unwrap_or_else(|error| panic!("round {round} chunk {chunk}: {error}"));
            assert_eq!(got, payload(chunk, 3000 + chunk as usize));
            assert!(store.open_chunks() <= MAX_OPEN_LOG_CHUNKS, "open chunks: {}", store.open_chunks());
            assert!(store.cache_bytes() <= 256 * 1024, "cache: {}", store.cache_bytes());
        }
    }
    assert_eq!(store.open_tails(), 0);
}

#[test]
fn a_tail_beyond_the_budget_is_refused_retryably_and_no_live_tail_is_ever_closed() {
    let dir = Dir::new("budget");
    let mut store = LogChunkStore::with_limits(&dir.0, 4, 3, 1024 * 1024).unwrap();
    for chunk in 1..=3u64 {
        append(&mut store, chunk, 0, payload(chunk, 1000)).unwrap();
    }
    assert_eq!(store.open_tails(), 3);
    // a fourth tail is refused with the retryable message, and nothing is closed for it
    let error = append(&mut store, 4, 0, payload(4, 1000)).unwrap_err();
    assert!(refused_for_the_tail_budget(&error), "{error}");
    assert_eq!(store.open_tails(), 3);
    assert_eq!(store.open_chunks(), 3);
    // the live tails still answer, byte for byte, and still accept appends
    for chunk in 1..=3u64 {
        assert_eq!(read(&mut store, chunk, 0, 5000).unwrap(), payload(chunk, 1000));
        append(&mut store, chunk, 1000, payload(chunk + 10, 500)).unwrap();
    }
    // a tail that seals frees its slot, and the writer that was refused is let in
    seal(&mut store, 1);
    append(&mut store, 4, 0, payload(4, 1000)).unwrap();
    assert_eq!(store.open_tails(), 3);
}

#[test]
fn sealed_chunks_are_closed_to_stay_inside_their_limit_and_reopened_while_tails_stay_open() {
    let dir = Dir::new("sealedlimit");
    // one sealed chunk open at once, two tails
    let mut store = LogChunkStore::with_limits(&dir.0, 1, 2, 1024 * 1024).unwrap();
    append(&mut store, 1, 0, payload(1, 800)).unwrap();
    append(&mut store, 2, 0, payload(2, 800)).unwrap();
    let error = append(&mut store, 3, 0, payload(3, 800)).unwrap_err();
    assert!(refused_for_the_tail_budget(&error), "{error}");
    assert_eq!(store.open_chunks(), 2);
    seal(&mut store, 1);
    append(&mut store, 3, 0, payload(3, 800)).unwrap();
    // the second seal takes the sealed chunks over their limit: the least recently used one closes
    seal(&mut store, 2);
    assert_eq!(store.open_sealed(), 1);
    assert!(store.is_open(1, 3), "the tail stays open");
    assert!(store.is_open(1, 2) && !store.is_open(1, 1));
    // a closed sealed chunk is reopened when it is read, and the other one closes in its place
    assert_eq!(read(&mut store, 1, 0, 800).unwrap(), payload(1, 800));
    assert_eq!(store.open_sealed(), 1);
    assert!(store.is_open(1, 1) && !store.is_open(1, 2));
    assert_eq!(read(&mut store, 2, 0, 800).unwrap(), payload(2, 800));
    assert!(store.is_open(1, 3) && store.open_tails() == 1);
    assert_eq!(read(&mut store, 3, 0, 800).unwrap(), payload(3, 800));
}

#[test]
fn a_sealed_chunk_with_a_sync_still_to_come_is_not_closed() {
    let dir = Dir::new("dirty");
    let mut store = LogChunkStore::with_limits(&dir.0, 1, 8, 1024 * 1024).unwrap();
    // chunk 1: appended without a sync (a group commit in flight), then sealed (the seal syncs)
    store.apply_deferred(&LogOpRequest::append(1, 1, 1, 1, 0, payload(1, 900))).unwrap();
    store.apply_deferred(&LogOpRequest::seal(1, 1, 1, 1, false)).unwrap();
    assert!(store.has_dirty());
    // chunk 2 seals too: over the limit, but chunk 1 has a sync to come, so chunk 2 is the one closed
    append(&mut store, 2, 0, payload(2, 900)).unwrap();
    seal(&mut store, 2);
    assert!(store.is_open(1, 1), "a chunk with a sync still to come stays open");
    assert!(!store.is_open(1, 2));
    let pending = store.take_pending_syncs().unwrap();
    sync_pending(&pending).unwrap();
    store.note_synced(&pending);
}

#[test]
fn a_tail_is_not_closed_even_when_it_is_the_least_recently_used_chunk() {
    let dir = Dir::new("oldesttail");
    // chunk 1 is a tail that has not been touched since it was opened: the oldest chunk of all
    let mut store = LogChunkStore::with_limits(&dir.0, 1, 3, 1024 * 1024).unwrap();
    append(&mut store, 1, 0, payload(1, 700)).unwrap();
    append(&mut store, 2, 0, payload(2, 700)).unwrap();
    append(&mut store, 3, 0, payload(3, 700)).unwrap();
    seal(&mut store, 2);
    seal(&mut store, 3); // two sealed chunks over a limit of one: one of them closes, never the tail
    assert!(store.is_open(1, 1), "the oldest chunk is a tail and stays open");
    assert_eq!(store.open_tails(), 1);
    assert_eq!(store.open_sealed(), 1);
    assert_eq!(read(&mut store, 1, 0, 700).unwrap(), payload(1, 700));
    append(&mut store, 1, 700, payload(9, 100)).unwrap();
}

#[test]
fn nested_log_directory_creation_finishes_before_serving_queries() {
    let dir = Dir::new("nested-directory");
    let nested = dir.0.join("first").join("second").join("store");
    let mut store = LogChunkStore::open(&nested).unwrap();
    assert!(nested.is_dir());
    assert_eq!(std::fs::read_dir(&nested).unwrap().count(), 0);
    let reply = store
        .apply(&LogOpRequest::publication_tail(3, 11, 7))
        .unwrap();
    assert!(reply.publication_supported);
    assert!(!reply.chunk_present);
    assert!(reply.published_prefix().is_none());
    assert_eq!(std::fs::read_dir(&nested).unwrap().count(), 0);
}
