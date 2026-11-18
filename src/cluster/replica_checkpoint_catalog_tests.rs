// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use crate::durable_payload::outcome_segments::MIN_SEGMENT_BYTES;
use crate::{outcome::PageOutcome, PageState};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Dir(PathBuf);
impl Dir {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "replica-checkpoint-catalog-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn open(&self) -> SegmentedOutcomeLog {
        SegmentedOutcomeLog::open(self.0.join("journal"), MIN_SEGMENT_BYTES).unwrap()
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn id(n: u64) -> PageId {
    PageId {
        chunk_id: ChunkId::new(1, 1).unwrap(),
        page_index: n,
    }
}
fn state(bytes: Vec<u8>, seq: u64) -> PageState {
    PageState {
        logical_length: bytes.len() as u64,
        write_sequence: seq,
        checksum_fnv1a64: crate::fnv1a64(&bytes),
        data: Arc::new(bytes),
        ec_parity_fragment: None,
        ec_fragment_digests: None,
        discarded_ranges: Vec::new(),
    }
}
fn add(
    log: &SegmentedOutcomeLog,
    catalog: &mut PageCatalog,
    n: u64,
    bytes: Vec<u8>,
) -> (StoredOutcomePosition, PageState) {
    let state = state(bytes, 1);
    let outcome = PageOutcome::between(&id(n), None, &state).unwrap();
    let record = log.append_page(&outcome).unwrap();
    catalog.fold(&record).unwrap();
    (record.position, state)
}
#[test]
fn checkpoint_catalog_publication_reopens_with_shared_extents_and_sparse_pages() {
    let dir = Dir::new();
    let log = dir.open();
    let mut catalog = PageCatalog::default();
    let before = state(vec![0x31; 4096], 1);
    let outcomes: Vec<_> = (0..32)
        .map(|n| PageOutcome::between(&id(n), None, &before).unwrap())
        .collect();
    let mutations: Vec<_> = outcomes.iter().map(DecidedMutation::Page).collect();
    let record = log.append_decided_batch(&mutations).unwrap();
    catalog.fold(&record).unwrap();
    let mut bytes = (*before.data).clone();
    bytes.resize(16 * 1024, 0);
    bytes[8192..12288].fill(0x77);
    let after = state(bytes, 2);
    let record = log
        .append_page(&PageOutcome::between(&id(0), Some(&before), &after).unwrap())
        .unwrap();
    catalog.fold(&record).unwrap();
    let cut = record.position;
    let prepared = prepare(
        &log,
        &catalog,
        b"replica-metadata".to_vec(),
        Some(cut.clone()),
        1,
        false,
    )
    .unwrap();
    assert!(latest(&log, 32).unwrap().is_none());
    prepared.publish(&log).unwrap();
    let bytes_before = log.logical_length().unwrap();
    drop(log);
    let log = dir.open();
    let loaded = latest(&log, 32).unwrap().unwrap();
    assert_eq!(loaded.commit.through, Some(cut));
    assert_eq!(loaded.header.metadata, b"replica-metadata");
    assert_eq!(loaded.catalog.pages.len(), 32);
    assert!(Arc::ptr_eq(
        &loaded.catalog.pages[&id(1)].spans[0].extent,
        &loaded.catalog.pages[&id(31)].spans[0].extent
    ));
    for (page_id, page) in &loaded.catalog.pages {
        let materialized = page
            .materialize(|e, o, n| log.read_payload_range(e, o, n))
            .unwrap();
        assert_eq!(
            &*materialized.data,
            if *page_id == id(0) {
                &*after.data
            } else {
                &*before.data
            }
        );
    }
    assert_eq!(log.logical_length().unwrap(), bytes_before);
    assert!(latest(&log, 31).is_err());
}
#[test]
fn checkpoint_catalog_unpublished_work_does_not_replace_committed_state() {
    let dir = Dir::new();
    let log = dir.open();
    let mut catalog = PageCatalog::default();
    let (cut, _) = add(&log, &mut catalog, 0, vec![0x41; 4096]);
    prepare(
        &log,
        &catalog,
        b"first".to_vec(),
        Some(cut.clone()),
        1,
        false,
    )
    .unwrap()
    .publish(&log)
    .unwrap();
    let (cut, _) = add(&log, &mut catalog, 1, vec![0x42; 4096]);
    let prepared = prepare(
        &log,
        &catalog,
        vec![0x22; 2 * 1024 * 1024],
        Some(cut),
        2,
        false,
    )
    .unwrap();
    assert_eq!(prepared.commit.fragments, 3);
    drop(prepared);
    drop(log);
    let log = dir.open();
    let loaded = latest(&log, 100).unwrap().unwrap();
    assert_eq!(loaded.header.epoch, 1);
    assert_eq!(loaded.catalog.pages.len(), 1);
    let mut replay = log
        .metadata_cursor_after(loaded.commit.through.as_ref())
        .unwrap();
    let mut logical = 0;
    while let Some(record) = replay.next_record().unwrap() {
        if record.batch.version != 2 {
            logical += 1;
        }
    }
    assert_eq!(logical, 1);
}
#[test]
fn checkpoint_catalog_metadata_spans_fragments_and_skips_interleaved_writes() {
    let dir = Dir::new();
    let log = dir.open();
    let id = vec![0x51; 16];
    let header = CatalogHeader {
        version: 1,
        log_id: log.store_id().to_vec(),
        checkpoint_id: id.clone(),
        epoch: 1,
        captured_cut: None,
        install: false,
        metadata: vec![0x61; 2 * 1024 * 1024],
        extent_count: 0,
        page_count: 0,
    };
    let encoded = Row {
        value: Some(row::Value::Header(header.clone())),
    }
    .encode_to_vec();
    let mut bytes = (encoded.len() as u32).to_le_bytes().to_vec();
    bytes.extend(encoded);
    let mut writer = CatalogWriter {
        log: &log,
        check: None,
        id: id.clone(),
        buffer: Vec::new(),
        hash: digest::Context::new(&digest::SHA256),
        bytes: 0,
        count: 0,
        first: None,
        last: None,
    };
    writer
        .write(&bytes[..CheckpointFragment::MAX_DATA_BYTES])
        .unwrap();
    let mut catalog = PageCatalog::default();
    let (logical, _) = add(&log, &mut catalog, 0, vec![0x72; 4096]);
    writer
        .write(&bytes[CheckpointFragment::MAX_DATA_BYTES..])
        .unwrap();
    writer.flush().unwrap();
    let commit = CheckpointCommit {
        index_root: None,
        version: 1,
        log_id: log.store_id().to_vec(),
        checkpoint_id: id,
        epoch: 1,
        first: writer.first,
        last: writer.last,
        fragments: writer.count,
        bytes: writer.bytes,
        sha256: writer.hash.finish().as_ref().to_vec(),
        through: None,
        install: false,
    };
    PreparedCheckpoint { commit }.publish(&log).unwrap();
    let loaded = latest(&log, 10).unwrap().unwrap();
    assert_eq!(loaded.header, header);
    assert!(loaded.commit.through.is_none());
    assert!(loaded.catalog.pages.is_empty());
    assert_eq!(log.read_outcomes(&logical).unwrap().len(), 1);
}
#[test]
fn checkpoint_catalog_install_cut_cannot_skip_interleaved_logical_writes() {
    let dir = Dir::new();
    let log = dir.open();
    let mut catalog = PageCatalog::default();
    let prepared = prepare(&log, &catalog, b"initial".to_vec(), None, 1, true).unwrap();
    let last = prepared.commit.last.clone();
    prepared.publish(&log).unwrap();
    let loaded = latest(&log, 10).unwrap().unwrap();
    assert_eq!(loaded.commit.through, last);
    let (cut, _) = add(&log, &mut catalog, 0, vec![0x41; 4096]);
    let prepared = prepare(&log, &catalog, b"install".to_vec(), Some(cut), 2, true).unwrap();
    // Rebuild a publication which claims the first install's old cut. The new
    // logical record makes that advancement unsafe even though the root hashes.
    let mut bad = prepared.commit.clone();
    bad.epoch = 3;
    // Epoch binding itself must also reject a forged publication.
    assert!(load(&log, bad, 10).is_err());
    let mut loaded = load(&log, prepared.commit, 10).unwrap();
    loaded.header.captured_cut = last;
    let encoded = Row {
        value: Some(row::Value::Header(loaded.header)),
    }
    .encode_to_vec();
    let mut writer = CatalogWriter {
        log: &log,
        check: None,
        id: vec![0x71; 16],
        buffer: Vec::new(),
        hash: digest::Context::new(&digest::SHA256),
        bytes: 0,
        count: 0,
        first: None,
        last: None,
    };
    // A separate empty-page install with a real intervening write is refused.
    let header = CatalogHeader {
        version: 1,
        log_id: log.store_id().to_vec(),
        checkpoint_id: writer.id.clone(),
        epoch: 4,
        captured_cut: None,
        install: true,
        metadata: encoded,
        extent_count: 0,
        page_count: 0,
    };
    writer.row(1, &header, &mut Vec::new()).unwrap();
    writer.flush().unwrap();
    let bad = CheckpointCommit {
        index_root: None,
        version: 1,
        log_id: log.store_id().to_vec(),
        checkpoint_id: writer.id,
        epoch: 4,
        first: writer.first,
        last: writer.last.clone(),
        fragments: writer.count,
        bytes: writer.bytes,
        sha256: writer.hash.finish().as_ref().to_vec(),
        through: writer.last,
        install: true,
    };
    assert!(load(&log, bad, 10).is_err());
}
#[test]
fn checkpoint_catalog_newest_corruption_is_refused_without_fallback_or_repair() {
    let dir = Dir::new();
    let log = dir.open();
    let mut catalog = PageCatalog::default();
    let (cut, _) = add(&log, &mut catalog, 0, vec![0x41; 4096]);
    prepare(
        &log,
        &catalog,
        b"first".to_vec(),
        Some(cut.clone()),
        1,
        false,
    )
    .unwrap()
    .publish(&log)
    .unwrap();
    let mut prepared = prepare(&log, &catalog, b"second".to_vec(), Some(cut), 2, false).unwrap();
    prepared.commit.sha256[0] ^= 1;
    prepared.publish(&log).unwrap();
    let before = log.logical_length().unwrap();
    assert!(latest(&log, 10).is_err());
    assert_eq!(log.logical_length().unwrap(), before);
    drop(log);
    let log = dir.open();
    assert!(latest(&log, 10).is_err());
    assert_eq!(log.logical_length().unwrap(), before);
}
#[test]
fn checkpoint_catalog_foreign_descriptors_duplicate_epochs_and_limits_are_refused() {
    let dir = Dir::new();
    let other = Dir::new();
    let log = dir.open();
    let foreign = other.open();
    let mut catalog = PageCatalog::default();
    let (cut, _) = add(&log, &mut catalog, 0, vec![0x41; 4096]);
    let prepared = prepare(
        &log,
        &catalog,
        b"first".to_vec(),
        Some(cut.clone()),
        1,
        false,
    )
    .unwrap();
    assert!(load(&foreign, prepared.commit.clone(), 10).is_err());
    let mut wrong = prepared.commit.clone();
    wrong.fragments += 1;
    assert!(load(&log, wrong, 10).is_err());
    let mut wrong = prepared.commit.clone();
    wrong.bytes = MAX_CATALOG_BYTES + 1;
    assert!(load(&log, wrong, 10).is_err());
    prepared.publish(&log).unwrap();
    prepare(&log, &catalog, b"stale".to_vec(), Some(cut), 1, false)
        .unwrap()
        .publish(&log)
        .unwrap();
    assert!(latest(&log, 10).is_err());
}
