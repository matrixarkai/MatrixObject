// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use crate::cluster::{
    replica_checkpoint_catalog as checkpoint, replica_checkpoint_index,
    replica_checkpoint_spans::PageCatalog, replica_page_backing::LivePageBacking,
};
use crate::durable_payload::{
    outcome_log::{DecidedMutation, StoredRecordMetadata},
    outcome_segments::SegmentedOutcomeLog,
};
use crate::log_page_outcome::*;
use crate::{outcome::PageOutcome, PageId, PageState};
use std::{
    collections::BTreeSet,
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "log-page-catalog-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn id(index: u64) -> PageId {
    PageId {
        chunk_id: ChunkId::new(1, 2).unwrap(),
        page_index: index,
    }
}
fn state() -> LogPageState {
    LogPageState {
        generation: 1,
        writer_epoch: 7,
        initialization: vec![8; 16],
        storage_incarnation: vec![9; 16],
        length: 0,
        published_length: 0,
        limit: 64 * 1024 * 1024,
        page_bytes: 4096,
        crc32c: 0,
        sealed: false,
        seal_sha256: Vec::new(),
    }
}
fn init() -> LogPageOutcome {
    LogPageOutcome {
        version: 1,
        volume_id: 1,
        chunk_id: 2,
        kind: LOG_PAGE_INITIALIZE,
        before: None,
        after: Some(state()),
        append: None,
    }
}
fn append(before: &LogPageState, data: &[u8], tag: u8, published: bool) -> LogPageOutcome {
    let mut after = before.clone();
    after.length += data.len() as u64;
    after.crc32c = crate::log_chunk::crc32c_update(before.crc32c, data);
    if published {
        after.published_length = after.length;
    }
    LogPageOutcome {
        version: 1,
        volume_id: 1,
        chunk_id: 2,
        kind: LOG_PAGE_APPEND,
        before: Some(before.clone()),
        after: Some(after),
        append: Some(LogPageAppendIdentity {
            attempt: vec![tag; 16],
            offset: before.length,
            length: data.len() as u32,
            crc32c: crate::log_chunk::crc32c(data),
        }),
    }
}
fn page_state(data: &[u8], sequence: u64) -> PageState {
    PageState {
        logical_length: data.len() as u64,
        write_sequence: sequence,
        checksum_fnv1a64: crate::fnv1a64(data),
        data: Arc::new(data.to_vec()),
        ec_parity_fragment: None,
        ec_fragment_digests: None,
        discarded_ranges: Vec::new(),
    }
}
fn record(
    log: &SegmentedOutcomeLog,
    pages: &[PageOutcome],
    outcome: &LogPageOutcome,
) -> StoredRecordMetadata {
    let values: Vec<_> = pages
        .iter()
        .map(DecidedMutation::Page)
        .chain(std::iter::once(DecidedMutation::LogPage(outcome)))
        .collect();
    log.append_decided_batch(&values).unwrap()
}
fn seeded(log: &SegmentedOutcomeLog) -> (PageCatalog, StoredRecordMetadata, LogPageOutcome) {
    let mut catalog = PageCatalog::default();
    catalog.fold(&record(log, &[], &init())).unwrap();
    let data = vec![23; 4209];
    let outcome = append(&state(), &data, 17, true);
    let pages: Vec<_> = data
        .chunks(4096)
        .enumerate()
        .map(|(i, data)| PageOutcome::between(&id(i as u64), None, &page_state(data, 1)).unwrap())
        .collect();
    let record = record(log, &pages, &outcome);
    catalog.fold(&record).unwrap();
    (catalog, record, outcome)
}
#[test]
fn log_catalog_snapshots_keep_original_receipts_while_fence_drops_only_incomplete_tail() {
    let mut catalog = LogPageCatalog::default();
    catalog.fold(&init()).unwrap();
    let first = append(&state(), b"published", 17, true);
    catalog.fold(&first).unwrap();
    let saved = catalog.clone();
    let second = append(first.after.as_ref().unwrap(), b"incomplete", 18, false);
    catalog.fold(&second).unwrap();
    assert_eq!(saved.chunks[&id(0).chunk_id].receipts.len(), 1);
    assert_eq!(catalog.chunks[&id(0).chunk_id].receipts.len(), 2);
    let mut after = first.after.clone().unwrap();
    after.writer_epoch += 1;
    let fence = LogPageOutcome {
        version: 1,
        volume_id: 1,
        chunk_id: 2,
        kind: LOG_PAGE_FENCE,
        before: second.after.clone(),
        after: Some(after),
        append: None,
    };
    catalog.fold(&fence).unwrap();
    let entry = &catalog.chunks[&id(0).chunk_id];
    entry.validate().unwrap();
    assert_eq!(entry.receipts.len(), 1);
    assert_eq!(
        entry.receipts[&0].append.as_ref().unwrap().attempt,
        vec![17; 16]
    );
    assert!(catalog.fold(&second).is_err());
}
#[test]
fn log_catalog_live_page_backing_and_snapshot_preserve_matching_authority() {
    let dir = Directory::new();
    let log =
        Arc::new(SegmentedOutcomeLog::open(dir.0.join("outcomes"), 32 * 1024 * 1024).unwrap());
    let (catalog, record, outcome) = seeded(&log);
    let backing = LivePageBacking::new(log.clone(), PageCatalog::default()).unwrap();
    let mut cursor = log.metadata_cursor_after(None).unwrap();
    let initialize = cursor.next_record().unwrap().unwrap();
    backing.fold(&initialize).unwrap();
    backing.fold(&record).unwrap();
    let snapshot = backing.catalog_snapshot().unwrap();
    snapshot.validate_log_pages().unwrap();
    assert_eq!(snapshot.logs, catalog.logs);
    assert_eq!(
        snapshot.logs.chunks[&id(0).chunk_id].state,
        outcome.after.unwrap()
    );
    let read = backing
        .capture_range(&id(1), 0, 113)
        .unwrap()
        .unwrap()
        .read()
        .unwrap();
    assert_eq!(read, vec![23; 113]);
    assert_eq!(backing.totals().unwrap(), (2, 4209));
}
#[test]
fn log_catalog_streamed_and_indexed_checkpoints_reopen_original_receipts_and_pages() {
    let dir = Directory::new();
    let log = SegmentedOutcomeLog::open(dir.0.join("outcomes"), 32 * 1024 * 1024).unwrap();
    let (catalog, record, _) = seeded(&log);
    let prepared = checkpoint::prepare_checked(
        &log,
        &catalog,
        vec![42],
        Some(record.position.clone()),
        1,
        false,
        &|| Ok(()),
    )
    .unwrap();
    let (commit, _) = prepared.publish_record(&log).unwrap();
    let loaded = checkpoint::load(&log, commit, 100).unwrap();
    assert_eq!(loaded.header.version, 2);
    assert_eq!(loaded.catalog.logs, catalog.logs);
    loaded.catalog.validate_log_pages().unwrap();
    let mut index = None;
    let mut dirty = BTreeSet::new();
    let prepared = replica_checkpoint_index::prepare(
        &log,
        &catalog,
        &mut index,
        &mut dirty,
        vec![42],
        Some(record.position),
        2,
        false,
        &|| Ok(()),
    )
    .unwrap();
    let (commit, _) = prepared.publish_record(&log).unwrap();
    drop(log);
    let log = SegmentedOutcomeLog::open(dir.0.join("outcomes"), 32 * 1024 * 1024).unwrap();
    let loaded = checkpoint::load(&log, commit, 100).unwrap();
    assert_eq!(loaded.catalog.logs, catalog.logs);
    loaded.catalog.validate_log_pages().unwrap();
    let data = loaded.catalog.pages[&id(1)]
        .materialize(|extent, offset, length| log.read_payload_range(extent, offset, length))
        .unwrap();
    assert_eq!(data.data.as_slice(), &[23; 113]);
}
#[test]
fn log_catalog_checkpoint_refuses_missing_pages_and_malformed_receipt_coverage() {
    let dir = Directory::new();
    let log = SegmentedOutcomeLog::open(dir.0.join("outcomes"), 32 * 1024 * 1024).unwrap();
    let (mut catalog, record, _) = seeded(&log);
    catalog.pages.remove(&id(1));
    assert!(checkpoint::prepare_checked(
        &log,
        &catalog,
        vec![],
        Some(record.position),
        1,
        false,
        &|| Ok(())
    )
    .is_err());
    let mut entry = catalog.logs.chunks[&id(0).chunk_id].clone();
    entry.receipts.remove(&0);
    assert!(entry.validate().is_err());
}
#[test]
fn log_catalog_checkpoint_worker_folds_committed_log_and_page_state_together() {
    let dir = Directory::new();
    let log = SegmentedOutcomeLog::open(dir.0.join("outcomes"), 32 * 1024 * 1024).unwrap();
    let mut worker = crate::cluster::replica_checkpoint_recovery::CheckpointState::new(
        crate::cluster::replica_checkpoint_recovery::scalar_template(),
    )
    .unwrap();
    let metadata = crate::cluster::replica_checkpoint_recovery::encode_scalars(
        crate::cluster::replica_checkpoint_recovery::scalar_template(),
    )
    .unwrap();
    let init = init();
    let record = log
        .append_decided_batch_with_metadata(&[DecidedMutation::LogPage(&init)], &metadata)
        .unwrap();
    worker.fold_record(&record, 100).unwrap();
    let data = vec![23; 113];
    let page = PageOutcome::between(&id(0), None, &page_state(&data, 1)).unwrap();
    let outcome = append(&state(), &data, 17, true);
    let record = log
        .append_decided_batch_with_metadata(
            &[
                DecidedMutation::Page(&page),
                DecidedMutation::LogPage(&outcome),
            ],
            &metadata,
        )
        .unwrap();
    worker.fold_record(&record, 100).unwrap();
    worker.catalog.validate_log_pages().unwrap();
    assert_eq!(
        worker.catalog.logs.chunks[&id(0).chunk_id].state,
        outcome.after.unwrap()
    );
    assert_eq!(
        worker.catalog.logs.chunks[&id(0).chunk_id].receipts[&0]
            .append
            .as_ref()
            .unwrap()
            .attempt,
        vec![17; 16]
    );
}

#[test]
fn log_catalog_recovery_refuses_page_only_mutations_and_existing_namespace_reuse() {
    let dir = Directory::new();
    let log = SegmentedOutcomeLog::open(dir.0.join("outcomes"), 32 * 1024 * 1024).unwrap();
    let (mut catalog, _, _) = seeded(&log);
    let before = catalog.pages[&id(1)].metadata.clone();
    let old = page_state(&[23; 113], 1);
    let changed = page_state(&[24; 113], 2);
    let page = PageOutcome::between(&id(1), Some(&old), &changed).unwrap();
    let forged = log.append_page(&page).unwrap();
    assert!(catalog.fold(&forged).is_err());
    assert_eq!(catalog.pages[&id(1)].metadata, before);
    let mut reused = PageCatalog::default();
    let page = PageOutcome::between(&id(0), None, &page_state(&[23; 113], 1)).unwrap();
    reused.fold(&log.append_page(&page).unwrap()).unwrap();
    assert!(reused.fold(&record(&log, &[], &init())).is_err());
    assert!(reused.logs.chunks.is_empty());
    assert_eq!(reused.pages.len(), 1);
}

#[test]
fn log_catalog_fence_truncates_pages_and_receipts_while_retained_read_keeps_old_bytes() {
    let dir = Directory::new();
    let log =
        Arc::new(SegmentedOutcomeLog::open(dir.0.join("outcomes"), 32 * 1024 * 1024).unwrap());
    let (catalog, _, first) = seeded(&log);
    let backing = LivePageBacking::new(log.clone(), catalog).unwrap();
    let next = append(first.after.as_ref().unwrap(), &[23; 100], 18, false);
    let before = page_state(&[23; 113], 1);
    let incomplete = page_state(&[23; 213], 2);
    let page = PageOutcome::between(&id(1), Some(&before), &incomplete).unwrap();
    backing.fold(&record(&log, &[page], &next)).unwrap();
    let retained = backing.capture_range(&id(1), 113, 100).unwrap().unwrap();
    let mut after = first.after.clone().unwrap();
    after.writer_epoch += 1;
    let fence = LogPageOutcome {
        version: 1,
        volume_id: 1,
        chunk_id: 2,
        kind: LOG_PAGE_FENCE,
        before: next.after,
        after: Some(after),
        append: None,
    };
    let truncated = page_state(&[23; 113], 3);
    let page = PageOutcome::between(&id(1), Some(&incomplete), &truncated).unwrap();
    assert!(page.patches.is_empty());
    backing.fold(&record(&log, &[page], &fence)).unwrap();
    let saved = backing.catalog_snapshot().unwrap();
    saved.validate_log_pages().unwrap();
    assert_eq!(
        saved.logs.chunks[&id(0).chunk_id].state,
        fence.after.unwrap()
    );
    assert_eq!(saved.logs.chunks[&id(0).chunk_id].receipts.len(), 1);
    assert_eq!(
        saved.logs.chunks[&id(0).chunk_id].receipts[&0]
            .append
            .as_ref()
            .unwrap()
            .attempt,
        vec![17; 16]
    );
    assert_eq!(backing.totals().unwrap(), (2, 4209));
    assert_eq!(retained.read().unwrap(), vec![23; 100]);
    assert_eq!(
        backing
            .capture_page(&id(1))
            .unwrap()
            .unwrap()
            .read()
            .unwrap(),
        vec![23; 113]
    );
}
