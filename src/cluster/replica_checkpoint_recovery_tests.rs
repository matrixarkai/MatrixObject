// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::super::replica_checkpoint_catalog;
use super::*;
use crate::durable_payload::{outcome_log::DecidedMutation, outcome_segments::MIN_SEGMENT_BYTES};
use crate::{
    chunk_namespace_fence::{ImmutablePage, NamespaceFence, NamespaceFenceOutcome},
    outcome::PageOutcome,
    ChunkId, PageId,
};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Dir(PathBuf);
impl Dir {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "replica-checkpoint-recovery-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&p).unwrap();
        Self(p)
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
fn service() -> MatrixReplicaService {
    let mut s = MatrixReplicaService::new();
    s.set_keep_ec_sidecar(false);
    s.configure_metadata_checkpoint_interval(17).unwrap();
    s.set_defer_metadata_checkpoints(true);
    s
}
fn append(
    log: &SegmentedOutcomeLog,
    service: &mut MatrixReplicaService,
    empty: &ReplicaServiceSnapshot,
    n: u64,
    step: u64,
) -> StoredRecordMetadata {
    let before = service.read_chunk_state(&id(n)).ok();
    let counters = PageOutcome::counters(service);
    service
        .write_chunk(id(n), step % 16 * 256, &vec![step as u8 + 1; 256])
        .unwrap();
    let after = service.read_chunk_state(&id(n)).unwrap();
    let outcome = PageOutcome::between(&id(n), before.as_ref(), &after)
        .unwrap()
        .with_counters(counters, PageOutcome::counters(service));
    let metadata = encode_scalars(capture_scalars(service, empty)).unwrap();
    log.append_decided_batch_with_metadata(&[DecidedMutation::Page(&outcome)], &metadata)
        .unwrap()
}
#[test]
fn checkpoint_recovery_folds_history_and_reopens_exact_page_and_scalar_state() {
    let dir = Dir::new();
    let log = dir.open();
    let empty = scalar_template();
    let mut service = service();
    let mut worker =
        CheckpointState::new(service.export_snapshot_unsealed_without_pages()).unwrap();
    let mut cut = None;
    for step in 0..96 {
        cut = Some(append(&log, &mut service, &empty, step % 8, step).position);
    }
    worker
        .fold_through(&log, cut.as_ref().unwrap(), 16)
        .unwrap();
    worker
        .set_scalars(capture_scalars(&service, &empty))
        .unwrap();
    replica_checkpoint_catalog::prepare(
        &log,
        &worker.catalog,
        worker.metadata_bytes().unwrap(),
        worker.cut.clone(),
        1,
        false,
    )
    .unwrap()
    .publish(&log)
    .unwrap();
    for step in 96..128 {
        append(&log, &mut service, &empty, step % 8, step);
    }
    let expected = service.export_snapshot();
    drop(worker);
    drop(log);
    let log = dir.open();
    let loaded = replica_checkpoint_catalog::latest(&log, 16)
        .unwrap()
        .unwrap();
    let mut worker = CheckpointState::from_checkpoint(loaded).unwrap();
    worker.fold_available(&log, 16).unwrap();
    let mut restored = MatrixReplicaService::new();
    worker.restore(&log, &mut restored).unwrap();
    assert_eq!(restored.export_snapshot(), expected);
    // Serving a subsequent mutation must not copy a catalog shared with the worker.
    let live = restored.export_snapshot_unsealed_without_pages();
    assert!(!Arc::ptr_eq(
        &live.write_request_receipts,
        &worker.metadata.write_request_receipts
    ));
    assert!(!Arc::ptr_eq(
        &live.discard_request_receipts,
        &worker.metadata.discard_request_receipts
    ));
}
#[test]
fn checkpoint_recovery_scalar_capture_does_not_pin_live_payloads_or_receipt_maps() {
    let mut service = service();
    service
        .write_chunk_idempotent(id(0), 0, b"seed", "seed-write")
        .unwrap();
    let empty = scalar_template();
    let live = service.export_snapshot_unsealed_without_pages();
    let page = service.read_chunk_state(&id(0)).unwrap();
    let maps = Arc::strong_count(&live.write_request_receipts);
    let payload = Arc::strong_count(&page.data);
    let captured: Vec<_> = (0..64).map(|_| capture_scalars(&service, &empty)).collect();
    assert_eq!(Arc::strong_count(&live.write_request_receipts), maps);
    assert_eq!(Arc::strong_count(&page.data), payload);
    for shot in captured {
        assert!(shot.pages.is_empty());
        assert!(!has_maps(&shot));
        assert!(Arc::ptr_eq(
            &shot.write_request_receipts,
            &empty.write_request_receipts
        ));
    }
    let worker = CheckpointState::new(service.export_snapshot_unsealed_without_pages()).unwrap();
    assert!(!Arc::ptr_eq(
        &live.write_request_receipts,
        &worker.metadata.write_request_receipts
    ));
}
#[test]
fn checkpoint_recovery_namespace_catalog_survives_scalar_replacement_and_checkpoint() {
    let dir = Dir::new();
    let log = dir.open();
    let mut service = service();
    let empty = scalar_template();
    let mut worker =
        CheckpointState::new(service.export_snapshot_unsealed_without_pages()).unwrap();
    let record = append(&log, &mut service, &empty, 0, 0);
    worker.fold_record(&record, 4).unwrap();
    let page = service.read_chunk_state(&id(0)).unwrap();
    let fence = NamespaceFence {
        version: 1,
        volume_id: 1,
        chunk_id: 1,
        generation: 1,
        pages: vec![ImmutablePage::from_state(0, &page).unwrap()],
    };
    let outcome = NamespaceFenceOutcome::decide(&service, fence).unwrap();
    outcome.apply(&mut service).unwrap();
    let metadata = encode_scalars(capture_scalars(&service, &empty)).unwrap();
    let record = log
        .append_decided_batch_with_metadata(&[DecidedMutation::NamespaceFence(&outcome)], &metadata)
        .unwrap();
    worker.fold_record(&record, 4).unwrap();
    worker
        .set_scalars(capture_scalars(&service, &empty))
        .unwrap();
    assert_eq!(
        worker.metadata.namespace_fences,
        service
            .export_snapshot_unsealed_without_pages()
            .namespace_fences
    );
    replica_checkpoint_catalog::prepare(
        &log,
        &worker.catalog,
        worker.metadata_bytes().unwrap(),
        worker.cut.clone(),
        1,
        false,
    )
    .unwrap()
    .publish(&log)
    .unwrap();
    let loaded = replica_checkpoint_catalog::latest(&log, 4)
        .unwrap()
        .unwrap();
    let worker = CheckpointState::from_checkpoint(loaded).unwrap();
    let mut restored = MatrixReplicaService::new();
    worker.restore(&log, &mut restored).unwrap();
    assert_eq!(restored.export_snapshot(), service.export_snapshot());
    assert!(restored.write_chunk(id(1), 0, b"closed").is_err());
}
#[test]
fn checkpoint_recovery_requires_scalar_context_and_enforces_page_limit() {
    let dir = Dir::new();
    let log = dir.open();
    let mut service = service();
    let empty = scalar_template();
    let mut worker =
        CheckpointState::new(service.export_snapshot_unsealed_without_pages()).unwrap();
    let first = append(&log, &mut service, &empty, 0, 0);
    worker.fold_record(&first, 1).unwrap();
    let next = append(&log, &mut service, &empty, 1, 1);
    assert!(worker.fold_record(&next, 1).is_err());
    let state = service.read_chunk_state(&id(0)).unwrap();
    let old = log
        .append_page(&PageOutcome::between(&id(0), None, &state).unwrap())
        .unwrap();
    let mut worker =
        CheckpointState::new(service.export_snapshot_unsealed_without_pages()).unwrap();
    assert!(worker.fold_record(&old, 16).is_err());
    let mut bad = first;
    bad.batch.replica_metadata = b"not-a-snapshot".to_vec();
    assert!(worker.fold_record(&bad, 16).is_err());
    assert!(encode_scalars(service.export_snapshot_unsealed()).is_err());
}
