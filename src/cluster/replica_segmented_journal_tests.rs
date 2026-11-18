// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use crate::{ChunkId, PageId};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};
static NEXT: AtomicU64 = AtomicU64::new(0);

#[test]
fn segmented_checkpoint_advances_storage_cut_and_reclaims_quiet_history() {
    let dir = Dir::new();
    let mut live = service();
    let mut journal = SegmentedReplicaJournal::open_with_settings(
        &dir.path(),
        &mut live,
        1024,
        crate::durable_payload::outcome_segments::MIN_SEGMENT_BYTES,
        2,
    )
    .unwrap();
    append(&mut journal, &mut live, 100, 0, b"cold", true);
    let log = journal.log.clone();
    let reader = log.metadata_cursor_after(None).unwrap();
    let fragment = CheckpointFragment {
        version: 1,
        checkpoint_id: log.store_id().to_vec(),
        kind: CheckpointFragment::PAYLOAD,
        ordinal: 0,
        data: vec![0x93; CheckpointFragment::MAX_DATA_BYTES],
    };
    let mut fence_position = None;
    for _ in 0..64 {
        if log.retained_segment_count().unwrap() == 2 && fence_position.is_none() {
            let fence = crate::chunk_namespace_fence::NamespaceFence {
                version: 1,
                volume_id: 1,
                chunk_id: 999,
                generation: 1,
                pages: Vec::new(),
            };
            let outcome = NamespaceFenceOutcome::decide(&live, fence).unwrap();
            outcome.apply(&mut live).unwrap();
            let context = journal.capture_context(&live);
            journal
                .append_namespace_owned(outcome, context, true)
                .unwrap();
            fence_position = journal.cut.clone();
        }
        if log.retained_segment_count().unwrap() >= 3 {
            break;
        }
        log.append_decided_batch(&[DecidedMutation::CheckpointFragment(&fragment)])
            .unwrap();
    }
    let fence_position = fence_position.expect("a metadata-only mutation in the middle segment");
    assert_eq!(log.retained_segment_count().unwrap(), 3);
    journal.checkpoint(&live).unwrap();
    finish(&mut journal);
    assert_eq!(log.retained_segment_count().unwrap(), 3);
    let first_root = journal
        .state
        .as_ref()
        .unwrap()
        .publication
        .as_ref()
        .unwrap()
        .1
        .clone();
    drop(reader);
    let expected = live.export_snapshot();
    journal.checkpoint_requested(&live).unwrap();
    finish(&mut journal);
    assert_eq!(
        journal.state.as_ref().unwrap().cut.as_ref(),
        Some(&first_root)
    );
    assert!(
        log.retained_segment_count().unwrap() < 3,
        "a quiet checkpoint must retire storage-only history before its captured root"
    );
    assert!(
        log.read_metadata(&fence_position).is_err(),
        "the old mutation record must be retired"
    );
    journal.checkpoint_requested(&live).unwrap();
    finish(&mut journal);
    assert_eq!(live.export_snapshot(), expected);
    drop(journal);
    drop(log);
    let mut restored = service();
    let mut journal = SegmentedReplicaJournal::open_with_settings(
        &dir.path(),
        &mut restored,
        1024,
        crate::durable_payload::outcome_segments::MIN_SEGMENT_BYTES,
        2,
    )
    .unwrap();
    assert_eq!(restored.export_snapshot(), expected);
    append(&mut journal, &mut restored, 100, 4, b"-tail", true);
    journal.checkpoint_requested(&restored).unwrap();
    finish(&mut journal);
    let expected = restored.export_snapshot();
    drop(journal);
    let mut restored = service();
    let _journal = SegmentedReplicaJournal::open_with_settings(
        &dir.path(),
        &mut restored,
        1024,
        crate::durable_payload::outcome_segments::MIN_SEGMENT_BYTES,
        2,
    )
    .unwrap();
    assert_eq!(restored.export_snapshot(), expected);
}

#[test]
fn segmented_replica_retirement_preserves_cold_payloads_and_reopens_overwritten_pages() {
    let dir = Dir::new();
    let mut live = service();
    let mut journal = SegmentedReplicaJournal::open_with_settings(
        &dir.path(),
        &mut live,
        1024,
        crate::durable_payload::outcome_segments::MIN_SEGMENT_BYTES,
        2,
    )
    .unwrap();
    append(&mut journal, &mut live, 100, 0, &[0xcd; 4096], true);
    let mut collections = 0;
    for generation in 0..8 {
        let bytes = vec![generation as u8 + 7; 1024 * 1024];
        for n in 0..16 {
            append(&mut journal, &mut live, n, 0, &bytes, false);
        }
        journal.sync_deferred().unwrap();
        let before = journal.log.retained_segment_count().unwrap();
        journal.checkpoint(&live).unwrap();
        finish(&mut journal);
        let after = journal.log.retained_segment_count().unwrap();
        if after < before {
            collections += 1;
        }
        assert_eq!(
            &*live.read_chunk_state(&page(100)).unwrap().data,
            &[0xcd; 4096]
        );
    }
    assert!(
        collections >= 2,
        "automatic collection did not retire overwritten history"
    );
    assert_eq!(
        &std::fs::read(dir.path().with_file_name("journal.segments")).unwrap()[..8],
        b"MOSEGM02"
    );
    assert_eq!(journal.log.retired_segment_count().unwrap(), 0);
    let expected = live.export_snapshot();
    drop(journal);
    let mut restored = MatrixReplicaService::new();
    let journal = SegmentedReplicaJournal::open_with_settings(
        &dir.path(),
        &mut restored,
        1024,
        crate::durable_payload::outcome_segments::MIN_SEGMENT_BYTES,
        2,
    )
    .unwrap();
    assert_eq!(restored.export_snapshot(), expected);
    assert!(journal.log.retained_segment_count().unwrap() < 8);
}
struct Dir(PathBuf);
impl Dir {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "replica-segmented-journal-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&p).unwrap();
        Self(p)
    }
    fn path(&self) -> PathBuf {
        self.0.join("journal")
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn service() -> MatrixReplicaService {
    let mut s = MatrixReplicaService::new();
    s.set_keep_ec_sidecar(false);
    s.configure_metadata_checkpoint_interval(19).unwrap();
    s.set_defer_metadata_checkpoints(true);
    s
}
fn page(n: u64) -> PageId {
    PageId {
        chunk_id: ChunkId::new(1, 1).unwrap(),
        page_index: n,
    }
}
fn append(
    journal: &mut SegmentedReplicaJournal,
    service: &mut MatrixReplicaService,
    n: u64,
    offset: u64,
    bytes: &[u8],
    sync: bool,
) {
    let before = service.read_chunk_state(&page(n)).ok();
    let counters = PageOutcome::counters(service);
    service.write_chunk(page(n), offset, bytes).unwrap();
    let after = service.read_chunk_state(&page(n)).unwrap();
    let outcome = PageOutcome::between(&page(n), before.as_ref(), &after)
        .unwrap()
        .with_counters(counters, PageOutcome::counters(service));
    let context = journal.capture_context(service);
    journal.append_owned(outcome, context, sync).unwrap();
}
fn finish(journal: &mut SegmentedReplicaJournal) {
    let until = Instant::now() + Duration::from_secs(20);
    while journal.job.is_some() {
        journal.check_health().unwrap();
        assert!(Instant::now() < until, "checkpoint worker did not finish");
        thread::sleep(Duration::from_millis(1));
    }
}
#[test]
fn segmented_replica_group_syncs_once_and_reopens_exact_state() {
    let dir = Dir::new();
    let mut service = service();
    let mut journal = SegmentedReplicaJournal::open(&dir.path(), &mut service, 1024).unwrap();
    for n in 0..64 {
        append(
            &mut journal,
            &mut service,
            n,
            0,
            &vec![n as u8; 4096],
            false,
        );
    }
    assert_eq!(journal.logical_length(), 64);
    assert_eq!(journal.sync_count(), 0);
    journal.sync_deferred().unwrap();
    assert_eq!(journal.sync_count(), 1);
    let expected = service.export_snapshot();
    drop(journal);
    let mut recovered = MatrixReplicaService::new();
    let journal = SegmentedReplicaJournal::open(&dir.path(), &mut recovered, 1024).unwrap();
    assert_eq!(recovered.export_snapshot(), expected);
    assert!(
        journal.checkpoint_due(1024),
        "recovery must retain uncheckpointed mutation debt"
    );
    drop(journal);
}
#[test]
fn segmented_replica_background_checkpoint_preserves_concurrent_grouped_tail() {
    let dir = Dir::new();
    let mut service = service();
    let mut journal = SegmentedReplicaJournal::open(&dir.path(), &mut service, 1024).unwrap();
    for n in 0..256 {
        append(
            &mut journal,
            &mut service,
            n,
            0,
            &vec![n as u8; 4096],
            false,
        );
    }
    journal.sync_deferred().unwrap();
    assert!(journal.checkpoint_due(1024));
    let cut = journal.cut.clone();
    let revision = journal.logical_length();
    journal.checkpoint(&service).unwrap();
    assert_eq!(journal.logical_length(), revision);
    for n in 0..128 {
        append(
            &mut journal,
            &mut service,
            n,
            4096,
            b"concurrent-tail",
            false,
        );
    }
    journal.sync_deferred().unwrap();
    finish(&mut journal);
    let loaded = replica_checkpoint_catalog::latest(&journal.log, 1024)
        .unwrap()
        .unwrap();
    assert_eq!(loaded.commit.through, cut);
    assert!(!loaded.header.install);
    assert!(
        journal.bytes_since_checkpoint > 0,
        "concurrent mutations must retain checkpoint debt"
    );
    let expected = service.export_snapshot();
    drop(journal);
    let mut recovered = MatrixReplicaService::new();
    let _journal = SegmentedReplicaJournal::open(&dir.path(), &mut recovered, 1024).unwrap();
    assert_eq!(recovered.export_snapshot(), expected);
}
#[test]
fn segmented_replica_install_supersedes_background_checkpoint_and_keeps_new_tail() {
    let dir = Dir::new();
    let mut service = service();
    let mut journal = SegmentedReplicaJournal::open(&dir.path(), &mut service, 1024).unwrap();
    for n in 0..128 {
        append(
            &mut journal,
            &mut service,
            n,
            0,
            &vec![n as u8; 4096],
            false,
        );
    }
    journal.sync_deferred().unwrap();
    journal.checkpoint(&service).unwrap();
    let mut candidate = MatrixReplicaService::new();
    candidate.set_keep_ec_sidecar(false);
    candidate
        .write_chunk(page(900), 0, &vec![0x61; 2 * MAX_EXTENT_BYTES + 19])
        .unwrap();
    candidate
        .write_chunk(page(901), 0, &vec![0; MAX_EXTENT_BYTES + 17])
        .unwrap();
    journal.checkpoint_for_install(&candidate).unwrap();
    service = candidate;
    append(
        &mut journal,
        &mut service,
        900,
        0,
        b"tail-after-install",
        true,
    );
    let expected = service.export_snapshot();
    drop(journal);
    let mut recovered = MatrixReplicaService::new();
    let _journal = SegmentedReplicaJournal::open(&dir.path(), &mut recovered, 1024).unwrap();
    assert_eq!(recovered.export_snapshot(), expected);
    assert!(recovered.read_chunk_state(&page(0)).is_err());
}
#[test]
fn segmented_replica_batch_overflow_keeps_context_for_each_committed_group() {
    let dir = Dir::new();
    let mut service = service();
    let mut journal = SegmentedReplicaJournal::open(&dir.path(), &mut service, 128).unwrap();
    // The second large outcome forces the first group to flush while service
    // already includes the second mutation. Its own earlier scalar context wins.
    append(
        &mut journal,
        &mut service,
        0,
        0,
        &vec![0x61; 5 * 1024 * 1024],
        false,
    );
    let first_expected = service.export_snapshot();
    append(
        &mut journal,
        &mut service,
        1,
        0,
        &vec![0x62; 5 * 1024 * 1024],
        false,
    );
    assert_eq!(journal.sync_count(), 1);
    let mut worker = CheckpointState::from_checkpoint(
        replica_checkpoint_catalog::latest(&journal.log, 128)
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    worker.fold_available(&journal.log, 128).unwrap();
    let mut first = MatrixReplicaService::new();
    worker.restore(&journal.log, &mut first).unwrap();
    assert_eq!(first.export_snapshot(), first_expected);
    journal.sync_deferred().unwrap();
    assert_eq!(journal.sync_count(), 2);
    let expected = service.export_snapshot();
    drop(journal);
    let mut recovered = MatrixReplicaService::new();
    let _journal = SegmentedReplicaJournal::open(&dir.path(), &mut recovered, 128).unwrap();
    assert_eq!(recovered.export_snapshot(), expected);
}
