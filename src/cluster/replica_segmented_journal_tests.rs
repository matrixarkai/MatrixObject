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

fn sparse_background_fixture() -> (
    Dir,
    MatrixReplicaService,
    SegmentedReplicaJournal,
    StoredOutcomePosition,
) {
    let dir = Dir::new();
    let mut live = service();
    live.set_capacity_bytes(512 * 1024 * 1024).unwrap();
    let mut journal = SegmentedReplicaJournal::open_with_settings(
        &dir.path(),
        &mut live,
        1024,
        crate::durable_payload::outcome_segments::MIN_SEGMENT_BYTES,
        2,
    )
    .unwrap();
    append(
        &mut journal,
        &mut live,
        100,
        0,
        &vec![0x5d; 64 * 1024],
        true,
    );
    let original = journal.cut.clone().unwrap();
    journal.checkpoint(&live).unwrap();
    finish(&mut journal);
    journal.state.as_mut().unwrap().next_compaction_attempt =
        Instant::now() + Duration::from_secs(3600);
    let fragment = CheckpointFragment {
        version: 1,
        checkpoint_id: vec![0x69; 16],
        kind: CheckpointFragment::PAYLOAD,
        ordinal: 0,
        data: vec![0x91; CheckpointFragment::MAX_DATA_BYTES],
    };
    while journal.log.retained_segment_count().unwrap() < 3 {
        journal
            .log
            .append_decided_batch(&[DecidedMutation::CheckpointFragment(&fragment)])
            .unwrap();
    }
    journal.checkpoint(&live).unwrap();
    finish(&mut journal);
    assert!(journal.log.read_metadata(&original).is_ok());
    (dir, live, journal, original)
}

#[test]
fn segmented_background_compaction_reclaims_sparse_history_after_reader_deferral() {
    let (_dir, live, mut journal, original) = sparse_background_fixture();
    let log = journal.log.clone();
    let reader = log.metadata_cursor_after(None).unwrap();
    let before = log.logical_length().unwrap();
    let epoch = journal.published_epoch;
    let state = journal.state.as_mut().unwrap();
    state.next_collection_segments = 2;
    state.next_collection_attempt = Instant::now();
    state.next_compaction_attempt = Instant::now();
    journal.poll_background(&live, 8 * 1024 * 1024).unwrap();
    finish(&mut journal);
    assert_eq!(journal.published_epoch, epoch);
    assert_eq!(
        log.logical_length().unwrap(),
        before,
        "a pinned idle pass wrote a checkpoint"
    );
    assert!(journal.state.as_ref().unwrap().next_compaction_attempt > Instant::now());
    journal.status().unwrap().validate().unwrap();
    journal.maintenance_status().unwrap().validate().unwrap();
    drop(reader);
    let state = journal.state.as_mut().unwrap();
    state.next_compaction_attempt = Instant::now();
    state.next_collection_attempt = Instant::now();
    journal.poll_background(&live, 8 * 1024 * 1024).unwrap();
    finish(&mut journal);
    assert!(journal.published_epoch > epoch);
    assert!(log.read_metadata(&original).is_err());
    assert!(log.retained_segment_count().unwrap() < 3);
    journal.status().unwrap().validate().unwrap();
}

#[test]
fn segmented_dense_background_analysis_writes_nothing_and_backs_off() {
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
    for n in 0..16 {
        append(
            &mut journal,
            &mut live,
            n,
            0,
            &vec![n as u8 + 1; 1024 * 1024],
            false,
        );
    }
    journal.sync_deferred().unwrap();
    journal.checkpoint(&live).unwrap();
    finish(&mut journal);
    journal.state.as_mut().unwrap().next_compaction_attempt =
        Instant::now() + Duration::from_secs(3600);
    let fragment = CheckpointFragment {
        version: 1,
        checkpoint_id: vec![0x39; 16],
        kind: CheckpointFragment::PAYLOAD,
        ordinal: 0,
        data: vec![0x71; CheckpointFragment::MAX_DATA_BYTES],
    };
    while journal.log.retained_segment_count().unwrap() < 2 {
        journal
            .log
            .append_decided_batch(&[DecidedMutation::CheckpointFragment(&fragment)])
            .unwrap();
    }
    journal.checkpoint(&live).unwrap();
    finish(&mut journal);
    let state = journal.state.as_mut().unwrap();
    state.next_collection_segments = usize::MAX;
    state.next_compaction_attempt = Instant::now();
    let before = journal.log.logical_length().unwrap();
    let epoch = journal.published_epoch;
    journal.poll_background(&live, 8 * 1024 * 1024).unwrap();
    finish(&mut journal);
    assert_eq!(journal.log.logical_length().unwrap(), before);
    let status = journal.status().unwrap();
    status.validate().unwrap();
    assert_eq!(status.started_epoch, epoch);
    assert_eq!(status.published_epoch, epoch);
    assert!(!status.checkpoint_in_flight);
    assert!(journal.state.as_ref().unwrap().next_compaction_attempt > Instant::now());
    journal.poll_background(&live, 8 * 1024 * 1024).unwrap();
    assert!(journal.job.is_none());
    assert_eq!(journal.log.logical_length().unwrap(), before);
}

#[test]
fn segmented_checkpoint_requested_during_analysis_has_a_new_durable_epoch() {
    let (_dir, live, mut journal, _) = sparse_background_fixture();
    let state = journal.state.take().unwrap();
    let epoch = journal.reserve_epoch().unwrap();
    let (release, wait) = std::sync::mpsc::channel::<()>();
    journal.job = Some(thread::spawn(move || {
        let _ = wait.recv();
        Ok(Some(Completed {
            epoch,
            published: false,
            state,
            covered_bytes: 0,
        }))
    }));
    let passive = journal.maintenance_status().unwrap();
    passive.validate().unwrap();
    assert!(!passive.checkpoint_in_flight && passive.retirement_in_flight);
    let before = journal.log.logical_length().unwrap();
    journal.checkpoint_requested(&live).unwrap();
    let target = journal.maintenance_status().unwrap();
    target.validate().unwrap();
    assert!(target.started_epoch > target.published_epoch);
    journal.checkpoint_requested(&live).unwrap();
    assert_eq!(
        journal.maintenance_status().unwrap().started_epoch,
        target.started_epoch,
        "queued barriers did not coalesce"
    );
    release.send(()).unwrap();
    finish(&mut journal);
    let waiting = journal.maintenance_status().unwrap();
    waiting.validate().unwrap();
    assert!(!waiting.maintenance_is_complete(&target).unwrap());
    assert_eq!(journal.log.logical_length().unwrap(), before);
    journal.poll_background(&live, 0).unwrap();
    finish(&mut journal);
    assert!(journal
        .maintenance_status()
        .unwrap()
        .maintenance_is_complete(&target)
        .unwrap());
    journal.checkpoint_for_install(&live).unwrap();
    let installed = journal.status().unwrap();
    installed.validate().unwrap();
    assert!(installed.published_epoch > target.started_epoch);
}

#[test]
fn segmented_install_uses_published_boundary_after_its_cached_cut_is_retired() {
    let (dir, live, mut journal, original) = sparse_background_fixture();
    journal.checkpoint_requested(&live).unwrap();
    finish(&mut journal);
    assert!(journal.log.read_metadata(&original).is_err());
    journal.cut = Some(original);
    let expected = live.export_snapshot();
    journal.checkpoint_for_install(&live).unwrap();
    drop(journal);
    let mut recovered = service();
    let _journal = SegmentedReplicaJournal::open_with_settings(
        &dir.path(),
        &mut recovered,
        1024,
        crate::durable_payload::outcome_segments::MIN_SEGMENT_BYTES,
        2,
    )
    .unwrap();
    assert_eq!(recovered.export_snapshot(), expected);
}

#[test]
fn segmented_install_refuses_an_unproven_retired_cut_with_new_durable_tail() {
    for reopen in [false, true] {
        let (dir, mut live, mut journal, original) = sparse_background_fixture();
        journal.checkpoint_requested(&live).unwrap();
        finish(&mut journal);
        assert!(journal.log.read_metadata(&original).is_err());
        append(&mut journal, &mut live, 100, 64 * 1024, b"-new", true);
        let expected = live.export_snapshot();
        if reopen {
            drop(journal);
            live = service();
            journal = SegmentedReplicaJournal::open_with_settings(
                &dir.path(),
                &mut live,
                1024,
                crate::durable_payload::outcome_segments::MIN_SEGMENT_BYTES,
                2,
            )
            .unwrap();
            assert_eq!(live.export_snapshot(), expected);
        }
        journal.cut = Some(original);
        assert!(
            journal.checkpoint_for_install(&live).is_err(),
            "an unproven stale position was ignored"
        );
        drop(journal);
        let mut recovered = service();
        let _journal = SegmentedReplicaJournal::open_with_settings(
            &dir.path(),
            &mut recovered,
            1024,
            crate::durable_payload::outcome_segments::MIN_SEGMENT_BYTES,
            2,
        )
        .unwrap();
        assert_eq!(recovered.export_snapshot(), expected);
    }
}

#[test]
fn segmented_background_poll_does_not_flush_a_pending_foreground_group() {
    let dir = Dir::new();
    let mut live = service();
    let mut journal = SegmentedReplicaJournal::open(&dir.path(), &mut live, 1024).unwrap();
    let before = journal.log.logical_length().unwrap();
    let syncs = journal.sync_count();
    append(&mut journal, &mut live, 0, 0, b"queued", false);
    let queued = journal.reserve_epoch().unwrap();
    journal.queued_checkpoint = Some(queued);
    journal.started_epoch.fetch_max(queued, Ordering::Release);
    journal.poll_background(&live, 1024).unwrap();
    assert_eq!(journal.pending.len(), 1);
    assert_eq!(journal.sync_count(), syncs);
    assert_eq!(journal.log.logical_length().unwrap(), before);
    assert!(journal.job.is_none());
    journal.checkpoint_requested(&live).unwrap();
    finish(&mut journal);
    assert_eq!(journal.sync_count(), syncs + 1);
}

#[test]
fn segmented_server_compacts_without_requests_or_heartbeats() {
    use crate::cluster::{ReplicaNodeOptions, ReplicaNodeServer, ReplicaSegmentedJournalOptions};
    let (dir, live, journal, _) = sparse_background_fixture();
    let original = dir
        .path()
        .with_file_name("journal.segment-0000000000000000");
    assert!(original.exists());
    let expected = live.export_snapshot();
    let capacity = live.capacity_bytes();
    drop(journal);
    drop(live);
    let mut server = ReplicaNodeServer::bind_with_segmented_journal_options(
        "127.0.0.1:0",
        ReplicaNodeOptions {
            local_recovery_sidecar: false,
            capacity_bytes: capacity,
            ..Default::default()
        },
        &dir.path(),
        ReplicaSegmentedJournalOptions {
            segment_capacity_bytes: crate::durable_payload::outcome_segments::MIN_SEGMENT_BYTES,
            retention_segment_threshold: 2,
        },
    )
    .unwrap();
    server.enable_journal_checkpoints(8 * 1024 * 1024).unwrap();
    let stop = server.shutdown_handle();
    let worker = thread::spawn(move || server.serve());
    let until = Instant::now() + Duration::from_secs(30);
    while original.exists() {
        if Instant::now() >= until {
            stop.stop();
            let _ = worker.join();
            panic!("idle server did not compact without traffic");
        }
        thread::sleep(Duration::from_millis(10));
    }
    let start = Instant::now();
    stop.stop();
    worker.join().unwrap().unwrap();
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "maintenance shutdown waited for its retry interval"
    );
    let mut recovered = service();
    let _journal = SegmentedReplicaJournal::open_with_settings(
        &dir.path(),
        &mut recovered,
        1024,
        crate::durable_payload::outcome_segments::MIN_SEGMENT_BYTES,
        2,
    )
    .unwrap();
    assert_eq!(recovered.export_snapshot(), expected);
}

#[test]
fn segmented_checkpoint_epoch_reservation_refuses_overflow_without_losing_state() {
    let dir = Dir::new();
    let mut live = service();
    let mut journal = SegmentedReplicaJournal::open(&dir.path(), &mut live, 1024).unwrap();
    let before = journal.log.logical_length().unwrap();
    journal.epoch_sequence = u64::MAX;
    assert!(journal.checkpoint_requested(&live).is_err());
    assert!(journal.state.is_some());
    assert!(journal.job.is_none());
    assert_eq!(journal.log.logical_length().unwrap(), before);
    journal.status().unwrap().validate().unwrap();
}

#[test]
fn segmented_manual_compaction_evacuates_cold_data_and_metadata_after_readers_leave() {
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
    let cold = vec![0x5d; 64 * 1024];
    append(&mut journal, &mut live, 100, 0, &cold, true);
    let original = journal.cut.clone().unwrap();
    journal.checkpoint(&live).unwrap();
    finish(&mut journal);
    journal.state.as_mut().unwrap().next_compaction_attempt =
        Instant::now() + Duration::from_secs(3600);
    let log = journal.log.clone();
    let fragment = CheckpointFragment {
        version: 1,
        checkpoint_id: vec![37; 16],
        kind: CheckpointFragment::PAYLOAD,
        ordinal: 0,
        data: vec![0x91; CheckpointFragment::MAX_DATA_BYTES],
    };
    while log.retained_segment_count().unwrap() < 3 {
        log.append_decided_batch(&[DecidedMutation::CheckpointFragment(&fragment)])
            .unwrap();
    }
    // Move the physical replay boundary to the active segment while reusing the
    // cold page's original payload and metadata records.
    journal.checkpoint(&live).unwrap();
    finish(&mut journal);
    let reader = log.metadata_cursor_after(None).unwrap();
    journal.checkpoint_requested(&live).unwrap();
    finish(&mut journal);
    assert!(
        log.read_metadata(&original).is_ok(),
        "an active reader lost its file"
    );
    drop(reader);
    journal.checkpoint_requested(&live).unwrap();
    finish(&mut journal);
    assert!(
        log.read_metadata(&original).is_err(),
        "cold data or reused metadata still pins the obsolete file"
    );
    assert!(log.retained_segment_count().unwrap() < 3);
    let expected = live.export_snapshot();
    drop(log);
    drop(journal);
    drop(live);
    let mut live = service();
    let mut journal = SegmentedReplicaJournal::open_with_settings(
        &dir.path(),
        &mut live,
        1024,
        crate::durable_payload::outcome_segments::MIN_SEGMENT_BYTES,
        2,
    )
    .unwrap();
    assert_eq!(live.export_snapshot(), expected);
    assert_eq!(
        live.read_chunk_state(&page(100)).unwrap().data.as_slice(),
        cold
    );
    append(
        &mut journal,
        &mut live,
        100,
        cold.len() as u64,
        b"-tail",
        true,
    );
    journal.checkpoint_requested(&live).unwrap();
    finish(&mut journal);
    let expected = live.export_snapshot();
    drop(journal);
    let mut live = service();
    let _journal = SegmentedReplicaJournal::open_with_settings(
        &dir.path(),
        &mut live,
        1024,
        crate::durable_payload::outcome_segments::MIN_SEGMENT_BYTES,
        2,
    )
    .unwrap();
    assert_eq!(live.export_snapshot(), expected);
}

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
/// The heartbeat reads a background failure through this handle, without the journal's owner.
#[test]
fn the_failure_handle_is_the_cell_the_journal_records_failure_in() {
    let dir = Dir::new();
    let mut service = service();
    let journal = SegmentedReplicaJournal::open(&dir.path(), &mut service, 1024).unwrap();
    let handle = ReplicaJournalBackend::failure_handle(&journal).unwrap();
    assert!(Arc::ptr_eq(&handle, &journal.failure));
    assert!(handle.lock().unwrap().is_none());
    *journal.failure.lock().unwrap() = Some("worker failed".into());
    assert!(handle.lock().unwrap().is_some());
}

#[test]
fn segmented_backing_observes_only_synchronized_pages() {
    let dir = Dir::new();
    let mut live = service();
    let mut journal = SegmentedReplicaJournal::open(&dir.path(), &mut live, 1024).unwrap();
    append(&mut journal, &mut live, 10, 0, b"before", true);
    let backing = journal.enable_page_backing().unwrap();
    let held = backing.capture_page(&page(10)).unwrap().unwrap();
    assert_eq!(backing.totals().unwrap(), (1, 6));
    append(&mut journal, &mut live, 10, 6, b"after", false);
    append(&mut journal, &mut live, 11, 0, b"sibling", false);
    assert_eq!(backing.totals().unwrap(), (1, 6));
    assert_eq!(
        backing
            .capture_page(&page(10))
            .unwrap()
            .unwrap()
            .read()
            .unwrap(),
        b"before"
    );
    assert!(backing.metadata(&page(11)).unwrap().is_none());
    journal.sync_deferred().unwrap();
    assert_eq!(backing.totals().unwrap(), (2, 18));
    assert_eq!(
        backing
            .capture_page(&page(10))
            .unwrap()
            .unwrap()
            .read()
            .unwrap(),
        b"beforeafter"
    );
    assert_eq!(
        backing
            .capture_page(&page(11))
            .unwrap()
            .unwrap()
            .read()
            .unwrap(),
        b"sibling"
    );
    assert_eq!(held.read().unwrap(), b"before");
    assert!(Arc::ptr_eq(
        &backing,
        &journal.enable_page_backing().unwrap()
    ));
}

#[test]
fn segmented_backing_install_keeps_shared_identity_and_sibling_pages() {
    let dir = Dir::new();
    let mut live = service();
    let mut journal = SegmentedReplicaJournal::open(&dir.path(), &mut live, 1024).unwrap();
    append(&mut journal, &mut live, 10, 0, b"original", true);
    append(&mut journal, &mut live, 11, 0, b"sibling", true);
    let backing = journal.enable_page_backing().unwrap();
    let held = backing.capture_page(&page(10)).unwrap().unwrap();
    live.write_chunk(page(10), 0, b"repaired").unwrap();
    journal.checkpoint_for_install(&live).unwrap();
    assert!(Arc::ptr_eq(
        &backing,
        &journal.enable_page_backing().unwrap()
    ));
    assert_eq!(
        backing
            .capture_page(&page(10))
            .unwrap()
            .unwrap()
            .read()
            .unwrap(),
        b"repaired"
    );
    assert_eq!(
        backing
            .capture_page(&page(11))
            .unwrap()
            .unwrap()
            .read()
            .unwrap(),
        b"sibling"
    );
    assert_eq!(held.read().unwrap(), b"original");
    assert_eq!(backing.totals().unwrap(), (2, 15));
    drop(held);
    drop(backing);
    drop(journal);
    let mut recovered = service();
    let mut journal = SegmentedReplicaJournal::open(&dir.path(), &mut recovered, 1024).unwrap();
    let backing = journal.enable_page_backing().unwrap();
    assert_eq!(
        backing
            .capture_page(&page(10))
            .unwrap()
            .unwrap()
            .read()
            .unwrap(),
        b"repaired"
    );
    assert_eq!(
        backing
            .capture_page(&page(11))
            .unwrap()
            .unwrap()
            .read()
            .unwrap(),
        b"sibling"
    );
    assert_eq!(recovered.export_snapshot(), live.export_snapshot());
}

#[test]
fn segmented_backing_compaction_publishes_locations_before_source_retirement() {
    let (_dir, live, mut journal, original) = sparse_background_fixture();
    let backing = journal.enable_page_backing().unwrap();
    let held = backing.capture_page(&page(100)).unwrap().unwrap();
    let expected = held.read().unwrap();
    let log = journal.log.clone();
    let old_sources = backing.protected_segments().unwrap();
    assert!(!old_sources.is_empty());
    assert!(!log.retention_readers_active());
    let state = journal.state.as_mut().unwrap();
    state.next_compaction_attempt = Instant::now();
    state.next_collection_attempt = Instant::now();
    state.next_collection_segments = 2;
    journal.poll_background(&live, 8 * 1024 * 1024).unwrap();
    finish(&mut journal);
    assert!(log.read_metadata(&original).is_err());
    assert!(backing
        .protected_segments()
        .unwrap()
        .is_disjoint(&old_sources));
    assert_eq!(
        backing
            .capture_page(&page(100))
            .unwrap()
            .unwrap()
            .read()
            .unwrap(),
        expected
    );
    assert_eq!(held.read().unwrap(), expected);
    // Drain the existing owner before acquiring the same cleanup gate below.
    if let Some(cleanup) = journal.cleanup.take() {
        cleanup.join().unwrap().unwrap();
    }
    drop(held);
    retention::finish_retirements(&log).unwrap();
    assert_eq!(log.retired_segment_count().unwrap(), 0);
}

#[test]
fn segmented_backing_activation_leaves_unpublished_fragments_outside_its_cut() {
    let dir = Dir::new();
    let mut live = service();
    let mut journal = SegmentedReplicaJournal::open(&dir.path(), &mut live, 1024).unwrap();
    append(&mut journal, &mut live, 10, 0, b"confirmed", true);
    journal.checkpoint(&live).unwrap();
    finish(&mut journal);
    let boundary = journal
        .log
        .checkpoint_cut(
            journal.cut.as_ref(),
            journal
                .state
                .as_ref()
                .unwrap()
                .publication
                .as_ref()
                .map(|(_, position)| position),
        )
        .unwrap()
        .unwrap();
    let fragment = CheckpointFragment {
        version: 1,
        checkpoint_id: vec![0xa7; 16],
        kind: CheckpointFragment::PAYLOAD,
        ordinal: 0,
        data: vec![0x71; CheckpointFragment::MAX_DATA_BYTES],
    };
    let unpublished = journal
        .log
        .append_decided_batch(&[DecidedMutation::CheckpointFragment(&fragment)])
        .unwrap();
    assert_ne!(unpublished.position, boundary);
    let backing = journal.enable_page_backing().unwrap();
    assert_eq!(
        journal.state.as_ref().unwrap().cut.as_ref(),
        Some(&boundary)
    );
    journal.checkpoint(&live).unwrap();
    finish(&mut journal);
    assert_eq!(
        backing
            .capture_page(&page(10))
            .unwrap()
            .unwrap()
            .read()
            .unwrap(),
        b"confirmed"
    );
    append(&mut journal, &mut live, 10, 9, b"-next", true);
    journal.checkpoint(&live).unwrap();
    finish(&mut journal);
    assert_eq!(
        backing
            .capture_page(&page(10))
            .unwrap()
            .unwrap()
            .read()
            .unwrap(),
        b"confirmed-next"
    );
}
