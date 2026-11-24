// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use crate::checkpoint_fragment::CheckpointFragment;

#[test]
fn outcome_write_statistics_separate_foreground_and_mixed_checkpoint_records() {
    let dir = Directory::new();
    let log = SegmentedOutcomeLog::open(dir.path(), MIN_SEGMENT_BYTES).unwrap();
    let before = log.write_statistics().unwrap();
    let a = outcome(0, 4096);
    let b = outcome(1, 4096);
    let foreground = log
        .append_decided_batch(&[DecidedMutation::Page(&a), DecidedMutation::Page(&b)])
        .unwrap();
    let fragment = |kind, ordinal, length| CheckpointFragment {
        version: 1,
        checkpoint_id: vec![7; 16],
        kind,
        ordinal,
        data: vec![9; length],
    };
    let payload = fragment(CheckpointFragment::PAYLOAD, 0, 4096);
    let catalog = fragment(CheckpointFragment::CATALOG, 1, 13);
    let commit = fragment(CheckpointFragment::COMMIT, 2, 7);
    let maintenance = log
        .append_decided_batch(&[
            DecidedMutation::CheckpointFragment(&payload),
            DecidedMutation::CheckpointFragment(&catalog),
            DecidedMutation::CheckpointFragment(&commit),
        ])
        .unwrap();
    let delta = log
        .write_statistics()
        .unwrap()
        .checked_delta(&before)
        .unwrap();
    assert_eq!(delta.foreground_batches, 1);
    assert_eq!(delta.foreground_decisions, 2);
    assert_eq!(delta.foreground_record_bytes, foreground.position.length);
    assert_eq!(delta.foreground_payload_bytes, 8192);
    assert_eq!(delta.maintenance_batches, 1);
    assert_eq!(delta.maintenance_record_bytes, maintenance.position.length);
    assert_eq!(delta.maintenance_payload_bytes, 4096);
    assert_eq!(delta.maintenance_metadata_bytes, 20);
    assert_eq!(
        delta.foreground_record_bytes + delta.maintenance_record_bytes,
        log.logical_length().unwrap()
    );
    assert!(log.append_decided_batch(&[]).is_err());
    assert_eq!(
        log.write_statistics()
            .unwrap()
            .checked_delta(&before)
            .unwrap(),
        delta
    );
}

#[test]
fn outcome_write_statistics_survive_rollover_but_not_journal_reopen() {
    let _large = LARGE.lock().unwrap_or_else(|error| error.into_inner());
    let dir = Directory::new();
    let log = SegmentedOutcomeLog::open(dir.path(), MIN_SEGMENT_BYTES).unwrap();
    let before = log.write_statistics().unwrap();
    let positions = full(&log);
    let last = log.append_page(&outcome(8, 3 * 1024 * 1024)).unwrap();
    assert!(log.segment_count().unwrap() > 1);
    let after = log.write_statistics().unwrap();
    let delta = after.checked_delta(&before).unwrap();
    assert_eq!(delta.foreground_batches, 8);
    assert_eq!(delta.foreground_decisions, 8);
    assert_eq!(delta.foreground_payload_bytes, 24 * 1024 * 1024);
    assert_eq!(
        delta.foreground_record_bytes,
        positions.iter().map(|p| p.length).sum::<u64>() + last.position.length
    );
    let retained = log
        .prepare_retention(&std::collections::BTreeSet::new(), &last.position, &|| {
            Ok(())
        })
        .unwrap()
        .unwrap()
        .commit()
        .unwrap()
        .finish()
        .unwrap();
    assert!(retained.removed > 0);
    assert_eq!(log.write_statistics().unwrap(), after);
    assert!(log.logical_length().unwrap() < after.foreground_record_bytes);
    let id = log.store_id().to_vec();
    drop(log);
    let reopened = SegmentedOutcomeLog::open(dir.path(), MIN_SEGMENT_BYTES).unwrap();
    assert_eq!(reopened.store_id(), id);
    let reset = reopened.write_statistics().unwrap();
    assert_ne!(reset.scope_id, after.scope_id);
    assert_eq!(reset.foreground_batches, 0);
    assert_eq!(reset.foreground_record_bytes, 0);
    assert!(reset.checked_delta(&after).is_err());
    assert!(before.checked_delta(&after).is_err());
}

#[test]
fn outcome_write_statistics_exclude_an_uncertain_failed_sync() {
    let dir = Directory::new();
    let log = SegmentedOutcomeLog::open(dir.path(), MIN_SEGMENT_BYTES).unwrap();
    let value = outcome(0, 4096);
    assert!(log
        .append_with(
            &[DecidedMutation::Page(&value)],
            |_| { Err(io::Error::other("injected sync failure")) },
            publish_rollover
        )
        .is_err());
    assert!(log.write_statistics().is_err());
    let writer = log.writer.lock().unwrap();
    assert_eq!(writer.write_statistics.foreground_batches, 0);
    assert_eq!(writer.write_statistics.foreground_record_bytes, 0);
}

#[test]
fn outcome_write_statistics_refuse_saturation_and_invalid_geometry() {
    let dir = Directory::new();
    let log = SegmentedOutcomeLog::open(dir.path(), MIN_SEGMENT_BYTES).unwrap();
    let record = log.append_page(&outcome(0, 4096)).unwrap();
    let before = log.write_statistics().unwrap();
    let mut saturated = before.clone();
    saturated.foreground_record_bytes = u64::MAX;
    saturated.observe(&record);
    assert!(saturated.saturated);
    assert_eq!(saturated.foreground_record_bytes, u64::MAX);
    assert!(saturated.validate().is_ok());
    assert!(saturated.checked_delta(&before).is_err());
    let mut invalid = before.clone();
    invalid.scope_id.pop();
    assert!(invalid.validate().is_err());
    let mut invalid = before.clone();
    invalid.foreground_decisions = 65;
    assert!(invalid.validate().is_err());
    let mut invalid = before;
    invalid.foreground_payload_bytes = invalid.foreground_record_bytes + 1;
    assert!(invalid.validate().is_err());
}
