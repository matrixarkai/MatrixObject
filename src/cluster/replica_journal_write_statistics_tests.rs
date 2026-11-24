// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use crate::durable_payload::outcome_segments::OutcomeWriteStatistics;

#[test]
fn journal_control_write_statistics_are_scoped_bounded_and_versioned() {
    let stats = OutcomeWriteStatistics {
        scope_id: vec![4; 16],
        foreground_batches: 2,
        foreground_decisions: 3,
        foreground_record_bytes: 24576,
        foreground_payload_bytes: 12288,
        maintenance_batches: 1,
        maintenance_record_bytes: 12288,
        maintenance_payload_bytes: 4096,
        maintenance_metadata_bytes: 128,
        ..Default::default()
    };
    let status = ReplicaJournalStatus {
        version: 3,
        layout: 2,
        journal_id: vec![6; 16],
        started_epoch: 7,
        published_epoch: 7,
        journal_bytes: 36864,
        segments: 2,
        write_statistics: Some(stats.clone()),
        ..Default::default()
    };
    let bytes = status.encode_to_vec();
    assert!(bytes.len() <= 256);
    assert_eq!(ReplicaJournalStatus::from_payload(&bytes).unwrap(), status);
    let mut legacy = status.clone();
    legacy.version = 2;
    legacy.write_statistics = None;
    assert!(status.maintenance_is_complete(&legacy).unwrap());
    assert!(legacy.maintenance_is_complete(&status).unwrap());
    for version in [1, 2] {
        let mut bad = status.clone();
        bad.version = version;
        assert!(ReplicaJournalStatus::from_payload(&bad.encode_to_vec()).is_err());
    }
    let mut missing = status.clone();
    missing.write_statistics = None;
    assert!(ReplicaJournalStatus::from_payload(&missing.encode_to_vec()).is_err());
    let mut bad = status.clone();
    bad.write_statistics.as_mut().unwrap().scope_id.pop();
    assert!(ReplicaJournalStatus::from_payload(&bad.encode_to_vec()).is_err());
    let mut largest = status.clone();
    let stats = largest.write_statistics.as_mut().unwrap();
    stats.foreground_batches = u64::MAX;
    stats.foreground_decisions = u64::MAX;
    stats.foreground_record_bytes = u64::MAX;
    stats.foreground_payload_bytes = u64::MAX;
    stats.maintenance_batches = u64::MAX;
    stats.maintenance_record_bytes = u64::MAX;
    stats.maintenance_payload_bytes = u64::MAX;
    stats.maintenance_metadata_bytes = u64::MAX;
    stats.saturated = true;
    largest.started_epoch = u64::MAX;
    largest.published_epoch = u64::MAX;
    largest.journal_bytes = u64::MAX;
    largest.segments = u64::MAX;
    largest.retired_segments = u64::MAX - 1;
    largest.retired_journal_bytes = u64::MAX;
    largest.bytes_since_checkpoint = u64::MAX;
    assert!(largest.encode_to_vec().len() <= 256);
    assert_eq!(
        ReplicaJournalStatus::from_payload(&largest.encode_to_vec()).unwrap(),
        largest
    );
    let mut duplicate = bytes;
    duplicate.extend([8, 3]);
    assert!(ReplicaJournalStatus::from_payload(&duplicate).is_err());
    for action in [
        ReplicaJournalControl::STATUS,
        ReplicaJournalControl::CHECKPOINT,
    ] {
        let request = ReplicaJournalControl { version: 3, action };
        assert_eq!(
            ReplicaJournalControl::from_payload(&request.encode_to_vec()).unwrap(),
            request
        );
    }
}
