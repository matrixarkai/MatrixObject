// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Per-open confirmed record traffic, sampled outside timed requests.
use super::*;
use matrixobjectstore_rs::durable_payload::outcome_segments::OutcomeWriteStatistics;

pub(super) fn snapshot(fixture: &Fixture) -> Vec<OutcomeWriteStatistics> {
    fixture
        .chunk_endpoints
        .iter()
        .map(|endpoint| {
            let mut client =
                RemoteReplicaClient::connect(endpoint, &ReplicaClientOptions::default()).unwrap();
            let deadline = Instant::now() + Duration::from_secs(120);
            let status = loop {
                let status = client.journal_write_statistics().unwrap();
                if !status.checkpoint_in_flight
                    && !status.retirement_in_flight
                    && status.retired_segments == 0
                {
                    break status;
                }
                assert!(
                    Instant::now() < deadline,
                    "journal traffic snapshot did not quiesce"
                );
                std::thread::sleep(Duration::from_millis(5));
            };
            assert_eq!(status.pending_records, 0);
            assert_eq!(status.bytes_since_checkpoint, 0);
            assert_eq!(status.version, 3);
            assert_eq!(status.layout, 2);
            let statistics = status.write_statistics.unwrap();
            statistics.validate().unwrap();
            assert!(!statistics.saturated);
            statistics
        })
        .collect()
}

pub(super) fn report(
    before: &[OutcomeWriteStatistics],
    after: &[OutcomeWriteStatistics],
    round: usize,
    position: usize,
    corpus: usize,
) {
    assert_eq!(before.len(), 3);
    assert_eq!(after.len(), before.len());
    for (replica, (after, before)) in after.iter().zip(before).enumerate() {
        let delta = after.checked_delta(before).unwrap();
        let scope = delta
            .scope_id
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        println!("STAR_MIXED_JOURNAL_TRAFFIC round={round} position={position} background_pages={corpus} replica={replica} scope_id={scope} foreground_batches={} foreground_decisions={} foreground_record_bytes={} foreground_payload_bytes={} maintenance_batches={} maintenance_record_bytes={} maintenance_payload_bytes={} maintenance_metadata_bytes={}",
            delta.foreground_batches, delta.foreground_decisions, delta.foreground_record_bytes, delta.foreground_payload_bytes,
            delta.maintenance_batches, delta.maintenance_record_bytes, delta.maintenance_payload_bytes, delta.maintenance_metadata_bytes);
    }
}
