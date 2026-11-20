// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Data replica I/O measured through completed checkpoints, with quiescent writers.
use super::*;

pub(super) fn barrier(fixture: &Fixture) {
    for endpoint in &fixture.chunk_endpoints {
        let mut client =
            RemoteReplicaClient::connect(endpoint, &ReplicaClientOptions::default()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(120);
        while {
            let status = client.journal_maintenance_status().unwrap();
            status.checkpoint_in_flight
                || status.retirement_in_flight
                || status.retired_segments != 0
        } {
            assert!(Instant::now() < deadline, "checkpoint did not finish");
            std::thread::sleep(Duration::from_millis(5));
        }
        let requested = client.checkpoint_journal_with_maintenance().unwrap();
        loop {
            let status = client.journal_maintenance_status().unwrap();
            if status.maintenance_is_complete(&requested).unwrap() {
                assert_eq!(status.pending_records, 0);
                assert_eq!(status.bytes_since_checkpoint, 0);
                break;
            }
            assert!(
                Instant::now() < deadline,
                "checkpoint publication did not finish"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

#[derive(Clone, Copy, Default)]
pub(super) struct IoCounts {
    pub(super) writes: u64,
    pub(super) cancelled: u64,
    pub(super) reads: u64,
    chars: u64,
    calls: u64,
}
pub(super) fn io_counts(fixture: &Fixture) -> IoCounts {
    let mut total = IoCounts::default();
    for node in &fixture.chunks {
        let text = std::fs::read_to_string(format!("/proc/{}/io", node.pid)).unwrap();
        let value = |name: &str| {
            text.lines()
                .find_map(|line| line.strip_prefix(name))
                .unwrap()
                .trim()
                .parse::<u64>()
                .unwrap()
        };
        total.writes += value("write_bytes:");
        total.cancelled += value("cancelled_write_bytes:");
        total.reads += value("read_bytes:");
        total.chars += value("wchar:");
        total.calls += value("syscw:");
    }
    total
}

/// Partition process counters without treating buffered writes as device I/O.
/// Request counters include background maintenance running during the workload.
#[allow(clippy::too_many_arguments)]
pub(super) fn report_mixed_phase(
    round: usize,
    position: usize,
    corpus: usize,
    phase: &str,
    elapsed: Duration,
    ticks: u64,
    before: IoCounts,
    after: IoCounts,
) {
    let writes = after.writes.checked_sub(before.writes).unwrap();
    let cancelled = after.cancelled.checked_sub(before.cancelled).unwrap();
    let reads = after.reads.checked_sub(before.reads).unwrap();
    let chars = after.chars.checked_sub(before.chars).unwrap();
    let calls = after.calls.checked_sub(before.calls).unwrap();
    println!("STAR_MIXED_IO_PHASE round={round} position={position} background_pages={corpus} phase={phase} elapsed_us={} data_cpu_ticks={ticks} data_kernel_write_bytes={writes} data_cancelled_write_bytes={cancelled} data_net_write_bytes={} data_kernel_read_bytes={reads} data_all_fd_wchar={chars} data_all_fd_syscw={calls}", elapsed.as_micros(), i128::from(writes) - i128::from(cancelled));
}

#[allow(clippy::too_many_arguments)]
pub(super) fn report(
    fixture: &Fixture,
    corpus: usize,
    clients: usize,
    operation: Operation,
    samples: &[u128],
    elapsed: Duration,
    cycle: Duration,
    ticks: u64,
    before: IoCounts,
    after: IoCounts,
    load: &str,
) {
    let writes = after.writes.checked_sub(before.writes).unwrap();
    let cancelled = after.cancelled.checked_sub(before.cancelled).unwrap();
    // Cancellation may account for earlier dirty pages. Preserve the signed result.
    let net = i128::from(writes) - i128::from(cancelled);
    let reads = after.reads.checked_sub(before.reads).unwrap();
    let chars = after.chars.checked_sub(before.chars).unwrap();
    let calls = after.calls.checked_sub(before.calls).unwrap();
    let logical = if matches!(operation, Operation::Read) {
        0
    } else {
        samples.len() * BYTES
    };
    if matches!(operation, Operation::Read) {
        assert_eq!(writes, 0);
    } else {
        assert!(writes > 0);
    }
    println!("STAR_CHECKPOINT_IO operation={operation:?} segmented={} group_commit={} background_pages={corpus} background_payload_bytes={} clients={clients} operations={} payload_bytes={BYTES} n=3 w=3 r=1 checkpoint_threshold_bytes=1048576 logical_write_bytes={logical} request_us={} cycle_us={} drain_us={} request_qps={:.2} cycle_qps={:.2} p50_us={} p95_us={} p99_us={} max_us={} data_cpu_ticks={ticks} data_kernel_write_bytes={writes} data_cancelled_write_bytes={cancelled} data_net_write_bytes={net} data_kernel_read_bytes={reads} data_all_fd_wchar={chars} data_all_fd_syscw={calls} host_load1_before={load} host_load1_after={}",
        fixture.segmented, fixture.group_commit, corpus * BYTES, samples.len(), elapsed.as_micros(), cycle.as_micros(), cycle.saturating_sub(elapsed).as_micros(),
        samples.len() as f64 / elapsed.as_secs_f64(), samples.len() as f64 / cycle.as_secs_f64(),
        percentile(samples, 50), percentile(samples, 95), percentile(samples, 99), samples.last().unwrap(), host_load());
    let rss: u64 = fixture
        .chunks
        .iter()
        .map(|node| {
            let status = std::fs::read_to_string(format!("/proc/{}/status", node.pid)).unwrap();
            status
                .lines()
                .find_map(|line| line.strip_prefix("VmRSS:"))
                .unwrap()
                .split_whitespace()
                .next()
                .unwrap()
                .parse::<u64>()
                .unwrap()
                * 1024
        })
        .sum();
    println!("STAR_CHECKPOINT_MEMORY operation={operation:?} background_payload_bytes_per_replica={} data_processes={} data_process_rss_bytes={rss} capacity_bytes_per_replica={} max_pages_per_replica={}", corpus * BYTES, fixture.chunks.len(), fixture.limits.capacity_bytes, fixture.limits.max_pages);
}

#[test]
fn checkpoint_corpus_measurement_drains_both_layouts_and_recovers() {
    for segmented in [false, true] {
        run_storage_workload(
            &[8, 32],
            &[2],
            2,
            true,
            &[Operation::Read, Operation::Write, Operation::Append],
            Some(segmented),
        );
    }
}

#[test]
#[ignore = "explicit local paired corpus checkpoint I/O measurement"]
fn measure_checkpoint_io_at_growing_corpora() {
    for (case, corpus) in [256, 4096, 16384].into_iter().enumerate() {
        let layouts = if case % 2 == 0 {
            [false, true]
        } else {
            [true, false]
        };
        for segmented in layouts {
            run_storage_workload(
                &[corpus],
                &[16],
                32,
                true,
                &[Operation::Read, Operation::Write, Operation::Append],
                Some(segmented),
            );
        }
    }
}

#[test]
fn sized_star_replicas_preserve_capacity_and_page_limits_after_restart() {
    let mut fixture = Fixture::with_limits(
        true,
        true,
        Some(1024),
        None,
        ReplicaLimits {
            capacity_bytes: 64 * 1024,
            max_pages: 4,
        },
    );
    let payload = vec![0x52; 16 * 1024];
    let mut set = replicas(&fixture.chunk_endpoints);
    for index in 0..3 {
        assert_eq!(
            set.write_chunk(&background_page(index), 0, &payload)
                .unwrap(),
            3
        );
    }
    assert!(set
        .write_chunk(&background_page(3), 0, &vec![0x52; 24 * 1024])
        .is_err());
    assert_eq!(
        set.write_chunk(&background_page(3), 0, &payload[..8192])
            .unwrap(),
        3
    );
    assert!(set.write_chunk(&background_page(4), 0, &[]).is_err());
    barrier(&fixture);
    drop(set);
    fixture.restart();
    for endpoint in &fixture.chunk_endpoints {
        let mut node =
            RemoteReplicaClient::connect(endpoint, &ReplicaClientOptions::default()).unwrap();
        for index in 0..4 {
            let expected = if index == 3 {
                &payload[..8192]
            } else {
                payload.as_slice()
            };
            assert_eq!(
                node.read_chunk(&background_page(index), 0, expected.len() as u64)
                    .unwrap()
                    .data,
                expected
            );
        }
    }
    let mut set = replicas(&fixture.chunk_endpoints);
    assert!(set
        .write_chunk(&background_page(3), 8192, &payload)
        .is_err());
    assert!(set.write_chunk(&background_page(4), 0, &[]).is_err());
    assert_eq!(
        set.write_chunk(&background_page(3), 8192, &payload[..4096])
            .unwrap(),
        3
    );
}

#[test]
#[ignore = "explicit local large live-corpus checkpoint I/O measurement"]
fn measure_segmented_star_io_at_large_live_corpora() {
    run_sized_storage_workload(
        &[16384, 65536, 262144],
        &[16],
        32,
        true,
        &[Operation::Read, Operation::Write, Operation::Append],
        Some(true),
        ReplicaLimits {
            capacity_bytes: 1536 * 1024 * 1024,
            max_pages: 524288,
        },
    );
}
