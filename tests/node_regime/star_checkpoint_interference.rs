// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! One foreground checkpoint during a fixed-suffix append phase.
use super::*;
use std::io::Read;

fn magic(path: &Path) -> [u8; 4] {
    let mut bytes = [0; 4];
    File::open(path).unwrap().read_exact(&mut bytes).unwrap();
    bytes
}

fn io_counts(fixture: &Fixture) -> (u64, u64) {
    fixture.chunks.iter().fold((0, 0), |(chars, writes), node| {
        let text = std::fs::read_to_string(format!("/proc/{}/io", node.pid)).unwrap();
        let value = |name: &str| {
            text.lines()
                .find_map(|line| line.strip_prefix(name))
                .unwrap()
                .trim()
                .parse::<u64>()
                .unwrap()
        };
        (chars + value("wchar:"), writes + value("write_bytes:"))
    })
}

fn run_checkpoint_case(corpus: usize, operations: usize, checkpoint: bool) {
    assert!((256..=32768).contains(&corpus));
    assert!((64..=128).contains(&operations));
    let mut fixture = Fixture::new();
    let mut set = replicas(&fixture.chunk_endpoints);
    for begin in (0..corpus).step_by(32) {
        let batch = (begin..(begin + 32).min(corpus))
            .map(|index| (background_page(index as u64), 0, PAYLOAD.as_slice(), 0))
            .collect::<Vec<_>>();
        assert!(set
            .write_chunks_if_sequence(&batch)
            .unwrap()
            .iter()
            .all(|n| *n == 3));
    }
    drop(set);
    let seeded = fixture.data_journals();
    for id in 0..3 {
        assert_eq!(&magic(&fixture.journals[id]), b"MOR2");
        fixture.chunks[id].child.kill().unwrap();
        assert!(
            processes::reap_within(&mut fixture.chunks[id].child, Duration::from_secs(3)).is_some()
        );
    }
    // Seeding never checkpoints. Restored outcome bytes count toward the interval,
    // leaving 128 KiB of new traffic before the one measured checkpoint.
    let intervals = seeded
        .iter()
        .map(|bytes| bytes + 128 * 1024)
        .collect::<Vec<_>>();
    for id in 0..3 {
        assert!((1024..=512 * 1024 * 1024).contains(&intervals[id]));
        // Even the complete timed suffix traffic cannot trigger a second checkpoint.
        assert!(intervals[id] > ((operations + 1) * (BYTES + 2048)) as u64);
        fixture.chunks[id] = start_chunk_with_checkpoint(
            &fixture.chunk_endpoints[id],
            &fixture.journals[id],
            "peers",
            true,
            checkpoint.then_some(intervals[id]),
        );
        assert!(fixture.chunks[id]
            .startup_line
            .contains("group_commit=true"));
        assert!(fixture.chunks[id]
            .startup_line
            .contains("local_recovery=peers"));
    }
    let mut set = replicas(&fixture.chunk_endpoints);
    let target = page(100, 0);
    assert_eq!(set.write_chunk(&target, 0, &PAYLOAD).unwrap(), 3);
    assert_eq!(
        set.read_chunk(&background_page(0), 0, BYTES as u64)
            .unwrap()
            .data,
        PAYLOAD
    );
    let before = fixture.data_journals().iter().sum::<u64>();
    assert!(fixture.journals.iter().all(|path| magic(path) == *b"MOR2"));
    let cpu = cpu_ticks(&fixture);
    let io = io_counts(&fixture);
    let load = host_load();
    let mut samples = Vec::with_capacity(operations);
    let mut transitions = [None; 3];
    let began = Instant::now();
    for index in 0..operations {
        let call = Instant::now();
        assert_eq!(
            set.write_chunk(&target, ((index + 1) * BYTES) as u64, &SUFFIX)
                .unwrap(),
            3
        );
        samples.push(call.elapsed().as_micros());
        // Probes follow RPC timing. They are included in overall phase elapsed time.
        for (id, path) in fixture.journals.iter().enumerate() {
            let compacted = magic(path) == *b"MOSJ";
            if compacted && transitions[id].is_none() {
                transitions[id] = Some(index);
            }
            assert!(transitions[id].is_none() || compacted);
        }
    }
    let elapsed = began.elapsed();
    let ticks = cpu_ticks(&fixture).checked_sub(cpu).unwrap();
    let after_io = io_counts(&fixture);
    let chars = after_io.0.checked_sub(io.0).unwrap();
    let writes = after_io.1.checked_sub(io.1).unwrap();
    let after = fixture.data_journals().iter().sum::<u64>();
    if checkpoint {
        assert!(
            transitions.iter().all(Option::is_some),
            "checkpoint not observed: {transitions:?}"
        );
        assert!(
            chars >= (corpus * BYTES * 3) as u64,
            "checkpoint payload was not written"
        );
    } else {
        assert!(transitions.iter().all(Option::is_none));
        assert!(
            chars < (operations * (BYTES * 5 + 2048)) as u64,
            "control wrote more than bounded suffix traffic"
        );
    }
    let (slowest, &maximum) = samples
        .iter()
        .enumerate()
        .max_by_key(|(_, value)| *value)
        .unwrap();
    let checkpoint_max = transitions
        .iter()
        .flatten()
        .map(|index| samples[*index])
        .max()
        .unwrap_or(0);
    samples.sort_unstable();
    println!("STAR_CHECKPOINT checkpoint={checkpoint} group_commit=true background_pages={corpus} background_payload_bytes={} clients=1 operations={operations} payload_bytes={BYTES} n=3 w=3 r=1 workers=4 elapsed_us={} qps={:.2} p50_us={} p95_us={} p99_us={} max_us={maximum} slowest_operation={slowest} checkpoint_rpc_max_us={checkpoint_max} data_cpu_ticks={ticks} data_wchar_bytes={chars} data_write_bytes={writes} journal_before_bytes={before} journal_after_bytes={after} host_load1_before={load} host_load1_after={} checkpoint_operations={transitions:?}",
        corpus * BYTES, elapsed.as_micros(), operations as f64 / elapsed.as_secs_f64(),
        percentile(&samples, 50), percentile(&samples, 95), percentile(&samples, 99), host_load());
    let expected = set.head_chunk(&target).unwrap();
    drop(set);
    fixture.restart();
    let mut expected_bytes = Vec::from(PAYLOAD);
    for _ in 0..operations {
        expected_bytes.extend_from_slice(&SUFFIX);
    }
    for endpoint in &fixture.chunk_endpoints {
        let mut client =
            RemoteReplicaClient::connect(endpoint, &ReplicaClientOptions::default()).unwrap();
        for index in [0, corpus - 1] {
            assert_eq!(
                client
                    .read_chunk(&background_page(index as u64), 0, BYTES as u64)
                    .unwrap()
                    .data,
                PAYLOAD
            );
        }
        assert_eq!(client.head_chunk(&target).unwrap(), expected);
        assert_eq!(
            client
                .read_chunk(&target, 0, expected_bytes.len() as u64)
                .unwrap()
                .data,
            expected_bytes
        );
    }
}

#[test]
fn checkpoint_interference_preserves_three_durable_append_copies() {
    for checkpoint in [false, true] {
        run_checkpoint_case(256, 64, checkpoint);
    }
}

#[test]
#[ignore = "explicit local checkpoint interference measurement"]
fn measure_star_append_checkpoint_interference_as_corpus_grows() {
    for (round, corpora) in [
        [1024, 8192, 32768],
        [32768, 1024, 8192],
        [8192, 32768, 1024],
    ]
    .iter()
    .enumerate()
    {
        for &corpus in corpora {
            let rank = [1024, 8192, 32768]
                .iter()
                .position(|value| *value == corpus)
                .unwrap();
            let modes = if (round + rank) % 2 == 0 {
                [false, true]
            } else {
                [true, false]
            };
            for checkpoint in modes {
                run_checkpoint_case(corpus, 128, checkpoint);
            }
        }
    }
}
