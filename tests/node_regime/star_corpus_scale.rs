// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use matrixobjectstore_rs::cluster::RemoteReplicaSet;
use std::sync::mpsc;

const BYTES: usize = 4096;
const PAYLOAD: [u8; BYTES] = [0x31; BYTES];
const SUFFIX: [u8; BYTES] = [0x7b; BYTES];

fn page(namespace: u64, index: u64) -> PageId {
    PageId {
        chunk_id: ChunkId::new(2_000_000_000, namespace).unwrap(),
        page_index: index,
    }
}

fn background_page(index: u64) -> PageId {
    PageId {
        chunk_id: ChunkId::new(1_000_000_000, index + 1).unwrap(),
        page_index: 0,
    }
}

fn replicas(endpoints: &[String]) -> RemoteReplicaSet {
    RemoteReplicaSet::connect(endpoints, 3, 1, &ReplicaClientOptions::default()).unwrap()
}

fn cpu_ticks(fixture: &Fixture) -> u64 {
    fixture
        .chunks
        .iter()
        .map(|node| {
            let stat = std::fs::read_to_string(format!("/proc/{}/stat", node.pid)).unwrap();
            let fields = stat[stat.rfind(')').unwrap() + 2..]
                .split_whitespace()
                .collect::<Vec<_>>();
            fields[11].parse::<u64>().unwrap() + fields[12].parse::<u64>().unwrap()
        })
        .sum()
}

fn host_load() -> String {
    std::fs::read_to_string("/proc/loadavg")
        .unwrap()
        .split_whitespace()
        .next()
        .unwrap()
        .to_owned()
}

#[derive(Clone, Copy, Debug)]
enum Operation {
    Read,
    Write,
    Append,
}

fn percentile(samples: &[u128], percent: usize) -> u128 {
    samples[(samples.len() * percent).div_ceil(100).saturating_sub(1)]
}

fn phase(
    fixture: &Fixture,
    corpus: usize,
    clients: usize,
    operations: usize,
    id: u64,
    operation: Operation,
) -> Vec<(PageId, usize)> {
    measured_phase(fixture, corpus, clients, operations, id, operation, false)
}

fn measured_phase(
    fixture: &Fixture,
    corpus: usize,
    clients: usize,
    operations: usize,
    id: u64,
    operation: Operation,
    checkpoint_io: bool,
) -> Vec<(PageId, usize)> {
    // Each appender owns one page. Its starting size and operation count are the
    // same at every corpus size; unrelated pages are the independent variable.
    let mut setup = replicas(&fixture.chunk_endpoints);
    for client in 0..clients {
        assert_eq!(
            setup
                .write_chunk(&page(id, client as u64), 0, &PAYLOAD)
                .unwrap(),
            3
        );
    }
    drop(setup);
    let (ready_tx, ready_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let mut finishers = Vec::new();
    let mut starters = Vec::new();
    let mut handles = Vec::new();
    for client in 0..clients {
        let endpoints = fixture.chunk_endpoints.clone();
        let ready = ready_tx.clone();
        let done = done_tx.clone();
        let (finish_tx, finish_rx) = mpsc::channel();
        finishers.push(finish_tx);
        let (start_tx, start_rx) = mpsc::channel();
        starters.push(start_tx);
        handles.push(std::thread::spawn(move || {
            let mut set = replicas(&endpoints);
            // Negotiate capabilities and warm each connection before timing.
            assert_eq!(
                set.write_chunk(&page(id + 1, client as u64), 0, &PAYLOAD)
                    .unwrap(),
                3
            );
            assert_eq!(
                set.read_chunk(&background_page(0), 0, BYTES as u64)
                    .unwrap()
                    .data,
                PAYLOAD
            );
            ready.send(()).unwrap();
            start_rx.recv_timeout(Duration::from_secs(30)).unwrap();
            let mut samples = Vec::with_capacity(operations);
            let mut written = Vec::new();
            for index in 0..operations {
                let began = Instant::now();
                match operation {
                    Operation::Read => {
                        let index = ((client * operations + index) * 7919) % corpus;
                        assert_eq!(
                            set.read_chunk(&background_page(index as u64), 0, BYTES as u64)
                                .unwrap()
                                .data,
                            PAYLOAD
                        );
                    }
                    Operation::Write => {
                        let target = page(id + 2, (client * operations + index) as u64);
                        assert_eq!(set.write_chunk(&target, 0, &SUFFIX).unwrap(), 3);
                        written.push((target, BYTES));
                    }
                    Operation::Append => {
                        let offset = (index + 1) * BYTES;
                        assert_eq!(
                            set.write_chunk(&page(id, client as u64), offset as u64, &SUFFIX)
                                .unwrap(),
                            3
                        );
                    }
                }
                samples.push(began.elapsed().as_micros());
            }
            if matches!(operation, Operation::Append) {
                written.push((page(id, client as u64), (operations + 1) * BYTES));
            }
            // Keep connections and their cleanup outside the measured phase.
            done.send(()).unwrap();
            finish_rx.recv_timeout(Duration::from_secs(120)).unwrap();
            (samples, written)
        }));
    }
    drop(ready_tx);
    drop(done_tx);
    for _ in 0..clients {
        ready_rx.recv_timeout(Duration::from_secs(30)).unwrap();
    }
    let counts = fixture
        .chunk_endpoints
        .iter()
        .map(|endpoint| {
            RemoteReplicaClient::connect(endpoint, &ReplicaClientOptions::default())
                .unwrap()
                .ping()
                .unwrap()
        })
        .collect::<Vec<_>>();
    assert!(counts.windows(2).all(|pair| pair[0] == pair[1]));
    if checkpoint_io {
        checkpoint_corpus_measurement::barrier(fixture);
    }
    let io_before = checkpoint_io.then(|| checkpoint_corpus_measurement::io_counts(fixture));
    let before = if checkpoint_io {
        0
    } else {
        fixture.data_journals().iter().sum::<u64>()
    };
    let cpu = cpu_ticks(fixture);
    let load = host_load();
    let began = Instant::now();
    for start in starters {
        start.send(()).unwrap();
    }
    for _ in 0..clients {
        done_rx.recv_timeout(Duration::from_secs(120)).unwrap();
    }
    let elapsed = began.elapsed();
    if checkpoint_io && !matches!(operation, Operation::Read) {
        checkpoint_corpus_measurement::barrier(fixture);
    }
    let cycle = began.elapsed();
    let ticks = cpu_ticks(fixture).checked_sub(cpu).unwrap();
    let io_after = checkpoint_io.then(|| checkpoint_corpus_measurement::io_counts(fixture));
    let journal = if checkpoint_io {
        0
    } else {
        fixture
            .data_journals()
            .iter()
            .sum::<u64>()
            .checked_sub(before)
            .unwrap()
    };
    for finish in finishers {
        finish.send(()).unwrap();
    }
    let mut samples = Vec::new();
    let mut written = Vec::new();
    for handle in handles {
        let (mut latencies, mut pages) = handle.join().unwrap();
        samples.append(&mut latencies);
        written.append(&mut pages);
    }
    samples.sort_unstable();
    assert_eq!(samples.len(), clients * operations);
    if let (Some(before), Some(after)) = (io_before, io_after) {
        checkpoint_corpus_measurement::report(
            fixture, corpus, clients, operation, &samples, elapsed, cycle, ticks, before, after,
            &load,
        );
        return written;
    }
    if matches!(operation, Operation::Read) {
        assert_eq!(journal, 0);
    } else {
        // Every acknowledged payload reaches all three journals. Fixed-suffix
        // appends must not persist the entire growing page on every operation.
        let logical = (clients * operations * BYTES) as u64;
        assert!(journal >= logical * 3);
        assert!(
            journal <= logical * 4 + (samples.len() * 2048) as u64,
            "unexpected journal amplification: {journal} for {logical} bytes"
        );
    }
    println!("STAR_CORPUS operation={operation:?} group_commit={} background_pages={corpus} stored_pages_before={} background_payload_bytes={} clients={clients} operations={} payload_bytes={BYTES} n=3 w=3 r=1 elapsed_us={} qps={:.2} p50_us={} p95_us={} p99_us={} data_cpu_ticks={ticks} data_journal_bytes={journal} host_load1_before={load} host_load1_after={}",
        fixture.group_commit,
        counts[0], corpus * BYTES, samples.len(), elapsed.as_micros(), samples.len() as f64 / elapsed.as_secs_f64(),
        percentile(&samples, 50), percentile(&samples, 95), percentile(&samples, 99), host_load());
    written
}

fn run(corpora: &[usize], concurrency: &[usize], operations: usize) {
    run_with_group_commit(corpora, concurrency, operations, true);
}

fn run_with_group_commit(
    corpora: &[usize],
    concurrency: &[usize],
    operations: usize,
    group_commit: bool,
) {
    run_workload(
        corpora,
        concurrency,
        operations,
        group_commit,
        &[Operation::Read, Operation::Write, Operation::Append],
    );
}

fn run_workload(
    corpora: &[usize],
    concurrency: &[usize],
    operations: usize,
    group_commit: bool,
    operations_to_measure: &[Operation],
) {
    run_storage_workload(
        corpora,
        concurrency,
        operations,
        group_commit,
        operations_to_measure,
        None,
    );
}

fn run_storage_workload(
    corpora: &[usize],
    concurrency: &[usize],
    operations: usize,
    group_commit: bool,
    operations_to_measure: &[Operation],
    segmented: Option<bool>,
) {
    for &corpus in corpora {
        assert!((1..=32768).contains(&corpus));
        // Matching phases have exactly the same prior benchmark traffic at
        // every background size. Earlier corpus cases cannot contaminate them.
        let mut fixture = match segmented {
            Some(enabled) => Fixture::with_storage(group_commit, enabled, Some(1024 * 1024)),
            None => Fixture::with_group_commit(group_commit),
        };
        let mut seeded = 0;
        let mut id = 10;
        let mut verify = Vec::new();
        let mut set = replicas(&fixture.chunk_endpoints);
        while seeded < corpus {
            let end = (seeded + 32).min(corpus);
            let batch = (seeded..end)
                .map(|index| (background_page(index as u64), 0, PAYLOAD.as_slice(), 0))
                .collect::<Vec<_>>();
            assert!(set
                .write_chunks_if_sequence(&batch)
                .unwrap()
                .iter()
                .all(|count| *count == 3));
            seeded = end;
        }
        drop(set);
        for &clients in concurrency {
            assert!((1..=32).contains(&clients));
            assert!((1..=8192).contains(&operations));
            for &operation in operations_to_measure {
                assert!(matches!(operation, Operation::Read) || operations <= 128);
                let written = if segmented.is_some() {
                    measured_phase(&fixture, corpus, clients, operations, id, operation, true)
                } else {
                    phase(&fixture, corpus, clients, operations, id, operation)
                };
                // Bound retained verification metadata to first/last acknowledged
                // pages per phase; all foreground reads verify their full payload.
                if let Some(first) = written.first() {
                    verify.push(first.clone());
                }
                if let Some(last) = written.last() {
                    verify.push(last.clone());
                }
                id += 4;
            }
        }
        fixture.restart();
        for endpoint in &fixture.chunk_endpoints {
            let mut client =
                RemoteReplicaClient::connect(endpoint, &ReplicaClientOptions::default()).unwrap();
            for index in [0, seeded - 1] {
                assert_eq!(
                    client
                        .read_chunk(&background_page(index as u64), 0, BYTES as u64)
                        .unwrap()
                        .data,
                    PAYLOAD
                );
            }
            for (page, length) in &verify {
                let head = client.head_chunk(page).unwrap();
                assert_eq!(head.logical_length, *length as u64);
                assert_eq!(head.write_sequence, (*length / BYTES) as u64);
                assert!(head.discarded_ranges.is_empty());
                let value = client.read_chunk(page, 0, *length as u64).unwrap();
                assert_eq!(value.data.len(), *length);
                if *length > BYTES {
                    assert_eq!(&value.data[..BYTES], &PAYLOAD);
                }
                let suffix = if *length > BYTES {
                    &value.data[BYTES..]
                } else {
                    &value.data[..]
                };
                assert!(suffix.iter().all(|byte| *byte == 0x7b));
            }
        }
    }
}

#[test]
fn star_corpus_workload_verifies_durable_reads_writes_and_appends() {
    for enabled in [false, true] {
        run_with_group_commit(&[8, 256], &[1, 2], 4, enabled);
    }
}

#[test]
#[ignore = "explicit local paired journal synchronization measurement"]
fn measure_star_shared_sync_at_growing_corpora() {
    for (round, corpora) in [
        [1024, 8192, 32768],
        [32768, 1024, 8192],
        [8192, 32768, 1024],
    ]
    .iter()
    .enumerate()
    {
        for &corpus in corpora {
            let case = [1024, 8192, 32768]
                .iter()
                .position(|value| *value == corpus)
                .unwrap();
            let modes = if (round + case) % 2 == 0 {
                [false, true]
            } else {
                [true, false]
            };
            for enabled in modes {
                run_with_group_commit(&[corpus], &[1, 32], 128, enabled);
            }
        }
    }
}

#[test]
#[ignore = "explicit local journal-backed Star corpus throughput measurement"]
fn measure_star_reads_writes_and_appends_as_corpus_grows() {
    // Rotate corpus order across fresh-process repetitions.
    for corpora in [
        [1024, 8192, 32768],
        [32768, 1024, 8192],
        [8192, 32768, 1024],
    ] {
        run(&corpora, &[1, 8, 32], 128);
    }
}

#[test]
#[ignore = "explicit local sustained paired read measurement"]
fn measure_star_sustained_reads_with_shared_sync() {
    for round in 0..3 {
        let corpora = if round % 2 == 0 {
            [1024, 32768]
        } else {
            [32768, 1024]
        };
        for corpus in corpora {
            let rank = usize::from(corpus == 32768);
            let modes = if (round + rank) % 2 == 0 {
                [false, true]
            } else {
                [true, false]
            };
            for enabled in modes {
                run_workload(&[corpus], &[1, 32], 8192, enabled, &[Operation::Read]);
            }
        }
    }
}

#[path = "star_checkpoint_interference.rs"]
mod checkpoint_interference;

#[path = "checkpoint_corpus_measurement.rs"]
mod checkpoint_corpus_measurement;
