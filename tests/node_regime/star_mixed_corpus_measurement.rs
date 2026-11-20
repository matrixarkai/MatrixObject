// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Concurrent durable reads, overwrites and bounded-page appends.
use super::*;
use std::array;

const READ: usize = 0;
const WRITE: usize = 1;
const APPEND: usize = 2;
const SETUP: usize = 3;
const KINDS: [&str; 4] = ["Read", "Write", "Append", "Setup"];

fn write_page(client: usize) -> PageId {
    page(40, client as u64)
}
fn append_page(client: usize, round: usize) -> PageId {
    page(100 + client as u64, round as u64)
}
fn write_byte(client: usize, step: usize) -> u8 {
    ((client * 31 + step) % 251 + 1) as u8
}
struct ClientResult {
    samples: [Vec<u128>; 4],
    appended: Vec<(PageId, usize, u64)>,
}
fn page_counts(fixture: &Fixture, expected: usize) {
    for endpoint in &fixture.chunk_endpoints {
        let mut client =
            RemoteReplicaClient::connect(endpoint, &ReplicaClientOptions::default()).unwrap();
        assert_eq!(client.ping().unwrap(), expected as u64);
    }
}
fn verify(fixture: &Fixture, corpus: usize, cycles: usize, results: &[ClientResult]) {
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
        for (number, result) in results.iter().enumerate() {
            let key = write_page(number);
            let head = client.head_chunk(&key).unwrap();
            assert_eq!(head.logical_length, BYTES as u64);
            assert_eq!(head.write_sequence, cycles as u64 + 1);
            assert!(head.discarded_ranges.is_empty());
            assert_eq!(
                client.read_chunk(&key, 0, BYTES as u64).unwrap().data,
                vec![write_byte(number, cycles - 1); BYTES]
            );
            for (key, length, sequence) in &result.appended {
                let head = client.head_chunk(key).unwrap();
                assert_eq!(head.logical_length, *length as u64);
                assert_eq!(head.write_sequence, *sequence);
                assert!(head.discarded_ranges.is_empty());
                let bytes = client.read_chunk(key, 0, *length as u64).unwrap().data;
                assert_eq!(bytes.len(), *length);
                assert_eq!(&bytes[..BYTES], &PAYLOAD);
                assert!(bytes[BYTES..].iter().all(|value| *value == SUFFIX[0]));
            }
        }
    }
}
#[path = "star_mixed_background_reads.rs"]
mod background_reads;
#[path = "star_mixed_journal_traffic.rs"]
mod journal_traffic;
#[path = "star_mixed_phase_probe.rs"]
mod phase_probe;
#[path = "star_mixed_progress_diagnostics.rs"]
mod progress_diagnostics;

fn measurement_worker_threads() -> usize {
    let workers = match std::env::var("MATRIXOBJECT_TEST_MIXED_WORKERS") {
        Ok(value) => value
            .parse::<usize>()
            .expect("mixed workers must be a number"),
        Err(std::env::VarError::NotPresent) => 4,
        Err(error) => panic!("invalid mixed worker setting: {error}"),
    };
    assert!(
        [4, 8, 16].contains(&workers),
        "mixed workers must be 4, 8 or 16"
    );
    workers
}

fn configured_io_threads(worker_threads: usize) -> usize {
    let io_threads = match std::env::var("MATRIXOBJECT_REPLICA_IO_THREADS") {
        Ok(value) => value.parse::<usize>().expect("I/O loops must be a number"),
        Err(std::env::VarError::NotPresent) => worker_threads,
        Err(error) => panic!("invalid I/O loop setting: {error}"),
    };
    assert!((1..=worker_threads).contains(&io_threads));
    io_threads
}

fn worker_profiles(fixture: &Fixture, worker_threads: usize, io_threads: usize) {
    let expected = format!("MATRIXOBJECT_REPLICA_THREADS={worker_threads}");
    for node in &fixture.chunks {
        let environment = std::fs::read(format!("/proc/{}/environ", node.pid)).unwrap();
        assert!(environment
            .split(|byte| *byte == 0)
            .any(|entry| entry == expected.as_bytes()));
        let io_setting = environment
            .split(|byte| *byte == 0)
            .find(|entry| entry.starts_with(b"MATRIXOBJECT_REPLICA_IO_THREADS="));
        match io_setting {
            Some(actual) => assert_eq!(
                actual,
                format!("MATRIXOBJECT_REPLICA_IO_THREADS={io_threads}").as_bytes()
            ),
            None => assert_eq!(io_threads, worker_threads),
        }
        println!(
            "STAR_MIXED_WORKER_PROFILE pid={} worker_threads={worker_threads} io_threads={io_threads}",
            node.pid
        );
    }
}

fn run_case(
    round: usize,
    position: usize,
    corpus: usize,
    clients: usize,
    cycles: usize,
    horizon: usize,
) {
    run_case_with_workers(round, position, corpus, clients, cycles, horizon, 4);
}
fn run_case_with_workers(
    round: usize,
    position: usize,
    corpus: usize,
    clients: usize,
    cycles: usize,
    horizon: usize,
    worker_threads: usize,
) {
    assert!([4, 8, 16].contains(&worker_threads));
    let io_threads = configured_io_threads(worker_threads);
    assert!((1..=262144).contains(&corpus));
    assert!((1..=32).contains(&clients));
    assert!((1..=512).contains(&cycles));
    assert!((1..=32).contains(&horizon));
    let limits = ReplicaLimits {
        capacity_bytes: 1536 * 1024 * 1024,
        max_pages: 524288,
    };
    let mut fixture =
        Fixture::with_workers(true, true, Some(1024 * 1024), None, limits, worker_threads);
    super::super::assert_resident_process_profile(&fixture);
    worker_profiles(&fixture, worker_threads, io_threads);
    let mut seed = replicas(&fixture.chunk_endpoints);
    for start in (0..corpus).step_by(32) {
        let batch = (start..(start + 32).min(corpus))
            .map(|index| (background_page(index as u64), 0, PAYLOAD.as_slice(), 0))
            .collect::<Vec<_>>();
        assert!(seed
            .write_chunks_if_sequence(&batch)
            .unwrap()
            .iter()
            .all(|count| *count == 3));
    }
    drop(seed);
    page_counts(&fixture, corpus);
    let (ready_tx, ready_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let mut starters = Vec::new();
    let mut finishers = Vec::new();
    let mut workers = Vec::new();
    let progress = progress_diagnostics::Progress::enabled(clients);
    for client in 0..clients {
        let client_progress = progress.clone();
        let endpoints = fixture.chunk_endpoints.clone();
        let ready = ready_tx.clone();
        let done = done_tx.clone();
        let (start_tx, start_rx) = mpsc::channel();
        let (finish_tx, finish_rx) = mpsc::channel();
        starters.push(start_tx);
        finishers.push(finish_tx);
        workers.push(std::thread::spawn(move || {
            let mut set = replicas(&endpoints);
            assert_eq!(
                set.write_chunk(&write_page(client), 0, &PAYLOAD).unwrap(),
                3
            );
            assert_eq!(
                set.read_chunk(&background_page(0), 0, BYTES as u64)
                    .unwrap()
                    .data,
                PAYLOAD
            );
            ready.send(()).unwrap();
            start_rx.recv_timeout(Duration::from_secs(180)).unwrap();
            let mut samples = array::from_fn(|index| {
                Vec::with_capacity(if index == SETUP {
                    cycles.div_ceil(horizon)
                } else {
                    cycles
                })
            });
            let mut appended = Vec::with_capacity(cycles.div_ceil(horizon));
            for step in 0..cycles {
                if step % horizon == 0 {
                    let key = append_page(client, step / horizon);
                    let began = Instant::now();
                    assert_eq!(set.write_chunk(&key, 0, &PAYLOAD).unwrap(), 3);
                    samples[SETUP].push(began.elapsed().as_micros());
                    appended.push((key, BYTES, 1));
                    if let Some(progress) = &client_progress {
                        progress.record(client, SETUP);
                    }
                }
                // Rotate operation order across clients and cycles without
                // changing the operation counts or append-size distribution.
                for phase in 0..3 {
                    let kind = (client + step + phase) % 3;
                    let began = Instant::now();
                    match kind {
                        READ => {
                            let index = ((client * cycles + step) * 7919) % corpus;
                            assert_eq!(
                                set.read_chunk(&background_page(index as u64), 0, BYTES as u64)
                                    .unwrap()
                                    .data,
                                PAYLOAD
                            );
                        }
                        WRITE => {
                            assert_eq!(
                                set.write_chunk(
                                    &write_page(client),
                                    0,
                                    &[write_byte(client, step); BYTES]
                                )
                                .unwrap(),
                                3
                            );
                        }
                        APPEND => {
                            let (key, length, sequence) = appended.last_mut().unwrap();
                            assert_eq!(set.write_chunk(key, *length as u64, &SUFFIX).unwrap(), 3);
                            *length += BYTES;
                            *sequence += 1;
                        }
                        _ => unreachable!(),
                    }
                    samples[kind].push(began.elapsed().as_micros());
                    if let Some(progress) = &client_progress {
                        progress.record(client, kind);
                    }
                }
            }
            done.send(()).unwrap();
            finish_rx.recv_timeout(Duration::from_secs(300)).unwrap();
            ClientResult { samples, appended }
        }));
    }
    drop(ready_tx);
    drop(done_tx);
    for _ in 0..clients {
        ready_rx.recv_timeout(Duration::from_secs(30)).unwrap();
    }
    checkpoint_corpus_measurement::barrier(&fixture);
    page_counts(&fixture, corpus + clients);
    let probe = phase_probe::Probe::prepare(round, position, corpus);
    let journal_before = journal_traffic::snapshot(&fixture);
    let io_before = checkpoint_corpus_measurement::io_counts(&fixture);
    let cpu_before = cpu_ticks(&fixture);
    let load_before = host_load();
    let began = Instant::now();
    for start in starters {
        start.send(()).unwrap();
    }
    for completed in 0..clients {
        if let Err(error) = done_rx.recv_timeout(Duration::from_secs(120)) {
            progress_diagnostics::report_failure(
                progress.as_deref(),
                completed,
                clients,
                began.elapsed(),
                &error,
            );
            panic!("mixed workload completion failed: {error:?}");
        }
    }
    let requests_elapsed = began.elapsed();
    let request_cpu = cpu_ticks(&fixture);
    let request_io = checkpoint_corpus_measurement::io_counts(&fixture);
    let (maintenance_cpu, maintenance_io) = if let Some(probe) = probe {
        probe.finish(requests_elapsed.as_micros());
        (
            cpu_ticks(&fixture),
            checkpoint_corpus_measurement::io_counts(&fixture),
        )
    } else {
        (request_cpu, request_io)
    };
    let maintenance_began = began.elapsed();
    checkpoint_corpus_measurement::barrier(&fixture);
    let journal_after = journal_traffic::snapshot(&fixture);
    let cycle_elapsed = began.elapsed();
    let cpu = cpu_ticks(&fixture).checked_sub(cpu_before).unwrap();
    let io_after = checkpoint_corpus_measurement::io_counts(&fixture);
    // Requests include concurrent checkpoint work. The final maintenance phase
    // includes all remaining publication and cleanup, with writers quiescent.
    for (phase, elapsed, ticks, before, after) in [
        (
            "requests",
            requests_elapsed,
            request_cpu.checked_sub(cpu_before).unwrap(),
            io_before,
            request_io,
        ),
        (
            "observer",
            maintenance_began.saturating_sub(requests_elapsed),
            maintenance_cpu.checked_sub(request_cpu).unwrap(),
            request_io,
            maintenance_io,
        ),
        (
            "maintenance",
            cycle_elapsed.saturating_sub(maintenance_began),
            (cpu + cpu_before).checked_sub(maintenance_cpu).unwrap(),
            maintenance_io,
            io_after,
        ),
    ] {
        checkpoint_corpus_measurement::report_mixed_phase(
            round, position, corpus, phase, elapsed, ticks, before, after,
        );
    }
    journal_traffic::report(&journal_before, &journal_after, round, position, corpus);
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
    for finish in finishers {
        finish.send(()).unwrap();
    }
    let results = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect::<Vec<_>>();
    let mut samples: [Vec<u128>; 4] = array::from_fn(|_| Vec::new());
    for result in &results {
        for (all, part) in samples.iter_mut().zip(&result.samples) {
            all.extend_from_slice(part);
        }
    }
    let count = clients * cycles;
    for index in [READ, WRITE, APPEND] {
        assert_eq!(samples[index].len(), count);
    }
    let setup = clients * cycles.div_ceil(horizon);
    assert_eq!(samples[SETUP].len(), setup);
    let pages_after = corpus + clients + setup;
    page_counts(&fixture, pages_after);
    let requests = count * 3 + setup;
    let logical = (count * 2 + setup) * BYTES;
    let writes = io_after.writes.checked_sub(io_before.writes).unwrap();
    let cancelled = io_after.cancelled.checked_sub(io_before.cancelled).unwrap();
    let reads = io_after.reads.checked_sub(io_before.reads).unwrap();
    assert!(writes > 0);
    println!("STAR_MIXED_IO round={round} position={position} background_pages={corpus} background_bytes={} worker_threads={worker_threads} io_threads={io_threads} clients={clients} cycles={cycles} append_horizon={horizon} payload_bytes={BYTES} n=3 w=3 r=1 metadata_allocation=false state_preflight=default_client read_ops={count} write_ops={count} append_ops={count} setup_ops={setup} requests={requests} logical_write_bytes={logical} pages_before={} pages_after={pages_after} request_us={} cycle_us={} drain_us={} request_qps={:.2} cycle_qps={:.2} data_cpu_ticks={cpu} data_process_rss_bytes={rss} data_kernel_write_bytes={writes} data_cancelled_write_bytes={cancelled} data_net_write_bytes={} data_kernel_read_bytes={reads} host_load1_before={load_before} host_load1_after={}",
        corpus * BYTES, corpus + clients, requests_elapsed.as_micros(), cycle_elapsed.as_micros(), cycle_elapsed.saturating_sub(requests_elapsed).as_micros(), requests as f64 / requests_elapsed.as_secs_f64(), requests as f64 / cycle_elapsed.as_secs_f64(), i128::from(writes) - i128::from(cancelled), host_load());
    for (index, values) in samples.iter_mut().enumerate() {
        values.sort_unstable();
        println!("STAR_MIXED_LATENCY round={round} position={position} background_pages={corpus} operation={} operations={} p50_us={} p95_us={} p99_us={} max_us={}", KINDS[index], values.len(), percentile(values, 50), percentile(values, 95), percentile(values, 99), values.last().unwrap());
    }
    background_reads::observe_space(round, position);
    background_reads::run(&fixture, round, position, corpus, clients);
    // Verify every foreground page and both background sentinels on every
    // replica before and after killing all six processes and reopening them.
    verify(&fixture, corpus, cycles, &results);
    fixture.restart();
    super::super::assert_resident_process_profile(&fixture);
    worker_profiles(&fixture, worker_threads, io_threads);
    page_counts(&fixture, pages_after);
    verify(&fixture, corpus, cycles, &results);
    println!("STAR_MIXED_RECOVERY round={round} position={position} background_pages={corpus} verified_foreground_pages_per_replica={} verified_replicas=3 full_process_restart=true", clients + setup);
}

#[test]
fn segmented_star_mixed_traffic_recovers_all_foreground_pages() {
    run_case(0, 0, 64, 2, 5, 2);
}

#[test]
#[ignore = "explicit local sustained mixed traffic measurement"]
fn measure_segmented_star_mixed_traffic_at_large_corpora() {
    let worker_threads = measurement_worker_threads();
    for (round, order) in [[16384, 65536, 262144], [262144, 65536, 16384]]
        .into_iter()
        .enumerate()
    {
        for (position, corpus) in order.into_iter().enumerate() {
            run_case_with_workers(round, position, corpus, 16, 512, 32, worker_threads);
        }
    }
}

#[test]
#[ignore = "explicit local single-corpus mixed traffic measurement"]
fn measure_segmented_star_mixed_traffic_at_one_large_corpus() {
    let worker_threads = measurement_worker_threads();
    run_case_with_workers(0, 0, 262144, 16, 512, 32, worker_threads);
}

#[test]
fn segmented_star_mixed_traffic_with_sixteen_workers_recovers_all_foreground_pages() {
    run_case_with_workers(0, 0, 64, 2, 5, 2, 16);
}
