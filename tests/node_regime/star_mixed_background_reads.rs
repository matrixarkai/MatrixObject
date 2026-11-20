// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Uniform background reads, separate from the timed mixed request window.
use super::*;
use std::{io::Write, path::PathBuf};

pub(super) fn observe_space(round: usize, position: usize) {
    let Some(prefix) = std::env::var_os("MATRIXOBJECT_TEST_MIXED_SPACE_CONTROL") else {
        return;
    };
    let prefix = PathBuf::from(prefix);
    let ready = prefix.with_extension(format!("{round}-{position}.space"));
    assert!(
        !ready.exists(),
        "space observer acknowledgement already exists"
    );
    println!("STAR_MIXED_SPACE_READY round={round} position={position}");
    std::io::stdout().flush().unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    while !ready.is_file() {
        assert!(
            Instant::now() < deadline,
            "space observer did not acknowledge"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

pub(super) fn run(fixture: &Fixture, round: usize, position: usize, corpus: usize, clients: usize) {
    let operations = if corpus >= 16384 { 512 } else { 8 };
    let (ready_tx, ready_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let mut starts = Vec::new();
    let mut workers = Vec::new();
    for client in 0..clients {
        let endpoints = fixture.chunk_endpoints.clone();
        let ready = ready_tx.clone();
        let done = done_tx.clone();
        let (start_tx, start_rx) = mpsc::channel();
        starts.push(start_tx);
        workers.push(std::thread::spawn(move || {
            let mut set = replicas(&endpoints);
            let mut samples = Vec::with_capacity(operations);
            let mut random =
                0x6a09e667f3bcc909u64 ^ (client as u64 + 1).wrapping_mul(0x9e3779b97f4a7c15);
            ready.send(()).unwrap();
            start_rx.recv_timeout(Duration::from_secs(30)).unwrap();
            for _ in 0..operations {
                random ^= random >> 12;
                random ^= random << 25;
                random ^= random >> 27;
                let index = random.wrapping_mul(2685821657736338717) % corpus as u64;
                let began = Instant::now();
                let read = set
                    .read_chunk(&background_page(index), 0, BYTES as u64)
                    .unwrap();
                assert_eq!(read.data, PAYLOAD);
                samples.push(began.elapsed().as_micros());
            }
            done.send(()).unwrap();
            samples
        }));
    }
    drop(ready_tx);
    drop(done_tx);
    for _ in 0..clients {
        ready_rx.recv_timeout(Duration::from_secs(30)).unwrap();
    }
    let io_before = checkpoint_corpus_measurement::io_counts(fixture);
    let cpu_before = cpu_ticks(fixture);
    let load = host_load();
    let began = Instant::now();
    for start in starts {
        start.send(()).unwrap();
    }
    for _ in 0..clients {
        done_rx.recv_timeout(Duration::from_secs(120)).unwrap();
    }
    let elapsed = began.elapsed();
    let cpu = cpu_ticks(fixture).checked_sub(cpu_before).unwrap();
    let io = checkpoint_corpus_measurement::io_counts(fixture);
    let mut samples = workers
        .into_iter()
        .flat_map(|worker| worker.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(samples.len(), clients * operations);
    samples.sort_unstable();
    println!("STAR_MIXED_BACKGROUND_READ round={round} position={position} background_pages={corpus} clients={clients} operations={} n=3 w=3 r=1 distribution=uniform_with_replacement payload_bytes={BYTES} elapsed_us={} qps={:.2} p50_us={} p95_us={} p99_us={} data_cpu_ticks={cpu} data_kernel_read_bytes={} data_kernel_write_bytes={} data_cancelled_write_bytes={} host_load1_before={load} host_load1_after={}",
        samples.len(), elapsed.as_micros(), samples.len() as f64 / elapsed.as_secs_f64(), percentile(&samples, 50), percentile(&samples, 95), percentile(&samples, 99), io.reads.checked_sub(io_before.reads).unwrap(), io.writes.checked_sub(io_before.writes).unwrap(), io.cancelled.checked_sub(io_before.cancelled).unwrap(), host_load());
}
