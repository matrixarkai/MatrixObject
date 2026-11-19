// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Local measurements of the log object, for a profiler and for a person. Nothing here runs by
//! default (`--ignored`), and none of it is pinned in the workflow.
//!
//! * `read_plan_cpu_cost_at_entries`: the client's work to turn a byte range into the chunk reads
//!   that make it up, by the number of entries the manifest holds. Run it under callgrind for
//!   instructions, or read the wall time it prints.
//!
//!       BENCH_ENTRIES      entries in the manifest (default 64: a 4 GiB object of 64 MiB chunks)
//!       BENCH_ENTRY_BYTES  bytes one entry covers (default 64 MiB; 1048576 with BENCH_ENTRIES=4096
//!                          is the same 4 GiB object as 4096 one-MiB page extents)
//!       BENCH_RANGE        bytes read (default 4 MiB, in the middle of the object)
//!       BENCH_REPS         repetitions (default 20000)
//!
//! * `append_latency_as_the_object_grows`: append latency per window of one chunk of growth, on
//!   three real nodes, and what the metadata plane was sent. THE NODES KEEP EVERY CHUNK IN MEMORY
//!   until sealed chunks are read from disk, three copies: size BENCH_BYTES to the memory of the
//!   machine (default 256 MiB), not to 4 GiB.
//!
//!       BENCH_BYTES   bytes appended in all (default 256 MiB)
//!       BENCH_RECORD  bytes per append (default 64 KiB)
//!       BENCH_CHUNK   bytes per chunk (default 64 MiB, the real size)
//!       BENCH_READ    bytes per ranged read at the end (default 4 MiB)
//!
//! Run with `cargo test --release --lib log_object::tests::probes:: -- --ignored --nocapture
//! --test-threads=1`.
mod probes {
    use super::*;

    fn env<T: std::str::FromStr>(name: &str, default: T) -> T {
        std::env::var(name).ok().and_then(|value| value.parse().ok()).unwrap_or(default)
    }

    fn manifest(entries: usize, entry_bytes: u64) -> ObjectManifest {
        let mut extents: Vec<ObjectExtent> = (0..entries - 1)
            .map(|index| ObjectExtent {
                group: "group-a".into(),
                volume_id: 1,
                chunk_id: index as u64 + 1,
                object_offset: index as u64 * entry_bytes,
                length: entry_bytes,
                kind: EXTENT_KIND_LOG_CHUNK,
                flags: EXTENT_FLAG_SEALED,
                footer_crc: 1 + index as u32,
                ..Default::default()
            })
            .collect();
        extents.push(ObjectExtent {
            group: "group-a".into(),
            volume_id: 1,
            chunk_id: entries as u64,
            object_offset: (entries as u64 - 1) * entry_bytes,
            kind: EXTENT_KIND_LOG_CHUNK,
            flags: EXTENT_FLAG_TAIL,
            ..Default::default()
        });
        ObjectManifest {
            format_version: 3,
            bucket: "bucket".into(),
            key: "log".into(),
            generation: entries as u64,
            length: (entries as u64 - 1) * entry_bytes,
            extents,
            deleted: false,
        }
    }

    #[test]
    #[ignore = "local measurement for a profiler"]
    fn read_plan_cpu_cost_at_entries() {
        let entries: usize = env("BENCH_ENTRIES", 64);
        let entry_bytes: u64 = env("BENCH_ENTRY_BYTES", 64 << 20);
        let range: u64 = env("BENCH_RANGE", 4 << 20);
        let reps: usize = env("BENCH_REPS", 20_000);
        let mut reader = LogObjectReader::new("bucket", "log", ReplicaClientOptions::default());
        reader.manifest = Some(manifest(entries, entry_bytes));
        let offset = (entries as u64 - 1) * entry_bytes / 2;
        let started = Instant::now();
        let mut pieces = 0usize;
        for _ in 0..reps {
            pieces += reader.plan(offset, offset + range).unwrap().len();
        }
        let elapsed = started.elapsed();
        println!(
            "read_plan entries={entries} entry_bytes={entry_bytes} range={range} reps={reps} pieces_per_plan={} ns_per_plan={}",
            pieces / reps,
            elapsed.as_nanos() / reps as u128
        );
    }

    fn summary(window: &mut Vec<u128>) -> (usize, u128, u128, u128) {
        window.sort_unstable();
        let at = |fraction: f64| window[((window.len() - 1) as f64 * fraction) as usize];
        (window.len(), at(0.5), at(0.99), *window.last().unwrap())
    }

    #[test]
    #[ignore = "local measurement: append latency as the object grows, on three real nodes"]
    fn append_latency_as_the_object_grows() {
        let total: u64 = env("BENCH_BYTES", 256 << 20);
        let record: usize = env("BENCH_RECORD", 64 << 10);
        let chunk: u64 = env("BENCH_CHUNK", 64 << 20);
        let read: usize = env("BENCH_READ", 4 << 20);
        let (_dir, nodes) = cluster();
        let mut meta = FakeMeta::new(endpoints(&nodes));
        let mut settings = options("gw-a");
        settings.chunk_bytes = chunk;
        let mut object = LogObject::new("bucket", "log", settings);
        object.open_for_append(&mut meta).unwrap();
        let payload = bytes(7, record);
        let mut written = 0u64;
        let mut window: Vec<u128> = Vec::new();
        let mut windows = 0u64;
        let started = Instant::now();
        while written < total {
            let at = Instant::now();
            object.append(&mut meta, &payload).unwrap();
            window.push(at.elapsed().as_micros());
            written += record as u64;
            if written / chunk > windows || written >= total {
                let (appends, p50, p99, max) = summary(&mut window);
                println!(
                    "window={windows} object_bytes={written} appends={appends} p50_us={p50} p99_us={p99} max_us={max} meta_commands_so_far={} (grants {} switches {} renewals {})",
                    meta.commands(),
                    meta.grants,
                    meta.switches,
                    meta.renews
                );
                window.clear();
                windows = written / chunk;
            }
        }
        let seconds = started.elapsed().as_secs_f64();
        println!("appended {written} bytes in {seconds:.1} s = {:.1} MB/s", written as f64 / seconds / 1e6);
        // ranged reads in the middle of the object, through the manifest
        let mut reader = reader();
        let length = reader.length(&mut meta).unwrap();
        let entries = reader.manifest.as_ref().map_or(0, |manifest| manifest.extents.len());
        let mut times: Vec<u128> = Vec::new();
        for _ in 0..20 {
            let at = Instant::now();
            let bytes = reader.read_range(&mut meta, length / 2, read).unwrap();
            assert_eq!(bytes.len(), read.min((length - length / 2) as usize));
            times.push(at.elapsed().as_micros());
        }
        let (reads, p50, p99, max) = summary(&mut times);
        println!("ranged_read object_bytes={length} entries={entries} bytes={read} reads={reads} p50_us={p50} p99_us={p99} max_us={max}");
    }
}
