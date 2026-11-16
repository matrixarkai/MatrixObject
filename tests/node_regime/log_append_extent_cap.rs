// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! A small-record log on one object: every append adds an extent, and an object holds at most
//! 4096 of them. The measurement below drives the real processes (three metadata nodes, three
//! chunk servers) through `append_object` until it refuses.
use super::*;
use matrixobjectstore_rs::{
    cluster::{
        ChunkGroupCatalog, ChunkGroupConfig, ChunkGroupIdentity, ObjectWriteOptions,
        ReplicaClientOptions,
    },
    object_manifest::{ManifestOutcome, ObjectManifest},
};
use matrixobjectstore_rs::staged_allocation::{
    PreparedStagedAllocation, StagedAllocation, StagedAllocationFailure,
};
use prost::Message as _;
use std::path::Path;

struct Directory(PathBuf);
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
struct Fixture {
    meta: Group,
    _chunks: Vec<NodeProcess>,
    group_id: u64,
    directory: Directory,
}
fn meta_options() -> MetaClientOptions {
    MetaClientOptions {
        connect_timeout: Duration::from_secs(3),
        read_timeout: Duration::from_secs(30),
        write_timeout: Duration::from_secs(30),
        ..Default::default()
    }
}
fn start_chunk(path: &Path) -> NodeProcess {
    processes::start(
        env!("CARGO_BIN_EXE_chunkserver"),
        &[
            ("MATRIXOBJECT_REPLICA_ADDR", "127.0.0.1:0"),
            ("MATRIXOBJECT_REPLICA_JOURNAL", path.to_str().unwrap()),
            ("MATRIXOBJECT_REPLICA_CAPACITY_BYTES", "1073741824"),
            ("MATRIXOBJECT_REPLICA_MAX_PAGES", "65536"),
        ],
    )
}
impl Fixture {
    fn new() -> Self {
        Self::with_fence(false)
    }
    fn with_fence(fence: bool) -> Self {
        let base = std::env::var("LOG_APPEND_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| std::env::temp_dir());
        let directory = Directory(base.join(node_regime::run_token("log-append-cap")));
        std::fs::create_dir(&directory.0).unwrap();
        let chunks: Vec<_> = (0..3)
            .map(|id| start_chunk(&directory.0.join(format!("{id}.journal"))))
            .collect();
        let endpoints: Vec<_> = chunks.iter().map(|node| node.endpoint.clone()).collect();
        let members: Vec<_> = (0..3).map(|id| format!("log-member-{id}")).collect();
        let catalog = ChunkGroupCatalog::new(vec![ChunkGroupConfig {
            placement_id: "log-group".into(),
            failure_domain: "local-domain".into(),
            identity: Some(ChunkGroupIdentity {
                version: 3,
                group_id: 1,
                node_id: 1,
                capacity_bytes: 1 << 30,
                max_pages: 65536,
                voters: vec![1, 2, 3],
                peer_endpoints: vec![],
                bootstrap_voters: vec![],
                bootstrap_id: 0,
            }),
            api_endpoints: endpoints,
            member_ids: members.clone(),
        }])
        .unwrap();
        let meta = Group::configured_with_timeout(
            None,
            None,
            Some(catalog.to_bytes().unwrap()),
            std::env::var("LOG_CHECKPOINT_BYTES").ok().and_then(|v| v.parse().ok()).unwrap_or(0),
            false,
            5000,
        );
        let group_id = meta
            .client(meta.leader())
            .metadata_group_identity()
            .unwrap()
            .group_id;
        let fixture = Self {
            meta,
            _chunks: chunks,
            group_id,
            directory,
        };
        let mut client = fixture.client();
        for member in members {
            client
                .heartbeat_placement_node(&member, 0, 1 << 30, false, true, 100)
                .unwrap();
        }
        if fence {
            client.enable_publication_fence().unwrap();
        }
        fixture
    }
    fn client(&self) -> RemoteMetaClient {
        RemoteMetaClient::connect_raft_catalog(
            &self
                .meta
                .api_addresses
                .iter()
                .map(|address| address.to_string())
                .collect::<Vec<_>>(),
            self.group_id,
            &meta_options(),
        )
        .unwrap()
    }
    fn reserve(&self, client: &mut RemoteMetaClient, key: &str, generation: Option<u64>, pages: u32, append: bool) -> StagedAllocation {
        for attempt in 0..16 {
            let prepared = client
                .prepare_staged_allocation("append", key, generation, pages, append, 3_600_000)
                .unwrap();
            let path = self.directory.0.join(format!("{key}-{attempt}.request"));
            let mut file = std::fs::File::create(&path).unwrap();
            std::io::Write::write_all(&mut file, &prepared.to_bytes()).unwrap();
            file.sync_all().unwrap();
            let prepared = PreparedStagedAllocation::from_bytes(&std::fs::read(&path).unwrap()).unwrap();
            match client.try_allocate_prepared_staged_object(&prepared) {
                Ok(record) => return record,
                Err(StagedAllocationFailure::PositionConsumed) => continue,
                Err(error) => panic!("reservation failed: {error:?}"),
            }
        }
        panic!("reservation position stayed busy");
    }
    fn wal(&self) -> u64 {
        (0..3)
            .map(|id| {
                std::fs::metadata(self.meta.root.join(id.to_string()).join("metadata.peer.wal"))
                    .map(|m| m.len())
                    .unwrap_or(0)
            })
            .sum()
    }
}

fn record(index: usize, bytes: usize) -> Vec<u8> {
    // A distinct, position-dependent payload so a misplaced byte cannot compare equal.
    (0..bytes)
        .map(|offset| (index.wrapping_mul(131).wrapping_add(offset.wrapping_mul(7)) & 0xff) as u8)
        .collect()
}

fn percentile(sorted: &[u128], p: usize) -> u128 {
    sorted[(sorted.len() * p / 100).min(sorted.len() - 1)]
}

/// Append fixed-size records to ONE object until the SDK refuses; print one line per window.
fn drive_to_failure(record_bytes: usize, window: usize, limit: usize, compacting: bool) {
    let fixture = Fixture::with_fence(compacting);
    let mut client = fixture.client();
    let options = ObjectWriteOptions::default();
    let mut manifest: Option<ObjectManifest> = None;
    let mut times: Vec<u128> = Vec::new();
    let mut count = 0usize;
    let mut wal_mark = fixture.wal();
    let mut failure = String::new();
    let bound: Option<usize> = std::env::var("LOG_BOUND").ok().and_then(|v| v.parse().ok());
    let mut compactions = 0usize;
    while count < limit {
        let data = record(count, record_bytes);
        let started = Instant::now();
        let result = match manifest.clone() {
            None if compacting => {
                let allocation = fixture.reserve(&mut client, "log", None, 1, false);
                client.write_staged_object(&allocation, None, &data, &options)
            }
            None => client.write_object("append", "log", None, &data, &options),
            Some(previous) if compacting => {
                // Owned allocation + digest-bound publication: the prefix list is not shipped.
                let allocation = fixture.reserve(&mut client, "log", Some(previous.generation), 1, true);
                client.append_staged_object_compact(&allocation, previous, &data, &options)
            }
            Some(previous) if bound.is_some() => {
                let before = previous.extents.len();
                let next = client.append_object_compacting(previous, &data, &options, bound.unwrap());
                if let Ok(next) = &next {
                    if next.extents.len() <= before {
                        compactions += 1;
                    }
                }
                next
            }
            Some(previous) => client.append_object(previous, &data, &options),
        };
        let elapsed = started.elapsed().as_micros();
        match result {
            Ok(next) => {
                manifest = Some(next);
                count += 1;
                times.push(elapsed);
            }
            Err(error) => {
                failure = error.to_string();
                break;
            }
        }
        if count % window == 0 {
            let current = manifest.as_ref().unwrap();
            let mut sorted = times.clone();
            sorted.sort_unstable();
            let wal = fixture.wal();
            let fds: Vec<usize> = fixture
                .meta
                .nodes
                .iter()
                .flatten()
                .map(|node| std::fs::read_dir(format!("/proc/{}/fd", node.pid)).map(|d| d.count()).unwrap_or(0))
                .collect();
            println!("LOG_APPEND_FDS meta_fds={fds:?}");
            println!(
                "LOG_APPEND_CURVE record_bytes={record_bytes} bound={bound:?} compactions={compactions} appends={count} extents={} manifest_bytes={} outcome_bytes={} p50_us={} p99_us={} max_us={} meta_wal_bytes_per_append={}",
                current.extents.len(),
                current.encoded_len(),
                ManifestOutcome::decide(Some(current.clone()), {
                    let mut next = current.clone();
                    next.generation += 1;
                    next
                })
                .unwrap()
                .encoded_len(),
                percentile(&sorted, 50),
                percentile(&sorted, 99),
                sorted[sorted.len() - 1],
                (wal as i64 - wal_mark as i64) / window as i64,
            );
            wal_mark = wal;
            times.clear();
        }
    }
    let current = manifest.unwrap();
    let mut fixture = fixture;
    for (i, node) in fixture.meta.nodes.iter_mut().enumerate() {
        if let Some(node) = node {
            println!("LOG_APPEND_NODE meta={i} pid={} exit={:?}", node.pid, node.child.try_wait());
        }
    }
    let mut client = fixture.client();
    println!(
        "LOG_APPEND_END record_bytes={record_bytes} appends={count} extents={} length={} failure={failure:?}",
        current.extents.len(),
        current.length
    );
    // The bytes that were accepted must still read back exactly.
    let expected: Vec<u8> = (0..count).flat_map(|i| record(i, record_bytes)).collect();
    let (read, bytes) = client
        .read_object("append", "log", expected.len(), &ReplicaClientOptions::default())
        .unwrap();
    assert_eq!(read, current);
    assert!(bytes == expected, "accepted bytes changed");
}

#[test]
#[ignore = "explicit local measurement: appends until the SDK refuses"]
fn measure_small_appends_until_the_extent_cap() {
    let bytes = std::env::var("LOG_RECORD_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(300);
    let window = std::env::var("LOG_WINDOW")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(250);
    let limit = std::env::var("LOG_LIMIT").ok().and_then(|v| v.parse().ok()).unwrap_or(usize::MAX / 2);
    drive_to_failure(bytes, window, limit, false);
}

#[test]
#[ignore = "explicit local measurement: compact publication until the SDK refuses"]
fn measure_compact_small_appends_until_the_extent_cap() {
    let bytes = std::env::var("LOG_RECORD_BYTES").ok().and_then(|v| v.parse().ok()).unwrap_or(300);
    let window = std::env::var("LOG_WINDOW").ok().and_then(|v| v.parse().ok()).unwrap_or(250);
    drive_to_failure(bytes, window, usize::MAX / 2, true);
}

fn small_options(page_bytes: usize) -> ObjectWriteOptions {
    ObjectWriteOptions {
        page_bytes,
        ..Default::default()
    }
}

fn read_all(client: &mut RemoteMetaClient, key: &str, length: usize) -> (ObjectManifest, Vec<u8>) {
    client
        .read_object("append", key, length, &ReplicaClientOptions::default())
        .unwrap()
}

#[test]
fn a_compacting_log_stays_under_its_extent_bound_and_every_byte_reads_back() {
    let fixture = Fixture::new();
    let mut client = fixture.client();
    // ~48 KB in 4 KiB pages is ~12 full pages, so a bound of 24 is reachable by merging.
    let options = small_options(4096);
    let bound = 24;
    let mut manifest = client
        .write_object("append", "log", None, &record(0, 100), &options)
        .unwrap();
    let mut expected = record(0, 100);
    let mut compactions = 0;
    for index in 1..200 {
        // Mixed sizes so a merge lands on, before and after a page boundary.
        let bytes = 40 + (index * 37) % 400;
        let data = record(index, bytes);
        let before = manifest.extents.len();
        manifest = client
            .append_object_compacting(manifest, &data, &options, bound)
            .unwrap_or_else(|error| panic!("append {index} failed: {error}"));
        expected.extend_from_slice(&data);
        if manifest.extents.len() < before + 1 {
            compactions += 1;
        }
        assert!(manifest.extents.len() <= bound, "extent bound broken at {index}");
        assert_eq!(manifest.length, expected.len() as u64);
        if index % 10 == 0 || manifest.extents.len() < before + 1 {
            assert_eq!(read_all(&mut client, "log", expected.len()), (manifest.clone(), expected.clone()));
        }
    }
    assert!(compactions >= 5, "the bound never forced a compaction: {compactions}");
    // A fresh client and a ranged read see the same bytes across the boundaries.
    let mut other = fixture.client();
    let (_, tail) = other
        .read_object_range("append", "log", 1000, 5000, expected.len(), &ReplicaClientOptions::default())
        .unwrap();
    assert_eq!(tail, expected[1000..6000]);
    assert_eq!(read_all(&mut other, "log", expected.len()).1, expected);
}

#[test]
fn two_compacting_appenders_race_and_exactly_one_wins_then_the_loser_retries() {
    let fixture = Fixture::new();
    let mut first = fixture.client();
    let mut second = fixture.client();
    let options = small_options(1024);
    let bound = 6;
    let mut manifest = first
        .write_object("append", "race", None, &record(0, 50), &options)
        .unwrap();
    let mut expected = record(0, 50);
    // Fill to exactly the bound with plain appends.
    while manifest.extents.len() < bound {
        let data = record(manifest.extents.len(), 50);
        manifest = first.append_object(manifest, &data, &options).unwrap();
        expected.extend_from_slice(&data);
    }
    let a = record(100, 70);
    let b = record(101, 90);
    let barrier = std::sync::Barrier::new(2);
    let (ra, rb) = std::thread::scope(|scope| {
        let ha = scope.spawn(|| {
            barrier.wait();
            first.append_object_compacting(manifest.clone(), &a, &options, bound)
        });
        let hb = scope.spawn(|| {
            barrier.wait();
            second.append_object_compacting(manifest.clone(), &b, &options, bound)
        });
        (ha.join().unwrap(), hb.join().unwrap())
    });
    assert!(ra.is_ok() != rb.is_ok(), "exactly one writer may publish: {ra:?} {rb:?}");
    let (winner, loser_data, loser_is_first) = match (ra, rb) {
        (Ok(m), Err(_)) => (m, b.clone(), false),
        (Err(_), Ok(m)) => (m, a.clone(), true),
        _ => unreachable!(),
    };
    let winner_data = if loser_is_first { &b } else { &a };
    expected.extend_from_slice(winner_data);
    let mut reader = fixture.client();
    // The loser's orphaned pages never became visible: the object is exactly the winner's.
    assert_eq!(read_all(&mut reader, "race", expected.len()), (winner.clone(), expected.clone()));
    // The loser re-reads and retries cleanly.
    let loser = if loser_is_first { &mut first } else { &mut second };
    let retried = loser
        .append_object_compacting(winner, &loser_data, &options, bound)
        .unwrap();
    expected.extend_from_slice(&loser_data);
    assert!(retried.extents.len() <= bound);
    assert_eq!(read_all(&mut reader, "race", expected.len()), (retried, expected));
}

/// Fill a fresh log to exactly `bound` extents of 50-byte records, so the next append compacts.
fn full_log(client: &mut RemoteMetaClient, key: &str, options: &ObjectWriteOptions, bound: usize) -> (ObjectManifest, Vec<u8>) {
    let mut manifest = client
        .write_object("append", key, None, &record(0, 50), options)
        .unwrap();
    let mut expected = record(0, 50);
    while manifest.extents.len() < bound {
        let data = record(manifest.extents.len(), 50);
        manifest = client.append_object(manifest, &data, options).unwrap();
        expected.extend_from_slice(&data);
    }
    (manifest, expected)
}

#[test]
fn a_chunkserver_killed_before_a_compaction_leaves_the_log_unchanged_and_readable() {
    let mut fixture = Fixture::new();
    let mut client = fixture.client();
    let options = small_options(1024);
    let bound = 6;
    let (manifest, expected) = full_log(&mut client, "crash-chunk", &options, bound);
    // A star group needs every member, so the compacting append cannot land its pages.
    fixture._chunks[0].child.kill().unwrap();
    let _ = fixture._chunks[0].child.wait();
    let data = record(200, 80);
    let result = client.append_object_compacting(manifest.clone(), &data, &options, bound);
    assert!(result.is_err(), "a write with a dead star member must not be acknowledged");
    // Nothing was published: same manifest, same bytes, readable with the member still down.
    let mut reader = fixture.client();
    assert_eq!(reader.read_manifest("append", "crash-chunk").unwrap(), manifest);
    assert_eq!(read_all(&mut reader, "crash-chunk", expected.len()), (manifest, expected));
}

#[test]
fn a_metadata_leader_killed_before_a_compaction_never_leaves_a_half_applied_log() {
    let mut fixture = Fixture::new();
    let mut client = fixture.client();
    let options = small_options(1024);
    let bound = 6;
    let (manifest, mut expected) = full_log(&mut client, "crash-meta", &options, bound);
    let leader = fixture.meta.leader();
    fixture.meta.kill(leader);
    let data = record(300, 80);
    let mut writer = fixture.client();
    let outcome = writer.append_object_compacting(manifest.clone(), &data, &options, bound);
    // Whatever the client was told, the object is exactly before or exactly after, never a mix.
    let mut reader = fixture.client();
    let mut current = None;
    for _ in 0..40 {
        match reader.read_manifest("append", "crash-meta") {
            Ok(m) => {
                current = Some(m);
                break;
            }
            Err(_) => std::thread::sleep(Duration::from_millis(500)),
        }
    }
    let current = current.expect("two of three metadata nodes are alive, a read must succeed");
    let published = current.length == (expected.len() + data.len()) as u64;
    if outcome.is_ok() {
        assert!(published, "an acknowledged append must be published");
    }
    if published {
        expected.extend_from_slice(&data);
    } else {
        assert_eq!(current, manifest, "an unpublished append must leave the manifest as it was");
    }
    assert_eq!(read_all(&mut reader, "crash-meta", expected.len()), (current.clone(), expected.clone()));
    assert!(current.extents.len() <= bound);
    if !published {
        let retried = reader
            .append_object_compacting(current, &data, &options, bound)
            .unwrap();
        expected.extend_from_slice(&data);
        assert_eq!(read_all(&mut reader, "crash-meta", expected.len()), (retried, expected));
    }
}
