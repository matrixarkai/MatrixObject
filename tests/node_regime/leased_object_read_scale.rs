// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use matrixobjectstore_rs::{
    cluster::{
        ChunkGroupCatalog, ChunkGroupConfig, ChunkGroupIdentity, ObjectWriteOptions,
        ReplicaClientOptions,
    },
    object_manifest::ObjectManifest,
};
use std::{
    path::Path,
    sync::{mpsc, Arc},
    thread,
};

struct Directory(PathBuf);
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Fixture {
    metadata: Group,
    chunks: Vec<NodeProcess>,
    endpoints: Vec<String>,
    manifests: BTreeMap<usize, Vec<ObjectManifest>>,
    bytes: Arc<Vec<u8>>,
    journals: Vec<PathBuf>,
    _directory: Directory,
}

fn metadata_options() -> MetaClientOptions {
    MetaClientOptions {
        connect_timeout: Duration::from_secs(3),
        read_timeout: Duration::from_secs(30),
        write_timeout: Duration::from_secs(30),
        ..Default::default()
    }
}

impl Fixture {
    fn new(extent_counts: &[usize], object_bytes: usize) -> Self {
        let directory =
            Directory(std::env::temp_dir().join(node_regime::run_token("leased-read-scale")));
        std::fs::create_dir(&directory.0).unwrap();
        let journals = (0..3)
            .map(|id| directory.0.join(format!("{id}.journal")))
            .collect::<Vec<_>>();
        let chunks = journals
            .iter()
            .map(|journal| {
                processes::start(
                    env!("CARGO_BIN_EXE_chunkserver"),
                    &[
                        ("MATRIXOBJECT_REPLICA_ADDR", "127.0.0.1:0"),
                        ("MATRIXOBJECT_REPLICA_JOURNAL", journal.to_str().unwrap()),
                    ],
                )
            })
            .collect::<Vec<_>>();
        let members = (0..3)
            .map(|id| format!("read-scale-{id}"))
            .collect::<Vec<_>>();
        let catalog = ChunkGroupCatalog::new(vec![ChunkGroupConfig {
            placement_id: "read-scale".into(),
            failure_domain: "local-domain".into(),
            identity: Some(ChunkGroupIdentity {
                version: 3,
                group_id: 1,
                node_id: 1,
                capacity_bytes: 1 << 28,
                max_pages: 65536,
                voters: vec![1, 2, 3],
                peer_endpoints: Vec::new(),
                bootstrap_voters: Vec::new(),
                bootstrap_id: 0,
            }),
            api_endpoints: chunks.iter().map(|node| node.endpoint.clone()).collect(),
            member_ids: members.clone(),
        }])
        .unwrap();
        // Use the shipped server default, rather than the 700-ms deadline
        // used by the fast failure-oriented process fixtures.
        let metadata = Group::configured_with_timeout(
            None,
            None,
            Some(catalog.to_bytes().unwrap()),
            0,
            false,
            5000,
        );
        metadata.leader();
        let endpoints = metadata
            .api_addresses
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        let mut client =
            RemoteMetaClient::connect_catalog(&endpoints, &metadata_options()).unwrap();
        for member in members {
            client
                .heartbeat_placement_node(&member, 0, 1 << 28, false, true, 100)
                .unwrap();
        }
        let bytes = Arc::new(
            (0..object_bytes)
                .map(|offset| (offset.wrapping_mul(31) % 251) as u8)
                .collect::<Vec<_>>(),
        );
        let mut manifests = BTreeMap::new();
        for &count in extent_counts {
            assert_eq!(object_bytes % count, 0);
            let options = ObjectWriteOptions {
                page_bytes: object_bytes / count,
                target_groups: 1,
                replica: ReplicaClientOptions {
                    read_timeout: Duration::from_secs(30),
                    write_timeout: Duration::from_secs(30),
                    ..Default::default()
                },
                ..Default::default()
            };
            let values = (0..4)
                .map(|object| {
                    let value = client
                        .write_object(
                            "scale",
                            &format!("extent-{count}-{object}"),
                            None,
                            &bytes,
                            &options,
                        )
                        .unwrap();
                    assert_eq!(value.extents.len(), count);
                    value
                })
                .collect();
            manifests.insert(count, values);
        }
        client.enable_publication_fence().unwrap();
        Self {
            metadata,
            chunks,
            endpoints,
            manifests,
            bytes,
            journals,
            _directory: directory,
        }
    }

    fn pids(&self) -> Vec<u32> {
        self.metadata
            .nodes
            .iter()
            .filter_map(|node| node.as_ref().map(|node| node.pid))
            .chain(self.chunks.iter().map(|node| node.pid))
            .collect()
    }

    fn wal_bytes(&self) -> u64 {
        (0..3)
            .map(|id| {
                file_bytes(
                    &self
                        .metadata
                        .root
                        .join(id.to_string())
                        .join("metadata.peer.wal"),
                )
            })
            .sum()
    }

    fn settle(&self) {
        let until = Instant::now() + Duration::from_secs(5);
        let mut previous = self.wal_bytes();
        let mut stable = 0;
        while stable < 3 {
            assert!(Instant::now() < until, "metadata WAL did not settle");
            thread::sleep(Duration::from_millis(20));
            let current = self.wal_bytes();
            stable = if current == previous { stable + 1 } else { 0 };
            previous = current;
        }
    }

    fn assert_released(&self) {
        let mut client =
            RemoteMetaClient::connect_catalog(&self.endpoints, &metadata_options()).unwrap();
        assert!(client.list_retained_manifests(0, 32).unwrap().0.is_empty());
    }
}

fn file_bytes(path: &Path) -> u64 {
    std::fs::metadata(path).unwrap().len()
}

fn cpu_ticks(pid: u32) -> u64 {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap();
    let fields = stat[stat.rfind(')').unwrap() + 1..]
        .split_whitespace()
        .collect::<Vec<_>>();
    fields[11].parse::<u64>().unwrap() + fields[12].parse::<u64>().unwrap()
}

fn quantile(values: &[u128], percentile: usize) -> u128 {
    values[(values.len() * percentile).div_ceil(100).saturating_sub(1)]
}

fn run(fixture: &Fixture, extents: usize, workers: usize, width: usize, operations: usize) {
    assert!((1..=32).contains(&workers) && matches!(width, 1 | 4) && operations > 0);
    fixture.assert_released();
    fixture.settle();
    let pids = fixture.pids();
    let (ready, readiness) = mpsc::channel();
    let mut starters = Vec::new();
    let selected = &fixture.manifests[&extents];
    let mut tasks = Vec::new();
    for _ in 0..workers {
        let endpoints = fixture.endpoints.clone();
        let expected = selected.clone();
        let bytes = fixture.bytes.clone();
        let ready = ready.clone();
        let (start, started) = mpsc::channel();
        starters.push(start);
        tasks.push(thread::spawn(move || {
            let prepared = RemoteMetaClient::connect_catalog(&endpoints, &metadata_options())
                .and_then(|mut client| {
                    // Warm the route and data socket before timing.
                    client.read_object(
                        "scale",
                        &expected[0].key,
                        bytes.len(),
                        &ReplicaClientOptions::default(),
                    )?;
                    Ok(client)
                });
            ready
                .send(prepared.as_ref().map(|_| ()).map_err(ToString::to_string))
                .unwrap();
            drop(ready);
            if started.recv().is_err() {
                return Vec::new();
            }
            let mut client = prepared.unwrap();
            let mut samples = Vec::with_capacity(operations);
            for _ in 0..operations {
                let options = ReplicaClientOptions {
                    deadline: Some(Instant::now() + Duration::from_secs(30)),
                    ..Default::default()
                };
                let started = Instant::now();
                let values = if width == 1 {
                    vec![client
                        .read_object("scale", &expected[0].key, bytes.len(), &options)
                        .unwrap()]
                } else {
                    let names = expected
                        .iter()
                        .map(|manifest| ("scale", manifest.key.as_str(), bytes.len()))
                        .collect::<Vec<_>>();
                    client.read_objects(&names, &options).unwrap()
                };
                samples.push(started.elapsed().as_micros());
                assert_eq!(values.len(), width);
                for ((manifest, actual), expected) in values.iter().zip(&expected) {
                    assert_eq!(manifest, expected);
                    assert_eq!(actual, bytes.as_ref());
                }
            }
            samples
        }));
    }
    drop(ready);
    let warmup = (0..workers).try_for_each(|_| {
        readiness
            .recv_timeout(Duration::from_secs(30))
            .map_err(|error| format!("reader warmup did not complete: {error}"))?
    });
    if let Err(error) = warmup {
        drop(starters);
        for task in tasks {
            let _ = task.join();
        }
        panic!("reader warmup failed: {error}");
    }
    fixture.assert_released();
    fixture.settle();
    let wal_before = fixture.wal_bytes();
    let data_before = fixture
        .journals
        .iter()
        .map(|path| file_bytes(path))
        .sum::<u64>();
    let server_before = pids.iter().map(|pid| cpu_ticks(*pid)).sum::<u64>();
    let client_before = cpu_ticks(std::process::id());
    let started = Instant::now();
    for start in starters {
        start.send(()).unwrap();
    }
    // Join every reader before propagating a failure so fixture teardown
    // cannot kill nodes while other workers are still using them.
    let finished = tasks
        .into_iter()
        .map(|task| task.join())
        .collect::<Vec<_>>();
    let mut samples = finished
        .into_iter()
        .flat_map(Result::unwrap)
        .collect::<Vec<_>>();
    let elapsed = started.elapsed();
    samples.sort_unstable();
    fixture.assert_released();
    fixture.settle();
    let wal_delta = fixture.wal_bytes().checked_sub(wal_before).unwrap();
    let data_after = fixture
        .journals
        .iter()
        .map(|path| file_bytes(path))
        .sum::<u64>();
    assert_eq!(
        data_after, data_before,
        "reads changed durable data journals"
    );
    // SAFETY: sysconf accepts this constant without pointers or owned resources.
    let hz = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    assert!(hz > 0);
    let server_us = (pids.iter().map(|pid| cpu_ticks(*pid)).sum::<u64>() - server_before)
        * 1_000_000
        / hz as u64;
    let client_us = (cpu_ticks(std::process::id()) - client_before) * 1_000_000 / hz as u64;
    assert_eq!(samples.len(), workers * operations);
    println!("LEASED_READ_SCALE metadata_timeout_ms=5000 extents={extents} workers={workers} width={width} operations={} object_bytes={} elapsed_us={} qps={:.1} p50_us={} p95_us={} p99_us={} server_cpu_us={server_us} client_cpu_us={client_us} metadata_wal_bytes={wal_delta} data_journal_bytes=0",
        samples.len(), fixture.bytes.len(), elapsed.as_micros(), samples.len() as f64 / elapsed.as_secs_f64(),
        quantile(&samples, 50), quantile(&samples, 95), quantile(&samples, 99));
}

#[test]
fn concurrent_reads_verify_payloads_and_release_all_protection() {
    let fixture = Fixture::new(&[8], 1024);
    run(&fixture, 8, 4, 1, 2);
    run(&fixture, 8, 4, 4, 2);
}

#[test]
#[ignore = "explicit local process performance measurement"]
fn measure_concurrent_fenced_reads() {
    let fixture = Fixture::new(&[1, 128, 1024], 16 * 1024);
    for extents in [1, 128, 1024] {
        for workers in [1, 8, 32] {
            for width in [1, 4] {
                run(&fixture, extents, workers, width, 8);
            }
        }
    }
}

#[test]
fn exhausted_reader_capacity_refuses_then_recovers_after_release() {
    let fixture = Fixture::new(&[8], 1024);
    let selected = &fixture.manifests[&8][0];
    let mut client =
        RemoteMetaClient::connect_catalog(&fixture.endpoints, &metadata_options()).unwrap();
    let mut handles = Vec::new();
    for _ in 0..128 {
        handles.push(
            client
                .open_read_session("scale", &selected.key, Some(selected.generation), 3_600_000)
                .unwrap()
                .retention_id,
        );
    }
    let error = client
        .read_object(
            "scale",
            &selected.key,
            1024,
            &ReplicaClientOptions::default(),
        )
        .unwrap_err();
    assert!(error.to_string().contains("catalog capacity"), "{error}");
    let mut listed = Vec::new();
    let mut after = 0;
    loop {
        let (ids, more) = client.list_retained_manifests(after, 32).unwrap();
        if let Some(id) = ids.last() {
            after = *id;
        }
        listed.extend(ids);
        if !more {
            break;
        }
    }
    assert_eq!(listed, handles);
    for handle in handles {
        client.release_manifest(handle).unwrap();
    }
    assert_eq!(
        client
            .read_object(
                "scale",
                &selected.key,
                1024,
                &ReplicaClientOptions::default()
            )
            .unwrap(),
        (selected.clone(), fixture.bytes.as_ref().clone())
    );
    fixture.assert_released();
}
