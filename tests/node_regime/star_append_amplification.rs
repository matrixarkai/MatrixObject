// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use matrixobjectstore_rs::{
    cluster::{
        ChunkGroupCatalog, ChunkGroupConfig, ChunkGroupIdentity, ObjectWriteOptions,
        RemoteMetaClient, RemoteReplicaClient, ReplicaClientOptions,
    },
    object_manifest::{ManifestOutcome, ObjectManifest},
    staged_allocation::{
        PreparedStagedAllocation, StagedAllocation, StagedAllocationFailure,
        StagedAllocationRequest,
    },
    ChunkId, PageId,
};
use prost::Message;
use std::{fs::File, path::Path};
#[path = "metadata_frame_meter.rs"]
mod metadata_frame_meter;
use metadata_frame_meter::{Counts, FrameMeter};

struct Directory(PathBuf);
#[derive(Clone, Copy)]
struct ReplicaLimits {
    capacity_bytes: u64,
    max_pages: usize,
}
impl Default for ReplicaLimits {
    fn default() -> Self {
        Self {
            capacity_bytes: 1 << 28,
            max_pages: 65536,
        }
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
struct Fixture {
    meters: Vec<FrameMeter>,
    metadata: Group,
    chunks: Vec<NodeProcess>,
    journals: Vec<PathBuf>,
    chunk_endpoints: Vec<String>,
    group_id: u64,
    group_commit: bool,
    segmented: bool,
    checkpoint: Option<u64>,
    retention: Option<(u64, usize)>,
    limits: ReplicaLimits,
    worker_threads: usize,
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
fn start_chunk(endpoint: &str, path: &Path) -> NodeProcess {
    start_chunk_with_recovery(endpoint, path, "peers")
}
fn start_chunk_with_recovery(endpoint: &str, path: &Path, recovery: &str) -> NodeProcess {
    start_chunk_with_policy(endpoint, path, recovery, true)
}
fn start_chunk_with_policy(
    endpoint: &str,
    path: &Path,
    recovery: &str,
    group_commit: bool,
) -> NodeProcess {
    start_chunk_with_checkpoint(endpoint, path, recovery, group_commit, None)
}
fn start_chunk_with_checkpoint(
    endpoint: &str,
    path: &Path,
    recovery: &str,
    group_commit: bool,
    checkpoint: Option<u64>,
) -> NodeProcess {
    start_chunk_with_layout(endpoint, path, recovery, group_commit, checkpoint, false)
}
fn start_chunk_with_layout(
    endpoint: &str,
    path: &Path,
    recovery: &str,
    group_commit: bool,
    checkpoint: Option<u64>,
    segmented: bool,
) -> NodeProcess {
    start_chunk_with_retention(
        endpoint,
        path,
        recovery,
        group_commit,
        checkpoint,
        segmented,
        None,
    )
}
fn start_chunk_with_retention(
    endpoint: &str,
    path: &Path,
    recovery: &str,
    group_commit: bool,
    checkpoint: Option<u64>,
    segmented: bool,
    retention: Option<(u64, usize)>,
) -> NodeProcess {
    start_chunk_with_limits(
        endpoint,
        path,
        recovery,
        group_commit,
        checkpoint,
        segmented,
        retention,
        ReplicaLimits::default(),
    )
}
#[allow(clippy::too_many_arguments)]
fn start_chunk_with_limits(
    endpoint: &str,
    path: &Path,
    recovery: &str,
    group_commit: bool,
    checkpoint: Option<u64>,
    segmented: bool,
    retention: Option<(u64, usize)>,
    limits: ReplicaLimits,
) -> NodeProcess {
    start_chunk_with_workers(
        endpoint,
        path,
        recovery,
        group_commit,
        checkpoint,
        segmented,
        retention,
        limits,
        4,
    )
}
fn start_chunk_with_workers(
    endpoint: &str,
    path: &Path,
    recovery: &str,
    group_commit: bool,
    checkpoint: Option<u64>,
    segmented: bool,
    retention: Option<(u64, usize)>,
    limits: ReplicaLimits,
    worker_threads: usize,
) -> NodeProcess {
    assert!(worker_threads > 0);
    let worker_threads = worker_threads.to_string();
    let capacity = limits.capacity_bytes.to_string();
    let max_pages = limits.max_pages.to_string();
    let segment_bytes = retention.map(|r| r.0.to_string());
    let retirement_threshold = retention.map(|r| r.1.to_string());
    let interval = checkpoint.map(|value| value.to_string());
    let mut settings = vec![
        ("MATRIXOBJECT_REPLICA_ADDR", endpoint),
        (
            "MATRIXOBJECT_REPLICA_SEGMENTED_OUTCOMES",
            if segmented { "true" } else { "false" },
        ),
        ("MATRIXOBJECT_REPLICA_JOURNAL", path.to_str().unwrap()),
        ("MATRIXOBJECT_REPLICA_CAPACITY_BYTES", capacity.as_str()),
        ("MATRIXOBJECT_REPLICA_MAX_PAGES", max_pages.as_str()),
        ("MATRIXOBJECT_REPLICA_THREADS", worker_threads.as_str()),
        ("MATRIXOBJECT_REPLICA_MAX_PAYLOAD_BYTES", "8388608"),
        ("MATRIXOBJECT_REPLICA_LOCAL_RECOVERY", recovery),
        (
            "MATRIXOBJECT_REPLICA_GROUP_COMMIT",
            if group_commit { "true" } else { "false" },
        ),
    ];
    if let Some(interval) = &interval {
        settings.push(("MATRIXOBJECT_REPLICA_CHECKPOINT_BYTES", interval));
    }
    if let Some(value) = &segment_bytes {
        settings.push(("MATRIXOBJECT_REPLICA_SEGMENT_BYTES", value));
    }
    if let Some(value) = &retirement_threshold {
        settings.push(("MATRIXOBJECT_REPLICA_RETENTION_SEGMENTS", value));
    }
    processes::start_without_env(
        env!("CARGO_BIN_EXE_chunkserver"),
        &settings,
        &[
            "MATRIXOBJECT_REPLICA_CHECKPOINT_BYTES",
            "MATRIXOBJECT_WAL_ZERO_FILL_BYTES",
            "MATRIXOBJECT_REPLICA_SEGMENT_BYTES",
            "MATRIXOBJECT_REPLICA_RETENTION_SEGMENTS",
        ],
    )
}

#[path = "star_corpus_scale.rs"]
mod star_corpus_scale;
#[path = "star_recovery_profiles.rs"]
mod star_recovery_profiles;
impl Fixture {
    fn new() -> Self {
        Self::with_group_commit(true)
    }
    fn with_group_commit(group_commit: bool) -> Self {
        Self::with_storage(group_commit, false, None)
    }
    fn with_storage(group_commit: bool, segmented: bool, checkpoint: Option<u64>) -> Self {
        Self::with_retention(group_commit, segmented, checkpoint, None)
    }
    fn with_retention(
        group_commit: bool,
        segmented: bool,
        checkpoint: Option<u64>,
        retention: Option<(u64, usize)>,
    ) -> Self {
        Self::with_limits(
            group_commit,
            segmented,
            checkpoint,
            retention,
            ReplicaLimits::default(),
        )
    }
    fn with_limits(
        group_commit: bool,
        segmented: bool,
        checkpoint: Option<u64>,
        retention: Option<(u64, usize)>,
        limits: ReplicaLimits,
    ) -> Self {
        Self::with_workers(group_commit, segmented, checkpoint, retention, limits, 4)
    }
    fn with_workers(
        group_commit: bool,
        segmented: bool,
        checkpoint: Option<u64>,
        retention: Option<(u64, usize)>,
        limits: ReplicaLimits,
        worker_threads: usize,
    ) -> Self {
        assert!((1..=32).contains(&worker_threads));
        assert!(limits.capacity_bytes > 0 && limits.max_pages > 0);
        let directory =
            Directory(std::env::temp_dir().join(node_regime::run_token("star-append-cost")));
        std::fs::create_dir(&directory.0).unwrap();
        let journals = (0..3)
            .map(|id| directory.0.join(format!("{id}.journal")))
            .collect::<Vec<_>>();
        let chunks = journals
            .iter()
            .map(|path| {
                start_chunk_with_workers(
                    "127.0.0.1:0",
                    path,
                    "peers",
                    group_commit,
                    checkpoint,
                    segmented,
                    retention,
                    limits,
                    worker_threads,
                )
            })
            .collect::<Vec<_>>();
        assert!(chunks
            .iter()
            .all(|node| node.startup_line.contains("local_recovery=peers")));
        assert!(chunks.iter().all(|node| node
            .startup_line
            .contains(&format!("group_commit={group_commit}"))));
        let chunk_endpoints = chunks
            .iter()
            .map(|node| node.endpoint.clone())
            .collect::<Vec<_>>();
        let members = (0..3)
            .map(|id| format!("append-member-{id}"))
            .collect::<Vec<_>>();
        let catalog = ChunkGroupCatalog::new(vec![ChunkGroupConfig {
            placement_id: "append-group".into(),
            failure_domain: "local-domain".into(),
            identity: Some(ChunkGroupIdentity {
                version: 3,
                group_id: 1,
                node_id: 1,
                capacity_bytes: limits.capacity_bytes,
                max_pages: limits.max_pages as u64,
                voters: vec![1, 2, 3],
                peer_endpoints: vec![],
                bootstrap_voters: vec![],
                bootstrap_id: 0,
            }),
            api_endpoints: chunk_endpoints.clone(),
            member_ids: members.clone(),
        }])
        .unwrap();
        let metadata = Group::configured_with_timeout(
            None,
            None,
            Some(catalog.to_bytes().unwrap()),
            0,
            false,
            5000,
        );
        let group_id = metadata
            .client(metadata.leader())
            .metadata_group_identity()
            .unwrap()
            .group_id;
        let meters = metadata
            .api_addresses
            .iter()
            .map(|address| FrameMeter::start(*address))
            .collect();
        let fixture = Self {
            meters,
            metadata,
            chunks,
            journals,
            chunk_endpoints,
            group_id,
            group_commit,
            segmented,
            checkpoint,
            retention,
            limits,
            worker_threads,
            directory,
        };
        let mut client = fixture.client();
        for member in members {
            client
                .heartbeat_placement_node(&member, 0, limits.capacity_bytes, false, true, 100)
                .unwrap();
        }
        client.enable_publication_fence().unwrap();
        fixture
    }
    fn client(&self) -> RemoteMetaClient {
        RemoteMetaClient::connect_raft_catalog(
            &self
                .meters
                .iter()
                .map(|meter| meter.endpoint.clone())
                .collect::<Vec<_>>(),
            self.group_id,
            &meta_options(),
        )
        .unwrap()
    }
    fn wire(&self) -> Counts {
        self.meters
            .iter()
            .map(FrameMeter::counts)
            .fold(Counts::default(), |mut total, value| {
                total.requests += value.requests;
                total.replies += value.replies;
                total.publications += value.publications;
                total.publication_bytes += value.publication_bytes;
                total.frames += value.frames;
                total.allocations += value.allocations;
                total
            })
    }
    fn wal(&self) -> u64 {
        (0..3)
            .map(|id| {
                std::fs::metadata(
                    self.metadata
                        .root
                        .join(id.to_string())
                        .join("metadata.peer.wal"),
                )
                .unwrap()
                .len()
            })
            .sum()
    }
    fn data_journals(&self) -> Vec<u64> {
        self.journals
            .iter()
            .map(|path| std::fs::metadata(path).unwrap().len())
            .collect()
    }
    fn settle(&self) {
        let until = Instant::now() + Duration::from_secs(5);
        let mut previous = self.wal();
        let mut stable = 0;
        while stable < 3 {
            assert!(Instant::now() < until, "metadata WAL did not settle");
            std::thread::sleep(Duration::from_millis(20));
            let current = self.wal();
            stable = if current == previous { stable + 1 } else { 0 };
            previous = current;
        }
    }
    fn reserve(
        &self,
        client: &mut RemoteMetaClient,
        key: &str,
        generation: Option<u64>,
        pages: u32,
        append: bool,
    ) -> StagedAllocation {
        for _ in 0..16 {
            let prepared = client
                .prepare_staged_allocation("append", key, generation, pages, append, 3_600_000)
                .unwrap();
            let path = self.directory.0.join(format!("{key}-{append}.request"));
            let mut file = File::create(&path).unwrap();
            file.write_all(&prepared.to_bytes()).unwrap();
            file.sync_all().unwrap();
            let prepared =
                PreparedStagedAllocation::from_bytes(&std::fs::read(path).unwrap()).unwrap();
            match client.try_allocate_prepared_staged_object(&prepared) {
                Ok(record) => return record,
                Err(StagedAllocationFailure::PositionConsumed) => continue,
                Err(error) => panic!("prepared reservation failed without safe retry: {error:?}"),
            }
        }
        panic!("prepared reservation position stayed busy");
    }
    fn restart(&mut self) {
        for id in 0..3 {
            self.metadata.kill(id);
            self.chunks[id].child.kill().unwrap();
            assert!(
                processes::reap_within(&mut self.chunks[id].child, Duration::from_secs(3))
                    .is_some()
            );
        }
        for id in 0..3 {
            self.metadata.start(id);
            self.chunks[id] = start_chunk_with_workers(
                &self.chunk_endpoints[id],
                &self.journals[id],
                "peers",
                self.group_commit,
                self.checkpoint,
                self.segmented,
                self.retention,
                self.limits,
                self.worker_threads,
            );
            assert!(self.chunks[id]
                .startup_line
                .contains(&format!("group_commit={}", self.group_commit)));
        }
        self.metadata.leader();
    }
}

fn verify_replicas(fixture: &Fixture, manifest: &ObjectManifest, expected: &[u8]) {
    for endpoint in &fixture.chunk_endpoints {
        let mut replica =
            RemoteReplicaClient::connect(endpoint, &ReplicaClientOptions::default()).unwrap();
        for extents in manifest.extents.chunks(8) {
            let pages = extents
                .iter()
                .map(|extent| {
                    (
                        PageId {
                            chunk_id: ChunkId::new(extent.volume_id, extent.chunk_id).unwrap(),
                            page_index: extent.page_index,
                        },
                        extent.page_offset,
                        extent.length,
                    )
                })
                .collect::<Vec<_>>();
            let values = replica.read_chunks_pipelined(&pages).unwrap();
            assert_eq!(values.len(), extents.len());
            for (value, extent) in values.into_iter().zip(extents) {
                let start = extent.object_offset as usize;
                assert_eq!(value.data, expected[start..start + extent.length as usize]);
                assert_eq!(value.write_sequence, extent.write_sequence);
            }
        }
    }
}

fn run(prefix_pages: &[usize], page_bytes: usize, compact: bool) {
    let mut fixture = Fixture::new();
    let mut client = fixture.client();
    let options = ObjectWriteOptions {
        page_bytes,
        target_groups: 1,
        replica: ReplicaClientOptions::default(),
        ..Default::default()
    };
    let mut committed = Vec::new();
    for &pages in prefix_pages {
        assert!((1..=4095).contains(&pages));
        let key = format!("prefix-{pages:04}");
        let prefix = vec![0x31; pages * page_bytes];
        let allocation = fixture.reserve(&mut client, &key, None, pages as u32, false);
        let base = client
            .write_staged_object(&allocation, None, &prefix, &options)
            .unwrap();
        assert_eq!(base.extents.len(), pages);
        fixture.settle();
        let before_wire = fixture.wire();
        let before_wal = fixture.wal();
        let before_data = fixture.data_journals();
        let suffix = vec![0x7b; page_bytes];
        let started = Instant::now();
        let allocation = fixture.reserve(&mut client, &key, Some(base.generation), 1, true);
        let after = if compact {
            client.append_staged_object_compact(&allocation, base.clone(), &suffix, &options)
        } else {
            client.write_staged_object(&allocation, Some(base.clone()), &suffix, &options)
        }
        .unwrap();
        let elapsed = started.elapsed();
        fixture.settle();
        let wire = fixture.wire().since(before_wire);
        let wal = fixture.wal().checked_sub(before_wal).unwrap();
        let data = fixture
            .data_journals()
            .iter()
            .zip(before_data)
            .map(|(after, before)| after.checked_sub(before).unwrap())
            .collect::<Vec<_>>();
        let outcome = ManifestOutcome::decide(Some(base.clone()), after.clone()).unwrap();
        let mut request = StagedAllocationRequest {
            version: 1,
            action: 4,
            allocation_id: allocation.allocation_id,
            outcome: Some(outcome.clone()),
            ..Default::default()
        };
        if compact {
            request.version = 4;
            request.action = 11;
            request.manifest_append = Some(
                matrixobjectstore_rs::manifest_append::ManifestAppend::decide(&outcome).unwrap(),
            );
            request.outcome = None;
        }
        let publication_frame = 6 + request.encoded_len();
        if compact {
            assert!(
                wire.requests < 1024,
                "compact append requests copied the prefix"
            );
            assert!(
                wire.replies < 2048,
                "compact append replies copied the prefix"
            );
            assert!(
                publication_frame < 256,
                "compact publication copied the prefix"
            );
            assert!(wal < 6000, "compact WAL copied the prefix: {wal}");
        }
        assert!(wire.publications >= 1);
        assert_eq!(
            wire.publication_bytes,
            wire.publications * publication_frame as u64
        );
        assert_eq!(after.extents[..pages], base.extents);
        assert_eq!(after.length, (prefix.len() + suffix.len()) as u64);
        assert_eq!(after.generation, base.generation + 1);
        for bytes in &data {
            assert!(*bytes >= suffix.len() as u64);
            assert!(
                *bytes <= (4 * suffix.len() + 2048) as u64,
                "one append rewrote more than its suffix: {bytes}"
            );
        }
        assert!(client
            .read_staged_allocation(allocation.allocation_id)
            .is_err());
        let extent = after.extents.last().unwrap();
        let page = PageId {
            chunk_id: ChunkId::new(extent.volume_id, extent.chunk_id).unwrap(),
            page_index: extent.page_index,
        };
        for endpoint in &fixture.chunk_endpoints {
            let value = RemoteReplicaClient::connect(endpoint, &ReplicaClientOptions::default())
                .unwrap()
                .read_chunk(&page, 0, extent.length)
                .unwrap();
            assert_eq!(value.data, suffix);
            assert_eq!(value.write_sequence, extent.write_sequence);
        }
        println!("STAR_APPEND_COST compact={compact} prefix_pages={pages} prefix_bytes={} suffix_bytes={} metadata_request_bytes={} metadata_reply_bytes={} publication_frame_bytes={publication_frame} publication_attempts={} manifest_outcome_bytes={} metadata_wal_bytes={wal} data_journal_bytes={} elapsed_us={}",
            prefix.len(), suffix.len(), wire.requests, wire.replies, wire.publications,
            outcome.encoded_len(), data.iter().sum::<u64>(), elapsed.as_micros());
        let mut expected = prefix;
        expected.extend_from_slice(&suffix);
        assert_eq!(
            client
                .read_object("append", &key, expected.len(), &options.replica)
                .unwrap(),
            (after.clone(), expected.clone())
        );
        committed.push((after, expected));
    }
    drop(client);
    fixture.restart();
    let mut client = fixture.client();
    let before_verification = fixture.data_journals();
    for (manifest, bytes) in committed {
        verify_replicas(&fixture, &manifest, &bytes);
        assert_eq!(
            client
                .read_object("append", &manifest.key, bytes.len(), &options.replica)
                .unwrap(),
            (manifest, bytes)
        );
    }
    assert!(client.list_staged_allocations(0, 32).unwrap().0.is_empty());
    assert!(client.list_retained_manifests(0, 32).unwrap().0.is_empty());
    assert_eq!(
        fixture.data_journals(),
        before_verification,
        "recovered data should not need journal mutations to serve verified reads"
    );
}

#[test]
fn owned_append_keeps_data_writes_bounded_and_recovers_all_replicas() {
    run(&[1, 32], 128, false);
}
#[test]
#[ignore = "explicit local Star append amplification measurement"]
fn measure_one_star_append_as_prefix_grows() {
    run(&[1, 128, 4095], 4096, false);
}

#[test]
fn compact_owned_append_keeps_publication_bounded_and_recovers_all_replicas() {
    run(&[1, 32], 128, true);
}
#[test]
#[ignore = "explicit local compact append measurement"]
fn measure_compact_star_append_as_prefix_grows() {
    run(&[1, 128, 4095], 4096, true);
}

/// An append under an allocation the metadata plane returned a moment ago does not read the
/// allocation back first: one metadata request fewer, the same result. The allocation is still
/// checked at publication, so one that is not the live one is refused there.
#[test]
fn a_confirmed_allocation_is_not_read_back_before_the_append() {
    let fixture = Fixture::new();
    let mut client = fixture.client();
    let options = ObjectWriteOptions {
        page_bytes: 4096,
        target_groups: 1,
        replica: ReplicaClientOptions::default(),
        ..Default::default()
    };
    let key = "confirmed";
    let first = fixture.reserve(&mut client, key, None, 1, false);
    let mut manifest = client
        .write_staged_object(&first, None, &vec![1u8; 4096], &options)
        .unwrap();
    let mut frames = Vec::new();
    for confirmed in [false, true, false, true] {
        fixture.settle();
        let allocation = fixture.reserve(&mut client, key, Some(manifest.generation), 1, true);
        fixture.settle();
        let before = fixture.wire().frames;
        if confirmed {
            client.confirm_staged_allocation(allocation.allocation_id);
        }
        let after = client
            .append_staged_object_compact(&allocation, manifest.clone(), &vec![2u8; 4096], &options)
            .unwrap();
        fixture.settle();
        frames.push(fixture.wire().frames - before);
        assert_eq!(after.generation, manifest.generation + 1);
        assert_eq!(after.length, manifest.length + 4096);
        manifest = after;
    }
    assert_eq!(
        frames[0], frames[2],
        "unconfirmed appends differ: {frames:?}"
    );
    assert_eq!(frames[1], frames[3], "confirmed appends differ: {frames:?}");
    assert_eq!(
        frames[0],
        frames[1] + 1,
        "a confirmed allocation must save exactly the read-back: {frames:?}"
    );
    // a confirmation names one allocation and is used once: the next append reads back again
    fixture.settle();
    let allocation = fixture.reserve(&mut client, key, Some(manifest.generation), 1, true);
    client.confirm_staged_allocation(allocation.allocation_id + 1);
    fixture.settle();
    let before = fixture.wire().frames;
    client
        .append_staged_object_compact(&allocation, manifest.clone(), &vec![3u8; 4096], &options)
        .unwrap();
    fixture.settle();
    assert_eq!(
        fixture.wire().frames - before,
        frames[0],
        "a confirmation for another allocation skipped the read-back"
    );
}

/// With plans cached, an append to an object the client appended to a moment ago does not ask
/// the metadata plane for a plan again: one metadata request fewer. A failed append drops the
/// plan, and the next one asks again.
#[test]
fn a_cached_append_plan_is_not_asked_for_again_until_an_append_fails() {
    let fixture = Fixture::new();
    let mut client = fixture.client();
    client.cache_append_plans(Duration::from_secs(60));
    let options = ObjectWriteOptions {
        page_bytes: 4096,
        target_groups: 1,
        replica: ReplicaClientOptions::default(),
        ..Default::default()
    };
    let key = "planned";
    let first = fixture.reserve(&mut client, key, None, 1, false);
    let mut manifest = client
        .write_staged_object(&first, None, &vec![1u8; 4096], &options)
        .unwrap();
    let mut frames = Vec::new();
    for step in 0..6 {
        fixture.settle();
        let allocation = fixture.reserve(&mut client, key, Some(manifest.generation), 1, true);
        fixture.settle();
        let before = fixture.wire().frames;
        client.confirm_staged_allocation(allocation.allocation_id);
        if step == 4 {
            // an append that is refused (it names the wrong generation) drops the plan
            let mut wrong = manifest.clone();
            wrong.generation += 1;
            assert!(client
                .append_staged_object_compact(&allocation, wrong, &vec![2u8; 4096], &options)
                .is_err());
            frames.push(u64::MAX);
            continue;
        }
        manifest = client
            .append_staged_object_compact(&allocation, manifest.clone(), &vec![2u8; 4096], &options)
            .unwrap();
        fixture.settle();
        frames.push(fixture.wire().frames - before);
    }
    // the plan and the publication, then the publication alone, until the refused append; the
    // append after it plans again
    assert_eq!(
        frames,
        vec![2, 1, 1, 1, u64::MAX, 2],
        "frames per append: {frames:?}"
    );
}

/// A cached plan is reused only within its lifetime: an append after it asks again.
#[test]
fn a_cached_append_plan_is_asked_for_again_once_it_is_old() {
    let fixture = Fixture::new();
    let mut client = fixture.client();
    client.cache_append_plans(Duration::from_millis(2500));
    let options = ObjectWriteOptions {
        page_bytes: 4096,
        target_groups: 1,
        replica: ReplicaClientOptions::default(),
        ..Default::default()
    };
    let key = "aged";
    let first = fixture.reserve(&mut client, key, None, 1, false);
    let mut manifest = client
        .write_staged_object(&first, None, &vec![1u8; 4096], &options)
        .unwrap();
    let mut frames = Vec::new();
    for pause_ms in [0u64, 0, 3000, 0] {
        std::thread::sleep(Duration::from_millis(pause_ms));
        fixture.settle();
        let allocation = fixture.reserve(&mut client, key, Some(manifest.generation), 1, true);
        fixture.settle();
        let before = fixture.wire().frames;
        client.confirm_staged_allocation(allocation.allocation_id);
        manifest = client
            .append_staged_object_compact(&allocation, manifest.clone(), &vec![2u8; 4096], &options)
            .unwrap();
        fixture.settle();
        frames.push(fixture.wire().frames - before);
    }
    // planned, reused, expired and planned again, reused
    assert_eq!(frames, vec![2, 1, 2, 1], "frames per append: {frames:?}");
}

fn staged_total(client: &mut RemoteMetaClient) -> usize {
    let (mut after, mut total) = (0, 0);
    loop {
        let (ids, more) = client.list_staged_allocations(after, 32).unwrap();
        total += ids.len();
        match ids.last() {
            Some(last) if more => after = *last,
            _ => return total,
        }
    }
}

fn create_object(fixture: &Fixture, client: &mut RemoteMetaClient, key: &str) -> ObjectManifest {
    let options = ObjectWriteOptions {
        page_bytes: 4096,
        target_groups: 1,
        replica: ReplicaClientOptions::default(),
        ..Default::default()
    };
    let allocation = fixture.reserve(client, key, None, 1, false);
    client
        .write_staged_object(&allocation, None, &vec![1u8; 4096], &options)
        .unwrap()
}

fn gateway_plane(fixture: &Fixture) -> matrixobjectstore_rs::net::cluster_gateway::SdkPlane {
    use matrixobjectstore_rs::net::cluster_gateway::{CompactState, GatewayOptions, SdkPlane};
    let mut options = GatewayOptions::default();
    options.compact_append = true;
    options.reserve_next = true;
    options.write = ObjectWriteOptions {
        page_bytes: 4096,
        target_groups: 1,
        replica: ReplicaClientOptions::default(),
        ..Default::default()
    };
    SdkPlane::new(fixture.client(), options, std::sync::Arc::new(CompactState::default()))
}

/// The gateway reserves the next allocation only for an object it is appending to repeatedly. A
/// cold object's append leaves no reservation in the metadata plane's table (it would hold a slot
/// until it expires, and cost an expiry commit); an append to a hot object whose reservation was
/// made needs no allocation request. Frames are metadata requests; the allocation request is the
/// difference between a cold append and an append that used a reservation.
#[test]
fn the_gateway_reserves_only_for_objects_appended_to_repeatedly() {
    use matrixobjectstore_rs::net::cluster_gateway::{ObjectPlane, HOT_WINDOW};
    let fixture = Fixture::new();
    let mut plane = gateway_plane(&fixture);
    let mut probe = fixture.client();
    let key = "hot-log";
    let mut manifest = create_object(&fixture, &mut probe, key);
    let mut allocations = Vec::new();
    let mut staged = Vec::new();
    let clock = Instant::now();
    let mut timing_ms = Vec::new();
    // cold, hot without a reservation yet, hot with one, hot with one, cold with one, cold
    for pause in [0u64, 0, 0, 0, HOT_WINDOW.as_millis() as u64 + 500, HOT_WINDOW.as_millis() as u64 + 500] {
        std::thread::sleep(Duration::from_millis(pause));
        fixture.settle();
        let before = fixture.wire().allocations;
        let started_ms = clock.elapsed().as_millis();
        manifest = plane.append_best(manifest, &vec![2u8; 4096]).map_err(|failure| failure.error).unwrap();
        let completed_ms = clock.elapsed().as_millis();
        fixture.settle();
        allocations.push(fixture.wire().allocations - before);
        staged.push(staged_total(&mut probe));
        timing_ms.push((started_ms, completed_ms, clock.elapsed().as_millis()));
    }
    assert_eq!(staged, vec![0, 1, 1, 1, 0, 0], "table entries after each append: {staged:?}; append start/end/probe milliseconds: {timing_ms:?}; hot window: {HOT_WINDOW:?}");
    // allocation commits per append: a cold append and a hot one without a reservation allocate; a
    // hot append whose reservation exists does not, and neither does a cold one that uses it
    assert_eq!(allocations, vec![1, 1, 0, 0, 0, 1], "allocation commits per append: {allocations:?}; append start/end/probe milliseconds: {timing_ms:?}; hot window: {HOT_WINDOW:?}");
}

/// With the metadata plane's table past the point where publications reserve, hot appends go on
/// as they did before reservations existed: no reservation, no failure, the table does not grow.
#[test]
fn hot_appends_with_a_nearly_full_table_carry_no_reservation_and_still_succeed() {
    use matrixobjectstore_rs::net::cluster_gateway::ObjectPlane;
    let fixture = Fixture::new();
    let mut plane = gateway_plane(&fixture);
    let mut probe = fixture.client();
    let mut manifest = create_object(&fixture, &mut probe, "busy");
    for index in 0..70 {
        fixture.reserve(&mut probe, &format!("filler-{index:03}"), None, 1, false);
    }
    fixture.settle();
    let table = staged_total(&mut probe);
    assert!(table > 64, "the table must be past the reservation limit: {table}");
    let mut allocations = Vec::new();
    for _ in 0..4 {
        fixture.settle();
        let before = fixture.wire().allocations;
        manifest = plane.append_best(manifest, &vec![2u8; 4096]).map_err(|failure| failure.error).unwrap();
        fixture.settle();
        allocations.push(fixture.wire().allocations - before);
    }
    assert_eq!(staged_total(&mut probe), table, "a hot append reserved with the table past its limit");
    assert_eq!(allocations, vec![1, 1, 1, 1], "every append allocates: {allocations:?}");
}

#[test]
fn compact_append_publishes_after_metadata_leader_loss_and_recovers() {
    let mut fixture = Fixture::new();
    let mut client = fixture.client();
    let options = ObjectWriteOptions {
        page_bytes: 128,
        ..Default::default()
    };
    let allocation = fixture.reserve(&mut client, "failover", None, 1, false);
    let base = client
        .write_staged_object(&allocation, None, b"prefix", &options)
        .unwrap();
    let allocation = fixture.reserve(&mut client, "failover", Some(base.generation), 1, true);
    let mut wrong = base.clone();
    wrong.extents[0].data_sha256[0] ^= 1;
    let data_before = fixture.data_journals();
    assert!(matches!(
        client.append_staged_object_compact(&allocation, wrong, b"suffix", &options),
        Err(matrixobjectstore_rs::cluster::ObjectWriteFailure::BeforePublication { .. })
    ));
    assert_eq!(
        fixture.data_journals(),
        data_before,
        "stale planning wrote data"
    );
    assert_eq!(
        client
            .read_staged_allocation(allocation.allocation_id)
            .unwrap(),
        allocation
    );
    drop(client);
    let old_leader = fixture.metadata.leader();
    fixture.metadata.kill(old_leader);
    fixture.metadata.leader();
    let after = fixture
        .client()
        .append_staged_object_compact(&allocation, base, b"suffix", &options)
        .unwrap();
    verify_replicas(&fixture, &after, b"prefixsuffix");
    let survivor = fixture.metadata.leader();
    let other = (0..3)
        .find(|id| *id != survivor && *id != old_leader)
        .unwrap();
    fixture.metadata.kill(other);
    let mut minority = fixture.metadata.client(survivor);
    let until = Instant::now() + Duration::from_secs(10);
    loop {
        if minority.plan_append_routes_compact(&after, 1, 1).is_err() {
            break;
        }
        assert!(
            Instant::now() < until,
            "minority kept serving compact append plans"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    drop(minority);
    fixture.metadata.start(other);
    fixture.metadata.start(old_leader);
    fixture.restart();
    let mut client = fixture.client();
    assert_eq!(
        client
            .read_object("append", "failover", 128, &options.replica)
            .unwrap(),
        (after.clone(), b"prefixsuffix".to_vec())
    );
    verify_replicas(&fixture, &after, b"prefixsuffix");
    assert!(client
        .read_staged_allocation(allocation.allocation_id)
        .is_err());
}

fn assert_resident_process_profile(fixture: &Fixture) {
    let Ok(raw) = std::env::var("MATRIXOBJECT_REPLICA_RESIDENT_PAYLOAD_BYTES") else {
        return;
    };
    let budget: u64 = raw.parse().unwrap();
    if budget == 0 {
        return;
    }
    let entries = std::env::var("MATRIXOBJECT_REPLICA_RESIDENT_PAGE_ENTRIES")
        .unwrap_or_else(|_| "4096".into());
    for node in &fixture.chunks {
        assert!(node
            .startup_line
            .contains(&format!("resident_payload_bytes={budget}")));
        assert!(node
            .startup_line
            .contains(&format!("resident_page_entries={entries}")));
        assert!(processes::environ_carries(
            node.pid,
            "MATRIXOBJECT_REPLICA_RESIDENT_PAYLOAD_BYTES",
            &raw
        ));
        assert!(processes::environ_carries(
            node.pid,
            "MATRIXOBJECT_REPLICA_RESIDENT_PAGE_ENTRIES",
            &entries
        ));
        println!(
            "RESIDENT_PROCESS_PROFILE pid={} payload_bytes={budget} page_entries={entries}",
            node.pid
        );
    }
}

#[test]
fn segmented_star_replicas_keep_objects_through_metadata_failover_and_full_restart() {
    let mut fixture = Fixture::with_storage(true, true, Some(1024));
    assert_resident_process_profile(&fixture);
    assert!(fixture
        .chunks
        .iter()
        .all(|n| n.startup_line.contains("storage=segmented-outcomes")));
    let options = ObjectWriteOptions {
        page_bytes: 4096,
        target_groups: 1,
        ..Default::default()
    };
    let mut client = fixture.client();
    let mut committed = Vec::new();
    for n in 0..16 {
        let key = format!("segmented-corpus-{n}");
        let bytes = vec![n as u8 + 1; 16 * 1024];
        let allocation = fixture.reserve(&mut client, &key, None, 4, false);
        let manifest = client
            .write_staged_object(&allocation, None, &bytes, &options)
            .unwrap();
        verify_replicas(&fixture, &manifest, &bytes);
        committed.push((key, manifest, bytes));
    }
    let key = "segmented-hot";
    let mut expected = vec![0x61; 8192];
    let allocation = fixture.reserve(&mut client, key, None, 2, false);
    let mut manifest = client
        .write_staged_object(&allocation, None, &expected, &options)
        .unwrap();
    let mut keep_alive = Vec::new();
    for (index, endpoint) in fixture.chunk_endpoints.iter().enumerate() {
        let mut replica = RemoteReplicaClient::connect(endpoint, &options.replica).unwrap();
        let extent = &manifest.extents[0];
        let page = PageId {
            chunk_id: ChunkId::new(extent.volume_id, extent.chunk_id).unwrap(),
            page_index: extent.page_index,
        };
        replica.read_chunk(&page, 0, extent.length).unwrap();
        keep_alive.push(replica);
        assert!(processes::environ_carries(
            fixture.chunks[index].pid,
            "MATRIXOBJECT_REPLICA_JOURNAL",
            fixture.journals[index].to_str().unwrap()
        ));
    }
    // Keep each API connection open while sampling the process socket tables;
    // Raft follower connections may otherwise close between the two readings.
    let metadata_connections: Vec<_> = fixture
        .metadata
        .api_addresses
        .iter()
        .map(|address| TcpStream::connect(address).unwrap())
        .collect();
    let mut pids: Vec<_> = fixture.chunks.iter().map(|n| n.pid).collect();
    for (index, node) in fixture.metadata.nodes.iter().enumerate() {
        let node = node.as_ref().unwrap();
        pids.push(node.pid);
        assert!(processes::environ_carries(
            node.pid,
            "MATRIXOBJECT_METASERVER_RAFT_STATE_DIR",
            fixture
                .metadata
                .root
                .join(index.to_string())
                .to_str()
                .unwrap()
        ));
    }
    let unique: std::collections::BTreeSet<_> = pids.iter().copied().collect();
    assert_eq!(unique.len(), 6);
    assert!(pids
        .iter()
        .all(|pid| *pid != std::process::id() && processes::is_live(*pid)));
    let until = Instant::now() + Duration::from_secs(3);
    while !pids
        .iter()
        .all(|pid| node_regime::sockets::has_established_socket(*pid))
    {
        assert!(
            Instant::now() < until,
            "node did not accept its API connection"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    node_regime::sockets::assert_established_two_ways(&pids);
    drop(metadata_connections);
    let leader = fixture.metadata.leader();
    fixture.metadata.kill(leader);
    fixture.metadata.leader();
    drop(client);
    let mut client = fixture.client();
    for n in 0..16 {
        let suffix = vec![n as u8 + 0x71; 4096];
        let allocation = fixture.reserve(&mut client, key, Some(manifest.generation), 1, true);
        manifest = client
            .append_staged_object_compact(&allocation, manifest, &suffix, &options)
            .unwrap();
        expected.extend(suffix);
    }
    verify_replicas(&fixture, &manifest, &expected);
    let mut journal_ids = std::collections::BTreeSet::new();
    for endpoint in &fixture.chunk_endpoints {
        let mut node = RemoteReplicaClient::connect(endpoint, &options.replica).unwrap();
        let until = Instant::now() + Duration::from_secs(30);
        while node.journal_status().unwrap().checkpoint_in_flight {
            assert!(Instant::now() < until, "automatic checkpoint did not drain");
            std::thread::sleep(Duration::from_millis(2));
        }
        let checkpoint = node.checkpoint_journal().unwrap();
        assert!(journal_ids.insert(checkpoint.journal_id.clone()));
        loop {
            let status = node.journal_status().unwrap();
            if status.checkpoint_is_published(&checkpoint).unwrap() && !status.checkpoint_in_flight
            {
                assert_eq!(status.pending_records, 0);
                assert_eq!(status.bytes_since_checkpoint, 0);
                // Observation itself must not append maintenance records.
                assert_eq!(
                    node.journal_status().unwrap().journal_bytes,
                    status.journal_bytes
                );
                break;
            }
            assert!(
                Instant::now() < until,
                "requested checkpoint did not publish"
            );
            std::thread::sleep(Duration::from_millis(2));
        }
    }
    assert_eq!(journal_ids.len(), 3);
    fixture.metadata.start(leader);
    drop(client);
    drop(keep_alive);
    fixture.restart();
    assert_resident_process_profile(&fixture);
    let mut client = fixture.client();
    // Each restart may need another read barrier while leadership converges.
    // One explicit deadline bounds the complete post-restart verification.
    let mut read_options = options.replica.clone();
    read_options.deadline = Some(Instant::now() + Duration::from_secs(30));
    for (key, manifest, bytes) in committed {
        assert_eq!(
            client
                .read_object("append", &key, bytes.len(), &read_options)
                .unwrap(),
            (manifest.clone(), bytes.clone())
        );
        verify_replicas(&fixture, &manifest, &bytes);
    }
    assert_eq!(
        client
            .read_object("append", key, expected.len(), &read_options)
            .unwrap(),
        (manifest.clone(), expected.clone())
    );
    verify_replicas(&fixture, &manifest, &expected);
}

#[path = "replica_segment_retirement.rs"]
mod replica_segment_retirement;

#[test]
fn compact_append_rejects_a_changed_interior_prefix_after_cache_warmup() {
    for cached_plan in [false, true] {
        let mut fixture = Fixture::new();
        let mut client = fixture.client();
        if cached_plan { client.cache_append_plans(Duration::from_secs(60)); }
        let options = ObjectWriteOptions { page_bytes: 4096, target_groups: 1, ..Default::default() };
        let key = if cached_plan { "interior-cached" } else { "interior-direct" };
        let first = fixture.reserve(&mut client, key, None, 2, false);
        let mut bytes = vec![1u8; 8192];
        let base = client.write_staged_object(&first, None, &bytes, &options).unwrap();
        let allocation = fixture.reserve(&mut client, key, Some(base.generation), 1, true);
        let warm = client.append_staged_object_compact(&allocation, base, &vec![2u8;4096], &options).unwrap();
        bytes.extend_from_slice(&vec![2u8;4096]);assert_eq!(warm.extents.len(),3);
        let allocation = fixture.reserve(&mut client, key, Some(warm.generation), 1, true);
        let mut wrong = warm.clone();wrong.extents[1].data_sha256[0] ^= 1;
        let before_data = fixture.data_journals();
        let result = client.append_staged_object_compact(&allocation, wrong, &vec![3u8;4096], &options);
        assert!(result.is_err(), "changed interior prefix must not publish with a warm digest memo");
        if !cached_plan {
            assert!(matches!(result, Err(matrixobjectstore_rs::cluster::ObjectWriteFailure::BeforePublication { .. })));
            assert_eq!(fixture.data_journals(),before_data,"uncached prefix validation must precede data I/O");
        }
        assert_eq!(client.read_manifest("append",key).unwrap(),warm);
        verify_replicas(&fixture,&warm,&bytes);
        // Use a fresh allocation after resolving visibility; never replay uncertain publication.
        let fresh = fixture.reserve(&mut client,key,Some(warm.generation),1,true);
        let after = client.append_staged_object_compact(&fresh,warm,&vec![3u8;4096],&options).unwrap();
        bytes.extend_from_slice(&vec![3u8;4096]);verify_replicas(&fixture,&after,&bytes);
        drop(client);fixture.restart();
        let read=fixture.client().read_object("append",key,bytes.len(),&options.replica).unwrap();
        assert_eq!(read,(after.clone(),bytes.clone()));verify_replicas(&fixture,&after,&bytes);
    }
}
