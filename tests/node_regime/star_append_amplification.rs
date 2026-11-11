// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use matrixobjectstore_rs::{
    cluster::{
        ChunkGroupCatalog, ChunkGroupConfig, ChunkGroupIdentity, ObjectWriteOptions,
        RemoteReplicaClient, ReplicaClientOptions,
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
    processes::start(
        env!("CARGO_BIN_EXE_chunkserver"),
        &[
            ("MATRIXOBJECT_REPLICA_ADDR", endpoint),
            ("MATRIXOBJECT_REPLICA_JOURNAL", path.to_str().unwrap()),
            ("MATRIXOBJECT_REPLICA_CAPACITY_BYTES", "268435456"),
            ("MATRIXOBJECT_REPLICA_MAX_PAGES", "65536"),
        ],
    )
}
impl Fixture {
    fn new() -> Self {
        let directory =
            Directory(std::env::temp_dir().join(node_regime::run_token("star-append-cost")));
        std::fs::create_dir(&directory.0).unwrap();
        let journals = (0..3)
            .map(|id| directory.0.join(format!("{id}.journal")))
            .collect::<Vec<_>>();
        let chunks = journals
            .iter()
            .map(|path| start_chunk("127.0.0.1:0", path))
            .collect::<Vec<_>>();
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
                capacity_bytes: 1 << 28,
                max_pages: 65536,
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
            directory,
        };
        let mut client = fixture.client();
        for member in members {
            client
                .heartbeat_placement_node(&member, 0, 1 << 28, false, true, 100)
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
            self.chunks[id] = start_chunk(&self.chunk_endpoints[id], &self.journals[id]);
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
        }.unwrap();
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
            request.manifest_append = Some(matrixobjectstore_rs::manifest_append::ManifestAppend::decide(&outcome).unwrap());
            request.outcome = None;
        }
        let publication_frame = 6 + request.encoded_len();
        if compact {
            assert!(wire.requests < 1024, "compact append requests copied the prefix");
            assert!(wire.replies < 2048, "compact append replies copied the prefix");
            assert!(publication_frame < 256, "compact publication copied the prefix");
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

#[test]
fn compact_append_publishes_after_metadata_leader_loss_and_recovers() {
    let mut fixture = Fixture::new();
    let mut client = fixture.client();
    let options = ObjectWriteOptions { page_bytes: 128, ..Default::default() };
    let allocation = fixture.reserve(&mut client, "failover", None, 1, false);
    let base = client.write_staged_object(&allocation, None, b"prefix", &options).unwrap();
    let allocation = fixture.reserve(&mut client, "failover", Some(base.generation), 1, true);
    let mut wrong = base.clone();
    wrong.extents[0].data_sha256[0] ^= 1;
    let data_before = fixture.data_journals();
    assert!(matches!(client.append_staged_object_compact(&allocation, wrong, b"suffix", &options),
        Err(matrixobjectstore_rs::cluster::ObjectWriteFailure::BeforePublication { .. })));
    assert_eq!(fixture.data_journals(), data_before, "stale planning wrote data");
    assert_eq!(client.read_staged_allocation(allocation.allocation_id).unwrap(), allocation);
    drop(client);
    let old_leader = fixture.metadata.leader();
    fixture.metadata.kill(old_leader);
    fixture.metadata.leader();
    let after = fixture.client().append_staged_object_compact(&allocation, base, b"suffix", &options).unwrap();
    verify_replicas(&fixture, &after, b"prefixsuffix");
    let survivor = fixture.metadata.leader();
    let other = (0..3).find(|id| *id != survivor && *id != old_leader).unwrap();
    fixture.metadata.kill(other);
    let mut minority = fixture.metadata.client(survivor);
    let until = Instant::now() + Duration::from_secs(10);
    loop {
        if minority.plan_append_routes_compact(&after, 1, 1).is_err() { break; }
        assert!(Instant::now() < until, "minority kept serving compact append plans");
        std::thread::sleep(Duration::from_millis(50));
    }
    drop(minority);
    fixture.metadata.start(other);
    fixture.metadata.start(old_leader);
    fixture.restart();
    let mut client = fixture.client();
    assert_eq!(client.read_object("append", "failover", 128, &options.replica).unwrap(),
        (after.clone(), b"prefixsuffix".to_vec()));
    verify_replicas(&fixture, &after, b"prefixsuffix");
    assert!(client.read_staged_allocation(allocation.allocation_id).is_err());
}
