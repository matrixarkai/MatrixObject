// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! A replicated append workload through the loss and return of one data node.
//!
//! Every test here runs real processes: one metaserver and `3 * groups` chunkservers, each a
//! fixed three-member Star group. A node is stopped with SIGKILL after `/proc/<pid>/environ`
//! shows it belongs to this run. The oracle is what the client was told succeeded: the bytes
//! of every acknowledged append, in order. A failed append contributes nothing to it.
//!
//! Run one test at a time: the process tests collide on ports when run in parallel.
#![cfg(all(target_os = "linux", feature = "replica-server", feature = "meta-server"))]
mod node_regime;

use matrixobjectstore_rs::cluster::{
    AppendFailoverPolicy, ChunkGroupCatalog, ChunkGroupConfig, ChunkGroupIdentity, MetaClientOptions, ObjectWriteFailure,
    ObjectWriteOptions, RemoteMetaClient, ReplicaClientOptions,
};
use matrixobjectstore_rs::object_manifest::ObjectManifest;
use node_regime::processes::{self, NodeProcess};
use std::net::TcpListener;
use std::path::PathBuf;
use std::time::{Duration, Instant};

pub const PAGE_BYTES: usize = 4096;

/// One metaserver and `groups` Star groups of three chunkservers, on fixed loopback ports so
/// a stopped node can return at the same address with the same journal.
pub struct Cluster {
    pub root: PathBuf,
    pub meta: Option<NodeProcess>,
    /// Comma-joined metadata API endpoints (one, or three with `voters == 3`).
    pub meta_addr: String,
    /// Three-voter metadata quorum: nodes and their API endpoints. Empty for one metaserver.
    pub raft: Vec<Option<NodeProcess>>,
    pub raft_addrs: Vec<String>,
    pub chunks: Vec<Option<NodeProcess>>,
    pub chunk_addrs: Vec<String>,
    pub journals: Vec<PathBuf>,
    pub groups: usize,
    pub heartbeat_ms: u64,
}

impl Cluster {
    /// `heartbeat_ttl_ms`: `None` leaves the metaserver on its own default.
    pub fn start(groups: usize, heartbeat_ttl_ms: Option<u64>, heartbeat_ms: u64) -> Self {
        Self::start_with(groups, heartbeat_ttl_ms, heartbeat_ms, 1)
    }

    /// `voters` is 1 (one metaserver) or 3 (a Raft metadata quorum).
    pub fn start_with(
        groups: usize,
        heartbeat_ttl_ms: Option<u64>,
        heartbeat_ms: u64,
        voters: usize,
    ) -> Self {
        let root = std::env::temp_dir().join(node_regime::run_token("member-failure"));
        std::fs::create_dir(&root).unwrap();
        let reservations: Vec<_> = (0..groups * 3 + 1 + if voters == 3 { 5 } else { 0 })
            .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let addrs: Vec<String> = reservations
            .iter()
            .map(|l| l.local_addr().unwrap().to_string())
            .collect();
        let meta_addr = addrs[0].clone();
        let chunk_addrs = addrs[1..1 + groups * 3].to_vec();
        // Voter API endpoints are addrs[0] and two spare ports; the other spares are peer ports.
        let spare = &addrs[1 + groups * 3..];
        let raft_addrs: Vec<String> = if voters == 3 {
            vec![addrs[0].clone(), spare[0].clone(), spare[1].clone()]
        } else {
            Vec::new()
        };
        let peer_ports: Vec<String> = if voters == 3 {
            vec![spare[2].clone(), spare[3].clone(), spare[4].clone()]
        } else {
            Vec::new()
        };
        let catalog = ChunkGroupCatalog::new(
            (0..groups)
                .map(|g| ChunkGroupConfig {
                    placement_id: format!("group-{g}"),
                    failure_domain: format!("rack-{g}"),
                    identity: Some(ChunkGroupIdentity {
                        version: 3,
                        group_id: g as u64 + 1,
                        node_id: 1,
                        capacity_bytes: 1 << 28,
                        max_pages: 4096,
                        voters: vec![1, 2, 3],
                        peer_endpoints: Vec::new(),
                        bootstrap_voters: Vec::new(),
                        bootstrap_id: 0,
                    }),
                    api_endpoints: chunk_addrs[g * 3..g * 3 + 3].to_vec(),
                    member_ids: (0..3).map(|m| format!("star-{g}-{m}")).collect(),
                })
                .collect(),
        )
        .unwrap();
        let catalog_path = root.join("catalog.bin");
        std::fs::write(&catalog_path, catalog.to_bytes().unwrap()).unwrap();
        drop(reservations);
        let state = root.join("meta-state");
        std::fs::create_dir(&state).unwrap();
        let mut env = vec![
            (
                "MATRIXOBJECT_METASERVER_STATE_DIR",
                state.to_str().unwrap().to_owned(),
            ),
            ("MATRIXOBJECT_METASERVER_ADDR", meta_addr.clone()),
            (
                "MATRIXOBJECT_METASERVER_CHUNK_GROUPS_FILE",
                catalog_path.to_str().unwrap().to_owned(),
            ),
        ];
        if let Some(ttl) = heartbeat_ttl_ms {
            env.push(("MATRIXOBJECT_METASERVER_HEARTBEAT_TTL_MS", ttl.to_string()));
        }
        let env: Vec<(&str, &str)> = env.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let mut raft = Vec::new();
        let meta = if voters == 3 {
            let peers = peer_ports
                .iter()
                .enumerate()
                .map(|(i, a)| format!("{}@{a}", i + 1))
                .collect::<Vec<_>>()
                .join(",");
            for i in 0..3 {
                let dir = root.join(format!("raft-{i}"));
                std::fs::create_dir(&dir).unwrap();
                let id = (i + 1).to_string();
                let mut voter_env: Vec<(&str, String)> = vec![
                    ("MATRIXOBJECT_METASERVER_ADDR", raft_addrs[i].clone()),
                    ("MATRIXOBJECT_METASERVER_RAFT_PEERS", peers.clone()),
                    ("MATRIXOBJECT_METASERVER_RAFT_NODE_ID", id),
                    ("MATRIXOBJECT_METASERVER_RAFT_STATE_DIR", dir.to_str().unwrap().to_owned()),
                    ("MATRIXOBJECT_METASERVER_RAFT_TIMEOUT_MS", "1500".into()),
                    (
                        "MATRIXOBJECT_METASERVER_CHUNK_GROUPS_FILE",
                        catalog_path.to_str().unwrap().to_owned(),
                    ),
                ];
                if let Some(ttl) = heartbeat_ttl_ms {
                    voter_env.push(("MATRIXOBJECT_METASERVER_HEARTBEAT_TTL_MS", ttl.to_string()));
                }
                let voter_env: Vec<(&str, &str)> =
                    voter_env.iter().map(|(k, v)| (*k, v.as_str())).collect();
                raft.push(Some(processes::start(env!("CARGO_BIN_EXE_metaserver"), &voter_env)));
            }
            None
        } else {
            Some(processes::start(env!("CARGO_BIN_EXE_metaserver"), &env))
        };
        let meta_addr = if voters == 3 { raft_addrs.join(",") } else { meta_addr };
        let mut cluster = Self {
            raft,
            raft_addrs,
            journals: (0..groups * 3)
                .map(|i| root.join(format!("chunk-{i}.log")))
                .collect(),
            root,
            meta,
            meta_addr,
            chunks: (0..groups * 3).map(|_| None).collect(),
            chunk_addrs,
            groups,
            heartbeat_ms,
        };
        for i in 0..groups * 3 {
            cluster.start_chunk(i);
        }
        cluster.wait_for_online(Duration::from_secs(20));
        cluster
    }

    pub fn start_chunk(&mut self, i: usize) {
        let id = format!("star-{}-{}", i / 3, i % 3);
        let heartbeat = self.heartbeat_ms.to_string();
        let node = processes::start(
            env!("CARGO_BIN_EXE_chunkserver"),
            &[
                ("MATRIXOBJECT_REPLICA_ADDR", self.chunk_addrs[i].as_str()),
                ("MATRIXOBJECT_REPLICA_JOURNAL", self.journals[i].to_str().unwrap()),
                ("MATRIXOBJECT_CHUNK_NODE_ID", id.as_str()),
                ("MATRIXOBJECT_CHUNK_METADATA_ENDPOINTS", self.meta_addr.as_str()),
                ("MATRIXOBJECT_CHUNK_HEARTBEAT_MS", heartbeat.as_str()),
            ],
        );
        assert_eq!(node.endpoint, self.chunk_addrs[i]);
        self.chunks[i] = Some(node);
    }

    /// SIGKILL one chunkserver and wait until the kernel has reaped it.
    pub fn kill_chunk(&mut self, i: usize) {
        let mut node = self.chunks[i].take().expect("chunk already stopped");
        node.child.kill().unwrap();
        assert!(processes::reap_within(&mut node.child, Duration::from_secs(3)).is_some());
        assert!(!processes::is_live(node.pid));
    }

    pub fn meta_client(&self) -> RemoteMetaClient {
        if !self.raft_addrs.is_empty() {
            let live: Vec<String> = self
                .raft
                .iter()
                .zip(&self.raft_addrs)
                .filter(|(node, _)| node.is_some())
                .map(|(_, addr)| addr.clone())
                .collect();
            let until = Instant::now() + Duration::from_secs(20);
            loop {
                match RemoteMetaClient::connect_raft_catalog(
                    &live,
                    1,
                    &MetaClientOptions {
                        read_timeout: Duration::from_secs(5),
                        write_timeout: Duration::from_secs(5),
                        ..Default::default()
                    },
                ) {
                    Ok(client) => return client,
                    Err(error) => {
                        assert!(Instant::now() < until, "no metadata leader: {error}");
                        std::thread::sleep(Duration::from_millis(100));
                    }
                }
            }
        }
        RemoteMetaClient::connect(
            &self.meta_addr,
            &MetaClientOptions {
                read_timeout: Duration::from_secs(10),
                write_timeout: Duration::from_secs(10),
                ..Default::default()
            },
        )
        .unwrap()
    }

    /// SIGKILL the metadata voter that currently answers a read barrier; returns its index.
    pub fn kill_meta_leader(&mut self) -> usize {
        let until = Instant::now() + Duration::from_secs(20);
        loop {
            for i in 0..self.raft.len() {
                if self.raft[i].is_none() {
                    continue;
                }
                let answered = RemoteMetaClient::connect(
                    &self.raft_addrs[i],
                    &MetaClientOptions {
                        read_timeout: Duration::from_secs(2),
                        write_timeout: Duration::from_secs(2),
                        ..Default::default()
                    },
                )
                .ok()
                .is_some_and(|mut c| c.probe().into_result().is_ok());
                if answered {
                    let mut node = self.raft[i].take().unwrap();
                    node.child.kill().unwrap();
                    assert!(processes::reap_within(&mut node.child, Duration::from_secs(3)).is_some());
                    return i;
                }
            }
            assert!(Instant::now() < until, "no metadata leader to kill");
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    /// Chunk indexes (0-based) of the group an extent names.
    pub fn members_of(&self, group: &str) -> std::ops::Range<usize> {
        let g: usize = group.strip_prefix("group-").unwrap().parse().unwrap();
        g * 3..g * 3 + 3
    }

    pub fn wait_for_online(&self, within: Duration) {
        let until = Instant::now() + within;
        loop {
            let Ok(nodes) = self.meta_client().list_placement_nodes() else {
                assert!(Instant::now() < until, "metadata never admitted a roster read");
                std::thread::sleep(Duration::from_millis(100));
                continue;
            };
            let online = |prefix: &str| {
                nodes
                    .iter()
                    .filter(|n| n.node_id.starts_with(prefix) && n.online)
                    .count()
            };
            if online("group-") == self.groups && online("star-") == self.groups * 3 {
                return;
            }
            assert!(Instant::now() < until, "roster never came online: {nodes:?}");
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        self.chunks.clear();
        self.meta = None;
        self.raft.clear();
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

pub fn write_options(deadline: Duration) -> ObjectWriteOptions {
    ObjectWriteOptions {
        page_bytes: PAGE_BYTES,
        target_groups: 1,
        replica: ReplicaClientOptions {
            connect_timeout: Duration::from_millis(500),
            read_timeout: Duration::from_secs(2),
            write_timeout: Duration::from_secs(2),
            deadline: Some(Instant::now() + deadline),
            ..Default::default()
        },
        ..Default::default()
    }
}

pub fn read_options() -> ReplicaClientOptions {
    ReplicaClientOptions {
        connect_timeout: Duration::from_millis(500),
        read_timeout: Duration::from_secs(2),
        write_timeout: Duration::from_secs(2),
        ..Default::default()
    }
}

/// The payload of append number `index`: deterministic, distinct per index.
pub fn payload(index: usize, bytes: usize) -> Vec<u8> {
    let mut state = 0x9e3779b97f4a7c15u64 ^ (index as u64).wrapping_mul(0x100000001b3);
    (0..bytes)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}

pub fn percentile(sorted: &[Duration], p: usize) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    sorted[((sorted.len() * p).div_ceil(100)).saturating_sub(1)]
}

pub fn describe(error: &ObjectWriteFailure) -> String {
    match error {
        ObjectWriteFailure::BeforePublication { error, staged } => format!(
            "BeforePublication({error}; staged_extents={})",
            staged.as_ref().map_or(0, |m| m.extents.len())
        ),
        ObjectWriteFailure::PublicationUnconfirmed { error, .. } => {
            format!("PublicationUnconfirmed({error})")
        }
    }
}

/// Item A and E: append in a loop, stop one member of the group holding the object's newest
/// extent, keep appending, and print what the client saw. `MO_APPEND_MODE=plain` (default)
/// uses the SDK as it is; any other value is a name for a later mode. Prints `RESULT` lines.
#[test]
#[ignore = "measurement: prints what the client sees when a data node dies"]
fn measure_append_through_member_kill() {
    let mode = std::env::var("MO_APPEND_MODE").unwrap_or_else(|_| "plain".into());
    let ttl: u64 = std::env::var("MO_TTL_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(3000);
    let appends: usize = std::env::var("MO_APPENDS").ok().and_then(|v| v.parse().ok()).unwrap_or(40);
    let kill_at: usize = std::env::var("MO_KILL_AT").ok().and_then(|v| v.parse().ok()).unwrap_or(15);
    let give_up = Duration::from_secs(
        std::env::var("MO_GIVE_UP_S").ok().and_then(|v| v.parse().ok()).unwrap_or(20),
    );
    let groups: usize = std::env::var("MO_GROUPS").ok().and_then(|v| v.parse().ok()).unwrap_or(2);
    let mut cluster = Cluster::start(groups, Some(ttl), 200);
    let mut client = cluster.meta_client();
    let mut oracle = payload(10_000, 3 * PAGE_BYTES + 17);
    let mut manifest: ObjectManifest = client
        .write_object("bucket", "log", None, &oracle, &write_options(Duration::from_secs(10)))
        .unwrap();
    let mut samples = Vec::new();
    let mut first_failure: Option<Instant> = None;
    let mut recovery = None;
    let mut killed = None;
    let mut index = 0;
    let failover_wait_ms: u64 =
        std::env::var("MO_WAIT_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(50);
    let mut switches_total = 0u32;
    let mut abandoned_pages = 0usize;
    while index < appends {
        if index == kill_at && killed.is_none() {
            let group = manifest.extents.last().unwrap().group.clone();
            let victim = cluster.members_of(&group).start + 1;
            cluster.kill_chunk(victim);
            killed = Some(victim);
            println!("RESULT kill group={group} chunk={victim} after_acked={index}");
        }
        let data = payload(index, 2 * PAGE_BYTES + 100 + index);
        let started = Instant::now();
        let result = if mode == "failover" {
            let policy = AppendFailoverPolicy {
                initial_wait: Duration::from_millis(failover_wait_ms),
                ..Default::default()
            };
            client
                .append_object_with_failover(
                    manifest.clone(),
                    &data,
                    &write_options(Duration::from_secs(30)),
                    &policy,
                )
                .map(|done| {
                    switches_total += done.switches;
                    abandoned_pages += done
                        .abandoned
                        .iter()
                        .map(|attempt| attempt.extents.len())
                        .sum::<usize>();
                    done.manifest
                })
                .map_err(|failed| failed.failure)
        } else {
            client.append_object(
                manifest.clone(),
                &data,
                &write_options(Duration::from_secs(10)),
            )
        };
        let elapsed = started.elapsed();
        match result {
            Ok(next) => {
                println!(
                    "RESULT append index={index} ms={} ok generation={} group={}",
                    elapsed.as_millis(), next.generation, next.extents.last().unwrap().group
                );
                if first_failure.take().is_some() {
                    recovery = Some(elapsed);
                }
                samples.push(elapsed);
                oracle.extend_from_slice(&data);
                manifest = next;
                index += 1;
            }
            Err(error) => {
                println!("RESULT append index={index} ms={} FAILED {}", elapsed.as_millis(), describe(&error));
                let since = *first_failure.get_or_insert(started);
                let current = client.read_manifest("bucket", "log").unwrap();
                println!(
                    "RESULT manifest_after_failure generation={} extents={} unchanged={}",
                    current.generation, current.extents.len(), current == manifest
                );
                if since.elapsed() > give_up {
                    println!("RESULT gave_up_after_ms={}", since.elapsed().as_millis());
                    break;
                }
                std::thread::sleep(Duration::from_millis(200));
            }
        }
    }
    samples.sort();
    println!(
        "RESULT mode={mode} ttl_ms={ttl} acked={index} p50_us={} p99_us={} max_us={} recovery_ms={}",
        percentile(&samples, 50).as_micros(),
        percentile(&samples, 99).as_micros(),
        samples.last().map_or(0, |d| d.as_micros()),
        recovery.map_or(-1, |d| d.as_millis() as i128)
    );
    let (final_manifest, bytes) = client
        .read_object("bucket", "log", oracle.len() + 1, &read_options())
        .unwrap();
    println!("RESULT switches={switches_total} abandoned_pages={abandoned_pages}");
    println!(
        "RESULT intact={} acked_bytes={} read_bytes={} generation={}",
        bytes == oracle, oracle.len(), bytes.len(), final_manifest.generation
    );
    assert_eq!(bytes, oracle, "acknowledged data differs from what is stored");
}

// ---------------------------------------------------------------------------------------
// Acceptance tests
// ---------------------------------------------------------------------------------------

use matrixobjectstore_rs::cluster::RemoteReplicaClient;
use matrixobjectstore_rs::object_manifest::ObjectExtent;
use matrixobjectstore_rs::{ChunkId, PageId};

fn page_of(extent: &ObjectExtent) -> PageId {
    PageId {
        chunk_id: ChunkId::new(extent.volume_id, extent.chunk_id).unwrap(),
        page_index: extent.page_index,
    }
}

fn fast_policy(max_switches: u32) -> AppendFailoverPolicy {
    AppendFailoverPolicy {
        initial_wait: Duration::from_millis(20),
        max_wait: Duration::from_millis(200),
        max_switches,
        report_unreachable: true,
        abandoned_log: None,
    }
}

fn is_online(client: &mut RemoteMetaClient, id: &str) -> bool {
    client
        .list_placement_nodes()
        .unwrap()
        .iter()
        .any(|node| node.node_id == id && node.online)
}

fn wait_until(what: &str, within: Duration, mut condition: impl FnMut() -> bool) -> Duration {
    let started = Instant::now();
    while !condition() {
        assert!(started.elapsed() < within, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(50));
    }
    started.elapsed()
}

/// True when the member at `chunk` holds `bytes` at the extent's page.
fn member_holds(cluster: &Cluster, chunk: usize, extent: &ObjectExtent, bytes: &[u8]) -> bool {
    let Ok(mut replica) = RemoteReplicaClient::connect(&cluster.chunk_addrs[chunk], &read_options())
    else {
        return false;
    };
    replica
        .read_chunk(&page_of(extent), extent.page_offset, extent.length)
        .is_ok_and(|read| read.data == bytes)
}

fn extent_bytes<'a>(oracle: &'a [u8], extent: &ObjectExtent) -> &'a [u8] {
    let start = extent.object_offset as usize;
    &oracle[start..start + extent.length as usize]
}

/// C: kill one member mid-stream. Every append is acknowledged exactly once, the object reads
/// back as exactly the acknowledged bytes, and what was written after the kill sits in a group
/// whose three members all hold it.
#[test]
fn appends_continue_without_loss_or_duplicates_when_a_member_is_killed() {
    let mut cluster = Cluster::start(2, Some(3000), 200);
    let mut client = cluster.meta_client();
    let mut oracle = payload(10_000, 3 * PAGE_BYTES + 17);
    let mut manifest = client
        .write_object("bucket", "log", None, &oracle, &write_options(Duration::from_secs(10)))
        .unwrap();
    let policy = fast_policy(6);
    let mut dead_group = None;
    let mut switches = 0;
    let mut after_kill: Vec<ObjectExtent> = Vec::new();
    for index in 0..30 {
        if index == 10 {
            let group = manifest.extents.last().unwrap().group.clone();
            cluster.kill_chunk(cluster.members_of(&group).start + 1);
            dead_group = Some(group);
        }
        let data = payload(index, 2 * PAGE_BYTES + 100 + index);
        let done = client
            .append_object_with_failover(
                manifest.clone(),
                &data,
                &write_options(Duration::from_secs(30)),
                &policy,
            )
            .unwrap_or_else(|failed| panic!("append {index} was not acknowledged: {failed}"));
        assert_eq!(done.manifest.generation, manifest.generation + 1, "append {index}");
        assert_eq!(
            done.manifest.length,
            manifest.length + data.len() as u64,
            "duplicate or lost bytes at append {index}"
        );
        assert_eq!(&done.manifest.extents[..manifest.extents.len()], &manifest.extents[..]);
        switches += done.switches;
        if let Some(group) = &dead_group {
            for extent in &done.manifest.extents[manifest.extents.len()..] {
                assert_ne!(&extent.group, group, "acknowledged into the group with a dead member");
                after_kill.push(extent.clone());
            }
        }
        oracle.extend_from_slice(&data);
        manifest = done.manifest;
    }
    assert!(switches >= 1, "the kill never forced a switch, so nothing was tested");
    let (read_manifest, bytes) = client
        .read_object("bucket", "log", oracle.len() + 1, &read_options())
        .unwrap();
    assert_eq!(read_manifest, manifest);
    assert_eq!(bytes, oracle, "the object differs from what was acknowledged");
    assert!(!after_kill.is_empty());
    for extent in &after_kill {
        for member in cluster.members_of(&extent.group) {
            assert!(
                member_holds(&cluster, member, extent, extent_bytes(&oracle, extent)),
                "member {member} of {} does not hold an acknowledged page",
                extent.group
            );
        }
    }
    println!("RESULT switches={switches} after_kill_pages={}", after_kill.len());
}

/// With no other group to move to, the append is refused before publication, says so, leaves the
/// manifest untouched, and works again once the member returns.
#[test]
fn with_no_healthy_group_the_append_is_refused_cleanly_and_heals_on_return() {
    let mut cluster = Cluster::start(1, Some(3000), 200);
    let mut client = cluster.meta_client();
    let mut oracle = payload(10_000, 2 * PAGE_BYTES);
    let manifest = client
        .write_object("bucket", "log", None, &oracle, &write_options(Duration::from_secs(10)))
        .unwrap();
    cluster.kill_chunk(1);
    let data = payload(1, PAGE_BYTES + 5);
    let failed = client
        .append_object_with_failover(
            manifest.clone(),
            &data,
            &write_options(Duration::from_secs(30)),
            &fast_policy(2),
        )
        .unwrap_err();
    assert!(matches!(failed.failure, ObjectWriteFailure::BeforePublication { .. }), "{failed}");
    assert_eq!(failed.switches, 2);
    assert_eq!(failed.abandoned.len(), 3, "one record per refused attempt");
    // Later attempts fail at planning once the group is offline, so only the first one probes.
    assert!(!failed.abandoned[0].unreachable_members.is_empty());
    assert_eq!(
        client.read_manifest("bucket", "log").unwrap(),
        manifest,
        "a refused append changed the manifest"
    );
    assert_eq!(
        client.read_object("bucket", "log", oracle.len() + 1, &read_options()).unwrap().1,
        oracle
    );
    cluster.start_chunk(1);
    let started = Instant::now();
    let done = loop {
        match client.append_object_with_failover(
            manifest.clone(),
            &data,
            &write_options(Duration::from_secs(30)),
            &fast_policy(2),
        ) {
            Ok(done) => break done,
            Err(error) => {
                assert!(started.elapsed() < Duration::from_secs(30), "never healed: {error}");
                std::thread::sleep(Duration::from_millis(200));
            }
        }
    };
    println!("RESULT heal_after_return_ms={}", started.elapsed().as_millis());
    oracle.extend_from_slice(&data);
    assert_eq!(
        client.read_object("bucket", "log", oracle.len() + 1, &read_options()).unwrap(),
        (done.manifest, oracle)
    );
}

/// B: a stopped node leaves the placement roster within the TTL and returns on its next
/// report, and placement stops choosing a group with a stopped member while it is gone.
#[test]
fn a_stopped_node_leaves_the_roster_within_the_ttl_and_returns_on_its_next_report() {
    let ttl = 1000;
    let mut cluster = Cluster::start(2, Some(ttl), 200);
    let mut client = cluster.meta_client();
    cluster.kill_chunk(1); // group-0, member 1
    let left = wait_until("the stopped node to expire", Duration::from_secs(10), || {
        !is_online(&mut client, "star-0-1")
    });
    println!("RESULT roster_leave_ms={} ttl_ms={ttl}", left.as_millis());
    assert!(left < Duration::from_millis(ttl + 3000), "expiry took {left:?}");
    assert!(
        !is_online(&mut client, "group-0"),
        "a Star group with a stopped member stayed available"
    );
    assert!(is_online(&mut client, "group-1"));
    for index in 0..24 {
        let plan = client
            .plan_object_placement("bucket", &format!("key-{index}"), 4096, 1)
            .unwrap();
        assert_eq!(plan.node_ids, ["group-1"], "placement chose the group with a stopped member");
    }
    cluster.start_chunk(1);
    let back = wait_until("the node to return", Duration::from_secs(10), || {
        is_online(&mut client, "star-0-1") && is_online(&mut client, "group-0")
    });
    println!("RESULT roster_return_ms={}", back.as_millis());
}

/// The metaserver's own default expires silent nodes, and `0` turns that off.
#[test]
fn the_metaserver_expires_silent_nodes_by_default_and_has_an_off_switch() {
    use std::io::{BufRead, BufReader};
    let line = |ttl: Option<&str>| {
        let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_metaserver"));
        command.env("MATRIXOBJECT_METASERVER_ADDR", "127.0.0.1:0");
        command.env_remove("MATRIXOBJECT_METASERVER_HEARTBEAT_TTL_MS");
        if let Some(ttl) = ttl {
            command.env("MATRIXOBJECT_METASERVER_HEARTBEAT_TTL_MS", ttl);
        }
        let mut child = command.stdout(std::process::Stdio::piped()).spawn().unwrap();
        let mut line = String::new();
        BufReader::new(child.stdout.take().unwrap()).read_line(&mut line).unwrap();
        let _ = child.kill();
        let _ = child.wait();
        line
    };
    assert!(line(None).contains("heartbeat_ttl_ms=15000"), "default is not 15000 ms");
    assert!(line(Some("0")).contains("heartbeat_ttl_ms=0"), "0 does not turn expiry off");
    assert!(line(Some("2500")).contains("heartbeat_ttl_ms=2500"));
}

struct RepairWorker(std::process::Child);
impl Drop for RepairWorker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// D: a member that returns with nothing (its disk was replaced) never causes a wrong byte to be
/// served, and the repair worker brings it back to holding every page.
#[test]
fn a_returning_member_with_no_data_never_serves_wrong_bytes_and_is_resynced() {
    let mut cluster = Cluster::start(1, Some(3000), 200);
    let mut client = cluster.meta_client();
    let mut oracle = payload(10_000, 3 * PAGE_BYTES + 17);
    let mut manifest = client
        .write_object("bucket", "log", None, &oracle, &write_options(Duration::from_secs(10)))
        .unwrap();
    for index in 0..12 {
        let data = payload(index, 2 * PAGE_BYTES + 100 + index);
        manifest = client
            .append_object(manifest.clone(), &data, &write_options(Duration::from_secs(10)))
            .unwrap();
        oracle.extend_from_slice(&data);
    }
    // Member 1 loses its disk and comes back empty.
    cluster.kill_chunk(1);
    // A replacement disk: the journal and its marker are gone. A journal that is merely
    // missing or emptied while its marker remains is refused at startup (fail closed).
    std::fs::remove_file(&cluster.journals[1]).unwrap();
    let mut marker = cluster.journals[1].clone().into_os_string();
    marker.push(".lock");
    std::fs::remove_file(PathBuf::from(marker)).unwrap();
    cluster.start_chunk(1);
    wait_until("the empty member to report online", Duration::from_secs(10), || {
        is_online(&mut client, "star-0-1") && is_online(&mut client, "group-0")
    });
    let first = manifest.extents[0].clone();
    assert!(
        !member_holds(&cluster, 1, &first, extent_bytes(&oracle, &first)),
        "the returned member was expected to be empty"
    );
    let read_all = |label: &str| -> (u32, u32) {
        let (mut ok, mut failed) = (0, 0);
        for _ in 0..60 {
            // A new connection each time so no read inherits a preferred member.
            let mut fresh = cluster.meta_client();
            match fresh.read_object("bucket", "log", oracle.len() + 1, &read_options()) {
                Ok((_, bytes)) => {
                    assert_eq!(bytes, oracle, "{label}: a read served wrong bytes");
                    ok += 1;
                }
                Err(_) => failed += 1,
            }
        }
        (ok, failed)
    };
    let (ok, failed) = read_all("before resync");
    println!("RESULT reads_before_resync ok={ok} failed={failed}");
    assert_eq!(failed, 0, "a member with no data made acknowledged reads fail");

    let worker = RepairWorker(
        std::process::Command::new(env!("CARGO_BIN_EXE_star_repaird"))
            .env("MATRIXOBJECT_REPAIR_METADATA_ENDPOINTS", &cluster.meta_addr)
            .env("MATRIXOBJECT_REPAIR_GROUP", "group-0")
            .env("MATRIXOBJECT_REPAIR_BATCH_PAGES", "16")
            .env("MATRIXOBJECT_REPAIR_INTERVAL_MS", "100")
            .env("MATRIXOBJECT_REPAIR_SWEEP_INTERVAL_MS", "1000")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap(),
    );
    let healed = wait_until("the returned member to hold every page", Duration::from_secs(60), || {
        manifest
            .extents
            .iter()
            .all(|extent| member_holds(&cluster, 1, extent, extent_bytes(&oracle, extent)))
    });
    println!("RESULT resync_ms={}", healed.as_millis());
    let (ok, failed) = read_all("after resync");
    assert_eq!(failed, 0);
    println!("RESULT reads_after_resync ok={ok}");
    drop(worker);
    for extent in &manifest.extents {
        for member in cluster.members_of(&extent.group) {
            assert!(member_holds(&cluster, member, extent, extent_bytes(&oracle, extent)));
        }
    }
}

/// A writer holding a stale manifest is told so at once; it is a conflict, not a node failure,
/// and no switch is attempted.
#[test]
fn a_conflicting_append_is_returned_to_the_caller_without_a_switch() {
    let cluster = Cluster::start(1, Some(3000), 200);
    let mut first = cluster.meta_client();
    let mut second = cluster.meta_client();
    let base = first
        .write_object("bucket", "log", None, &payload(1, PAGE_BYTES), &write_options(Duration::from_secs(10)))
        .unwrap();
    let winner = first
        .append_object(base.clone(), &payload(2, 100), &write_options(Duration::from_secs(10)))
        .unwrap();
    let failed = second
        .append_object_with_failover(
            base,
            &payload(3, 100),
            &write_options(Duration::from_secs(10)),
            &fast_policy(4),
        )
        .unwrap_err();
    assert_eq!(failed.switches, 0, "a conflict was retried as if a node had failed");
    assert!(matches!(failed.failure, ObjectWriteFailure::BeforePublication { .. }));
    assert_eq!(first.read_manifest("bucket", "log").unwrap(), winner);
}

/// The same kill under a three-voter metadata quorum, and the metadata leader is killed too,
/// in the middle of the stream. Every append is acknowledged exactly once.
#[cfg(feature = "metadata-consensus")]
#[test]
fn appends_survive_a_dead_data_node_and_a_dead_metadata_leader_under_a_metadata_quorum() {
    let mut cluster = Cluster::start_with(2, Some(3000), 200, 3);
    let mut client = cluster.meta_client();
    let mut oracle = payload(10_000, 3 * PAGE_BYTES + 17);
    let mut manifest = client
        .write_object("bucket", "log", None, &oracle, &write_options(Duration::from_secs(20)))
        .unwrap();
    let policy = fast_policy(6);
    let (mut switches, mut retries) = (0, 0u32);
    for index in 0..40 {
        if index == 10 {
            let group = manifest.extents.last().unwrap().group.clone();
            cluster.kill_chunk(cluster.members_of(&group).start + 1);
        }
        if index == 20 {
            let leader = cluster.kill_meta_leader();
            println!("RESULT killed_metadata_leader={leader} at_append={index}");
        }
        let data = payload(index, 2 * PAGE_BYTES + 100 + index);
        // A caller that gets a failure re-reads the manifest: the append may have landed.
        let started = Instant::now();
        loop {
            match client.append_object_with_failover(
                manifest.clone(),
                &data,
                &write_options(Duration::from_secs(40)),
                &policy,
            ) {
                Ok(done) => {
                    switches += done.switches;
                    assert_eq!(done.manifest.length, manifest.length + data.len() as u64, "append {index}");
                    oracle.extend_from_slice(&data);
                    manifest = done.manifest;
                    break;
                }
                Err(failed) => {
                    assert!(started.elapsed() < Duration::from_secs(60), "append {index}: {failed}");
                    retries += 1;
                    let current = loop {
                        match client.read_manifest("bucket", "log") {
                            Ok(current) => break current,
                            Err(_) if started.elapsed() < Duration::from_secs(60) => {
                                std::thread::sleep(Duration::from_millis(200))
                            }
                            Err(error) => panic!("manifest unreadable at append {index}: {error}"),
                        }
                    };
                    if current.length == manifest.length + data.len() as u64 {
                        let (_, bytes) = client
                            .read_object("bucket", "log", current.length as usize + 1, &read_options())
                            .unwrap();
                        assert_eq!(&bytes[manifest.length as usize..], &data[..], "landed bytes differ");
                        manifest = current;
                        oracle.extend_from_slice(&data);
                        break;
                    }
                    assert_eq!(current, manifest, "manifest moved unexpectedly at append {index}");
                    std::thread::sleep(Duration::from_millis(200));
                }
            }
        }
    }
    println!("RESULT quorum switches={switches} caller_retries={retries}");
    let (final_manifest, bytes) = client
        .read_object("bucket", "log", oracle.len() + 1, &read_options())
        .unwrap();
    assert_eq!(final_manifest.length, oracle.len() as u64);
    assert_eq!(bytes, oracle, "the object differs from what was acknowledged");
}

/// Repair order inside a batch: a page short of copies is repaired before a complete one, and
/// a page that no source quorum can vouch for goes last, so it cannot stall the others. Page
/// ids run opposite to that order, so a repair in id order fails this test.
#[test]
fn repair_visits_short_pages_before_complete_ones_and_unvouched_pages_last() {
    use matrixobjectstore_rs::cluster::RemoteReplicaSet;
    let token = node_regime::run_token("repair-order");
    let nodes: Vec<_> = (0..3)
        .map(|_| processes::start_replica(env!("CARGO_BIN_EXE_chunkserver"), &token))
        .collect();
    let endpoints: Vec<String> = nodes.iter().map(|n| n.endpoint.clone()).collect();
    let page = |index: u64| PageId {
        chunk_id: ChunkId::new(1, 500).unwrap(),
        page_index: index,
    };
    // Page 0 on all three members, page 1 on two, page 2 on one.
    for (index, holders) in [(0u64, 3usize), (1, 2), (2, 1)] {
        for endpoint in &endpoints[..holders] {
            RemoteReplicaClient::connect(endpoint, &read_options())
                .unwrap()
                .write_chunk(page(index), 0, &[index as u8 + 1; 64])
                .unwrap();
        }
    }
    let mut set = RemoteReplicaSet::connect(&endpoints, 2, 2, &read_options()).unwrap();
    let batch = set.repair_inventory_batch(None, 64).unwrap();
    let order: Vec<u64> = batch.pages.iter().map(|entry| entry.page.page_index).collect();
    assert_eq!(order, [1, 0, 2], "repair did not visit pages by remaining copies");
    assert!(batch.blocked, "a page with one copy has no source quorum");
    assert_eq!(batch.resume_after, None, "a reordered, unfinished batch must not move the cursor");
    // The short page was completed on the member that lacked it.
    let mut third = RemoteReplicaClient::connect(&endpoints[2], &read_options()).unwrap();
    assert_eq!(third.read_chunk(&page(1), 0, 64).unwrap().data, vec![2u8; 64]);
}

/// The pages of refused attempts are recorded durably before the client switches, can be read
/// back after the client is gone, belong to no manifest, and a torn tail is tolerated while a
/// damaged complete record is reported.
#[test]
fn refused_attempts_leave_a_durable_record_of_their_pages() {
    use matrixobjectstore_rs::cluster::read_abandoned_log;
    let mut cluster = Cluster::start(1, Some(3000), 200);
    let mut client = cluster.meta_client();
    let oracle = payload(10_000, 2 * PAGE_BYTES);
    let manifest = client
        .write_object("bucket", "log", None, &oracle, &write_options(Duration::from_secs(10)))
        .unwrap();
    cluster.kill_chunk(1);
    let log = cluster.root.join("abandoned.log");
    let policy = AppendFailoverPolicy { abandoned_log: Some(log.clone()), ..fast_policy(2) };
    let failed = client
        .append_object_with_failover(
            manifest.clone(),
            &payload(1, PAGE_BYTES + 5),
            &write_options(Duration::from_secs(30)),
            &policy,
        )
        .unwrap_err();
    drop(client);
    let records = read_abandoned_log(&log).unwrap();
    assert_eq!(records.len(), failed.abandoned.len());
    assert!(records.iter().all(|r| r.bucket == "bucket" && r.key == "log" && r.expected_generation == manifest.generation));
    let recorded: usize = records.iter().map(|r| r.extents.len()).sum();
    assert!(recorded > 0, "no pages were recorded");
    let live: Vec<_> = manifest.extents.iter().map(|e| (e.chunk_id, e.page_index)).collect();
    for extent in records.iter().flat_map(|r| &r.extents) {
        assert!(!live.contains(&(extent.chunk_id, extent.page_index)), "a live page was recorded as abandoned");
    }
    // A torn final frame is ignored; a damaged complete frame is an error, not a skip.
    let mut bytes = std::fs::read(&log).unwrap();
    let torn: Vec<u8> = bytes[..bytes.len() - 3].to_vec();
    std::fs::write(&log, &torn).unwrap();
    assert_eq!(read_abandoned_log(&log).unwrap().len(), records.len() - 1);
    bytes[10] ^= 0xff;
    std::fs::write(&log, &bytes).unwrap();
    assert!(read_abandoned_log(&log).is_err());
}

// ---------------------------------------------------------------------------------------
// Returning-node audit: every path that can acknowledge with fewer than all members
// ---------------------------------------------------------------------------------------

/// The one configurable path with a write quorum below the member count: a remote placement
/// set built with `write_quorum = read_quorum = 2` of 3. A member that was stopped while writes
/// were acknowledged by the other two returns stale. Reads vote over a read quorum that
/// overlaps every write quorum, so the stale bytes are outvoted and never served, and repair
/// then completes the member.
#[test]
fn a_returning_member_of_a_lower_quorum_set_is_outvoted_and_then_repaired() {
    use matrixobjectstore_rs::cluster::RemoteReplicaSet;
    let root = std::env::temp_dir().join(node_regime::run_token("lower-quorum"));
    std::fs::create_dir(&root).unwrap();
    let journal = |i: usize| root.join(format!("{i}.log"));
    let start = |i: usize, addr: &str| {
        processes::start(
            env!("CARGO_BIN_EXE_chunkserver"),
            &[
                ("MATRIXOBJECT_REPLICA_ADDR", addr),
                ("MATRIXOBJECT_REPLICA_JOURNAL", journal(i).to_str().unwrap()),
            ],
        )
    };
    let mut nodes: Vec<Option<NodeProcess>> =
        (0..3).map(|i| Some(start(i, "127.0.0.1:0"))).collect();
    let endpoints: Vec<String> = nodes.iter().map(|n| n.as_ref().unwrap().endpoint.clone()).collect();
    let page = |index: u64| PageId {
        chunk_id: ChunkId::new(1, 700).unwrap(),
        page_index: index,
    };
    let bytes = |index: u64| payload(index as usize, 128);
    let mut set = RemoteReplicaSet::connect(&endpoints, 2, 2, &read_options()).unwrap();
    for index in 0..8 {
        set.write_chunk(&page(index), 0, &bytes(index)).unwrap();
    }
    let mut dead = nodes[2].take().unwrap();
    dead.child.kill().unwrap();
    assert!(processes::reap_within(&mut dead.child, Duration::from_secs(3)).is_some());
    for index in 8..16 {
        set.write_chunk(&page(index), 0, &bytes(index)).unwrap(); // acknowledged by two of three
    }
    nodes[2] = Some(start(2, &endpoints[2]));
    // The returned member is stale: it lacks pages the other two acknowledged.
    let mut direct = RemoteReplicaClient::connect(&endpoints[2], &read_options()).unwrap();
    assert!(direct.read_chunk(&page(8), 0, 128).is_err(), "the returned member should be stale");
    // Reads through the set never return anything but the acknowledged bytes.
    for round in 0..20 {
        let mut fresh = RemoteReplicaSet::connect(&endpoints, 2, 2, &read_options()).unwrap();
        for index in 0..16 {
            let read = fresh
                .read_chunk(&page(index), 0, 128)
                .unwrap_or_else(|e| panic!("round {round} page {index}: {e}"));
            assert_eq!(read.data, bytes(index), "round {round}: stale or wrong bytes for page {index}");
        }
    }
    // Repair completes the member.
    let mut repair = RemoteReplicaSet::connect(&endpoints, 2, 2, &read_options()).unwrap();
    let mut after = None;
    loop {
        let batch = repair.repair_inventory_batch(after.as_ref(), 16).unwrap();
        assert!(!batch.blocked);
        if !batch.has_more {
            break;
        }
        after = batch.resume_after;
    }
    for index in 0..16 {
        assert_eq!(direct.read_chunk(&page(index), 0, 128).unwrap().data, bytes(index));
    }
    drop(nodes);
    let _ = std::fs::remove_dir_all(&root);
}

impl Cluster {
    /// Restart a stopped chunkserver in maintenance mode: it refuses ordinary mutations and
    /// reports itself offline.
    pub fn start_chunk_in_maintenance(&mut self, i: usize) {
        let id = format!("star-{}-{}", i / 3, i % 3);
        let heartbeat = self.heartbeat_ms.to_string();
        let node = processes::start(
            env!("CARGO_BIN_EXE_chunkserver"),
            &[
                ("MATRIXOBJECT_REPLICA_ADDR", self.chunk_addrs[i].as_str()),
                ("MATRIXOBJECT_REPLICA_JOURNAL", self.journals[i].to_str().unwrap()),
                ("MATRIXOBJECT_REPLICA_MAINTENANCE", "true"),
                ("MATRIXOBJECT_CHUNK_NODE_ID", id.as_str()),
                ("MATRIXOBJECT_CHUNK_METADATA_ENDPOINTS", self.meta_addr.as_str()),
                ("MATRIXOBJECT_CHUNK_HEARTBEAT_MS", heartbeat.as_str()),
            ],
        );
        self.chunks[i] = Some(node);
    }
}

/// A member in maintenance mode stays in its group, so a write is not acknowledged with fewer
/// than all members; it is refused and the manifest is untouched. When the member leaves
/// maintenance the next append succeeds and everything reads back.
#[test]
fn a_member_in_maintenance_never_lets_a_group_acknowledge_without_it() {
    let mut cluster = Cluster::start(1, Some(3000), 200);
    let mut client = cluster.meta_client();
    let mut oracle = payload(10_000, 2 * PAGE_BYTES);
    let manifest = client
        .write_object("bucket", "log", None, &oracle, &write_options(Duration::from_secs(10)))
        .unwrap();
    cluster.kill_chunk(1);
    cluster.start_chunk_in_maintenance(1);
    let data = payload(1, PAGE_BYTES + 9);
    for _ in 0..3 {
        assert!(
            client
                .append_object(manifest.clone(), &data, &write_options(Duration::from_secs(5)))
                .is_err(),
            "a write was acknowledged while a member refused mutations"
        );
        assert_eq!(client.read_manifest("bucket", "log").unwrap(), manifest);
    }
    cluster.kill_chunk(1);
    cluster.start_chunk(1);
    let started = Instant::now();
    let done = loop {
        match client.append_object_with_failover(
            manifest.clone(),
            &data,
            &write_options(Duration::from_secs(30)),
            &fast_policy(2),
        ) {
            Ok(done) => break done,
            Err(error) => {
                assert!(started.elapsed() < Duration::from_secs(30), "never healed: {error}");
                std::thread::sleep(Duration::from_millis(200));
            }
        }
    };
    oracle.extend_from_slice(&data);
    assert_eq!(
        client.read_object("bucket", "log", oracle.len() + 1, &read_options()).unwrap(),
        (done.manifest, oracle)
    );
}
