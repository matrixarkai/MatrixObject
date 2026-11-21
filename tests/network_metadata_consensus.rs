// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#![cfg(all(target_os = "linux", feature = "metadata-consensus"))]
mod node_regime;
use matrixobjectstore_rs::cluster::{
    CommittedPlacement, MetaClientOptions, MetaNodeOptions, MetaNodeServer, MetaRaftOptions,
    RemoteMetaClient,
};
use node_regime::processes::{self, NodeProcess};
use std::{
    collections::BTreeMap,
    io::Write,
    net::{TcpListener, TcpStream},
    path::PathBuf,
    time::{Duration, Instant},
};
thread_local! {
    /// The zero-fill chunk the next `Group` created on this thread gives its voters.
    static ZERO_FILL: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
    /// Whether the next `Group` created on this thread answers unchanged chunk reports without a log entry.
    static SUPPRESS_REPORTS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    // MATRIXOBJECT_METASERVER_MAX_OBJECTS for the voters of the next Group built on this thread.
    static OBJECT_CAP: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
    /// Whether the next `Group` created on this thread lets its leader decide expiry from what it has heard.
    static LEADER_LIVENESS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    // MATRIXOBJECT_METASERVER_FORGET_PREFIXES for the voters of the next Group built on this thread.
    static FORGET_PREFIXES: std::cell::Cell<Option<&'static str>> = const { std::cell::Cell::new(None) };
}
struct Group {
    zero_fill: Option<u64>,
    object_cap: Option<usize>,
    forget_prefixes: Option<&'static str>,
    nodes: Vec<Option<NodeProcess>>,
    peers: BTreeMap<u64, std::net::SocketAddr>,
    root: PathBuf,
    api_addresses: Vec<std::net::SocketAddr>,
    reservations: Vec<Option<(TcpListener, TcpListener)>>,
    roster: Option<String>,
    catalog: Option<PathBuf>,
    heartbeat_ttl: Option<String>,
    checkpoint_bytes: u64,
    membership: bool,
    request_timeout_ms: u64,
}
impl Group {
    fn new() -> Self {
        Self::with_roster(None)
    }
    fn with_roster(roster: Option<String>) -> Self {
        Self::with_health(roster, None)
    }
    fn with_health(roster: Option<String>, heartbeat_ttl: Option<String>) -> Self {
        Self::with_catalog(roster, heartbeat_ttl, None)
    }
    fn with_catalog(
        roster: Option<String>,
        heartbeat_ttl: Option<String>,
        catalog: Option<Vec<u8>>,
    ) -> Self {
        Self::with_catalog_checkpoints(roster, heartbeat_ttl, catalog, 0)
    }
    fn with_catalog_checkpoints(
        roster: Option<String>,
        heartbeat_ttl: Option<String>,
        catalog: Option<Vec<u8>>,
        checkpoint_bytes: u64,
    ) -> Self {
        Self::with_membership_options(roster, heartbeat_ttl, catalog, checkpoint_bytes, false)
    }
    fn with_membership_options(
        roster: Option<String>,
        heartbeat_ttl: Option<String>,
        catalog: Option<Vec<u8>>,
        checkpoint_bytes: u64,
        membership: bool,
    ) -> Self {
        Self::configured_with_timeout(
            roster, heartbeat_ttl, catalog, checkpoint_bytes, membership,
            if membership { 3000 } else { 700 },
        )
    }
    fn configured_with_timeout(
        roster: Option<String>,
        heartbeat_ttl: Option<String>,
        catalog: Option<Vec<u8>>,
        checkpoint_bytes: u64,
        membership: bool,
        request_timeout_ms: u64,
    ) -> Self {
        let root = std::env::temp_dir().join(node_regime::run_token("metadata-peers"));
        std::fs::create_dir(&root).unwrap();
        let reserved: Vec<_> = (0..if membership { 4 } else { 3 })
            .map(|_| {
                (
                    TcpListener::bind("127.0.0.1:0").unwrap(),
                    TcpListener::bind("127.0.0.1:0").unwrap(),
                )
            })
            .collect();
        let peers = reserved
            .iter()
            .enumerate()
            .map(|(i, (_, peer))| (i as u64 + 1, peer.local_addr().unwrap()))
            .collect();
        let api_addresses = reserved
            .iter()
            .map(|(api, _)| api.local_addr().unwrap())
            .collect();
        let catalog = catalog.map(|bytes| {
            let path = root.join("catalog.bin");
            std::fs::write(&path, bytes).unwrap();
            path
        });
        let mut group = Self {
            zero_fill: ZERO_FILL.with(|chunk| chunk.get()),
            object_cap: OBJECT_CAP.with(|cap| cap.get()),
            forget_prefixes: FORGET_PREFIXES.with(|prefixes| prefixes.get()),
            nodes: (0..if membership { 4 } else { 3 }).map(|_| None).collect(),
            peers,
            root,
            api_addresses,
            reservations: reserved.into_iter().map(Some).collect(),
            roster,
            catalog,
            heartbeat_ttl,
            checkpoint_bytes,
            membership,
            request_timeout_ms,
        };
        for i in 0..group.nodes.len() {
            std::fs::create_dir(group.root.join(i.to_string())).unwrap();
            if i < 3 {
                group.start(i);
            }
        }
        group
    }
    fn start(&mut self, i: usize) {
        let peers = self
            .peers
            .iter()
            .map(|(id, a)| format!("{id}@{a}"))
            .collect::<Vec<_>>()
            .join(",");
        let id = (i + 1).to_string();
        let checkpoint_bytes = self.checkpoint_bytes.to_string();
        let request_timeout_ms = self.request_timeout_ms.to_string();
        let zero_fill = self.zero_fill.map(|bytes| bytes.to_string());
        let object_cap = self.object_cap.map(|cap| cap.to_string());
        let path = self.root.join(i.to_string());
        let bind = self.api_addresses[i].to_string();
        let mut environment = vec![
            ("MATRIXOBJECT_METASERVER_ADDR", bind.as_str()),
            ("MATRIXOBJECT_METASERVER_RAFT_PEERS", &peers),
            ("MATRIXOBJECT_METASERVER_RAFT_NODE_ID", &id),
            (
                "MATRIXOBJECT_METASERVER_RAFT_STATE_DIR",
                path.to_str().unwrap(),
            ),
            // Membership recovery writes can include checkpoint I/O on loaded hosts.
            (
                "MATRIXOBJECT_METASERVER_RAFT_TIMEOUT_MS",
                request_timeout_ms.as_str(),
            ),
            (
                "MATRIXOBJECT_METASERVER_RAFT_CHECKPOINT_BYTES",
                checkpoint_bytes.as_str(),
            ),
        ];
        if self.membership {
            environment.push(("MATRIXOBJECT_METASERVER_RAFT_BOOTSTRAP_VOTERS", "1,2,3"));
        }
        if let Some(bytes) = &zero_fill {
            environment.push(("MATRIXOBJECT_WAL_ZERO_FILL_BYTES", bytes.as_str()));
        }
        if let Some(cap) = &object_cap {
            environment.push(("MATRIXOBJECT_METASERVER_MAX_OBJECTS", cap.as_str()));
        }
        if let Some(prefixes) = self.forget_prefixes {
            environment.push(("MATRIXOBJECT_METASERVER_FORGET_PREFIXES", prefixes));
        }
        let roster = self.roster.as_ref().map(|roster| {
            let mut entries: Vec<_> = roster.split(',').collect();
            if i % 2 == 1 {
                entries.reverse();
            }
            entries.join(",")
        });
        if let Some(path) = &self.catalog {
            environment.push((
                "MATRIXOBJECT_METASERVER_CHUNK_GROUPS_FILE",
                path.to_str().unwrap(),
            ));
        } else if let Some(roster) = &roster {
            environment.push(("MATRIXOBJECT_METASERVER_DATA_ROSTER", roster));
        } else {
            environment.push(("MATRIXOBJECT_METASERVER_SEED_DATA_NODES", "3"));
        }
        if let Some(ttl) = &self.heartbeat_ttl {
            environment.push(("MATRIXOBJECT_METASERVER_HEARTBEAT_TTL_MS", ttl));
        }
        if SUPPRESS_REPORTS.with(|flag| flag.get()) {
            environment.push(("MATRIXOBJECT_METASERVER_SUPPRESS_UNCHANGED_REPORTS", "1"));
        }
        if LEADER_LIVENESS.with(|flag| flag.get()) {
            environment.push(("MATRIXOBJECT_METASERVER_LEADER_LIVENESS", "1"));
        }
        drop(self.reservations[i].take());
        let node = processes::start(env!("CARGO_BIN_EXE_metaserver"), &environment);
        assert_eq!(node.endpoint, bind);
        assert!(!self
            .peers
            .values()
            .any(|peer| peer.to_string() == node.endpoint));
        assert_ne!(node.pid, std::process::id());
        assert!(node.startup_line.contains("storage=raft"));
        self.nodes[i] = Some(node);
    }
    fn client(&self, i: usize) -> RemoteMetaClient {
        RemoteMetaClient::connect(
            &self.nodes[i].as_ref().unwrap().endpoint,
            &MetaClientOptions {
                read_timeout: Duration::from_secs(2),
                write_timeout: Duration::from_secs(2),
                ..Default::default()
            },
        )
        .unwrap()
    }
    #[track_caller]
    fn leader(&self) -> usize {
        let until = Instant::now() + Duration::from_secs(15);
        let mut last = vec![String::new(); self.nodes.len()];
        loop {
            for (i, reply) in last.iter_mut().enumerate() {
                if self.nodes[i].is_some() {
                    match self.client(i).probe().into_result() {
                        Ok(_) => return i,
                        Err(error) => *reply = error.to_string(),
                    }
                } else {
                    *reply = "process stopped".into();
                }
            }
            assert!(
                Instant::now() < until,
                "no metadata leader committed a read barrier; last replies: {last:?}"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }
    fn kill(&mut self, i: usize) {
        let mut node = self.nodes[i].take().unwrap();
        node.child.kill().unwrap();
        assert!(processes::reap_within(&mut node.child, Duration::from_secs(3)).is_some());
        assert!(!processes::is_live(node.pid));
    }
}
impl Drop for Group {
    fn drop(&mut self) {
        self.nodes.clear();
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

#[test]
fn read_sessions_survive_lagging_voter_transfer_and_expire_after_quorum_recovery() {
    use matrixobjectstore_rs::object_manifest::{ManifestOutcome, ObjectManifest};
    use matrixobjectstore_rs::cluster::ReplicaClientOptions;
    let mut group = Group::with_catalog_checkpoints(None, None, None, 1024);
    let leader = group.leader();
    let mut client = group.client(leader);
    let original = client.publish_manifest(ManifestOutcome::decide(None, ObjectManifest {
        format_version: 2, bucket: "bucket".into(), key: "reader".into(), generation: 1,
        ..Default::default()
    }).unwrap(), 0).unwrap();
    let permanent = client.retain_manifest("bucket", "reader", 1).unwrap();
    let lagging = (leader + 1) % 3;
    group.kill(lagging);
    let mut session = client.open_read_session("bucket", "reader", Some(1), 60_000).unwrap();
    client.delete_object("bucket", "reader", 0).unwrap();
    client.renew_read_session(&mut session, 120_000).unwrap();
    let wal = group.root.join(leader.to_string()).join("metadata.peer.wal");
    let protected_commit = node_regime::checkpoints::committed_index(&wal);
    assert!(protected_commit > 0);
    for index in 0..16 {
        client.allocate_staged_object("bucket", &format!("reader-checkpoint-{index}"), None, 1, false).unwrap();
    }
    node_regime::checkpoints::wait_for_checkpoint_index(&wal, protected_commit);
    group.start(lagging);
    node_regime::checkpoints::wait_for_checkpoint_index(
        &group.root.join(lagging.to_string()).join("metadata.peer.wal"), protected_commit);
    group.kill(leader);
    let mut client = group.client(group.leader());
    assert_eq!(client.read_retained_object(session.retention_id, 0, &ReplicaClientOptions::default()).unwrap(),
        (original, Vec::new()));
    client.publish_manifest(ManifestOutcome::decide(None, ObjectManifest {
        format_version: 2, bucket: "bucket".into(), key: "abandoned-reader".into(), generation: 1,
        ..Default::default()
    }).unwrap(), 0).unwrap();
    let abandoned = client.open_read_session("bucket", "abandoned-reader", None, 1000).unwrap();
    for index in 0..3 { if group.nodes[index].is_some() { group.kill(index); } }
    std::thread::sleep(Duration::from_millis(1200));
    group.start(0);
    let mut minority = group.client(0);
    assert!(minority.read_retained_manifest(session.retention_id).is_err());
    assert!(minority.release_manifest(session.retention_id).is_err());
    group.start(1);
    group.start(2);
    let mut client = group.client(group.leader());
    let until = Instant::now() + Duration::from_secs(10);
    loop {
        if !client.list_retained_manifests(0, 32).unwrap().0.contains(&abandoned.retention_id) { break; }
        assert!(Instant::now() < until);
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(client.read_retained_manifest(abandoned.retention_id).is_err());
    assert_eq!(client.read_retained_manifest(session.retention_id).unwrap(), session);
    assert_eq!(client.read_retained_manifest(permanent.retention_id).unwrap(), permanent);
    client.expire_read_session(&abandoned).unwrap();
    for index in 0..3 { group.kill(index); }
    for index in 0..3 { group.start(index); }
    let mut client = group.client(group.leader());
    assert_eq!(client.read_retained_manifest(session.retention_id).unwrap(), session);
    assert!(client.read_retained_manifest(abandoned.retention_id).is_err());
}

#[test]
fn prepared_staging_identity_survives_lagging_voter_transfer_leader_loss_and_retirement() {
    use matrixobjectstore_rs::staged_allocation::PreparedStagedAllocation;
    let mut group = Group::with_catalog_checkpoints(None, None, None, 1024);
    let leader = group.leader();
    let mut client = group.client(leader);
    client.enable_publication_fence().unwrap();
    let lagging = (leader + 1) % 3;
    group.kill(lagging);
    let prepared = client.prepare_staged_allocation("bucket", "identified", None, 2, false, 60_000).unwrap();
    let prepared = PreparedStagedAllocation::from_bytes(&prepared.to_bytes()).unwrap();
    let competing = client.prepare_staged_allocation("bucket", "identified", None, 2, false, 60_000).unwrap();
    let first = client.allocate_prepared_staged_object(&prepared).unwrap();
    assert!(client.allocate_prepared_staged_object(&competing).is_err());
    let renewed = client.renew_staged_allocation(&first, 120_000).unwrap();
    assert_eq!(client.allocate_prepared_staged_object(&prepared).unwrap(), renewed);
    let wal = group.root.join(leader.to_string()).join("metadata.peer.wal");
    let identity_commit = node_regime::checkpoints::committed_index(&wal);
    assert!(identity_commit > 0);
    for index in 0..16 {
        client.allocate_staged_object("bucket", &format!("checkpoint-{index}"), None, 1, false).unwrap();
    }
    node_regime::checkpoints::wait_for_checkpoint_index(&wal, identity_commit);
    group.start(lagging);
    node_regime::checkpoints::wait_for_checkpoint_index(
        &group.root.join(lagging.to_string()).join("metadata.peer.wal"), identity_commit);
    group.kill(leader);
    let mut client = group.client(group.leader());
    assert_eq!(client.allocate_prepared_staged_object(&prepared).unwrap(), renewed);
    for index in 0..3 { if group.nodes[index].is_some() { group.kill(index); } }
    for index in 0..3 { group.start(index); }
    let leader = group.leader();
    let mut client = group.client(leader);
    assert_eq!(client.allocate_prepared_staged_object(&prepared).unwrap(), renewed);
    assert!(client.allocate_prepared_staged_object(&competing).is_err());
    assert_eq!(client.list_staged_allocations(0, 32).unwrap().0.len(), 17);
    client.abort_staged_allocation(renewed.allocation_id).unwrap();
    assert!(client.allocate_prepared_staged_object(&prepared).is_err());
    for index in 0..3 { group.kill(index); }
    for index in 0..3 { group.start(index); }
    let leader = group.leader();
    let mut client = group.client(leader);
    assert!(client.allocate_prepared_staged_object(&prepared).is_err());
    let next = client.prepare_staged_allocation("bucket", "identified", None, 1, false, 0).unwrap();
    assert!(next.allocation_id() > renewed.allocation_id);
    assert!(client.allocate_prepared_staged_object(&next).is_ok());
    for index in 0..3 { if index != leader { group.kill(index); } }
    assert!(client.prepare_staged_allocation("bucket", "minority", None, 1, false, 0).is_err());
    assert!(client.allocate_prepared_staged_object(&next).is_err());
}

#[test]
fn object_tombstone_survives_metadata_failover_and_restart() {
    use matrixobjectstore_rs::object_manifest::{ManifestOutcome, ObjectManifest};

    let mut group = Group::new();
    let mut client = group.client(group.leader());
    let live = ObjectManifest {
        format_version: 2,
        bucket: "bucket".into(),
        key: "deleted-object".into(),
        generation: 1,
        length: 0,
        extents: Vec::new(),
        deleted: false,
    };
    assert_eq!(
        client
            .publish_manifest(ManifestOutcome::decide(None, live.clone()).unwrap(), 1)
            .unwrap(),
        live
    );
    let tombstone = client.delete_object("bucket", "deleted-object", 2).unwrap();
    assert!(tombstone.deleted);
    assert_eq!(tombstone.generation, 2);
    assert_eq!(
        client.delete_object("bucket", "deleted-object", 3).unwrap(),
        tombstone
    );
    #[cfg(feature = "replica-server")]
    assert!(matches!(
        client.read_object(
            "bucket",
            "deleted-object",
            0,
            &matrixobjectstore_rs::cluster::ReplicaClientOptions::default(),
        ),
        Err(matrixobjectstore_rs::ObjectError::NotFound(_))
    ));

    let former_leader = group.leader();
    group.kill(former_leader);
    let mut client = group.client(group.leader());
    assert_eq!(
        client.read_manifest("bucket", "deleted-object").unwrap(),
        tombstone
    );
    for index in 0..3 {
        if group.nodes[index].is_some() {
            group.kill(index);
        }
    }
    for index in 0..3 {
        group.start(index);
    }
    let mut client = group.client(group.leader());
    assert_eq!(
        client.read_manifest("bucket", "deleted-object").unwrap(),
        tombstone
    );
    let mut recreated = live;
    recreated.generation = 3;
    assert_eq!(
        client
            .publish_manifest(
                ManifestOutcome::decide(Some(tombstone), recreated.clone()).unwrap(),
                4,
            )
            .unwrap(),
        recreated
    );
}

#[test]
fn manifest_listing_continues_after_leader_loss_and_preserves_tombstones() {
    use matrixobjectstore_rs::object_manifest::{ManifestOutcome, ObjectManifest};
    let mut group = Group::new();
    let mut client = group.client(group.leader());
    for (bucket, key) in [("a", "z"), ("bucket", "a"), ("bucket", "b"), ("bucket", "c"), ("z", "a")] {
        client.publish_manifest(ManifestOutcome::decide(None, ObjectManifest {
            format_version: 2, bucket: bucket.into(), key: key.into(), generation: 1,
            length: 0, extents: Vec::new(), deleted: false,
        }).unwrap(), 1).unwrap();
    }
    client.delete_object("bucket", "b", 2).unwrap();
    let commit_index = |client: &mut RemoteMetaClient| match client.probe().into_result().unwrap() {
        matrixobjectstore_rs::cluster::MetaReply::Pong { commit_index, .. } => commit_index,
        reply => panic!("unexpected metadata status: {reply:?}"),
    };
    let before = commit_index(&mut client);
    let first = client.list_manifests("bucket", None, 2).unwrap();
    assert_eq!(commit_index(&mut client), before);
    assert!(first.revision.is_empty());
    let fenced = client.list_manifests_at_revision("bucket", None, 2, None).unwrap();
    let global = client.list_manifest_catalog(None, 3, None).unwrap();
    assert_eq!(global.entries.iter().map(|entry| (entry.bucket.as_str(), entry.key.as_str()))
        .collect::<Vec<_>>(), [("a", "z"), ("bucket", "a"), ("bucket", "b")]);
    assert_eq!(global.revision, fenced.revision);
    assert_eq!(fenced.entries, first.entries);
    client.allocate_object_pages(1).unwrap();
    assert_eq!(first.entries.iter().map(|entry| entry.key.as_str()).collect::<Vec<_>>(), ["a", "b"]);
    assert!(first.entries[1].deleted);
    assert_eq!(first.entries[1].generation, 2);
    assert!(first.has_more);
    group.kill(group.leader());
    let mut client = group.client(group.leader());
    let second = client.list_manifests("bucket", Some("b"), 2).unwrap();
    let global_next = client.list_manifest_catalog(Some(("bucket", "b")), 3, Some(&global.revision)).unwrap();
    assert_eq!(global_next.entries.iter().map(|entry| (entry.bucket.as_str(), entry.key.as_str()))
        .collect::<Vec<_>>(), [("bucket", "c"), ("z", "a")]);
    assert!(!global_next.has_more);
    assert_eq!(client.list_manifests_at_revision("bucket", Some("b"), 2,
        Some(&fenced.revision)).unwrap().entries, second.entries);
    assert_eq!(second.entries[0].key, "c");
    assert!(!second.has_more);
    assert!(client.list_manifests("bucket", Some("c"), 2).unwrap().entries.is_empty());
    assert!(client.list_manifests("missing", None, 2).unwrap().entries.is_empty());
    assert!(client.list_manifests("bucket", None, 33).is_err());
    for index in 0..3 { if group.nodes[index].is_some() { group.kill(index); } }
    for index in 0..3 { group.start(index); }
    let leader = group.leader();
    let mut client = group.client(leader);
    let restored = client.list_manifests("bucket", None, 32).unwrap();
    assert_eq!(restored.entries.len(), 3);
    assert!(restored.entries[1].deleted);
    assert_eq!(client.list_manifest_catalog(None, 32, Some(&global.revision)).unwrap().entries.len(), 5);
    assert_eq!(client.list_manifests_at_revision("bucket", None, 32,
        Some(&fenced.revision)).unwrap().revision, fenced.revision);
    client.publish_manifest(ManifestOutcome::decide(None, ObjectManifest {
        format_version: 2, bucket: "bucket".into(), key: "d".into(), generation: 1,
        length: 0, extents: Vec::new(), deleted: false,
    }).unwrap(), 3).unwrap();
    assert!(client.list_manifests_at_revision("bucket", Some("b"), 2,
        Some(&fenced.revision)).is_err());
    let changed = client.list_manifests_at_revision("bucket", None, 32, None).unwrap();
    assert_eq!(changed.entries.len(), 4);
    assert_ne!(changed.revision, fenced.revision);
    assert!(client.list_manifest_catalog(None, 32, Some(&global.revision)).is_err());
    for index in 0..3 { if index != leader { group.kill(index); } }
    assert!(client.list_manifests("bucket", None, 2).is_err());
}

#[test]
fn retained_generations_survive_overwrite_deletion_failover_and_checkpoint_restart() {
    use matrixobjectstore_rs::object_manifest::{ManifestOutcome, ObjectManifest};
    let mut group = Group::with_catalog_checkpoints(None, None, None, 1024);
    let mut client = group.client(group.leader());
    let original = ObjectManifest { format_version: 2, bucket: "bucket".into(), key: "retained".into(),
        generation: 1, length: 0, extents: Vec::new(), deleted: false };
    client.publish_manifest(ManifestOutcome::decide(None, original.clone()).unwrap(), 0).unwrap();
    let mut handles = Vec::new();
    for _ in 0..20 { handles.push(client.retain_manifest("bucket", "retained", 1).unwrap()); }
    assert!(handles.windows(2).all(|pair| pair[0].retention_id < pair[1].retention_id));
    let mut changed = original.clone();
    changed.generation = 2;
    client.publish_manifest(ManifestOutcome::decide(Some(original.clone()), changed).unwrap(), 1).unwrap();
    assert!(client.retain_manifest("bucket", "retained", 1).is_err());
    client.delete_object("bucket", "retained", 2).unwrap();
    assert!(client.retain_manifest("bucket", "retained", 3).is_err());
    node_regime::checkpoints::wait_for_checkpoint(
        &group.root.join(group.leader().to_string()).join("metadata.peer.wal"), 4,
    );
    group.kill(group.leader());
    let mut client = group.client(group.leader());
    assert_eq!(client.read_retained_manifest(handles[0].retention_id).unwrap(), handles[0]);
    let (first, more) = client.list_retained_manifests(0, 7).unwrap();
    assert!(more);
    assert_eq!(first, handles[..7].iter().map(|record| record.retention_id).collect::<Vec<_>>());
    let (rest, more) = client.list_retained_manifests(*first.last().unwrap(), 32).unwrap();
    assert!(!more);
    assert_eq!(rest.len(), 13);
    for index in 0..3 { if group.nodes[index].is_some() { group.kill(index); } }
    for index in 0..3 { group.start(index); }
    let leader = group.leader();
    let mut client = group.client(leader);
    assert_eq!(client.read_retained_manifest(handles[19].retention_id).unwrap(), handles[19]);
    #[cfg(feature = "replica-server")]
    assert_eq!(client.read_retained_object(handles[0].retention_id, 0,
        &matrixobjectstore_rs::cluster::ReplicaClientOptions::default()).unwrap(), (original, Vec::new()));
    client.release_manifest(handles[0].retention_id).unwrap();
    client.release_manifest(handles[0].retention_id).unwrap();
    assert!(client.read_retained_manifest(handles[0].retention_id).is_err());
    assert_eq!(client.list_retained_manifests(0, 32).unwrap().0.len(), 19);
    for index in 0..3 { group.kill(index); }
    for index in 0..3 { group.start(index); }
    let leader = group.leader();
    let mut client = group.client(leader);
    assert!(client.read_retained_manifest(handles[0].retention_id).is_err());
    assert_eq!(client.list_retained_manifests(0, 32).unwrap().0.len(), 19);
    for index in 0..3 { if index != leader { group.kill(index); } }
    assert!(client.read_retained_manifest(handles[1].retention_id).is_err());
    assert!(client.list_retained_manifests(0, 32).is_err());
    assert!(client.release_manifest(handles[1].retention_id).is_err());
}

#[test]
fn staged_allocations_survive_failover_and_restart_and_fence_aborted_publication() {
    use matrixobjectstore_rs::object_manifest::{ManifestOutcome, ObjectManifest};
    let mut group = Group::with_catalog_checkpoints(None, None, None, 1024);
    let mut client = group.client(group.leader());
    let mut records = Vec::new();
    for _ in 0..24 { records.push(client.allocate_staged_object("bucket", "owned", None, 1, false).unwrap()); }
    assert!(client.allocate_staged_object("bucket", "owned", Some(1), 1, false).is_err());
    node_regime::checkpoints::wait_for_checkpoint(
        &group.root.join(group.leader().to_string()).join("metadata.peer.wal"), 4,
    );
    assert!(client.read_manifest("bucket", "owned").is_err());
    let (first, more) = client.list_staged_allocations(0, 7).unwrap();
    assert!(more);
    assert_eq!(first, records[..7].iter().map(|record| record.allocation_id).collect::<Vec<_>>());
    group.kill(group.leader());
    let mut client = group.client(group.leader());
    assert_eq!(client.read_staged_allocation(records[0].allocation_id).unwrap(), records[0]);
    for index in 0..3 { if group.nodes[index].is_some() { group.kill(index); } }
    for index in 0..3 { group.start(index); }
    let mut client = group.client(group.leader());
    assert_eq!(client.read_staged_allocation(records[23].allocation_id).unwrap(), records[23]);
    client.abort_staged_allocation(records[0].allocation_id).unwrap();
    client.abort_staged_allocation(records[0].allocation_id).unwrap();
    let live = ObjectManifest { format_version: 2, bucket: "bucket".into(), key: "owned".into(),
        generation: 1, length: 0, extents: Vec::new(), deleted: false };
    let outcome = ManifestOutcome::decide(None, live.clone()).unwrap();
    assert!(client.publish_staged_manifest(records[0].allocation_id, outcome.clone(), 0).is_err());
    assert_eq!(client.publish_staged_manifest(records[1].allocation_id, outcome.clone(), 0).unwrap(), live);
    assert!(client.read_staged_allocation(records[1].allocation_id).is_err());
    assert!(client.publish_staged_manifest(records[2].allocation_id, outcome.clone(), 0).is_err());
    let reader = client.retain_manifest("bucket", "owned", live.generation).unwrap();
    for index in 0..3 { group.kill(index); }
    for index in 0..3 { group.start(index); }
    let leader = group.leader();
    let mut client = group.client(leader);
    assert!(client.read_staged_allocation(records[0].allocation_id).is_err());
    assert!(client.read_staged_allocation(records[1].allocation_id).is_err());
    assert_eq!(client.read_manifest("bucket", "owned").unwrap(), live);
    assert_eq!(client.read_retained_manifest(reader.retention_id).unwrap(), reader);
    client.release_manifest(reader.retention_id).unwrap();
    assert_eq!(client.list_staged_allocations(0, 32).unwrap().0.len(), 22);
    assert!(client.publish_staged_manifest(records[0].allocation_id, outcome, 0).is_err());
    for index in 0..3 { if index != leader { group.kill(index); } }
    assert!(client.read_staged_allocation(records[2].allocation_id).is_err());
    assert!(client.list_staged_allocations(0, 32).is_err());
    assert!(client.abort_staged_allocation(records[2].allocation_id).is_err());
}

/// A feature declaration is a proposal like any other: it must be admitted through the real leader
/// pipeline of a three-voter group without stopping the leader's consensus worker, and what it
/// decides must survive a leader loss and a total restart. (The leader once prepared the outcome
/// against a draft that did not restore the feature set, found the live set changed when the entry
/// committed, and stopped: the group had no leader after the first declaration.)
#[test]
fn cluster_features_are_declared_and_raised_through_the_leader_and_survive_failover_and_restart() {
    const COMPACT_APPEND: u64 = 1;
    const RESERVE_NEXT: u64 = 2; // reserve-next: any second bit this build supports
    let mut group = Group::new();
    let leader = group.leader();
    let mut client = group.client(leader);
    assert!(client.read_cluster_features().unwrap().is_default());
    for voter in 1..=3 {
        let features = client.declare_voter_features(voter, COMPACT_APPEND | RESERVE_NEXT).unwrap();
        assert_eq!(features.declared.get(&voter), Some(&(COMPACT_APPEND | RESERVE_NEXT)));
    }
    // a repeated declaration proposes nothing and answers with the set
    client.declare_voter_features(1, COMPACT_APPEND | RESERVE_NEXT).unwrap();
    assert!(client.raise_cluster_features(1 << 2, vec![1, 2, 3]).is_err(), "a bit no voter declared is not raised");
    let raised = client.raise_cluster_features(COMPACT_APPEND | RESERVE_NEXT, vec![1, 2, 3]).unwrap();
    assert_eq!(raised.raised, COMPACT_APPEND | RESERVE_NEXT);
    // the leader still leads and still commits
    assert_eq!(group.leader(), leader, "a declaration or a raise changed the leader");
    client.allocate_object_pages(1).unwrap();
    assert_eq!(client.read_cluster_features().unwrap().raised, COMPACT_APPEND | RESERVE_NEXT);
    // leader loss: the set is the same on the new leader
    group.kill(leader);
    let mut client = group.client(group.leader());
    assert_eq!(client.read_cluster_features().unwrap().raised, COMPACT_APPEND | RESERVE_NEXT);
    client.allocate_object_pages(1).unwrap();
    // total restart: replay reaches the same set
    for index in 0..3 { if group.nodes[index].is_some() { group.kill(index); } }
    for index in 0..3 { group.start(index); }
    let mut client = group.client(group.leader());
    let features = client.read_cluster_features().unwrap();
    assert_eq!(features.raised, COMPACT_APPEND | RESERVE_NEXT);
    assert_eq!(features.declared.len(), 3);
}

#[test]
fn publication_fence_and_atomic_copy_survive_checkpoint_restart_and_quorum_loss() {
    use matrixobjectstore_rs::object_manifest::{ManifestOutcome, ObjectManifest};
    use prost::Message;
    #[derive(Clone, PartialEq, Message)]
    struct WalPosition {
        #[prost(bytes = "vec", tag = "5")]
        hard_state: Vec<u8>,
        #[prost(bytes = "vec", tag = "7")]
        snapshot: Vec<u8>,
    }
    #[derive(Clone, PartialEq, Message)]
    struct HardStatePosition {
        #[prost(uint64, tag = "3")]
        commit: u64,
    }
    #[derive(Clone, PartialEq, Message)]
    struct SnapshotPosition {
        #[prost(message, optional, tag = "2")]
        metadata: Option<SnapshotIndex>,
    }
    #[derive(Clone, PartialEq, Message)]
    struct SnapshotIndex {
        #[prost(uint64, tag = "2")]
        index: u64,
    }
    fn wal_positions(path: &std::path::Path) -> (u64, u64) {
        let bytes = std::fs::read(path).unwrap();
        let (mut offset, mut checkpoint, mut commit) = (0, 0, 0);
        while bytes.len() - offset >= 24 {
            assert_eq!(&bytes[offset..offset + 4], b"MRPW");
            let length = u32::from_be_bytes(bytes[offset + 4..offset + 8].try_into().unwrap()) as usize;
            assert!(length < 8 * 1024 * 1024);
            let end = offset + 24 + length;
            // A concurrent append may not have finished its final frame.
            if end > bytes.len() { break; }
            let record = WalPosition::decode(&bytes[offset + 24..end]).unwrap();
            commit = commit.max(HardStatePosition::decode(record.hard_state.as_slice()).unwrap().commit);
            if !record.snapshot.is_empty() {
                checkpoint = SnapshotPosition::decode(record.snapshot.as_slice()).unwrap().metadata.unwrap().index;
            }
            offset = end;
        }
        (checkpoint, commit)
    }
    let mut group = Group::with_catalog_checkpoints(None, None, None, 1024);
    let mut client = group.client(group.leader());
    assert_eq!(client.read_publication_fence().unwrap(), 0);
    client.allocate_object_pages(1).unwrap();
    let owner = client.allocate_staged_object("bucket", "source", None, 1, false).unwrap();
    let fence = client.enable_publication_fence().unwrap();
    assert!(fence > 0);
    assert_eq!(client.enable_publication_fence().unwrap(), fence);
    let leader = group.leader();
    let wal = group.root.join(leader.to_string()).join("metadata.peer.wal");
    let (_, fence_commit) = wal_positions(&wal);
    assert!(fence_commit > 0);
    assert!(client.allocate_object_pages(1).is_err());
    let source = ObjectManifest { format_version: 2, bucket: "bucket".into(), key: "source".into(),
        generation: 1, length: 0, extents: Vec::new(), deleted: false };
    client.publish_staged_manifest(owner.allocation_id, ManifestOutcome::decide(None, source.clone()).unwrap(), 0).unwrap();
    for index in 0..16 {
        client.copy_object_atomic("bucket", "source", "copies", &format!("copy-{index}"), None, 0, 0).unwrap();
    }
    node_regime::checkpoints::wait_for_checkpoint(&wal, 4);
    for index in 16..32 {
        client.copy_object_atomic("bucket", "source", "copies", &format!("copy-{index}"), None, 0, 0).unwrap();
    }
    let until = Instant::now() + Duration::from_secs(30);
    while wal_positions(&wal).0 < fence_commit {
        assert!(Instant::now() < until, "fenced state was not checkpointed");
        // Idempotent commands advance the WAL without growing the catalog.
        // Checkpoint scheduling amortizes capture over the current image size.
        assert_eq!(client.enable_publication_fence().unwrap(), fence);
        std::thread::sleep(Duration::from_millis(20));
    }
    group.kill(leader);
    let mut client = group.client(group.leader());
    assert_eq!(client.read_publication_fence().unwrap(), fence);
    assert_eq!(client.read_manifest("copies", "copy-31").unwrap().extents, source.extents);
    for index in 0..3 { if group.nodes[index].is_some() { group.kill(index); } }
    for index in 0..3 { group.start(index); }
    let leader = group.leader();
    let mut client = group.client(leader);
    assert_eq!(client.read_publication_fence().unwrap(), fence);
    assert!(client.allocate_object_pages(1).is_err());
    let copy = client.copy_object_atomic("bucket", "source", "copies", "after-restart", None, 0, 0).unwrap();
    assert!(copy.extents.is_empty());
    client.delete_object("bucket", "source", 0).unwrap();
    assert!(client.copy_object_atomic("bucket", "source", "copies", "deleted", None, 0, 0).is_err());
    for index in 0..3 { if index != leader { group.kill(index); } }
    assert!(client.read_publication_fence().is_err());
    assert!(client.copy_object_atomic("copies", "copy-31", "copies", "minority", None, 0, 0).is_err());
}

#[test]
fn staged_lease_deadlines_and_renewals_survive_failover_restart_and_quorum_loss() {
    use matrixobjectstore_rs::object_manifest::{ManifestOutcome, ObjectManifest};
    let mut group = Group::with_catalog_checkpoints(None, None, None, 1024);
    let mut client = group.client(group.leader());
    assert!(client.allocate_leased_staged_object("bucket", "leased", None, 1, false, 1000).is_err());
    client.enable_publication_fence().unwrap();
    let original = client.allocate_leased_staged_object("bucket", "leased", None, 1, false, 60_000).unwrap();
    let renewed = client.renew_staged_allocation(&original, 120_000).unwrap();
    assert!(renewed.expires_at_ms > original.expires_at_ms);
    assert!(client.renew_staged_allocation(&original, 120_000).is_err());
    assert!(client.expire_staged_allocation(&original).is_err());
    let expiring = client.allocate_leased_staged_object("bucket", "expired", None, 1, false, 1000).unwrap();
    for index in 0..16 {
        client.allocate_staged_object("padding", &format!("key-{index}"), None, 1, false).unwrap();
    }
    node_regime::checkpoints::wait_for_checkpoint(
        &group.root.join(group.leader().to_string()).join("metadata.peer.wal"), 4);
    group.kill(group.leader());
    let mut client = group.client(group.leader());
    assert_eq!(client.read_staged_allocation(renewed.allocation_id).unwrap(), renewed);
    while std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis()
        < u128::from(expiring.expires_at_ms) {
        std::thread::sleep(Duration::from_millis(20));
    }
    let value = ObjectManifest { format_version: 2, bucket: "bucket".into(), key: "expired".into(),
        generation: 1, length: 0, extents: Vec::new(), deleted: false };
    let publish = ManifestOutcome::decide(None, value).unwrap();
    assert!(client.publish_staged_manifest(expiring.allocation_id, publish.clone(), 0).is_err());
    assert!(client.renew_staged_allocation(&expiring, 1000).is_err());
    client.expire_staged_allocation(&expiring).unwrap();
    client.expire_staged_allocation(&expiring).unwrap();
    for index in 0..3 { if group.nodes[index].is_some() { group.kill(index); } }
    for index in 0..3 { group.start(index); }
    let leader = group.leader();
    let mut client = group.client(leader);
    assert_eq!(client.read_staged_allocation(renewed.allocation_id).unwrap(), renewed);
    assert!(client.read_staged_allocation(expiring.allocation_id).is_err());
    assert!(client.publish_staged_manifest(expiring.allocation_id, publish, 0).is_err());
    let next = client.allocate_leased_staged_object("bucket", "expired", None, 1, false, 1000).unwrap();
    assert!(next.allocation_id > expiring.allocation_id);
    for index in 0..3 { if index != leader { group.kill(index); } }
    assert!(client.read_staged_allocation(renewed.allocation_id).is_err());
    assert!(client.renew_staged_allocation(&renewed, 180_000).is_err());
    assert!(client.expire_staged_allocation(&expiring).is_err());
}

#[test]
fn automatic_staged_expiry_resumes_after_outage_requires_quorum_and_keeps_idle_wal_stable() {
    use matrixobjectstore_rs::ObjectError;
    let mut group = Group::new();
    let mut client = group.client(group.leader());
    client.enable_publication_fence().unwrap();
    let permanent = client.allocate_staged_object("bucket", "permanent", None, 1, false).unwrap();
    let old = client.allocate_leased_staged_object("bucket", "renewed", None, 1, false, 5000).unwrap();
    let renewed = client.renew_staged_allocation(&old, 120_000).unwrap();
    let mut abandoned = Vec::new();
    for index in 0..6 {
        abandoned.push(client.allocate_leased_staged_object("bucket", &format!("abandoned-{index}"),
            None, 1, false, 5000).unwrap());
    }
    for record in &abandoned {
        assert_eq!(client.read_staged_allocation(record.allocation_id).unwrap(), *record);
    }
    drop(client);
    for index in 0..3 { group.kill(index); }
    let last_deadline = abandoned.iter().map(|r| r.expires_at_ms).max().unwrap();
    while std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis()
        <= u128::from(last_deadline) {
        std::thread::sleep(Duration::from_millis(20));
    }
    group.start(0);
    std::thread::sleep(Duration::from_millis(2200));
    assert!(group.client(0).read_staged_allocation(abandoned[0].allocation_id).is_err());
    group.start(1);
    group.start(2);
    let leader = group.leader();
    let mut client = group.client(leader);
    let until = Instant::now() + Duration::from_secs(15);
    loop {
        if abandoned.iter().all(|record| matches!(client.read_staged_allocation(record.allocation_id),
            Err(ObjectError::NotFound(_)))) { break; }
        assert!(Instant::now() < until, "new leader did not retire all abandoned staged leases");
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(client.read_staged_allocation(permanent.allocation_id).unwrap(), permanent);
    assert_eq!(client.read_staged_allocation(renewed.allocation_id).unwrap(), renewed);
    let wal = group.root.join(leader.to_string()).join("metadata.peer.wal");
    let before = std::fs::read(&wal).unwrap();
    std::thread::sleep(Duration::from_millis(2200));
    assert_eq!(std::fs::read(&wal).unwrap(), before, "empty automatic sweeps appended WAL records");
    drop(client);
    for index in 0..3 { group.kill(index); }
    for index in 0..3 { group.start(index); }
    let mut client = group.client(group.leader());
    for record in &abandoned {
        assert!(matches!(client.read_staged_allocation(record.allocation_id), Err(ObjectError::NotFound(_))));
    }
    assert_eq!(client.read_staged_allocation(permanent.allocation_id).unwrap(), permanent);
    assert_eq!(client.read_staged_allocation(renewed.allocation_id).unwrap(), renewed);
    let next = client.allocate_staged_object("bucket", "abandoned-0", None, 1, false).unwrap();
    assert!(next.allocation_id > abandoned.last().unwrap().allocation_id);
}

#[test]
fn process_quorum_preserves_outcomes_across_leader_loss_and_total_restart() {
    let mut group = Group::new();
    let first = group.leader();
    let mut endpoints: Vec<_> = (0..3)
        .filter(|i| *i != first)
        .map(|i| group.nodes[i].as_ref().unwrap().endpoint.clone())
        .collect();
    endpoints.push(group.nodes[first].as_ref().unwrap().endpoint.clone());
    let mut client = RemoteMetaClient::connect_catalog(
        &endpoints,
        &MetaClientOptions {
            connect_timeout: Duration::from_millis(200),
            read_timeout: Duration::from_secs(2),
            write_timeout: Duration::from_secs(2),
            ..Default::default()
        },
    )
    .unwrap();
    assert_ne!(
        client.endpoint(),
        group.nodes[first].as_ref().unwrap().endpoint
    );
    let mut saved = Vec::new();
    for n in 0..32 {
        let key = format!("object-{n}");
        let plan = client
            .plan_object_placement("bucket", &key, 1024, 2)
            .unwrap();
        let placement = CommittedPlacement {
            bucket: plan.bucket,
            key: plan.key,
            length: plan.length,
            node_ids: plan.node_ids,
            version: 1,
            update_time_ms: n + 10,
        };
        saved.push(
            client
                .commit_object_placement(placement, &plan.plan_checksum)
                .unwrap(),
        );
    }
    assert_eq!(
        client.endpoint(),
        group.nodes[first].as_ref().unwrap().endpoint
    );
    group.kill(first);
    let next = group.leader();
    assert_ne!(next, first);
    for value in &saved {
        assert_eq!(
            client.object_placement(&value.bucket, &value.key).unwrap(),
            *value
        );
    }
    {
        let mut directory = matrixobjectstore_rs::cluster::MetaDirectory::open(
            &endpoints.join(","),
            &MetaClientOptions {
                connect_timeout: Duration::from_millis(200),
                read_timeout: Duration::from_secs(2),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            directory
                .object_placement(&saved[0].bucket, &saved[0].key)
                .unwrap(),
            saved[0]
        );
    }
    client
        .heartbeat_placement_node("data-0", 321, 1 << 30, true, true, 500)
        .unwrap();
    let roster = client.list_placement_nodes().unwrap();
    drop(client);
    let other = (0..3).find(|i| *i != first && *i != next).unwrap();
    group.kill(other);
    let mut isolated = group.client(next);
    assert!(
        isolated.probe().into_result().is_err(),
        "isolated leader answered a read"
    );
    assert!(
        isolated
            .heartbeat_placement_node("data-0", 999, 1 << 30, false, true, 600)
            .is_err(),
        "isolated leader acknowledged a mutation"
    );
    drop(isolated);
    group.kill(next);
    for i in 0..3 {
        group.start(i);
    }
    let leader = group.leader();
    let mut client = group.client(leader);
    for value in &saved {
        assert_eq!(
            client.object_placement(&value.bucket, &value.key).unwrap(),
            *value
        );
    }
    let recovered = client.list_placement_nodes().unwrap();
    // A timed-out proposal may commit after connectivity returns. It must never
    // be acknowledged without quorum, and either complete state is valid here.
    let mut attempted = roster.clone();
    let node = attempted
        .iter_mut()
        .find(|n| n.node_id == "data-0")
        .unwrap();
    node.used_bytes = 999;
    node.draining = false;
    node.last_heartbeat_ms = 600;
    assert!(recovered == roster || recovered == attempted);
    for i in 0..3 {
        let bytes =
            std::fs::read(group.root.join(i.to_string()).join("metadata.peer.wal")).unwrap();
        assert!(bytes.starts_with(b"MRPW"));
        assert!(bytes.windows(4).any(|w| w == b"MOMP"));
    }
}
#[test]
fn a_slow_peer_connection_does_not_prevent_shutdown() {
    let directory = std::env::temp_dir().join(node_regime::run_token("peer-shutdown"));
    std::fs::create_dir(&directory).unwrap();
    let reserved = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = reserved.local_addr().unwrap();
    drop(reserved);
    let mut server = MetaNodeServer::bind("127.0.0.1:0", MetaNodeOptions::default()).unwrap();
    server
        .enable_raft(MetaRaftOptions {
            bootstrap_voters: None,
            peer_tls: None,
            checkpoint_after_bytes: 0,
            wal_zero_fill_bytes: None,
            node_id: 1,
            group_id: 1,
            peers: BTreeMap::from([(1, address)]),
            directory: directory.clone(),
            request_timeout: Duration::from_secs(1),
        })
        .unwrap();
    let stop = server.shutdown_handle();
    let handle = std::thread::spawn(move || server.serve().unwrap());
    let mut stream = TcpStream::connect(address).unwrap();
    stream.write_all(&[0]).unwrap();
    let start = Instant::now();
    stop.stop();
    handle.join().unwrap();
    assert!(start.elapsed() < Duration::from_secs(2));
    drop(stream);
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn peer_storage_refuses_missing_zeroed_or_reidentified_history() {
    for (setting, value) in [
        ("MATRIXOBJECT_METASERVER_RAFT_NODE_ID", "1"),
        ("MATRIXOBJECT_METASERVER_RAFT_BOOTSTRAP_VOTERS", "1,2,3"),
    ] {
        let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_metaserver"))
            .env("MATRIXOBJECT_METASERVER_ADDR", "127.0.0.1:0")
            .env_remove("MATRIXOBJECT_METASERVER_RAFT_NODE_ID")
            .env_remove("MATRIXOBJECT_METASERVER_RAFT_BOOTSTRAP_VOTERS")
            .env(setting, value)
            .env_remove("MATRIXOBJECT_METASERVER_RAFT_PEERS")
            .env_remove("MATRIXOBJECT_METASERVER_STATE_DIR")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let status = processes::reap_within(&mut child, Duration::from_secs(3));
        if status.is_none() {
            child.kill().unwrap();
            child.wait().unwrap();
        }
        assert_eq!(
            status,
            Some(2),
            "partial Raft configuration silently selected local mode"
        );
    }

    let directory = std::env::temp_dir().join(node_regime::run_token("peer-storage"));
    std::fs::create_dir(&directory).unwrap();
    let reserved = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = reserved.local_addr().unwrap();
    drop(reserved);
    let options = MetaRaftOptions {
        bootstrap_voters: None,
        peer_tls: None,
        checkpoint_after_bytes: 0,
        wal_zero_fill_bytes: None,
        node_id: 1,
        group_id: 1,
        peers: BTreeMap::from([(1, address)]),
        directory: directory.clone(),
        request_timeout: Duration::from_secs(1),
    };
    let make = || MetaNodeServer::bind("127.0.0.1:0", MetaNodeOptions::default()).unwrap();
    let mut server = make();
    server.enable_raft(options.clone()).unwrap();
    assert!(
        make().enable_raft(options.clone()).is_err(),
        "duplicate peer opened the directory"
    );
    drop(server);
    let mut wrong = options.clone();
    wrong.group_id = 2;
    assert!(
        make().enable_raft(wrong).is_err(),
        "changed group accepted old history"
    );
    let wal = directory.join("metadata.peer.wal");
    let original = std::fs::read(&wal).unwrap();
    std::fs::write(&wal, []).unwrap();
    assert!(
        make().enable_raft(options.clone()).is_err(),
        "zeroed WAL became a fresh peer"
    );
    std::fs::write(&wal, original).unwrap();
    std::fs::remove_file(directory.join("metadata.peer.base")).unwrap();
    assert!(
        make().enable_raft(options).is_err(),
        "missing bootstrap accepted old WAL"
    );
    std::fs::remove_dir_all(directory).unwrap();
}

#[cfg(feature = "replica-server")]
#[test]
fn configured_placement_writes_real_chunk_processes_and_recovers_after_failover() {
    use matrixobjectstore_rs::cluster::{
        RemoteReplicaClient, RemoteReplicaSet, ReplicaClientOptions,
    };
    use matrixobjectstore_rs::{ChunkId, PageId};
    struct Chunks {
        nodes: Vec<NodeProcess>,
        root: PathBuf,
    }
    impl Drop for Chunks {
        fn drop(&mut self) {
            self.nodes.clear();
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }
    let root = std::env::temp_dir().join(node_regime::run_token("roster-chunks"));
    std::fs::create_dir(&root).unwrap();
    let mut chunks = Chunks {
        nodes: Vec::new(),
        root,
    };
    for index in 0..3 {
        let path = chunks.root.join(format!("{index}.log"));
        chunks.nodes.push(processes::start(
            env!("CARGO_BIN_EXE_chunkserver"),
            &[
                ("MATRIXOBJECT_REPLICA_ADDR", "127.0.0.1:0"),
                ("MATRIXOBJECT_REPLICA_JOURNAL", path.to_str().unwrap()),
            ],
        ));
    }
    let roster = chunks
        .nodes
        .iter()
        .enumerate()
        .map(|(i, node)| format!("chunk-{i}@{}@rack-{i}", node.endpoint))
        .collect::<Vec<_>>()
        .join(",");
    let mut group = Group::with_roster(Some(roster));
    let leader = group.leader();
    let mut client = group.client(leader);
    assert!(client
        .list_placement_nodes()
        .unwrap()
        .iter()
        .all(|n| !n.online));
    assert!(client
        .plan_object_placement("bucket", "object", 16, 3)
        .is_err());
    // The controller admits only endpoints whose actual chunk process answers.
    for (i, node) in chunks.nodes.iter().enumerate() {
        assert_ne!(node.pid, std::process::id());
        let mut replica =
            RemoteReplicaClient::connect(&node.endpoint, &ReplicaClientOptions::default()).unwrap();
        replica.ping().unwrap();
        client
            .heartbeat_placement_node(&format!("chunk-{i}"), 0, 1 << 28, false, true, 100)
            .unwrap();
    }
    let roster = client.list_placement_nodes().unwrap();
    let plan = client
        .plan_object_placement("bucket", "object", 16, 3)
        .unwrap();
    let endpoints: Vec<_> = plan
        .node_ids
        .iter()
        .map(|id| {
            roster
                .iter()
                .find(|node| &node.node_id == id)
                .unwrap()
                .endpoint
                .clone()
        })
        .collect();
    assert_eq!(endpoints.len(), 3);
    for endpoint in &endpoints {
        assert!(chunks.nodes.iter().any(|node| &node.endpoint == endpoint));
    }
    let page = PageId {
        chunk_id: ChunkId::new(9, 1).unwrap(),
        page_index: 0,
    };
    let payload = b"binary\0outcome\xff!";
    let mut replicas =
        RemoteReplicaSet::connect(&endpoints, 2, 2, &ReplicaClientOptions::default()).unwrap();
    let confirmations = replicas.write_chunk(&page, 0, payload).unwrap();
    assert!((2..=3).contains(&confirmations));
    let placement = client
        .commit_object_placement(
            CommittedPlacement {
                bucket: plan.bucket,
                key: plan.key,
                length: plan.length,
                node_ids: plan.node_ids,
                version: 1,
                update_time_ms: 101,
            },
            &plan.plan_checksum,
        )
        .unwrap();
    drop(client);
    group.kill(leader);
    let mut client = group.client(group.leader());
    assert_eq!(
        client.object_placement("bucket", "object").unwrap(),
        placement
    );
    assert_eq!(
        replicas
            .read_chunk(&page, 0, payload.len() as u64)
            .unwrap()
            .data,
        payload
    );
    drop(replicas);
    drop(client);
    let addresses: Vec<_> = chunks
        .nodes
        .iter()
        .map(|node| node.endpoint.clone())
        .collect();
    chunks.nodes.clear();
    for (i, address) in addresses.iter().enumerate() {
        let path = chunks.root.join(format!("{i}.log"));
        assert_eq!(&std::fs::read(&path).unwrap()[..4], b"MOR2");
        chunks.nodes.push(processes::start(
            env!("CARGO_BIN_EXE_chunkserver"),
            &[
                ("MATRIXOBJECT_REPLICA_ADDR", address),
                ("MATRIXOBJECT_REPLICA_JOURNAL", path.to_str().unwrap()),
            ],
        ));
    }
    for i in 0..3 {
        if group.nodes[i].is_some() {
            group.kill(i);
        }
    }
    for i in 0..3 {
        group.start(i);
    }
    let mut client = group.client(group.leader());
    assert_eq!(
        client
            .list_placement_nodes()
            .unwrap()
            .iter()
            .map(|n| &n.endpoint)
            .collect::<Vec<_>>(),
        roster.iter().map(|n| &n.endpoint).collect::<Vec<_>>()
    );
    assert_eq!(
        client.object_placement("bucket", "object").unwrap(),
        placement
    );
    let mut replicas =
        RemoteReplicaSet::connect(&endpoints, 2, 2, &ReplicaClientOptions::default()).unwrap();
    assert_eq!(
        replicas
            .read_chunk(&page, 0, payload.len() as u64)
            .unwrap()
            .data,
        payload
    );
}

#[cfg(feature = "replica-server")]
#[test]
fn metadata_routes_object_writes_through_a_standalone_star_quorum() {
    exercise_star_object_route(40, 4);
}

#[test]
fn chunk_route_listing_resumes_after_metadata_leader_loss() {
    use matrixobjectstore_rs::cluster::{ChunkGroupCatalog, ChunkGroupConfig, ChunkGroupIdentity};
    let groups: Vec<_> = (0..7)
        .map(|index| ChunkGroupConfig {
            placement_id: format!("group-{index}"),
            failure_domain: format!("rack-{index}"),
            identity: Some(ChunkGroupIdentity {
                version: 3,
                group_id: index + 1,
                node_id: 1,
                capacity_bytes: 1 << 20,
                max_pages: 128,
                voters: vec![1, 2, 3],
                peer_endpoints: Vec::new(),
                bootstrap_voters: Vec::new(),
                bootstrap_id: 0,
            }),
            api_endpoints: (0..3)
                .map(|member| format!("127.0.0.1:{}", 24000 + index * 3 + member))
                .collect(),
            member_ids: (0..3)
                .map(|member| format!("group-{index}-member-{member}"))
                .collect(),
        })
        .collect();
    let catalog = ChunkGroupCatalog::new(groups).unwrap();
    let mut metadata = Group::with_catalog(None, None, Some(catalog.to_bytes().unwrap()));
    let mut client = metadata.client(metadata.leader());
    let first = client.list_chunk_routes(None, 3).unwrap();
    assert!(first.has_more);
    assert_eq!(first.routes.len(), 3);
    assert_eq!(first.routes[0].group.as_ref().unwrap().placement_id, "group-0");
    assert_eq!(first.routes[2].group.as_ref().unwrap().placement_id, "group-2");
    let leader = metadata.leader();
    metadata.kill(leader);
    let mut client = metadata.client(metadata.leader());
    let second = client.list_chunk_routes(Some("group-2"), 3).unwrap();
    assert!(second.has_more);
    assert_eq!(second.routes.len(), 3);
    assert_eq!(second.routes[0].group.as_ref().unwrap().placement_id, "group-3");
    let last = client.list_chunk_routes(Some("group-5"), 3).unwrap();
    assert!(!last.has_more);
    assert_eq!(last.routes.len(), 1);
    assert_eq!(last.routes[0].group.as_ref().unwrap().placement_id, "group-6");
    assert!(client.list_chunk_routes(Some("group-6"), 3).unwrap().routes.is_empty());
}

#[cfg(feature = "replica-server")]
#[test]
#[ignore = "explicit local distributed object scale run"]
fn standalone_star_quorum_writes_and_recovers_256_object_pages() {
    exercise_star_object_route(256, 64 * 1024);
}

#[cfg(feature = "replica-server")]
#[cfg(feature = "replica-server")]
#[test]
fn a_chunk_object_round_trips_through_a_star_quorum_and_a_wrong_digest_fails_its_read() {
    use matrixobjectstore_rs::chunk_catalog::ChunkOutcome;
    use matrixobjectstore_rs::cluster::{
        ChunkGroupCatalog, ChunkGroupConfig, ChunkGroupIdentity, ChunkWriteFailure, ObjectWriteOptions,
        ReplicaClientOptions,
    };
    use matrixobjectstore_rs::object_manifest::ManifestListRequest;
    use matrixobjectstore_rs::ObjectError;

    let root = std::env::temp_dir().join(node_regime::run_token("chunk-object-star"));
    std::fs::create_dir(&root).unwrap();
    let mut chunks = Vec::new();
    for index in 0..3 {
        let journal = root.join(format!("{index}.log"));
        chunks.push(processes::start(
            env!("CARGO_BIN_EXE_chunkserver"),
            &[
                ("MATRIXOBJECT_REPLICA_ADDR", "127.0.0.1:0"),
                ("MATRIXOBJECT_REPLICA_JOURNAL", journal.to_str().unwrap()),
            ],
        ));
    }
    let endpoints: Vec<_> = chunks.iter().map(|node| node.endpoint.clone()).collect();
    let members: Vec<_> = (1..=3).map(|id| format!("star-{id}")).collect();
    let config = ChunkGroupConfig {
        placement_id: "group-a".into(),
        failure_domain: "rack-a".into(),
        identity: Some(ChunkGroupIdentity {
            version: 3,
            group_id: 1,
            node_id: 1,
            capacity_bytes: 1 << 28,
            max_pages: 4096,
            voters: vec![1, 2, 3],
            peer_endpoints: Vec::new(),
            bootstrap_voters: Vec::new(),
            bootstrap_id: 0,
        }),
        api_endpoints: endpoints,
        member_ids: members.clone(),
    };
    let catalog = ChunkGroupCatalog::new(vec![config]).unwrap();
    let metadata = Group::with_catalog(None, None, Some(catalog.to_bytes().unwrap()));
    let mut client = RemoteMetaClient::connect(
        &metadata.nodes[metadata.leader()].as_ref().unwrap().endpoint,
        &MetaClientOptions {
            read_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(10),
            ..Default::default()
        },
    )
    .unwrap();
    for member in &members {
        client.heartbeat_placement_node(member, 0, 1 << 28, false, true, 100).unwrap();
    }
    let options = ObjectWriteOptions {
        page_bytes: 4096,
        target_groups: 1,
        replica: ReplicaClientOptions {
            read_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(10),
            ..Default::default()
        },
        ..Default::default()
    };
    let bytes = |length: usize, salt: usize| -> Vec<u8> { (0..length).map(|index| (index * 31 + salt) as u8).collect() };
    // 4 pages per stripe, so a 40,000-byte object is three extents on the one group
    let stripe = 4 * 4096;

    // one small page, a few pages in one extent, and several extents
    for (name, length) in [("tiny", 100usize), ("few", 3 * 4096 + 17), ("many", 40_000)] {
        let data = bytes(length, length);
        let manifest = client.write_chunk_object("chunk", name, None, &data, &options, stripe).unwrap();
        assert_eq!((manifest.generation, manifest.length), (1, length as u64));
        let (read, got) = client.read_chunk_object("chunk", name, &options).unwrap();
        assert_eq!(read, manifest);
        assert_eq!(got, data, "{name} reads back whole");
        for (offset, span) in [(0usize, 1usize), (length / 2, length / 3), (length - 1, 1), (4095usize.min(length - 1), 2usize.min(length - (4095usize.min(length - 1))))] {
            let slice = client.read_chunk_object_range(&manifest, offset as u64, span as u64, &options).unwrap();
            assert_eq!(slice, &data[offset..offset + span], "{name} range {offset}+{span}");
        }
    }
    assert_eq!(client.read_chunk_manifest("chunk", "many").unwrap().extents.len(), 3);
    assert_eq!(client.read_chunk_manifest("chunk", "tiny").unwrap().extents.len(), 1);

    // an overwrite is one generation later, and one decided from a stale manifest is refused
    let first = client.read_chunk_manifest("chunk", "few").unwrap();
    let second_data = bytes(5000, 7);
    let second = client.write_chunk_object("chunk", "few", Some(&first), &second_data, &options, stripe).unwrap();
    assert_eq!(second.generation, 2);
    assert_eq!(client.read_chunk_object("chunk", "few", &options).unwrap().1, second_data);
    match client.write_chunk_object("chunk", "few", Some(&first), &second_data, &options, stripe) {
        Err(ChunkWriteFailure::BeforePublication(ObjectError::PreconditionFailed(_))) => {}
        other => panic!("a stale writer must be refused before publication: {other:?}"),
    }
    assert_eq!(client.read_chunk_manifest("chunk", "few").unwrap().generation, 2, "the refusal changed nothing");

    // an empty object has no extents and reads back empty
    client.write_chunk_object("chunk", "empty", None, &[], &options, stripe).unwrap();
    assert!(client.read_chunk_object("chunk", "empty", &options).unwrap().1.is_empty());

    // a manifest whose digest does not match the pages it names fails the read by the digest check,
    // whatever the pages hold
    let good = client.read_chunk_manifest("chunk", "many").unwrap();
    let mut wrong = good.clone();
    wrong.generation += 1;
    wrong.extents[1].digest = vec![0xAA; 32];
    client.publish_chunk_manifest(ChunkOutcome::decide(Some(good.clone()), wrong.clone()).unwrap()).unwrap();
    let error = client.read_chunk_object("chunk", "many", &options).unwrap_err();
    assert!(matches!(&error, ObjectError::Corruption(message) if message.contains("integrity")), "{error:?}");
    // a range inside the extents whose digests are right is still served
    assert_eq!(
        client.read_chunk_object_range(&wrong, 0, 100, &options).unwrap(),
        bytes(40_000, 40_000)[0..100].to_vec()
    );

    // a delete is a tombstone: not found, and listed as deleted
    client.delete_chunk_object("chunk", "tiny").unwrap();
    assert!(matches!(client.read_chunk_object("chunk", "tiny", &options), Err(ObjectError::NotFound(_))));
    let page = client
        .list_chunk_manifests(ManifestListRequest {
            version: 2,
            bucket: "chunk".into(),
            after: String::new(),
            limit: 32,
            expected_revision: Vec::new(),
            after_bucket: String::new(),
        })
        .unwrap();
    let listed: Vec<(&str, bool)> = page.entries.iter().map(|entry| (entry.key.as_str(), entry.deleted)).collect();
    assert_eq!(listed, [("empty", false), ("few", false), ("many", false), ("tiny", true)]);

    // the same cluster through the S3 door, with objects stored as chunk manifests
    {
        use matrixobjectstore_rs::net::cluster_gateway::{ClusterGateway, GatewayOptions};
        use matrixobjectstore_rs::net::{Request, RequestHandler};
        let endpoint = metadata.nodes[metadata.leader()].as_ref().unwrap().endpoint.clone();
        let gateway_options = GatewayOptions { write: options.clone(), replica: options.replica.clone(), ..Default::default() };
        let gateway = ClusterGateway::over_chunk_objects(
            move || {
                RemoteMetaClient::connect(
                    &endpoint,
                    &MetaClientOptions {
                        read_timeout: Duration::from_secs(10),
                        write_timeout: Duration::from_secs(10),
                        ..Default::default()
                    },
                )
            },
            gateway_options,
            "gw",
        );
        let call = |method: &str, target: &str, headers: &[(&str, &str)], body: &[u8]| {
            let (path, query) = target.split_once('?').unwrap_or((target, ""));
            gateway.handle(&Request {
                method: method.into(),
                path: path.into(),
                query: query.into(),
                headers: headers.iter().map(|(name, value)| (name.to_ascii_lowercase(), (*value).to_string())).collect(),
                body: body.to_vec(),
                keep_alive: true,
            })
        };
        let body = bytes(30_000, 3);
        let put = call("PUT", "/gw/object", &[], &body);
        assert_eq!(put.status, 200, "{}", String::from_utf8_lossy(&put.body));
        assert!(put.headers.iter().any(|(name, _)| name.eq_ignore_ascii_case("etag")));
        let get = call("GET", "/gw/object", &[], &[]);
        assert_eq!((get.status, get.body.as_slice()), (200, body.as_slice()));
        let head = call("HEAD", "/gw/object", &[], &[]);
        assert_eq!(head.status, 200);
        let ranged = call("GET", "/gw/object", &[("range", "bytes=4090-4101")], &[]);
        assert_eq!((ranged.status, ranged.body.as_slice()), (206, &body[4090..4102]));
        // a replaced object is the next generation and reads back as the new bytes
        let second = bytes(100, 9);
        assert_eq!(call("PUT", "/gw/object", &[], &second).status, 200);
        assert_eq!(call("GET", "/gw/object", &[], &[]).body, second);
        // a copy moves the bytes
        let copy = call("PUT", "/gw/copied", &[("x-amz-copy-source", "/gw/object")], &[]);
        assert_eq!(copy.status, 200, "{}", String::from_utf8_lossy(&copy.body));
        assert_eq!(call("GET", "/gw/copied", &[], &[]).body, second);
        let listing = call("GET", "/gw?list-type=2", &[], &[]);
        assert_eq!(listing.status, 200);
        let listing = String::from_utf8_lossy(&listing.body).to_string();
        assert!(listing.contains("object") && listing.contains("copied"), "{listing}");
        // an append is refused on this path until the log writer serves it
        let append = call("PUT", "/gw/object", &[("x-amz-write-offset-bytes", "100")], b"more");
        assert!(append.status >= 400, "an append must be refused, got {}", append.status);
        assert_eq!(call("DELETE", "/gw/object", &[], &[]).status, 204);
        assert_eq!(call("GET", "/gw/object", &[], &[]).status, 404);
    }
    let _ = std::fs::remove_dir_all(&root);
}

#[cfg(feature = "replica-server")]
#[test]
fn a_reclaim_sweep_frees_the_pages_of_deleted_and_overwritten_objects_and_only_those() {
    use matrixobjectstore_rs::chunk_reclaim::{live_pages_of, Candidates, PageKey};
    use matrixobjectstore_rs::cluster::{
        ChunkGroupCatalog, ChunkGroupConfig, ChunkGroupIdentity, ObjectWriteOptions, ReplicaClientOptions,
    };
    use std::collections::BTreeSet;

    let root = std::env::temp_dir().join(node_regime::run_token("chunk-reclaim-star"));
    std::fs::create_dir(&root).unwrap();
    let mut chunks = Vec::new();
    for index in 0..3 {
        let journal = root.join(format!("{index}.log"));
        chunks.push(processes::start(
            env!("CARGO_BIN_EXE_chunkserver"),
            &[
                ("MATRIXOBJECT_REPLICA_ADDR", "127.0.0.1:0"),
                ("MATRIXOBJECT_REPLICA_JOURNAL", journal.to_str().unwrap()),
            ],
        ));
    }
    let endpoints: Vec<_> = chunks.iter().map(|node| node.endpoint.clone()).collect();
    let members: Vec<_> = (1..=3).map(|id| format!("star-{id}")).collect();
    let config = ChunkGroupConfig {
        placement_id: "group-a".into(),
        failure_domain: "rack-a".into(),
        identity: Some(ChunkGroupIdentity {
            version: 3,
            group_id: 1,
            node_id: 1,
            capacity_bytes: 1 << 28,
            max_pages: 4096,
            voters: vec![1, 2, 3],
            peer_endpoints: Vec::new(),
            bootstrap_voters: Vec::new(),
            bootstrap_id: 0,
        }),
        api_endpoints: endpoints,
        member_ids: members.clone(),
    };
    let catalog = ChunkGroupCatalog::new(vec![config]).unwrap();
    let metadata = Group::with_catalog(None, None, Some(catalog.to_bytes().unwrap()));
    let mut client = RemoteMetaClient::connect(
        &metadata.nodes[metadata.leader()].as_ref().unwrap().endpoint,
        &MetaClientOptions {
            read_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(10),
            ..Default::default()
        },
    )
    .unwrap();
    for member in &members {
        client.heartbeat_placement_node(member, 0, 1 << 28, false, true, 100).unwrap();
    }
    let options = ObjectWriteOptions {
        page_bytes: 4096,
        target_groups: 1,
        replica: ReplicaClientOptions {
            read_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(10),
            ..Default::default()
        },
        ..Default::default()
    };
    let bytes = |length: usize, salt: usize| -> Vec<u8> { (0..length).map(|index| (index * 31 + salt) as u8).collect() };
    let stripe = 4 * 4096;
    let put = |client: &mut RemoteMetaClient, key: &str, data: &[u8], expected: Option<&matrixobjectstore_rs::chunk_manifest::ChunkManifest>| {
        client.write_chunk_object("chunk", key, expected, data, &options, stripe).unwrap()
    };

    // A and B take four pages each (three data pages and a digest page); C takes one, then is rewritten
    let a_data = bytes(3 * 4096, 1);
    let b_data = bytes(3 * 4096, 2);
    let a = put(&mut client, "a", &a_data, None);
    let b = put(&mut client, "b", &b_data, None);
    let c_old = put(&mut client, "c", &bytes(100, 3), None);
    let c_data = bytes(200, 4);
    let c_new = put(&mut client, "c", &c_data, Some(&c_old));
    let pages = |manifests: &[&matrixobjectstore_rs::chunk_manifest::ChunkManifest]| -> BTreeSet<PageKey> {
        live_pages_of(manifests.iter().copied())
    };
    let (a_pages, b_pages, c_old_pages, c_new_pages) = (pages(&[&a]), pages(&[&b]), pages(&[&c_old]), pages(&[&c_new]));
    assert_eq!((a_pages.len(), b_pages.len(), c_old_pages.len(), c_new_pages.len()), (4, 4, 1, 1));
    let inventory = client.reclaim_inventory(&options).unwrap();
    assert_eq!(inventory.len(), 10);
    assert!(inventory.iter().all(|(_, sequence)| *sequence == 1));

    client.delete_chunk_object("chunk", "a").unwrap();
    let mut candidates = Candidates::new();
    let grace = 1_000;

    // the first sight starts the clocks and frees nothing; neither does a sweep before the grace is out
    let first = client.reclaim_sweep(&mut candidates, 0, grace, &options).unwrap();
    assert_eq!((first.inventory_pages, first.unnamed_pages, first.freed.len()), (10, 5, 0));
    let early = client.reclaim_sweep(&mut candidates, grace - 1, grace, &options).unwrap();
    assert!(early.freed.is_empty());

    // once it is out, exactly the deleted object's pages and the overwritten generation's page go
    let swept = client.reclaim_sweep(&mut candidates, grace, grace, &options).unwrap();
    let freed: BTreeSet<PageKey> = swept.freed.iter().cloned().collect();
    let garbage: BTreeSet<PageKey> = a_pages.union(&c_old_pages).cloned().collect();
    assert_eq!(freed, garbage, "the garbage is freed and nothing else");
    let after = client.reclaim_inventory(&options).unwrap();
    for (page, sequence) in &after {
        let expected = if garbage.contains(page) { 2 } else { 1 };
        assert_eq!(*sequence, expected, "{page:?}");
    }
    assert!(after.iter().filter(|(page, _)| b_pages.contains(page) || c_new_pages.contains(page)).all(|(_, s)| *s == 1));

    // the live objects still read back, verified, and a swept one is gone
    assert_eq!(client.read_chunk_object("chunk", "b", &options).unwrap().1, b_data);
    assert_eq!(client.read_chunk_object("chunk", "c", &options).unwrap().1, c_data);
    assert!(client.read_chunk_object("chunk", "a", &options).is_err());
    // a later sweep has nothing left to do
    let idle = client.reclaim_sweep(&mut candidates, 10 * grace, grace, &options).unwrap();
    assert!(idle.freed.is_empty() && idle.unnamed_pages == 0, "{idle:?}");

    // THE LAST CHECK, END TO END: an object that is deleted and then published again between the scan
    // that found its pages unnamed and the discard keeps every page, and still reads back verified.
    let f_data = bytes(3 * 4096, 7);
    let f = put(&mut client, "f", &f_data, None);
    client.delete_chunk_object("chunk", "f").unwrap();
    let tombstone = client.read_chunk_manifest("chunk", "f").unwrap();
    let f_pages = pages(&[&f]);
    let mut watch = Candidates::new();
    let seen = client.reclaim_scan(&mut watch, 20 * grace, grace, &options).unwrap();
    assert!(seen.due_pages().is_empty(), "the first sight frees nothing");
    client.reclaim_apply(seen).unwrap();
    let ripe = client.reclaim_scan(&mut watch, 21 * grace, grace, &options).unwrap();
    let due: BTreeSet<PageKey> = ripe.due_pages().iter().cloned().collect();
    assert_eq!(due, f_pages, "every page of the deleted object is due");
    // the writer comes back between the scan and the discard
    let mut again = f.clone();
    again.generation = tombstone.generation + 1;
    client
        .publish_chunk_manifest(matrixobjectstore_rs::chunk_catalog::ChunkOutcome::decide(Some(tombstone), again).unwrap())
        .unwrap();
    let kept = client.reclaim_apply(ripe).unwrap();
    assert!(kept.freed.is_empty(), "the catalog read again names every page: {kept:?}");
    for (page, sequence) in client.reclaim_inventory(&options).unwrap() {
        if f_pages.contains(&page) {
            assert_eq!(sequence, 1, "{page:?}");
        }
    }
    assert_eq!(client.read_chunk_object("chunk", "f", &options).unwrap().1, f_data);
    let _ = std::fs::remove_dir_all(&root);
}

fn exercise_star_object_route(page_count: usize, page_bytes: usize) {
    use matrixobjectstore_rs::cluster::{
        ChunkGroupCatalog, ChunkGroupConfig, ChunkGroupIdentity, ObjectWriteOptions,
        RemoteReplicaClient, RemoteReplicaSet, ReplicaClientOptions,
    };
    use matrixobjectstore_rs::{ChunkId, PageId};

    let root = std::env::temp_dir().join(node_regime::run_token("star-object-route"));
    std::fs::create_dir(&root).unwrap();
    let journals: Vec<_> = (0..3).map(|index| root.join(format!("{index}.log"))).collect();
    let mut chunks = Vec::new();
    for index in 0..3 {
        chunks.push(processes::start(
            env!("CARGO_BIN_EXE_chunkserver"),
            &[
                ("MATRIXOBJECT_REPLICA_ADDR", "127.0.0.1:0"),
                (
                    "MATRIXOBJECT_REPLICA_JOURNAL",
                    journals[index].to_str().unwrap(),
                ),
            ],
        ));
    }
    let endpoints: Vec<_> = chunks.iter().map(|node| node.endpoint.clone()).collect();
    let members: Vec<_> = (1..=3).map(|id| format!("star-{id}")).collect();
    let config = ChunkGroupConfig {
        placement_id: "group-a".into(),
        failure_domain: "rack-a".into(),
        identity: Some(ChunkGroupIdentity {
            version: 3,
            group_id: 1,
            node_id: 1,
            capacity_bytes: 1 << 28,
            max_pages: 4096,
            voters: vec![1, 2, 3],
            peer_endpoints: Vec::new(),
            bootstrap_voters: Vec::new(),
            bootstrap_id: 0,
        }),
        api_endpoints: endpoints.clone(),
        member_ids: members.clone(),
    };
    let catalog = ChunkGroupCatalog::new(vec![config]).unwrap();
    let metadata = Group::with_catalog(None, None, Some(catalog.to_bytes().unwrap()));
    let connect_metadata = || {
        RemoteMetaClient::connect(
            &metadata.nodes[metadata.leader()].as_ref().unwrap().endpoint,
            &MetaClientOptions {
                read_timeout: Duration::from_secs(10),
                write_timeout: Duration::from_secs(10),
                ..Default::default()
            },
        )
        .unwrap()
    };
    let mut client = connect_metadata();
    for member in &members {
        client
            .heartbeat_placement_node(member, 0, 1 << 28, false, true, 100)
            .unwrap();
    }

    let options = ObjectWriteOptions {
        page_bytes,
        target_groups: 1,
        replica: ReplicaClientOptions {
            read_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(10),
            ..Default::default()
        },
        ..Default::default()
    };
    let first_bytes: Vec<u8> = (0..page_count * page_bytes).map(|index| index as u8).collect();
    let base = client.write_object("star", "append", None, b"prefix", &options).unwrap();
    let suffix = vec![0x7b; page_bytes + 1];
    let appended = client.append_object(base.clone(), &suffix, &options).unwrap();
    assert_eq!(&appended.extents[..base.extents.len()], &base.extents);
    assert_eq!(appended.generation, base.generation + 1);
    let mut append_bytes = b"prefix".to_vec();
    append_bytes.extend_from_slice(&suffix);
    assert_eq!(client.read_object("star", "append", append_bytes.len(), &options.replica)
        .unwrap(), (appended.clone(), append_bytes.clone()));
    assert_eq!(client.read_object_range("star", "append", 5, 2, 2, &options.replica)
        .unwrap().1, [b'x', 0x7b]);
    let mut bounded_append = options.clone();
    bounded_append.max_object_bytes = append_bytes.len();
    assert!(client.append_object(appended.clone(), b"limit", &bounded_append).is_err());
    assert!(client.append_object(base, b"stale", &options).is_err());
    let started = Instant::now();
    let first = client
        .write_object("star", "first", None, &first_bytes, &options)
        .unwrap();
    let write_elapsed = started.elapsed();
    assert_eq!(first.extents.len(), page_count);
    for (offset, length) in [(0, 0), (1, page_bytes + 1), (first_bytes.len(), 0)] {
        let (manifest, bytes) = client.read_object_range(
            "star", "first", offset as u64, length as u64, length, &options.replica,
        ).unwrap();
        assert_eq!(manifest, first);
        assert_eq!(bytes, first_bytes[offset..offset + length]);
    }
    for (offset, length, limit) in [(u64::MAX, 1, 1), (0, 2, 1), (first.length, 1, 1)] {
        assert!(client.read_object_range(
            "star", "first", offset, length, limit, &options.replica,
        ).is_err());
    }
    // The ranged read through the metaserver's slice gives the same bytes, the same identity,
    // the extents under the range and the same refusals as the whole-manifest read.
    for (offset, length) in [(0, 0), (1, page_bytes + 1), (first_bytes.len(), 0), (0, 1),
        (first_bytes.len() - 1, 1), (0, first_bytes.len())] {
        let (slice, bytes) = client.read_object_range_partial(
            "star", "first", offset as u64, length as u64, length, &options.replica,
        ).unwrap();
        assert_eq!(bytes, first_bytes[offset..offset + length]);
        assert_eq!((slice.generation, slice.length), (first.generation, first.length));
        let under: Vec<_> = first.extents.iter().filter(|extent| length != 0
            && extent.object_offset < (offset + length) as u64
            && extent.object_offset + extent.length > offset as u64).cloned().collect();
        assert_eq!(slice.extents, under, "range {offset}+{length}");
    }
    for (offset, length, limit) in [(u64::MAX, 1, 1), (0, 2, 1), (first.length, 1, 1)] {
        let whole = client.read_object_range("star", "first", offset, length, limit, &options.replica)
            .unwrap_err();
        let partial = client.read_object_range_partial("star", "first", offset, length, limit, &options.replica)
            .unwrap_err();
        assert_eq!(format!("{partial:?}"), format!("{whole:?}"));
    }
    assert!(client.read_object_range_partial("star", "absent", 0, 1, 1, &options.replica).is_err());
    // A page freed and reused under a reader (what a reclaimer would do once the grace window had
    // passed): the extent no longer matches its manifest, so the read fails and returns neither the
    // old bytes nor the new ones.
    let reused = client.write_object("star", "reused", None, b"original bytes", &options).unwrap();
    let extent = &reused.extents[0];
    let reused_page = PageId {
        chunk_id: ChunkId::new(extent.volume_id, extent.chunk_id).unwrap(),
        page_index: extent.page_index,
    };
    for endpoint in &endpoints {
        let mut replica = RemoteReplicaClient::connect(endpoint, &options.replica).unwrap();
        replica.write_chunk(reused_page.clone(), extent.page_offset, b"FOREIGN BYTES").unwrap();
    }
    let partial = client.read_object_range_partial("star", "reused", 0, 14, 14, &options.replica);
    assert!(partial.is_err(), "a read of a reused page must fail, got {partial:?}");
    let whole = client.read_object_range("star", "reused", 0, 14, 14, &options.replica);
    assert!(whole.is_err(), "a read of a reused page must fail, got {whole:?}");
    assert_eq!(client.read_object_range_partial("star", "append", 5, 2, 2, &options.replica)
        .unwrap().1, [b'x', 0x7b]);
    assert_eq!(
        client
            .read_object("star", "first", first_bytes.len(), &options.replica)
            .unwrap(),
        (first.clone(), first_bytes.clone())
    );

    // Inventory repair also checks that the initial object is present across
    // the full membership before the failure below exercises routing.
    let mut initial = RemoteReplicaSet::connect(&endpoints, 2, 2, &options.replica).unwrap();
    let mut after = None;
    let mut repaired_pages = 0;
    let mut repair_batches = 0;
    loop {
        let repaired = initial.repair_inventory_batch(after.as_ref(), 64).unwrap();
        assert!(!repaired.blocked, "blocked repair: {repaired:?}");
        assert!(repaired.pages.iter().all(|entry| entry.result.is_ok()));
        repaired_pages += repaired.pages.len();
        repair_batches += 1;
        if !repaired.has_more {
            break;
        }
        after = repaired.resume_after;
    }
    assert_eq!(repaired_pages, first.extents.len() + appended.extents.len() + reused.extents.len());

    let extent = &first.extents[0];
    let page = PageId {
        chunk_id: ChunkId::new(extent.volume_id, extent.chunk_id).unwrap(),
        page_index: extent.page_index,
    };
    for endpoint in &endpoints {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let mut replica =
                RemoteReplicaClient::connect(endpoint, &options.replica).unwrap();
            match replica.read_chunk(&page, 0, extent.length) {
                Ok(read) => {
                    assert_eq!(read.data, &first_bytes[..extent.length as usize]);
                    break;
                }
                Err(error) => {
                    assert!(Instant::now() < deadline, "star member missed write: {error}");
                    std::thread::sleep(Duration::from_millis(20));
                }
            }
        }
    }

    chunks[0].child.kill().unwrap();
    assert!(processes::reap_within(&mut chunks[0].child, Duration::from_secs(3)).is_some());
    let pending_suffix = vec![0x39; page_bytes * 33 + 1];
    let append_candidate = match client.append_object(
        appended.clone(), &pending_suffix, &options,
    ).unwrap_err() {
        matrixobjectstore_rs::cluster::ObjectWriteFailure::BeforePublication {
            staged: Some(candidate), ..
        } => *candidate,
        error => panic!("missing staged append: {error:?}"),
    };
    assert!(client.resume_unpublished_append(
        appended.clone(), append_candidate.clone(), &pending_suffix, &options,
    ).is_err());
    assert_eq!(client.read_manifest("star", "append").unwrap(), appended);
    drop(client);
    let mut client = connect_metadata();
    assert_eq!(
        client
            .read_object("star", "first", first_bytes.len(), &options.replica)
            .unwrap()
            .1,
        first_bytes
    );
    let second_bytes = b"all members acknowledge writes";
    let candidate = match client
        .write_object("star", "second", None, second_bytes, &options)
        .unwrap_err() {
        matrixobjectstore_rs::cluster::ObjectWriteFailure::BeforePublication {
            staged: Some(candidate), ..
        } => *candidate,
        error => panic!("missing staged candidate: {error:?}"),
    };
    assert!(client.resume_unpublished_object(
        None, candidate.clone(), second_bytes, &options,
    ).is_err());
    assert!(client.read_manifest("star", "second").is_err());

    let owned_payload = vec![0x6d; page_bytes * 33 + 1];
    let owned_bytes = owned_payload.as_slice();
    let owned = client.allocate_staged_object("star", "owned-pending", None,
        owned_bytes.len().div_ceil(page_bytes) as u32, false).unwrap();
    let owned_candidate = match client.write_staged_object(&owned, None, owned_bytes, &options).unwrap_err() {
        matrixobjectstore_rs::cluster::ObjectWriteFailure::BeforePublication { staged: Some(candidate), .. } => *candidate,
        error => panic!("missing owned candidate: {error:?}"),
    };
    assert_eq!(client.read_staged_allocation(owned.allocation_id).unwrap(), owned);
    assert!(client.publish_manifest(matrixobjectstore_rs::object_manifest::ManifestOutcome::decide(
        None, owned_candidate.clone()).unwrap(), 0).is_err());
    assert!(client.resume_staged_object(&owned, None, owned_candidate.clone(), owned_bytes, &options).is_err());
    let aborted = client.allocate_staged_object("star", "owned-aborted", None,
        owned_bytes.len().div_ceil(page_bytes) as u32, false).unwrap();
    let aborted_candidate = match client.write_staged_object(&aborted, None, owned_bytes, &options).unwrap_err() {
        matrixobjectstore_rs::cluster::ObjectWriteFailure::BeforePublication { staged: Some(candidate), .. } => *candidate,
        error => panic!("missing aborted candidate: {error:?}"),
    };
    client.abort_staged_allocation(aborted.allocation_id).unwrap();
    assert!(client.resume_staged_object(&aborted, None, aborted_candidate.clone(), owned_bytes, &options).is_err());

    chunks[0] = processes::start(
        env!("CARGO_BIN_EXE_chunkserver"),
        &[
            ("MATRIXOBJECT_REPLICA_ADDR", &endpoints[0]),
            (
                "MATRIXOBJECT_REPLICA_JOURNAL",
                journals[0].to_str().unwrap(),
            ),
        ],
    );
    assert_eq!(chunks[0].endpoint, endpoints[0]);
    drop(client);
    let mut client = connect_metadata();
    assert!(client.resume_unpublished_object(
        None, candidate.clone(), second_bytes, &options,
    ).is_err());
    assert!(client.read_manifest("star", "second").is_err());
    let mut repair = RemoteReplicaSet::connect(&endpoints, 2, 2, &options.replica).unwrap();
    let mut cursor = None;
    loop {
        let batch = repair.repair_inventory_batch(cursor.as_ref(), 64).unwrap();
        assert!(!batch.blocked, "blocked candidate repair: {batch:?}");
        assert!(batch.pages.iter().all(|entry| entry.result.is_ok()));
        if !batch.has_more { break; }
        cursor = batch.resume_after;
    }
    drop(repair);
    let unwritten = append_candidate.extents.last().unwrap();
    let unwritten_page = PageId {
        chunk_id: ChunkId::new(unwritten.volume_id, unwritten.chunk_id).unwrap(),
        page_index: unwritten.page_index,
    };
    for endpoint in &endpoints {
        let mut replica = RemoteReplicaClient::connect(endpoint, &options.replica).unwrap();
        assert!(matches!(replica.read_chunk(&unwritten_page, 0, unwritten.length),
            Err(matrixobjectstore_rs::ObjectError::NotFound(_))));
    }
    let mut wrong_suffix = pending_suffix.clone();
    wrong_suffix[0] ^= 1;
    assert!(client.resume_unpublished_append(
        appended.clone(), append_candidate.clone(), &wrong_suffix, &options,
    ).is_err());
    let resumed = client.resume_unpublished_append(
        appended.clone(), append_candidate.clone(), &pending_suffix, &options,
    ).unwrap();
    assert_eq!(client.resume_staged_object(&owned, None, owned_candidate.clone(), owned_bytes, &options)
        .unwrap(), owned_candidate);
    assert!(client.read_staged_allocation(owned.allocation_id).is_err());
    assert_eq!(client.resume_staged_object(&owned, None, owned_candidate.clone(), owned_bytes, &options)
        .unwrap(), owned_candidate);
    assert!(client.resume_staged_object(&aborted, None, aborted_candidate.clone(), owned_bytes, &options).is_err());
    let owned_append = client.allocate_staged_object("star", "owned-pending", Some(owned_candidate.generation), 1, true).unwrap();
    let owned_updated = client.write_staged_object(&owned_append, Some(owned_candidate.clone()), b"!", &options).unwrap();
    assert_eq!(&owned_updated.extents[..owned_candidate.extents.len()], &owned_candidate.extents);
    let mut owned_expected = owned_bytes.to_vec();
    owned_expected.push(b'!');
    assert_eq!(client.read_object("star", "owned-pending", owned_expected.len(), &options.replica).unwrap(),
        (owned_updated, owned_expected));
    assert_eq!(resumed, append_candidate);
    assert_eq!(&resumed.extents[..appended.extents.len()], &appended.extents);
    append_bytes.extend_from_slice(&pending_suffix);
    assert_eq!(client.read_object("star", "append", append_bytes.len(), &options.replica)
        .unwrap(), (resumed.clone(), append_bytes));
    assert_eq!(client.resume_unpublished_append(
        appended.clone(), resumed.clone(), &pending_suffix, &options,
    ).unwrap(), resumed);
    let second = client.resume_unpublished_object(
        None, candidate.clone(), second_bytes, &options,
    ).unwrap();
    assert_eq!(second, candidate);
    assert_eq!(
        client
            .read_object("star", "second", second_bytes.len(), &options.replica)
            .unwrap(),
        (second, second_bytes.to_vec())
    );
    let mut returned = RemoteReplicaClient::connect(&endpoints[0], &options.replica).unwrap();
    let rejoin_deadline = Instant::now() + Duration::from_secs(12);
    let mut attempt = 0;
    let (third_key, third, third_bytes) = loop {
        let key = format!("third-{attempt}");
        let bytes = format!("returning member rejoins later write {attempt}").into_bytes();
        let manifest = client
            .write_object("star", &key, None, &bytes, &options)
            .unwrap();
        let extent = &manifest.extents[0];
        let page = PageId {
            chunk_id: ChunkId::new(extent.volume_id, extent.chunk_id).unwrap(),
            page_index: extent.page_index,
        };
        match returned.read_chunk(&page, 0, extent.length) {
            Ok(read) => {
                assert_eq!(read.data, bytes[..extent.length as usize]);
                break (key, manifest, bytes);
            }
            Err(_) => {
                assert!(
                    Instant::now() < rejoin_deadline,
                    "returning star member did not rejoin later writes"
                );
                attempt += 1;
                std::thread::sleep(Duration::from_millis(20));
            }
        }
    };
    assert_eq!(
        client
            .read_object("star", &third_key, third_bytes.len(), &options.replica)
            .unwrap(),
        (third.clone(), third_bytes.clone())
    );
    drop(returned);
    for node in &mut chunks[..2] {
        node.child.kill().unwrap();
        assert!(processes::reap_within(&mut node.child, Duration::from_secs(3)).is_some());
    }
    drop(client);
    let mut minority = connect_metadata();
    assert_eq!(
        minority
            .read_object("star", &third_key, third_bytes.len(), &options.replica)
            .unwrap(),
        (third.clone(), third_bytes.clone())
    );
    chunks[2].child.kill().unwrap();
    assert!(processes::reap_within(&mut chunks[2].child, Duration::from_secs(3)).is_some());
    let retained = minority.retain_manifest("star", &third_key, third.generation).unwrap();
    let copied = minority.copy_object(
        "star", &third_key, "star", "copy", None, &options,
    ).unwrap();
    assert_eq!(copied.extents, third.extents);
    assert_eq!(copied.length, third.length);
    assert!(minority.copy_object(
        "star", &third_key, "star", "copy", None, &options,
    ).is_err());
    let mut bounded_copy = options.clone();
    bounded_copy.max_object_bytes = 0;
    assert!(minority.copy_object(
        "star", &third_key, "star", "too-large-copy", None, &bounded_copy,
    ).is_err());
    let publication_fence = minority.enable_publication_fence().unwrap();
    assert_eq!(minority.read_publication_fence().unwrap(), publication_fence);
    assert!(minority.allocate_object_pages(1).is_err());
    assert!(minority.publish_manifest(matrixobjectstore_rs::object_manifest::ManifestOutcome::decide(
        None, aborted_candidate).unwrap(), 0).is_err());
    assert!(minority.copy_object("star", &third_key, "star", "legacy-copy", None, &options).is_err());
    for member in &members {
        minority.heartbeat_placement_node(member, 0, 1 << 28, false, false, 101).unwrap();
    }
    let atomic = minority.copy_object_atomic("star", &third_key, "other", "atomic-copy", None,
        options.max_object_bytes as u64, 0).unwrap();
    assert_eq!(atomic.extents, third.extents);
    minority.delete_object("star", &third_key, 0).unwrap();
    assert!(minority.copy_object(
        "star", &third_key, "star", "deleted-copy", None, &options,
    ).is_err());
    chunks[2] = processes::start(
        env!("CARGO_BIN_EXE_chunkserver"),
        &[("MATRIXOBJECT_REPLICA_ADDR", &endpoints[2]),
          ("MATRIXOBJECT_REPLICA_JOURNAL", journals[2].to_str().unwrap())],
    );
    drop(minority);
    let mut minority = connect_metadata();
    assert_eq!(minority.read_publication_fence().unwrap(), publication_fence);
    assert_eq!(minority.read_object("other", "atomic-copy", third_bytes.len(), &options.replica).unwrap(),
        (atomic, third_bytes.clone()));
    assert_eq!(minority.read_retained_object(retained.retention_id, third_bytes.len(), &options.replica)
        .unwrap(), (third.clone(), third_bytes.clone()));
    assert!(minority.read_retained_object(retained.retention_id, 0, &options.replica).is_err());
    minority.release_manifest(retained.retention_id).unwrap();
    assert!(minority.read_retained_object(retained.retention_id, third_bytes.len(), &options.replica).is_err());
    assert_eq!(minority.read_object("star", "copy", third_bytes.len(), &options.replica)
        .unwrap(), (copied, third_bytes));
    println!(
        "STAR_OBJECT pages={page_count} page_bytes={page_bytes} write_ms={} repair_batches={repair_batches} verified_copies={}",
        write_elapsed.as_millis(),
        page_count * endpoints.len()
    );

    // Damage an unrequested extent on the only surviving replica. A small
    // range still succeeds, while reading the damaged extent must fail.
    let last = first.extents.last().unwrap();
    let last_page = PageId {
        chunk_id: ChunkId::new(last.volume_id, last.chunk_id).unwrap(),
        page_index: last.page_index,
    };
    let mut survivor = RemoteReplicaClient::connect(&endpoints[2], &options.replica).unwrap();
    survivor.write_chunk(last_page, 0, b"damage").unwrap();
    assert_eq!(minority.read_object_range(
        "star", "first", 0, 1, 1, &options.replica,
    ).unwrap().1, first_bytes[..1]);
    assert!(minority.read_object_range(
        "star", "first", last.object_offset, last.length, last.length as usize, &options.replica,
    ).is_err());
    drop(survivor);

    for index in 0..2 {
        chunks[index] = processes::start(env!("CARGO_BIN_EXE_chunkserver"),
            &[("MATRIXOBJECT_REPLICA_ADDR", &endpoints[index]),
              ("MATRIXOBJECT_REPLICA_JOURNAL", journals[index].to_str().unwrap())]);
    }
    for member in &members {
        minority.heartbeat_placement_node(member, 0, 1 << 28, false, true, 102).unwrap();
    }
    let prepared = minority.prepare_staged_allocation("star", "leased", None, 4, false, 60_000).unwrap();
    let leased = minority.allocate_prepared_staged_object(&prepared).unwrap();
    assert_eq!(minority.allocate_prepared_staged_object(&prepared).unwrap(), leased);
    let leased = minority.renew_staged_allocation(&leased, 120_000).unwrap();
    let leased_value = minority.write_staged_object(&leased, None, b"leased star", &options).unwrap();
    assert!(minority.allocate_prepared_staged_object(&prepared).is_err());

    assert_eq!(minority.read_object("star", "leased", 32, &options.replica).unwrap(),
        (leased_value.clone(), b"leased star".to_vec()));
    assert_eq!(minority.read_object_range("star", "leased", 1, 1, 1, &options.replica).unwrap().1,
        b"e");
    assert_eq!(minority.read_objects(&[("star", "leased", 32), ("star", "leased", 32)],
        &options.replica).unwrap(),
        vec![(leased_value.clone(), b"leased star".to_vec()); 2]);
    assert!(minority.list_retained_manifests(0, 32).unwrap().0.is_empty());
    assert_eq!(minority.read_object_in_session("star", "leased", 32, &options.replica, 60_000).unwrap(),
        (leased_value, b"leased star".to_vec()));
    assert!(minority.read_staged_allocation(leased.allocation_id).is_err());

    drop(minority);
    drop(metadata);
    drop(chunks);
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(feature = "replica-server")]
#[test]
fn chunk_processes_report_usage_follow_leaders_and_expire_without_manual_heartbeats() {
    run_chunk_health(false);
}
#[cfg(feature = "replica-server")]
#[test]
fn verified_chunk_heartbeats_follow_metadata_failover_and_chunk_restart() {
    run_chunk_health(true);
}
#[cfg(feature = "replica-server")]
fn run_chunk_health(verified: bool) {
    use matrixobjectstore_rs::cluster::{RemoteReplicaClient, ReplicaClientOptions};
    use matrixobjectstore_rs::{ChunkId, PageId};
    struct Chunks {
        nodes: Vec<Option<NodeProcess>>,
        root: PathBuf,
    }
    impl Drop for Chunks {
        fn drop(&mut self) {
            self.nodes.clear();
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }
    fn wait_roster(
        client: &mut RemoteMetaClient,
        expected: impl Fn(&[matrixobjectstore_rs::cluster::PlacementNodeSummary]) -> bool,
    ) {
        let until = Instant::now() + Duration::from_secs(12);
        loop {
            if let Ok(nodes) = client.list_placement_nodes() {
                if expected(&nodes) {
                    return;
                }
            }
            assert!(
                Instant::now() < until,
                "automatic health did not reach the expected state"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }
    let reserved: Vec<_> = (0..3)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let endpoints: Vec<_> = reserved
        .iter()
        .map(|s| s.local_addr().unwrap().to_string())
        .collect();
    let roster = endpoints
        .iter()
        .enumerate()
        .map(|(i, e)| format!("auto-{i}@{e}@rack-{i}"))
        .collect::<Vec<_>>()
        .join(",");
    let mut group = Group::with_health(Some(roster), Some("1500".into()));
    let initial_leader = group.leader();
    let metadata = group
        .nodes
        .iter()
        .map(|n| n.as_ref().unwrap().endpoint.clone())
        .collect::<Vec<_>>()
        .join(",");
    let root = std::env::temp_dir().join(node_regime::run_token("automatic-health"));
    std::fs::create_dir(&root).unwrap();
    let mut chunks = Chunks {
        nodes: Vec::new(),
        root,
    };
    drop(reserved);
    let identity_file = chunks.root.join("metadata.identity");
    if verified {
        use prost::Message;
        let identity = group
            .client(initial_leader)
            .metadata_group_identity()
            .unwrap();
        std::fs::write(&identity_file, identity.encode_to_vec()).unwrap();
    }
    let start = |i: usize, path: &std::path::Path| {
        let node_id = format!("auto-{i}");
        let mut environment: Vec<(&str, &str)> = vec![
            ("MATRIXOBJECT_REPLICA_ADDR", &endpoints[i]),
            ("MATRIXOBJECT_REPLICA_JOURNAL", path.to_str().unwrap()),
            ("MATRIXOBJECT_REPLICA_CAPACITY_BYTES", "8192"),
            ("MATRIXOBJECT_CHUNK_NODE_ID", node_id.as_str()),
            ("MATRIXOBJECT_CHUNK_METADATA_ENDPOINTS", &metadata),
            ("MATRIXOBJECT_CHUNK_HEARTBEAT_MS", "100"),
        ];
        if verified {
            environment.push((
                "MATRIXOBJECT_CHUNK_METADATA_IDENTITY_FILE",
                identity_file.to_str().unwrap(),
            ));
        }
        processes::start(env!("CARGO_BIN_EXE_chunkserver"), &environment)
    };
    for i in 0..3 {
        chunks
            .nodes
            .push(Some(start(i, &chunks.root.join(format!("{i}.log")))));
    }
    let mut client = group.client(initial_leader);
    wait_roster(&mut client, |nodes| {
        nodes.len() == 3
            && nodes
                .iter()
                .all(|n| n.online && n.capacity_bytes == 8192 && n.used_bytes == 0)
    });
    let page = PageId {
        chunk_id: ChunkId::new(7, 2).unwrap(),
        page_index: 0,
    };
    let mut replica =
        RemoteReplicaClient::connect(&endpoints[0], &ReplicaClientOptions::default()).unwrap();
    replica.write_chunk(page.clone(), 4096, b"abc").unwrap();
    replica.discard_chunk(&page, 0, 1024).unwrap();
    wait_roster(&mut client, |nodes| {
        nodes
            .iter()
            .any(|n| n.node_id == "auto-0" && n.used_bytes == 4099)
    });
    assert!(replica.write_chunk(page.clone(), 8192, b"x").is_err());
    wait_roster(&mut client, |nodes| {
        nodes
            .iter()
            .any(|n| n.node_id == "auto-0" && n.used_bytes == 4099)
    });
    drop(client);
    group.kill(initial_leader);
    let mut client = group.client(group.leader());
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    wait_roster(&mut client, |nodes| {
        nodes.iter().all(|n| n.online && n.last_heartbeat_ms >= now)
    });
    drop(replica);
    let mut killed = chunks.nodes[0].take().unwrap();
    killed.child.kill().unwrap();
    assert!(processes::reap_within(&mut killed.child, Duration::from_secs(3)).is_some());
    drop(killed);
    wait_roster(&mut client, |nodes| {
        nodes.iter().all(|n| n.online == (n.node_id != "auto-0"))
    });
    let plan = client
        .plan_object_placement("bucket", "remaining", 1, 2)
        .unwrap();
    assert!(!plan.node_ids.contains(&"auto-0".into()));
    chunks.nodes[0] = Some(start(0, &chunks.root.join("0.log")));
    wait_roster(&mut client, |nodes| {
        nodes.iter().all(|n| n.online)
            && nodes
                .iter()
                .any(|n| n.node_id == "auto-0" && n.used_bytes == 4099)
    });
    let mut replica =
        RemoteReplicaClient::connect(&endpoints[0], &ReplicaClientOptions::default()).unwrap();
    assert_eq!(replica.read_chunk(&page, 4096, 3).unwrap().data, b"abc");
}

#[test]
fn metadata_raft_recovers_chunk_catalog_and_group_health() {
    use matrixobjectstore_rs::cluster::{ChunkGroupCatalog, ChunkGroupConfig, ChunkGroupIdentity};
    let config = ChunkGroupConfig {
        placement_id: "group-a".into(),
        failure_domain: "rack-a".into(),
        identity: Some(ChunkGroupIdentity {
            bootstrap_voters: Vec::new(),
            bootstrap_id: 0,
            version: 1,
            group_id: 1,
            node_id: 1,
            capacity_bytes: 65536,
            max_pages: 64,
            voters: vec![1, 2, 3],
            peer_endpoints: vec![
                "127.0.0.1:9101".into(),
                "127.0.0.1:9102".into(),
                "127.0.0.1:9103".into(),
            ],
        }),
        api_endpoints: vec![
            "127.0.0.1:9201".into(),
            "127.0.0.1:9202".into(),
            "127.0.0.1:9203".into(),
        ],
        member_ids: vec!["member-a".into(), "member-b".into(), "member-c".into()],
    };
    let catalog = ChunkGroupCatalog::new(vec![config.clone()]).unwrap();
    let mut group = Group::with_catalog(None, None, Some(catalog.to_bytes().unwrap()));
    let leader = group.leader();
    let mut client = group.client(leader);
    // Starting with a follower must route to a confirmed leader for catalog reads.
    let mut endpoints: Vec<_> = (0..3).filter(|id| *id != leader)
        .map(|id| group.nodes[id].as_ref().unwrap().endpoint.clone()).collect();
    endpoints.push(group.nodes[leader].as_ref().unwrap().endpoint.clone());
    let mut routed = RemoteMetaClient::connect_catalog(&endpoints, &MetaClientOptions {
        read_timeout: Duration::from_secs(2),
        ..Default::default()
    }).unwrap();
    assert_ne!(routed.endpoint(), group.nodes[leader].as_ref().unwrap().endpoint);
    assert_eq!(routed.read_chunk_route("group-a").unwrap().group.unwrap(), config);
    assert_eq!(routed.endpoint(), group.nodes[leader].as_ref().unwrap().endpoint);
    let wal = group.root.join(leader.to_string()).join("metadata.peer.wal");
    let before = std::fs::read(&wal).unwrap();
    for _ in 0..16 {
        assert_eq!(client.read_chunk_group("group-a").unwrap(), config);
        assert_eq!(client.read_chunk_route("group-a").unwrap().group.unwrap(), config);
        assert!(matches!(client.read_chunk_group("missing"),
            Err(matrixobjectstore_rs::ObjectError::NotFound(_))));
    }
    assert_eq!(std::fs::read(&wal).unwrap(), before, "catalog reads must not append outcomes");
    for id in ["member-a", "member-b"] {
        client
            .heartbeat_placement_node(id, 123, 65536, false, true, 1000)
            .unwrap();
    }
    let plan = client
        .plan_object_placement("bucket", "key", 100, 1)
        .unwrap();
    assert_eq!(plan.node_ids, vec!["group-a"]);
    group.kill(leader);
    let next = group.leader();
    assert_eq!(
        group.client(next).read_chunk_group("group-a").unwrap(),
        config
    );
    for i in 0..3 {
        if group.nodes[i].is_some() {
            group.kill(i);
        }
    }
    // A different valid bootstrap cannot replace already committed membership.
    std::fs::write(
        group.catalog.as_ref().unwrap(),
        ChunkGroupCatalog::default().to_bytes().unwrap(),
    )
    .unwrap();
    for i in 0..3 {
        group.start(i);
    }
    let current = group.leader();
    let mut client = group.client(current);
    assert_eq!(client.read_chunk_group("group-a").unwrap(), config);
        assert_eq!(client.read_chunk_route("group-a").unwrap().group.unwrap(), config);
    let nodes = client.list_placement_nodes().unwrap();
    let logical = nodes.iter().find(|n| n.node_id == "group-a").unwrap();
    assert!(logical.online);
    assert_eq!(logical.used_bytes, 123);
    for i in 0..3 {
        assert!(
            std::fs::read(group.root.join(i.to_string()).join("metadata.peer.base"))
                .unwrap()
                .starts_with(b"MOGB")
        );
    }
    // A minority can describe its immutable identity, but cannot expose catalog
    // state, health, placements, manifests, or fresh page namespaces.
    for id in 0..3 {
        if id != current {
            group.kill(id);
        }
    }
    assert!(client.read_chunk_group("group-a").is_err());
    assert!(group.client(current).read_chunk_route("group-a").is_err());
    assert!(!matches!(group.client(current).read_chunk_route("missing").unwrap_err(), matrixobjectstore_rs::ObjectError::NotFound(_)));
    let missing_error = group.client(current).read_chunk_group("missing").unwrap_err();
    assert!(!matches!(missing_error, matrixobjectstore_rs::ObjectError::NotFound(_)),
        "missing catalog entries also require a confirmed read");
    assert!(client.list_placement_nodes().is_err());
    assert!(group
        .client(current)
        .plan_object_placement("bucket", "key", 100, 1)
        .is_err());
    let error = group
        .client(current)
        .read_manifest("bucket", "key")
        .unwrap_err();
    assert!(
        !matches!(error, matrixobjectstore_rs::ObjectError::NotFound(_)),
        "a minority must not answer from its mutable manifest catalog"
    );
    assert!(group.client(current).allocate_object_pages(1).is_err());
}

#[test]
fn invalid_chunk_catalog_startup_is_refused_before_serving() {
    use std::process::{Command, Stdio};
    let root = std::env::temp_dir().join(node_regime::run_token("invalid-catalog"));
    std::fs::create_dir(&root).unwrap();
    let path = root.join("catalog.bin");
    let valid = matrixobjectstore_rs::cluster::ChunkGroupCatalog::default()
        .to_bytes()
        .unwrap();
    for (payload, conflict) in [
        (b"bad".to_vec(), None),
        (vec![0; 2 * 1024 * 1024 + 5], None),
        (valid.clone(), Some("MATRIXOBJECT_METASERVER_DATA_ROSTER")),
        (
            valid.clone(),
            Some("MATRIXOBJECT_METASERVER_SEED_DATA_NODES"),
        ),
        (valid, Some("MATRIXOBJECT_METASERVER_SEED_CAPACITY_BYTES")),
    ] {
        std::fs::write(&path, payload).unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_metaserver"));
        for (key, _) in std::env::vars_os() {
            if key.to_string_lossy().starts_with("MATRIXOBJECT_") {
                command.env_remove(key);
            }
        }
        command
            .env("MATRIXOBJECT_METASERVER_ADDR", "127.0.0.1:0")
            .env("MATRIXOBJECT_METASERVER_CHUNK_GROUPS_FILE", &path)
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        if let Some(key) = conflict {
            command.env(key, "0");
        }
        let mut child = command.spawn().unwrap();
        match processes::reap_within(&mut child, Duration::from_secs(3)) {
            Some(status) => assert_eq!(status, 2),
            None => {
                let _ = child.kill();
                let _ = child.wait();
                panic!("invalid catalog started serving");
            }
        }
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn metadata_quorum_reads_leave_the_wal_unchanged_and_refuse_a_minority() {
    let mut group = Group::new();
    let leader = group.leader();
    let mut client = group.client(leader);
    let plan = client
        .plan_object_placement("bucket", "readable", 1024, 2)
        .unwrap();
    let placement = CommittedPlacement {
        bucket: plan.bucket,
        key: plan.key,
        length: plan.length,
        node_ids: plan.node_ids,
        version: 1,
        update_time_ms: 1,
    };
    let saved = client
        .commit_object_placement(placement, &plan.plan_checksum)
        .unwrap();
    let path = group
        .root
        .join(leader.to_string())
        .join("metadata.peer.wal");
    let before = std::fs::read(&path).unwrap();
    std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for _ in 0..4 {
            let mut client = group.client(leader);
            let saved = &saved;
            handles.push(scope.spawn(move || {
                for _ in 0..16 {
                    assert_eq!(
                        client.object_placement("bucket", "readable").unwrap(),
                        *saved
                    );
                    assert_eq!(client.list_placement_nodes().unwrap().len(), 3);
                    client
                        .plan_object_placement("bucket", "planned", 1024, 2)
                        .unwrap();
                    client.probe().into_result().unwrap();
                }
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }
    });
    assert_eq!(std::fs::read(&path).unwrap(), before);
    for i in 0..3 {
        if i != leader {
            group.kill(i);
        }
    }
    assert!(client.object_placement("bucket", "readable").is_err());
    assert!(group.client(leader).probe().into_result().is_err());
}

#[test]
fn metadata_checkpoints_restore_placements_roster_and_lagging_processes() {
    let mut group = Group::with_catalog_checkpoints(None, None, None, 4096);
    let leader = group.leader();
    let lagging = (leader + 1) % 3;
    let other = (leader + 2) % 3;
    group.kill(lagging);
    let mut client = group.client(leader);
    let mut saved = Vec::new();
    for id in 0..80u64 {
        let plan = client
            .plan_object_placement("bucket", &format!("checkpoint-{id}"), 1024, 2)
            .unwrap();
        let placement = CommittedPlacement {
            bucket: plan.bucket,
            key: plan.key,
            length: plan.length,
            node_ids: plan.node_ids,
            version: 1,
            update_time_ms: id + 10,
        };
        saved.push(
            client
                .commit_object_placement(placement, &plan.plan_checksum)
                .unwrap(),
        );
    }
    client
        .heartbeat_placement_node("data-0", 12345, 1 << 30, true, true, 900)
        .unwrap();
    let roster = client.list_placement_nodes().unwrap();
    node_regime::checkpoints::wait_for_checkpoint(
        &group
            .root
            .join(leader.to_string())
            .join("metadata.peer.wal"),
        4,
    );
    group.start(lagging);
    node_regime::checkpoints::wait_for_checkpoint(
        &group
            .root
            .join(lagging.to_string())
            .join("metadata.peer.wal"),
        4,
    );
    group.kill(other);
    // Publishing a snapshot does not promise the old leader retains its term.
    // Confirm a read quorum on the two remaining voters before issuing a write.
    drop(client);
    let active = group.leader();
    let mut client = group.client(active);
    client
        .heartbeat_placement_node("data-1", 777, 1 << 30, true, false, 901)
        .unwrap();
    let final_roster = client.list_placement_nodes().unwrap();
    assert_ne!(final_roster, roster);
    drop(client);
    group.kill(leader);
    group.kill(lagging);
    for id in 0..3 {
        group.start(id);
    }
    let recovered = group.leader();
    let mut client = group.client(recovered);
    for placement in saved {
        assert_eq!(
            client
                .object_placement(&placement.bucket, &placement.key)
                .unwrap(),
            placement
        );
    }
    assert_eq!(client.list_placement_nodes().unwrap(), final_roster);
    // Persisted draining/offline nodes must remain ineligible after recovery.
    assert!(client
        .plan_object_placement("bucket", "after-recovery", 64, 2)
        .is_err());
    client
        .heartbeat_placement_node("data-1", 777, 1 << 30, false, true, 902)
        .unwrap();
    let plan = client
        .plan_object_placement("bucket", "after-recovery", 64, 2)
        .unwrap();
    client
        .commit_object_placement(
            CommittedPlacement {
                bucket: plan.bucket,
                key: plan.key,
                length: plan.length,
                node_ids: plan.node_ids,
                version: 1,
                update_time_ms: 902,
            },
            &plan.plan_checksum,
        )
        .unwrap();
}

#[test]
fn verified_metadata_catalog_refuses_foreign_groups_and_survives_recovery() {
    use matrixobjectstore_rs::ObjectError;
    let mut group = Group::new();
    let other = Group::new();
    let leader = group.leader();
    let foreign_leader = other.leader();
    let follower = (leader + 1) % 3;
    let expected = group.client(leader).metadata_group_identity().unwrap();
    for i in 0..3 {
        let identity = group.client(i).metadata_group_identity().unwrap();
        assert_eq!(identity.node_id, i as u64 + 1);
        assert!(expected.same_group(&identity));
    }
    let foreign = other
        .client(foreign_leader)
        .metadata_group_identity()
        .unwrap();
    assert_eq!(expected.group_id, foreign.group_id);
    assert!(!expected.same_group(&foreign));
    let opts = MetaClientOptions {
        read_timeout: Duration::from_secs(2),
        write_timeout: Duration::from_secs(2),
        ..Default::default()
    };
    let mixed = vec![
        group.api_addresses[follower].to_string(),
        other.api_addresses[foreign_leader].to_string(),
    ];
    let mut client =
        RemoteMetaClient::connect_verified_raft_catalog(&mixed, &expected, &opts).unwrap();
    // The first follower refuses admission; the foreign leader must never receive this mutation.
    let before = other.client(foreign_leader).probe().into_result().unwrap();
    let plan = group
        .client(leader)
        .plan_object_placement("bucket", "isolated", 4, 1)
        .unwrap();
    let placement = CommittedPlacement {
        bucket: "bucket".into(),
        key: "isolated".into(),
        version: 1,
        length: 4,
        node_ids: plan.node_ids,
        update_time_ms: 1,
    };
    assert!(matches!(
        client.commit_object_placement_if_current(placement.clone(), None, &plan.plan_checksum),
        Err(ObjectError::PreconditionFailed(_))
    ));
    assert_eq!(
        other.client(foreign_leader).probe().into_result().unwrap(),
        before
    );
    assert!(matches!(
        other
            .client(foreign_leader)
            .object_placement("bucket", "isolated"),
        Err(ObjectError::NotFound(_))
    ));
    assert!(RemoteMetaClient::connect_verified_raft_catalog(
        &[other.api_addresses[foreign_leader].to_string()],
        &expected,
        &opts
    )
    .is_err());
    let mut wrong = expected.clone();
    wrong.bootstrap_id ^= 1;
    if wrong.bootstrap_id == 0 {
        wrong.bootstrap_id = 2;
    }
    assert!(RemoteMetaClient::connect_verified_raft_catalog(
        &[group.api_addresses[leader].to_string()],
        &wrong,
        &opts
    )
    .is_err());
    assert!(RemoteMetaClient::connect_raft_catalog(
        &[group.api_addresses[leader].to_string()],
        expected.group_id + 1,
        &opts
    )
    .is_err());
    let catalog: Vec<_> = (0..3)
        .map(|i| group.api_addresses[(follower + i) % 3].to_string())
        .collect();
    let mut client =
        RemoteMetaClient::connect_verified_raft_catalog(&catalog, &expected, &opts).unwrap();
    assert_eq!(
        client
            .commit_object_placement_if_current(placement.clone(), None, &plan.plan_checksum)
            .unwrap(),
        placement
    );
    group.kill(leader);
    group.leader();
    assert_eq!(
        client.object_placement("bucket", "isolated").unwrap(),
        placement
    );
    for i in 0..3 {
        if group.nodes[i].is_some() {
            group.kill(i);
        }
    }
    for i in 0..3 {
        group.start(i);
    }
    group.leader();
    let mut client =
        RemoteMetaClient::connect_verified_raft_catalog(&catalog, &expected, &opts).unwrap();
    assert_eq!(
        client.object_placement("bucket", "isolated").unwrap(),
        placement
    );
    assert!(expected.same_group(&client.metadata_group_identity().unwrap()));
    let remaining = group.leader();
    for i in 0..3 {
        if i != remaining {
            group.kill(i);
        }
    }
    // Identity remains inspectable without claiming quorum health.
    assert!(expected.same_group(&group.client(remaining).metadata_group_identity().unwrap()));
    assert!(group.client(remaining).probe().into_result().is_err());
}

#[test]
fn chunk_heartbeat_identity_files_refuse_invalid_data_and_foreign_metadata() {
    use prost::Message;
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};
    let reserved = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = reserved.local_addr().unwrap().to_string();
    let roster = format!("guarded@{address}@rack");
    let intended = Group::with_roster(Some(roster.clone()));
    let foreign = Group::with_roster(Some(roster));
    let leader = intended.leader();
    let foreign_leader = foreign.leader();
    let identity = intended.client(leader).metadata_group_identity().unwrap();
    let path = intended.root.join("metadata.identity");
    let bytes = identity.encode_to_vec();
    let endpoints = foreign.api_addresses[foreign_leader].to_string();
    // Bad files must stop startup, not silently select an unverified catalog.
    for bad in [
        Vec::new(),
        vec![0],
        vec![0; 4097],
        bytes[..bytes.len() - 1].to_vec(),
    ] {
        std::fs::write(&path, bad).unwrap();
        let mut child = Command::new(env!("CARGO_BIN_EXE_chunkserver"))
            .env("MATRIXOBJECT_REPLICA_ADDR", "127.0.0.1:0")
            .env("MATRIXOBJECT_CHUNK_NODE_ID", "guarded")
            .env("MATRIXOBJECT_CHUNK_METADATA_ENDPOINTS", &endpoints)
            .env("MATRIXOBJECT_CHUNK_METADATA_IDENTITY_FILE", &path)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let status = processes::reap_within(&mut child, Duration::from_secs(3));
        if status.is_none() {
            let _ = child.kill();
            let _ = child.wait();
            panic!("invalid identity did not stop startup");
        }
        assert_eq!(status.unwrap(), 2);
    }
    std::fs::write(&path, bytes).unwrap();
    let before = foreign
        .client(foreign_leader)
        .list_placement_nodes()
        .unwrap();
    drop(reserved);
    let start = |metadata: &str| {
        processes::start(
            env!("CARGO_BIN_EXE_chunkserver"),
            &[
                ("MATRIXOBJECT_REPLICA_ADDR", &address),
                ("MATRIXOBJECT_CHUNK_NODE_ID", "guarded"),
                ("MATRIXOBJECT_CHUNK_METADATA_ENDPOINTS", metadata),
                (
                    "MATRIXOBJECT_CHUNK_METADATA_IDENTITY_FILE",
                    path.to_str().unwrap(),
                ),
                ("MATRIXOBJECT_CHUNK_HEARTBEAT_MS", "100"),
                ("MATRIXOBJECT_REPLICA_CAPACITY_BYTES", "8192"),
            ],
        )
    };
    let mut chunk = start(&endpoints);
    let stderr = chunk.child.stderr.take().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        let mut line = String::new();
        BufReader::new(stderr).read_line(&mut line).unwrap();
        let _ = tx.send(line);
    });
    let report = rx.recv_timeout(Duration::from_secs(5));
    let _ = chunk.child.kill();
    let _ = processes::reap_within(&mut chunk.child, Duration::from_secs(3));
    reader.join().unwrap();
    assert!(report
        .unwrap()
        .contains("initial metadata configuration does not match catalog"));
    assert_eq!(
        foreign
            .client(foreign_leader)
            .list_placement_nodes()
            .unwrap(),
        before
    );
    drop(chunk);
    let _chunk = start(&intended.api_addresses[leader].to_string());
    let until = Instant::now() + Duration::from_secs(5);
    loop {
        let nodes = intended.client(leader).list_placement_nodes().unwrap();
        if nodes[0].online && nodes[0].last_heartbeat_ms > 0 && nodes[0].capacity_bytes == 8192 {
            break;
        }
        assert!(Instant::now() < until, "trusted heartbeat never arrived");
        std::thread::sleep(Duration::from_millis(25));
    }
    assert_eq!(
        foreign
            .client(foreign_leader)
            .list_placement_nodes()
            .unwrap(),
        before
    );
}


#[test]
fn received_chunk_health_keeps_decided_time_through_failover_restart_and_expiry() {
    let mut group = Group::with_roster(Some("chunk-1@127.0.0.1:9100@rack".into()));
    let first = group.leader();
    let mut endpoints: Vec<_> = (0..3)
        .filter(|i| *i != first)
        .map(|i| group.nodes[i].as_ref().unwrap().endpoint.clone())
        .collect();
    endpoints.push(group.nodes[first].as_ref().unwrap().endpoint.clone());
    let mut client = RemoteMetaClient::connect_catalog(
        &endpoints,
        &MetaClientOptions {
            read_timeout: Duration::from_secs(2),
            ..Default::default()
        },
    )
    .unwrap();
    let clock = || {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    };
    let earliest = clock();
    let first_report = client
        .report_chunk_node("chunk-1", "127.0.0.1:9100", 17, 1 << 28, true)
        .unwrap();
    assert!((earliest..=clock()).contains(&first_report.last_heartbeat_ms));
    assert_eq!(
        client.endpoint(),
        group.nodes[first].as_ref().unwrap().endpoint
    );
    assert_eq!(first_report.used_bytes, 17);
    assert!(client
        .report_chunk_node("chunk-1", "127.0.0.1:9101", 18, 1 << 28, true)
        .is_err());
    assert_eq!(
        client.list_placement_nodes().unwrap(),
        vec![first_report.clone()]
    );
    group.kill(first);
    let second = group.leader();
    assert_ne!(first, second);
    assert_eq!(client.list_placement_nodes().unwrap(), vec![first_report]);
    let second_report = client
        .report_chunk_node("chunk-1", "127.0.0.1:9100", 19, 1 << 28, true)
        .unwrap();
    drop(client);
    for i in 0..3 {
        if group.nodes[i].is_some() {
            group.kill(i);
        }
    }
    for i in 0..3 {
        group.start(i);
    }
    let leader = group.leader();
    let mut client = group.client(leader);
    assert_eq!(
        client.list_placement_nodes().unwrap(),
        vec![second_report.clone()]
    );
    assert_eq!(
        client
            .expire_placement_node_heartbeats(1000, second_report.last_heartbeat_ms + 1001)
            .unwrap(),
        1
    );
    let expired = client.list_placement_nodes().unwrap().remove(0);
    assert!(!expired.online);
    assert_eq!(expired.last_heartbeat_ms, second_report.last_heartbeat_ms);
    assert_eq!(expired.used_bytes, 19);
    let revived = client
        .report_chunk_node("chunk-1", "127.0.0.1:9100", 20, 1 << 28, true)
        .unwrap();
    assert!(revived.online);
    assert!(revived.last_heartbeat_ms >= second_report.last_heartbeat_ms);
}

#[cfg(feature = "replica-server")]
#[test]
fn maintenance_repair_stays_excluded_through_metadata_failover_and_total_restart() {
    use matrixobjectstore_rs::cluster::{
        PageRepairStatus, RemoteReplicaClient, RemoteReplicaSet, ReplicaClientOptions,
    };
    use matrixobjectstore_rs::{ChunkId, PageId};
    let reserved: Vec<_> = (0..3)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let endpoints: Vec<_> = reserved
        .iter()
        .map(|s| s.local_addr().unwrap().to_string())
        .collect();
    let roster = endpoints
        .iter()
        .enumerate()
        .map(|(i, e)| format!("maintenance-{i}@{e}@rack-{i}"))
        .collect::<Vec<_>>()
        .join(",");
    let mut group = Group::with_catalog_checkpoints(Some(roster), Some("60000".into()), None, 1024);
    let metadata = group
        .api_addresses
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let paths: Vec<_> = (0..3)
        .map(|i| group.root.join(format!("maintenance-{i}.log")))
        .collect();
    drop(reserved);
    let start_chunk = |i: usize, maintenance: bool| {
        let id = format!("maintenance-{i}");
        processes::start(
            env!("CARGO_BIN_EXE_chunkserver"),
            &[
                ("MATRIXOBJECT_REPLICA_ADDR", &endpoints[i]),
                ("MATRIXOBJECT_REPLICA_JOURNAL", paths[i].to_str().unwrap()),
                ("MATRIXOBJECT_REPLICA_CAPACITY_BYTES", "8192"),
                (
                    "MATRIXOBJECT_REPLICA_MAINTENANCE",
                    if maintenance { "true" } else { "false" },
                ),
                ("MATRIXOBJECT_CHUNK_NODE_ID", &id),
                ("MATRIXOBJECT_CHUNK_METADATA_ENDPOINTS", &metadata),
                ("MATRIXOBJECT_CHUNK_HEARTBEAT_MS", "100"),
            ],
        )
    };
    let mut chunks: Vec<_> = (0..3).map(|i| Some(start_chunk(i, false))).collect();
    let wait_health = |catalog: &mut RemoteMetaClient, maintenance: bool, used: u64| {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if let Ok(nodes) = catalog.list_placement_nodes() {
                if nodes.len() == 3
                    && nodes.iter().all(|n| {
                        n.online == (!maintenance || n.node_id != "maintenance-0")
                            && n.used_bytes == used
                            && n.capacity_bytes == 8192
                    })
                {
                    return nodes;
                }
            }
            assert!(
                Instant::now() < deadline,
                "maintenance roster did not converge"
            );
            std::thread::sleep(Duration::from_millis(25));
        }
    };
    let mut catalog = group.client(group.leader());
    wait_health(&mut catalog, false, 0);
    let options = ReplicaClientOptions::default();
    let page = PageId {
        chunk_id: ChunkId::new(1, 9).unwrap(),
        page_index: 0,
    };
    let mut initial = RemoteReplicaSet::connect(&endpoints, 3, 3, &options).unwrap();
    assert_eq!(initial.write_chunk(&page, 0, b"before").unwrap(), 3);
    drop(initial);
    let mut set = RemoteReplicaSet::connect(&endpoints, 2, 2, &options).unwrap();
    drop(chunks[0].take());
    assert_eq!(set.write_chunk(&page, 0, b"repaired-data").unwrap(), 2);
    assert_eq!(set.discard_chunk(&page, 2, 3).unwrap(), 2);
    chunks[0] = Some(start_chunk(0, true));
    let repaired = set.repair_inventory_batch(None, 8).unwrap();
    assert!(!repaired.blocked);
    assert!(!repaired.has_more);
    assert_eq!(repaired.pages.len(), 1);
    let report = repaired.pages[0].result.as_ref().unwrap();
    assert_eq!(
        report.members,
        vec![
            PageRepairStatus::Installed,
            PageRepairStatus::Current,
            PageRepairStatus::Current
        ]
    );
    let expected = report.source.clone();
    wait_health(&mut catalog, true, 13);
    let mut held = RemoteReplicaClient::connect(&endpoints[0], &options).unwrap();
    assert!(held.write_chunk(page.clone(), 0, b"blocked").is_err());
    assert_eq!(held.head_chunk(&page).unwrap(), expected);
    drop(held);
    let leader = group.leader();
    group.kill(leader);
    drop(catalog);
    catalog = group.client(group.leader());
    wait_health(&mut catalog, true, 13);
    let plan = catalog
        .plan_object_placement("bucket", "survivors", 1, 2)
        .unwrap();
    assert!(!plan.node_ids.contains(&"maintenance-0".to_string()));
    assert!(catalog
        .plan_object_placement("bucket", "too-many", 1, 3)
        .is_err());
    // Stop every reporter before metadata restart so fresh reports cannot hide recovery bugs.
    drop(set);
    for node in &mut chunks {
        drop(node.take());
    }
    let saved = wait_health(&mut catalog, true, 13);
    drop(catalog);
    for i in 0..3 {
        if group.nodes[i].is_some() {
            group.kill(i);
        }
    }
    for i in 0..3 {
        group.start(i);
    }
    catalog = group.client(group.leader());
    let recovered = wait_health(&mut catalog, true, 13);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    for node in &recovered {
        let old = saved
            .iter()
            .find(|old| old.node_id == node.node_id)
            .unwrap();
        assert!(node.last_heartbeat_ms >= old.last_heartbeat_ms);
        assert!(
            now.saturating_sub(node.last_heartbeat_ms) < 60000,
            "offline state must not be explained by heartbeat expiry"
        );
    }
    assert!(catalog
        .plan_object_placement("bucket", "still-offline", 1, 3)
        .is_err());
    for (i, node) in chunks.iter_mut().enumerate() {
        *node = Some(start_chunk(i, i == 0));
    }
    wait_health(&mut catalog, true, 13);
    for endpoint in &endpoints {
        let mut data = RemoteReplicaClient::connect(endpoint, &options).unwrap();
        assert_eq!(data.head_chunk(&page).unwrap(), expected);
        assert_eq!(
            data.read_chunk_state(&page).unwrap().data.as_slice(),
            b"repaired-data"
        );
        assert!(data.read_chunk(&page, 2, 1).is_err());
    }
    let mut held = RemoteReplicaClient::connect(&endpoints[0], &options).unwrap();
    assert!(held.write_chunk(page.clone(), 0, b"blocked").is_err());
    drop(held);
    // Resumption is a deliberate operator restart, not an effect of successful repair.
    drop(chunks[0].take());
    chunks[0] = Some(start_chunk(0, false));
    wait_health(&mut catalog, false, 13);
    // Require every replica here: a quorum write may return after two replies.
    let mut set = RemoteReplicaSet::connect(&endpoints, 3, 3, &options).unwrap();
    assert_eq!(set.write_chunk(&page, 0, b"ready-to-write").unwrap(), 3);
    wait_health(&mut catalog, false, 14);
    assert_eq!(
        catalog
            .plan_object_placement("bucket", "resumed", 1, 3)
            .unwrap()
            .node_ids
            .len(),
        3
    );
    assert_eq!(
        set.read_chunk(&page, 0, 14).unwrap().data,
        b"ready-to-write"
    );
}

#[test]
fn empty_expiry_ticks_keep_the_metadata_log_index_stable() {
    use matrixobjectstore_rs::cluster::{MembershipOperation as Op, MembershipRequest};
    let group = Group::with_membership_options(
        Some("chunk-1@127.0.0.1:45678@rack".into()),
        Some("3000".into()),
        None,
        0,
        true,
    );
    let leader = group.leader();
    let mut client = group.client(leader);
    let identity = client.metadata_group_identity().unwrap();
    let read = MembershipRequest {
        version: 1,
        bootstrap_id: identity.bootstrap_id,
        operation: Op::Read as i32,
        node_id: 0,
    };
    let before = client.metadata_membership(read.clone()).unwrap().applied_index;
    std::thread::sleep(Duration::from_millis(2500));
    let after = client.metadata_membership(read).unwrap().applied_index;
    assert_eq!(after, before, "empty expiry ticks appended Raft entries");
}

#[test]
fn metadata_membership_replaces_a_lost_voter_with_a_new_process() {
    use matrixobjectstore_rs::cluster::{MembershipOperation as Op, MembershipRequest};
    let mut group = Group::with_membership_options(None, None, None, 1024, true);
    let first = group.leader();
    let identity = group.client(first).metadata_group_identity().unwrap();
    assert_eq!(identity.version, 2);
    assert_eq!(identity.bootstrap_voters, vec![1, 2, 3]);
    assert_eq!(identity.voters, vec![1, 2, 3, 4]);
    let command = |op: Op, id| MembershipRequest {
        version: 1,
        bootstrap_id: identity.bootstrap_id,
        operation: op as i32,
        node_id: id,
    };
    let mut saved = Vec::new();
    for n in 0..24 {
        let mut client = group.client(group.leader());
        let plan = client
            .plan_object_placement("membership", &format!("key-{n}"), 64, 2)
            .unwrap();
        saved.push(
            client
                .commit_object_placement(
                    CommittedPlacement {
                        bucket: plan.bucket,
                        key: plan.key,
                        length: plan.length,
                        node_ids: plan.node_ids,
                        version: 1,
                        update_time_ms: n + 1,
                    },
                    &plan.plan_checksum,
                )
                .unwrap(),
        );
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    while !group
        .root
        .join(first.to_string())
        .join("metadata.peer.wal.payloads")
        .is_dir()
    {
        assert!(Instant::now() < deadline, "checkpoint was not captured");
        std::thread::sleep(Duration::from_millis(20));
    }
    // The old voter is fenced. Its durable files are preserved and never reused.
    group.kill(first);
    let leader = group.leader();
    let mut client = group.client(leader);
    let mut wrong = command(Op::Read, 0);
    wrong.bootstrap_id ^= 1;
    assert!(client.metadata_membership(wrong).is_err());
    let added = client
        .metadata_membership(command(Op::AddLearner, 4))
        .unwrap();
    assert_eq!(added.learners, vec![4]);
    assert!(client.metadata_membership(command(Op::Promote, 4)).is_err());
    group.start(3);
    let new_identity = group.client(3).metadata_group_identity().unwrap();
    assert!(identity.same_group(&new_identity));
    assert!(group.client(3).probe().into_result().is_err());
    let deadline = Instant::now() + Duration::from_secs(15);
    let joint = loop {
        match client.metadata_membership(command(Op::Promote, 4)) {
            Ok(status) => break status,
            Err(error) if error.to_string().contains("caught-up learner") => {
                assert!(
                    Instant::now() < deadline,
                    "learner did not catch up: {error}"
                );
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(error) => panic!("promotion failed: {error}"),
        }
    };
    assert_eq!(joint.voters, vec![1, 2, 3, 4]);
    assert_eq!(joint.outgoing_voters, vec![1, 2, 3]);
    client
        .metadata_membership(command(Op::LeaveJoint, 0))
        .unwrap();
    client
        .metadata_membership(command(Op::Remove, first as u64 + 1))
        .unwrap();
    let expected: Vec<_> = (1..=4).filter(|id| *id != first as u64 + 1).collect();
    let final_status = match client.metadata_membership(command(Op::LeaveJoint, 0)) {
        Ok(status) => status,
        Err(error) if error.to_string().contains("write outcome may be committed; do not replay") => {
            // Confirmation can time out after the configuration has committed.
            // Resolve the outcome with reads; never submit the mutation again.
            let deadline = Instant::now() + Duration::from_secs(30);
            loop {
                if let Ok(status) = client.metadata_membership(command(Op::Read, 0)) {
                    if status.voters == expected && status.outgoing_voters.is_empty() {
                        break status;
                    }
                }
                assert!(Instant::now() < deadline, "membership outcome remained uncertain: {error}");
                std::thread::sleep(Duration::from_millis(50));
            }
        }
        Err(error) => panic!("final membership change failed: {error}"),
    };
    assert_eq!(final_status.voters, expected);
    assert!(final_status.outgoing_voters.is_empty());
    // Remove another original process: successful reads now require the new voter.
    group.kill(leader);
    let serving = group.leader();
    let mut client = group.client(serving);
    for item in &saved {
        assert_eq!(
            client.object_placement(&item.bucket, &item.key).unwrap(),
            *item
        );
    }
    let allocation = client.allocate_object_pages(3).unwrap();
    drop(client);
    for i in 0..group.nodes.len() {
        if group.nodes[i].is_some() {
            group.kill(i);
        }
    }
    for i in 0..group.nodes.len() {
        group.start(i);
    }
    let serving = group.leader();
    assert_ne!(serving, first, "removed voter became leader after replay");
    let mut client = group.client(serving);
    let status = client.metadata_membership(command(Op::Read, 0)).unwrap();
    assert_eq!(status.voters, expected);
    for item in &saved {
        assert_eq!(
            client.object_placement(&item.bucket, &item.key).unwrap(),
            *item
        );
    }
    let next = client.allocate_object_pages(3).unwrap();
    assert_ne!(next, allocation);
    assert!(group.client(first).probe().into_result().is_err());
}

#[test]
fn metadata_membership_never_acknowledges_without_quorum() {
    use matrixobjectstore_rs::cluster::{MembershipOperation as Op, MembershipRequest};
    let mut group = Group::with_membership_options(None, None, None, 0, true);
    let leader = group.leader();
    let mut client = group.client(leader);
    let identity = client.metadata_group_identity().unwrap();
    for i in 0..3 {
        if i != leader {
            group.kill(i);
        }
    }
    let command = |op: Op, id| MembershipRequest {
        version: 1,
        bootstrap_id: identity.bootstrap_id,
        operation: op as i32,
        node_id: id,
    };
    assert!(client
        .metadata_membership(command(Op::AddLearner, 4))
        .is_err());
    assert!(client.metadata_membership(command(Op::Read, 0)).is_err());
    for i in 0..3 {
        if i != leader {
            group.start(i);
        }
    }
    let serving = group.leader();
    let mut client = group.client(serving);
    let status = client.metadata_membership(command(Op::Read, 0)).unwrap();
    assert_eq!(status.voters, vec![1, 2, 3]);
    assert!(status.outgoing_voters.is_empty());
    // The timed-out proposal may commit after healing; read before a new action.
    assert!(status.learners.is_empty() || status.learners == vec![4]);
    if status.learners.is_empty() {
        client
            .metadata_membership(command(Op::AddLearner, 4))
            .unwrap();
    }
    let status = client.metadata_membership(command(Op::Remove, 4)).unwrap();
    assert!(status.learners.is_empty());
    // Removing the current leader from a three-voter group needs two commits.
    client
        .metadata_membership(command(Op::Remove, serving as u64 + 1))
        .unwrap();
    let status = client
        .metadata_membership(command(Op::LeaveJoint, 0))
        .unwrap();
    assert!(!status.voters.contains(&(serving as u64 + 1)));
    assert!(status.outgoing_voters.is_empty());
    assert!(client.allocate_object_pages(1).is_err());
    let next = group.leader();
    assert_ne!(next, serving);
    group.client(next).allocate_object_pages(1).unwrap();
}

#[cfg(feature = "replica-server")]
#[path = "node_regime/star_repair_metadata_scale.rs"]
mod star_repair_metadata_scale;

#[cfg(feature = "replica-server")]
#[path = "node_regime/star_auto_repair.rs"]
mod star_auto_repair;

#[cfg(feature = "replica-server")]
#[path = "node_regime/leased_object_read_scale.rs"]
mod leased_object_read_scale;

#[cfg(feature = "replica-server")]
#[path = "node_regime/star_append_amplification.rs"]
mod star_append_amplification;

#[cfg(feature = "replica-server")]
#[path = "node_regime/log_append_extent_cap.rs"]
mod log_append_extent_cap;

#[cfg(feature = "replica-server")]
#[path = "node_regime/log_object_end_to_end.rs"]
mod log_object_end_to_end;


#[test]
fn zero_filled_metadata_wals_keep_every_committed_placement_across_leader_loss_and_restart() {
    ZERO_FILL.with(|chunk| chunk.set(Some(1 << 20)));
    let mut group = Group::new();
    ZERO_FILL.with(|chunk| chunk.set(None));
    let first = group.leader();
    let mut saved = Vec::new();
    let mut client = group.client(first);
    for n in 0..32 {
        let key = format!("object-{n}");
        let plan = client
            .plan_object_placement("bucket", &key, 1024, 2)
            .unwrap();
        let placement = CommittedPlacement {
            bucket: plan.bucket,
            key: plan.key,
            length: plan.length,
            node_ids: plan.node_ids,
            version: 1,
            update_time_ms: n + 10,
        };
        saved.push(
            client
                .commit_object_placement(placement, &plan.plan_checksum)
                .unwrap(),
        );
    }
    drop(client);
    group.kill(first);
    let next = group.leader();
    assert_ne!(next, first);
    let mut client = group.client(next);
    for value in &saved {
        assert_eq!(
            client.object_placement(&value.bucket, &value.key).unwrap(),
            *value
        );
    }
    drop(client);
    for i in 0..3 {
        if group.nodes[i].is_some() {
            group.kill(i);
        }
    }
    for i in 0..3 {
        group.start(i);
    }
    let leader = group.leader();
    let mut client = group.client(leader);
    for value in &saved {
        assert_eq!(
            client.object_placement(&value.bucket, &value.key).unwrap(),
            *value
        );
    }
    for i in 0..3 {
        let bytes =
            std::fs::read(group.root.join(i.to_string()).join("metadata.peer.wal")).unwrap();
        assert_eq!(&bytes[..4], b"MRPZ", "voter {i} must use the zero-filled layout");
        assert!(bytes.len() >= 1 << 20);
        assert!(bytes.windows(4).any(|w| w == b"MOMP"));
    }
}

#[test]
fn an_out_of_range_zero_fill_stops_the_metaserver_at_startup() {
    for value in ["4095", "536870913", "lots"] {
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_metaserver"))
            .env("MATRIXOBJECT_WAL_ZERO_FILL_BYTES", value)
            .env("MATRIXOBJECT_METASERVER_ADDR", "127.0.0.1:0")
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2), "{value}");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("MATRIXOBJECT_WAL_ZERO_FILL_BYTES"),
            "{value}"
        );
    }
}

fn applied_index(client: &mut RemoteMetaClient) -> u64 {
    use matrixobjectstore_rs::cluster::{MembershipOperation as Op, MembershipRequest};
    let identity = client.metadata_group_identity().unwrap();
    let read = MembershipRequest {
        version: 1,
        bootstrap_id: identity.bootstrap_id,
        operation: Op::Read as i32,
        node_id: 0,
    };
    client.metadata_membership(read).unwrap().applied_index
}

/// Three real voters that answer unchanged chunk reports without a log entry.
fn suppression_group(ttl_ms: &str) -> Group {
    SUPPRESS_REPORTS.with(|flag| flag.set(true));
    let group = Group::with_membership_options(
        Some("chunk-1@127.0.0.1:9100@rack".into()),
        Some(ttl_ms.into()),
        None,
        0,
        true,
    );
    SUPPRESS_REPORTS.with(|flag| flag.set(false));
    group
}

#[test]
fn unchanged_chunk_reports_add_no_raft_entries_while_changes_and_the_keepalive_are_replicated() {
    let group = suppression_group("15000");
    let leader = group.leader();
    let mut client = group.client(leader);
    let first = client
        .report_chunk_node("chunk-1", "127.0.0.1:9100", 100, 1 << 28, true)
        .unwrap();
    assert!(first.online);
    let steady = applied_index(&mut client);
    for _ in 0..10 {
        std::thread::sleep(Duration::from_millis(200));
        let reply = client
            .report_chunk_node("chunk-1", "127.0.0.1:9100", 101, 1 << 28, true)
            .unwrap();
        assert_eq!(
            reply.last_heartbeat_ms, first.last_heartbeat_ms,
            "an unchanged report must not advance the replicated stamp"
        );
    }
    assert_eq!(applied_index(&mut client), steady, "unchanged reports appended Raft entries");
    // Past a third of the 15 s TTL the keepalive is replicated.
    std::thread::sleep(Duration::from_millis(3600));
    let refreshed = client
        .report_chunk_node("chunk-1", "127.0.0.1:9100", 100, 1 << 28, true)
        .unwrap();
    assert!(refreshed.last_heartbeat_ms > first.last_heartbeat_ms);
    assert_eq!(applied_index(&mut client), steady + 1, "the keepalive is one entry");
    // A change in usage beyond 1% of capacity is replicated at once.
    let used = 100 + (1u64 << 28) / 20;
    let changed = client
        .report_chunk_node("chunk-1", "127.0.0.1:9100", used, 1 << 28, true)
        .unwrap();
    assert_eq!(changed.used_bytes, used);
    assert_eq!(applied_index(&mut client), steady + 2, "a real change is one entry");
}

#[test]
fn a_live_node_stays_online_through_a_leader_change_while_its_reports_are_suppressed() {
    let mut group = suppression_group("15000");
    let first = group.leader();
    let mut endpoints: Vec<_> = (0..3)
        .filter(|i| *i != first)
        .map(|i| group.nodes[i].as_ref().unwrap().endpoint.clone())
        .collect();
    endpoints.push(group.nodes[first].as_ref().unwrap().endpoint.clone());
    let mut client = RemoteMetaClient::connect_catalog(
        &endpoints,
        &MetaClientOptions {
            read_timeout: Duration::from_secs(3),
            write_timeout: Duration::from_secs(3),
            ..Default::default()
        },
    )
    .unwrap();
    client
        .report_chunk_node("chunk-1", "127.0.0.1:9100", 100, 1 << 28, true)
        .unwrap();
    let start = Instant::now();
    let mut killed = false;
    let mut offline_samples = 0;
    let mut online_samples = 0;
    while start.elapsed() < Duration::from_secs(30) {
        if !killed && start.elapsed() >= Duration::from_secs(8) {
            group.kill(first);
            killed = true;
        }
        // The chunk process keeps reporting every second; a report during the change may fail.
        let _ = client.report_chunk_node("chunk-1", "127.0.0.1:9100", 100, 1 << 28, true);
        if let Ok(nodes) = client.list_placement_nodes() {
            if nodes[0].online {
                online_samples += 1;
            } else {
                offline_samples += 1;
            }
        }
        std::thread::sleep(Duration::from_secs(1));
    }
    assert!(killed);
    assert_eq!(offline_samples, 0, "a node that reported on time was expired");
    assert!(online_samples >= 15, "too few samples: {online_samples}");
    let second = group.leader();
    assert_ne!(first, second);
}

#[test]
fn a_node_that_stops_reporting_is_still_expired_when_reports_are_suppressed() {
    let group = suppression_group("6000");
    let leader = group.leader();
    let mut client = group.client(leader);
    let first = client
        .report_chunk_node("chunk-1", "127.0.0.1:9100", 100, 1 << 28, true)
        .unwrap();
    assert!(first.online);
    let until = Instant::now() + Duration::from_secs(25);
    loop {
        let nodes = client.list_placement_nodes().unwrap();
        if !nodes[0].online {
            assert_eq!(nodes[0].last_heartbeat_ms, first.last_heartbeat_ms);
            return;
        }
        assert!(Instant::now() < until, "a silent node was never expired");
        std::thread::sleep(Duration::from_millis(500));
    }
}

/// Three real voters whose leader decides who is silent from what it has heard.
fn liveness_group(roster: Option<String>, catalog: Option<Vec<u8>>, ttl_ms: &str, membership: bool) -> Group {
    LEADER_LIVENESS.with(|flag| flag.set(true));
    let group = Group::with_membership_options(roster, Some(ttl_ms.into()), catalog, 0, membership);
    LEADER_LIVENESS.with(|flag| flag.set(false));
    group
}

/// A client that follows the leader, starting with a follower so the first call must route.
fn failover_client(group: &Group, leader: usize) -> RemoteMetaClient {
    let mut endpoints: Vec<_> = (0..3)
        .filter(|i| *i != leader)
        .map(|i| group.nodes[i].as_ref().unwrap().endpoint.clone())
        .collect();
    endpoints.push(group.nodes[leader].as_ref().unwrap().endpoint.clone());
    RemoteMetaClient::connect_catalog(
        &endpoints,
        &MetaClientOptions {
            read_timeout: Duration::from_secs(3),
            write_timeout: Duration::from_secs(3),
            ..Default::default()
        },
    )
    .unwrap()
}

/// Every entry of the catalog, tombstones included, read through the leader at one revision.
fn catalog_entries(client: &mut RemoteMetaClient) -> (Vec<(String, String, bool)>, Vec<u8>) {
    let mut seen = Vec::new();
    let mut after: Option<(String, String)> = None;
    loop {
        let page = client
            .list_manifest_catalog(after.as_ref().map(|(b, k)| (b.as_str(), k.as_str())), 32, None)
            .unwrap();
        for entry in &page.entries {
            seen.push((entry.bucket.clone(), entry.key.clone(), entry.deleted));
        }
        if !page.has_more {
            return (seen, page.revision);
        }
        let last = page.entries.last().unwrap();
        after = Some((last.bucket.clone(), last.key.clone()));
    }
}

fn forget_group(prefixes: &'static str) -> Group {
    FORGET_PREFIXES.with(|cell| cell.set(Some(prefixes)));
    let group = Group::new();
    FORGET_PREFIXES.with(|cell| cell.set(None));
    let mut client = group.client(group.leader());
    // Every voter declares the bit and the group raises it: the only way emission starts.
    for voter in 1..=3u64 {
        client
            .declare_voter_features(voter, matrixobjectstore_rs::cluster_features::FEATURE_FORGET_PREFIX)
            .unwrap();
    }
    client
        .raise_cluster_features(matrixobjectstore_rs::cluster_features::FEATURE_FORGET_PREFIX, vec![1, 2, 3])
        .unwrap();
    group
}

fn created(bucket: &str, key: &str) -> matrixobjectstore_rs::object_manifest::ManifestOutcome {
    use matrixobjectstore_rs::object_manifest::{ManifestOutcome, ObjectManifest};
    ManifestOutcome::decide(
        None,
        ObjectManifest { format_version: 2, bucket: bucket.into(), key: key.into(), generation: 1, ..Default::default() },
    )
    .unwrap()
}

const TWO_NODES: &str = "chunk-1@127.0.0.1:9100@rack,chunk-2@127.0.0.1:9101@rack2";

fn wall_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[test]
fn leader_liveness_costs_no_raft_entries_for_a_node_that_reports_on_time_however_old_its_stamp() {
    let group = liveness_group(Some("chunk-1@127.0.0.1:9100@rack".into()), None, "6000", true);
    let leader = group.leader();
    let mut client = group.client(leader);
    let first = client
        .report_chunk_node("chunk-1", "127.0.0.1:9100", 100, 1 << 28, true)
        .unwrap();
    assert!(first.online);
    let steady = applied_index(&mut client);
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(20) {
        std::thread::sleep(Duration::from_millis(500));
        let reply = client
            .report_chunk_node("chunk-1", "127.0.0.1:9100", 101, 1 << 28, true)
            .unwrap();
        assert_eq!(reply.last_heartbeat_ms, first.last_heartbeat_ms, "an unchanged report must not advance the stamp");
        let nodes = client.list_placement_nodes().unwrap();
        assert!(nodes[0].online, "a node that reported on time was expired after {:?}", start.elapsed());
    }
    assert!(wall_ms() - first.last_heartbeat_ms > 18_000, "the replicated stamp should be older than three TTLs");
    assert_eq!(applied_index(&mut client), steady, "reports that changed nothing appended Raft entries");
    // A real change is still exactly one entry.
    let used = 100 + (1u64 << 28) / 20;
    client
        .report_chunk_node("chunk-1", "127.0.0.1:9100", used, 1 << 28, true)
        .unwrap();
    assert_eq!(applied_index(&mut client), steady + 1);
}

#[test]
fn after_a_leader_change_a_reporting_node_stays_online_and_a_silent_one_expires_after_the_grace() {
    let mut group = liveness_group(Some(TWO_NODES.into()), None, "6000", false);
    let first_leader = group.leader();
    let mut client = failover_client(&group, first_leader);
    for (id, endpoint) in [("chunk-1", "127.0.0.1:9100"), ("chunk-2", "127.0.0.1:9101")] {
        client.report_chunk_node(id, endpoint, 100, 1 << 28, true).unwrap();
    }
    let start = Instant::now();
    let (mut killed, mut last_report_of_2) = (false, Duration::ZERO);
    let (mut offline_1, mut offline_2_at) = (0, None);
    while start.elapsed() < Duration::from_secs(45) && offline_2_at.is_none() {
        let elapsed = start.elapsed();
        if !killed && elapsed >= Duration::from_secs(8) {
            group.kill(first_leader);
            killed = true;
        }
        let _ = client.report_chunk_node("chunk-1", "127.0.0.1:9100", 100, 1 << 28, true);
        // chunk-2 stops reporting at the moment the leader dies: it is the node that really went away.
        if elapsed < Duration::from_secs(8)
            && client.report_chunk_node("chunk-2", "127.0.0.1:9101", 100, 1 << 28, true).is_ok()
        {
            last_report_of_2 = elapsed;
        }
        if let Ok(nodes) = client.list_placement_nodes() {
            for node in nodes {
                match (node.node_id.as_str(), node.online) {
                    ("chunk-1", false) => offline_1 += 1,
                    ("chunk-2", false) if offline_2_at.is_none() => offline_2_at = Some(elapsed),
                    _ => {}
                }
            }
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    assert!(killed);
    assert_eq!(offline_1, 0, "a node that kept reporting through the leader change was expired");
    let offline_2_at = offline_2_at.expect("a node that really stopped reporting was never expired");
    assert!(
        offline_2_at >= last_report_of_2 + Duration::from_millis(5_500),
        "expired {:?} after its last report, before the TTL",
        offline_2_at - last_report_of_2
    );
    assert!(
        offline_2_at <= last_report_of_2 + Duration::from_secs(30),
        "expired only {:?} after its last report",
        offline_2_at - last_report_of_2
    );
    assert_ne!(group.leader(), first_leader);
}

#[test]
fn a_node_that_really_dies_is_expired_within_the_ttl_without_any_leader_change() {
    let group = liveness_group(Some(TWO_NODES.into()), None, "6000", false);
    let leader = group.leader();
    let mut client = group.client(leader);
    let start = Instant::now();
    let mut last_report_of_2 = Duration::ZERO;
    let mut offline_2_at = None;
    while start.elapsed() < Duration::from_secs(30) && offline_2_at.is_none() {
        let elapsed = start.elapsed();
        client.report_chunk_node("chunk-1", "127.0.0.1:9100", 100, 1 << 28, true).unwrap();
        if elapsed < Duration::from_secs(4) {
            client.report_chunk_node("chunk-2", "127.0.0.1:9101", 100, 1 << 28, true).unwrap();
            last_report_of_2 = elapsed;
        }
        for node in client.list_placement_nodes().unwrap() {
            assert!(node.node_id != "chunk-1" || node.online, "the reporting node was expired");
            if node.node_id == "chunk-2" && !node.online && elapsed > Duration::from_secs(1) {
                offline_2_at = Some(elapsed);
            }
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    let offline_2_at = offline_2_at.expect("a dead node was never expired");
    let age = offline_2_at - last_report_of_2;
    assert!(age >= Duration::from_millis(5_500), "expired {age:?} after its last report, before the TTL");
    assert!(age <= Duration::from_secs(12), "expired only {age:?} after its last report");
}

#[test]
fn a_restarted_follower_catches_up_while_the_leader_keeps_a_reporting_node_online() {
    let mut group = liveness_group(Some("chunk-1@127.0.0.1:9100@rack".into()), None, "6000", true);
    let leader = group.leader();
    let mut client = failover_client(&group, leader);
    client.report_chunk_node("chunk-1", "127.0.0.1:9100", 100, 1 << 28, true).unwrap();
    let follower = (0..3).find(|i| *i != leader).unwrap();
    let start = Instant::now();
    let mut restarted = false;
    let mut killed = false;
    while start.elapsed() < Duration::from_secs(20) {
        let elapsed = start.elapsed();
        if !killed && elapsed >= Duration::from_secs(3) {
            group.kill(follower);
            killed = true;
        }
        if killed && !restarted && elapsed >= Duration::from_secs(9) {
            group.start(follower);
            restarted = true;
        }
        client.report_chunk_node("chunk-1", "127.0.0.1:9100", 100, 1 << 28, true).unwrap();
        assert!(client.list_placement_nodes().unwrap()[0].online, "the node went offline at {elapsed:?}");
        std::thread::sleep(Duration::from_millis(500));
    }
    assert!(restarted);
    // The restarted follower converges on the leader's log position. A follower cannot answer a membership
    // read (only a leader admits it), so the leader's applied index is asked through the routing client and
    // the follower's committed index is read from its own log file.
    let target = applied_index(&mut client);
    let wal = group.root.join(follower.to_string()).join("metadata.peer.wal");
    let until = Instant::now() + Duration::from_secs(15);
    loop {
        let held = node_regime::checkpoints::committed_index(&wal);
        if held + 2 >= target {
            break;
        }
        assert!(Instant::now() < until, "the restarted follower holds {held}, the leader has applied {target}");
        std::thread::sleep(Duration::from_millis(300));
    }
}

#[test]
fn a_healthy_chunk_group_stays_in_placement_through_a_leader_change_and_leaves_when_its_members_die() {
    use matrixobjectstore_rs::cluster::{ChunkGroupCatalog, ChunkGroupConfig, ChunkGroupIdentity};
    let config = ChunkGroupConfig {
        placement_id: "group-a".into(),
        failure_domain: "rack-a".into(),
        identity: Some(ChunkGroupIdentity {
            bootstrap_voters: Vec::new(),
            bootstrap_id: 0,
            version: 1,
            group_id: 1,
            node_id: 1,
            capacity_bytes: 65536,
            max_pages: 64,
            voters: vec![1, 2, 3],
            peer_endpoints: vec!["127.0.0.1:9101".into(), "127.0.0.1:9102".into(), "127.0.0.1:9103".into()],
        }),
        api_endpoints: vec!["127.0.0.1:9201".into(), "127.0.0.1:9202".into(), "127.0.0.1:9203".into()],
        member_ids: vec!["member-a".into(), "member-b".into(), "member-c".into()],
    };
    let catalog = ChunkGroupCatalog::new(vec![config.clone()]).unwrap();
    let mut group = liveness_group(None, Some(catalog.to_bytes().unwrap()), "6000", false);
    let first_leader = group.leader();
    let mut client = failover_client(&group, first_leader);
    let members = [("member-a", "127.0.0.1:9201"), ("member-b", "127.0.0.1:9202"), ("member-c", "127.0.0.1:9203")];
    let report_all = |client: &mut RemoteMetaClient| {
        for (id, endpoint) in members {
            let _ = client.report_chunk_node(id, endpoint, 123, 65536, true);
        }
    };
    report_all(&mut client);
    let start = Instant::now();
    let (mut killed, mut silent_since) = (false, None);
    let (mut healthy_samples, mut healthy_placements, mut left_placement_early) = (0, 0, 0);
    let mut group_left_at = None;
    while start.elapsed() < Duration::from_secs(60) && group_left_at.is_none() {
        let elapsed = start.elapsed();
        if !killed && elapsed >= Duration::from_secs(8) {
            group.kill(first_leader);
            killed = true;
        }
        // Members report until 24 s, then every one of them dies.
        if elapsed < Duration::from_secs(24) {
            report_all(&mut client);
        } else if silent_since.is_none() {
            silent_since = Some(elapsed);
        }
        let listed = client.list_placement_nodes().ok();
        let online = listed.as_ref().and_then(|nodes| nodes.iter().find(|n| n.node_id == "group-a")).map(|n| n.online);
        let placed = client.plan_object_placement("bucket", "key", 100, 1).ok().map(|plan| plan.node_ids);
        if silent_since.is_none() {
            match (online, placed) {
                (Some(true), Some(ids)) => {
                    healthy_samples += 1;
                    if ids == vec!["group-a".to_string()] {
                        healthy_placements += 1;
                    } else {
                        left_placement_early += 1;
                    }
                }
                (Some(false), _) => left_placement_early += 1,
                _ => {}
            }
        } else if online == Some(false) {
            group_left_at = Some(elapsed);
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    assert!(killed);
    assert_eq!(left_placement_early, 0, "a healthy group left placement while its members reported");
    assert!(healthy_samples >= 10 && healthy_placements >= 10, "{healthy_samples} samples, {healthy_placements} placements");
    let left = group_left_at.expect("a group whose members all died never left placement");
    assert!(left - silent_since.unwrap() >= Duration::from_millis(5_000), "left only {:?} after the members fell silent", left - silent_since.unwrap());
    assert!(client.plan_object_placement("bucket", "key", 100, 1).map_or(true, |plan| plan.node_ids != vec!["group-a".to_string()]));
}
/// A WAL that rolls a segment 100 times, creating a never-reused key and deleting the one before it, leaves the
/// catalog at its live objects. An ordinary object deleted in the same group keeps its tombstone (it is not under
/// the prefix). Without the prefix every rollover would leave one more tombstone.
#[test]
fn forget_prefix_keeps_the_catalog_bounded_across_rollovers() {
    let group = forget_group("wal/seg_");
    let mut client = group.client(group.leader());
    client.publish_manifest(created("ordinary", "k"), 0).unwrap();
    client.delete_object("ordinary", "k", 0).unwrap();
    client.publish_manifest(created("wal", "seg_live"), 0).unwrap();
    for index in 0..100 {
        let key = format!("seg_{index:05}");
        client.publish_manifest(created("wal", &key), 0).unwrap();
        client.delete_object("wal", &key, 0).unwrap();
    }
    let (entries, _) = catalog_entries(&mut client);
    assert_eq!(
        entries,
        vec![
            ("ordinary".to_string(), "k".to_string(), true),
            ("wal".to_string(), "seg_live".to_string(), false),
        ],
        "100 rollovers left only the live segment and the ordinary tombstone"
    );
    // A forgotten key can be created again under the contract.
    client.publish_manifest(created("wal", "seg_00007"), 0).unwrap();
}

/// The same rollovers with the leader killed in the middle and then every voter restarted: the catalog is the same
/// on the voter that was restored from its log and on the one that applied the forgets live (compared through the
/// leader before and after a leadership change), and nothing is resurrected.
#[test]
fn forget_prefix_survives_a_leader_kill_and_a_total_restart() {
    let mut group = forget_group("wal/seg_");
    let mut client = group.client(group.leader());
    client.publish_manifest(created("ordinary", "k"), 0).unwrap();
    client.delete_object("ordinary", "k", 0).unwrap();
    client.publish_manifest(created("wal", "seg_live"), 0).unwrap();
    // idempotent steps, so that the one in flight when the leader dies can simply be repeated
    let roll = |group: &Group, index: usize| {
        let key = format!("seg_{index:05}");
        let until = Instant::now() + Duration::from_secs(30);
        loop {
            let mut client = group.client(group.leader());
            let made = client.read_manifest("wal", &key).is_ok() || client.publish_manifest(created("wal", &key), 0).is_ok();
            let gone = made && (client.read_manifest("wal", &key).map_or(true, |m| m.deleted)
                || client.delete_object("wal", &key, 0).is_ok());
            if gone && client.read_manifest("wal", &key).map_or(true, |m| m.deleted) {
                return;
            }
            assert!(Instant::now() < until, "segment {index} did not roll");
            std::thread::sleep(Duration::from_millis(100));
        }
    };
    for index in 0..15 {
        roll(&group, index);
    }
    let killed = group.leader();
    group.kill(killed);
    for index in 15..30 {
        roll(&group, index);
    }
    group.start(killed);
    for index in 30..40 {
        roll(&group, index);
    }
    let expected = vec![
        ("ordinary".to_string(), "k".to_string(), true),
        ("wal".to_string(), "seg_live".to_string(), false),
    ];
    let mut client = group.client(group.leader());
    assert_eq!(catalog_entries(&mut client).0, expected);
    for index in 0..3 {
        if group.nodes[index].is_some() {
            group.kill(index);
        }
    }
    for index in 0..3 {
        group.start(index);
    }
    let first = group.leader();
    let (after_restart, revision) = catalog_entries(&mut group.client(first));
    assert_eq!(after_restart, expected, "a total restart replays the forgets to the same catalog");
    // A different voter as leader sees the same entries and the same revision.
    group.kill(first);
    let (second_entries, second_revision) = catalog_entries(&mut group.client(group.leader()));
    assert_eq!(second_entries, expected);
    assert_eq!(second_revision, revision, "the voters hold the same revision");
}

#[path = "node_regime/cluster_gateway_processes.rs"]
mod cluster_gateway_processes;

/// The log chunk commands and the feature commands through the REAL leader pipeline: three voter
/// processes, each request prepared against a draft and committed through Raft, then applied by
/// every voter. A plain-service test cannot see a field the draft forgets to restore; here the
/// commit-apply fails on a service that already holds the change and the leader's consensus worker
/// dies, so the next request is refused. The state is read back from the leader, a follower is
/// killed and restarted, and the state must come back identical from its journal and the leader.
#[test]
fn log_chunk_and_feature_commands_commit_through_the_real_leader_pipeline() {
    use matrixobjectstore_rs::cluster::{ChunkGroupCatalog, ChunkGroupConfig, ChunkGroupIdentity};
    use matrixobjectstore_rs::cluster_features::{FEATURE_CHUNK_MANIFEST, FEATURE_LOG_CHUNK_LEASES, SUPPORTED_FEATURES};
    use matrixobjectstore_rs::log_chunk_outcome::LeaseRef;
    let config = ChunkGroupConfig {
        placement_id: "group-a".into(),
        failure_domain: "rack-a".into(),
        identity: Some(ChunkGroupIdentity {
            bootstrap_voters: Vec::new(),
            bootstrap_id: 0,
            version: 1,
            group_id: 1,
            node_id: 1,
            capacity_bytes: 1 << 40,
            max_pages: 1 << 20,
            voters: vec![1, 2, 3],
            peer_endpoints: vec!["127.0.0.1:9101".into(), "127.0.0.1:9102".into(), "127.0.0.1:9103".into()],
        }),
        api_endpoints: vec!["127.0.0.1:9201".into(), "127.0.0.1:9202".into(), "127.0.0.1:9203".into()],
        member_ids: vec!["member-a".into(), "member-b".into(), "member-c".into()],
    };
    let catalog = ChunkGroupCatalog::new(vec![config]).unwrap();
    let mut group = Group::with_catalog(None, None, Some(catalog.to_bytes().unwrap()));
    let leader = group.leader();
    let mut client = group.client(leader);
    // nothing is enabled by default: a grant is refused
    assert!(client.grant_log_chunk("bucket", "early", "gw-a", 30_000, "group-a").is_err());
    // declare and raise, each a request the leader prepares against a draft then commits
    for voter in 1..=3u64 {
        client.declare_voter_features(voter, SUPPORTED_FEATURES).unwrap();
    }
    let both = FEATURE_CHUNK_MANIFEST | FEATURE_LOG_CHUNK_LEASES;
    assert!(client.raise_cluster_features(both, vec![1, 2, 3]).unwrap().has(both));
    // the leader survived: its next requests are still served
    assert!(client.read_cluster_features().unwrap().has(both));
    // grants of several objects, switches, a batched renewal, a takeover after expiry, a sweep
    let mut leases = Vec::new();
    for object in 0..4 {
        let key = format!("log-{object}");
        let reply = client.grant_log_chunk("bucket", &key, "gw-a", 30_000, "group-a").unwrap();
        leases.push((key, reply.lease.unwrap().epoch));
    }
    for (key, epoch) in &leases {
        client.switch_log_chunk("bucket", key, "gw-a", *epoch, "group-a", 4096, 7, Vec::new()).unwrap();
    }
    let refs: Vec<LeaseRef> = leases.iter().map(|(key, epoch)| LeaseRef { bucket: "bucket".into(), key: key.clone(), epoch: *epoch }).collect();
    let renewed = client.renew_log_leases("gw-a", 60_000, refs).unwrap();
    assert_eq!((renewed.renewed, renewed.lost.len()), (4, 0));
    // a short lease runs out, another writer takes it over (a later epoch), and the sweep drops the rest
    let short = client.grant_log_chunk("bucket", "short", "gw-a", 1_000, "group-a").unwrap().lease.unwrap();
    std::thread::sleep(Duration::from_millis(1_300));
    let taken = client.grant_log_chunk("bucket", "short", "gw-b", 30_000, "group-a").unwrap().lease.unwrap();
    assert!(taken.epoch > short.epoch);
    // a lease nobody renews is dropped by the leader's own cleanup loop, through the same pipeline
    client.grant_log_chunk("bucket", "gone", "gw-a", 1_000, "group-a").unwrap();
    let until = Instant::now() + Duration::from_secs(15);
    while client.read_log_chunk("bucket", "gone").unwrap().lease.is_some() {
        assert!(Instant::now() < until, "the expired lease was never swept");
        std::thread::sleep(Duration::from_millis(200));
    }
    // what the leader answers is what a read of each object says, and the leader is still alive
    let read = client.read_log_chunk("bucket", "log-0").unwrap();
    assert_eq!(read.lease.unwrap().epoch, leases[0].1);
    assert_eq!(read.length, 4096);
    // kill a follower and restart it: its journal replays every entry on top of the leader's state
    let lagging = (leader + 1) % 3;
    group.kill(lagging);
    client.switch_log_chunk("bucket", "log-1", "gw-a", leases[1].1, "group-a", 8192, 7, Vec::new()).unwrap();
    group.start(lagging);
    // after a full restart of every voter the committed state is the same from any of them
    for index in 0..3 {
        group.kill(index);
    }
    for index in 0..3 {
        group.start(index);
    }
    let leader = group.leader();
    let mut client = group.client(leader);
    let after = client.read_log_chunk("bucket", "log-1").unwrap();
    assert_eq!(after.lease.unwrap().epoch, leases[1].1);
    assert_eq!(after.length, 4096 + 8192);
    assert!(client.read_cluster_features().unwrap().has(both));
    assert_eq!(client.read_log_chunk("bucket", "short").unwrap().lease.unwrap().holder, "gw-b");
}

/// A log object against the REAL metadata group (three voter processes, Raft) and three real chunk
/// nodes. The metadata group's commit index is read before and after each step, so what the
/// metadata plane is charged for is counted, not assumed: ONE entry for a grant, NOTHING for an
/// append inside a chunk, exactly ONE per chunk boundary crossed, one per renewal (one for the
/// leases of every object a writer holds), and nothing for a read. Every voter is then killed and
/// restarted, and a second writer takes the object over: no acknowledged byte is lost.
#[test]
fn a_log_object_costs_the_real_metadata_group_one_entry_per_switch_and_nothing_per_append() {
    use matrixobjectstore_rs::cluster::{
        ChunkGroupCatalog, ChunkGroupConfig, ChunkGroupIdentity, LogObject, LogObjectOptions, LogObjectReader,
        MetaReply, ReplicaClientOptions, ReplicaNodeOptions, ReplicaNodeServer, ReplicaNodeShutdown,
    };
    use matrixobjectstore_rs::cluster_features::{FEATURE_CHUNK_MANIFEST, FEATURE_LOG_CHUNK_LEASES, SUPPORTED_FEATURES};
    const CHUNK: u64 = 256 * 1024;
    // three real chunk nodes, each with a durable journal
    let root = std::env::temp_dir().join(format!("log-object-rig-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    struct Rig(Vec<ReplicaNodeShutdown>, Vec<Option<std::thread::JoinHandle<std::io::Result<()>>>>, PathBuf);
    impl Drop for Rig {
        fn drop(&mut self) {
            for stop in &self.0 {
                stop.stop();
            }
            for worker in self.1.iter_mut().filter_map(Option::take) {
                let _ = worker.join();
            }
            let _ = std::fs::remove_dir_all(&self.2);
        }
    }
    let mut rig = Rig(Vec::new(), Vec::new(), root.clone());
    let mut addresses = Vec::new();
    for node in 0..3 {
        std::fs::create_dir_all(root.join(format!("n{node}"))).unwrap();
        let server = ReplicaNodeServer::bind_with_journal(
            "127.0.0.1:0",
            ReplicaNodeOptions::default(),
            &root.join(format!("n{node}")).join("journal"),
        )
        .unwrap();
        addresses.push(server.local_addr().unwrap().to_string());
        rig.0.push(server.shutdown_handle());
        rig.1.push(Some(std::thread::spawn(move || server.serve())));
    }
    let config = ChunkGroupConfig {
        placement_id: "group-a".into(),
        failure_domain: "rack-a".into(),
        identity: Some(ChunkGroupIdentity {
            bootstrap_voters: Vec::new(),
            bootstrap_id: 0,
            version: 1,
            group_id: 1,
            node_id: 1,
            capacity_bytes: 1 << 40,
            max_pages: 1 << 20,
            voters: vec![1, 2, 3],
            peer_endpoints: vec!["127.0.0.1:9101".into(), "127.0.0.1:9102".into(), "127.0.0.1:9103".into()],
        }),
        api_endpoints: addresses.clone(),
        member_ids: vec!["member-a".into(), "member-b".into(), "member-c".into()],
    };
    let catalog = ChunkGroupCatalog::new(vec![config]).unwrap();
    let mut group = Group::with_catalog(None, None, Some(catalog.to_bytes().unwrap()));
    let leader = group.leader();
    let mut meta = group.client(leader);
    let commit_index = |client: &mut RemoteMetaClient| match client.probe().into_result().unwrap() {
        MetaReply::Pong { commit_index, .. } => commit_index,
        reply => panic!("unexpected metadata status: {reply:?}"),
    };
    let options = |holder: &str, ttl_ms: u64| {
        let mut options = LogObjectOptions::new(holder, "group-a");
        options.chunk_bytes = CHUNK;
        options.ttl_ms = ttl_ms;
        options.replica = ReplicaClientOptions { read_timeout: Duration::from_secs(10), connect_timeout: Duration::from_secs(2), ..Default::default() };
        options
    };
    // nothing is enabled by default: the object cannot be opened, and nothing was written for it
    let before = commit_index(&mut meta);
    let mut early = LogObject::new("bucket", "wal-early", options("gw-a", 30_000));
    assert!(early.open_for_append(&mut meta).is_err());
    assert_eq!(commit_index(&mut meta), before);
    // an operator declares and raises the two features
    for voter in 1..=3u64 {
        meta.declare_voter_features(voter, SUPPORTED_FEATURES).unwrap();
    }
    let both = FEATURE_CHUNK_MANIFEST | FEATURE_LOG_CHUNK_LEASES;
    assert!(meta.raise_cluster_features(both, vec![1, 2, 3]).unwrap().has(both));

    // the grant: ONE entry
    let mut object = LogObject::new("bucket", "wal-0", options("gw-a", 30_000));
    let before = commit_index(&mut meta);
    assert_eq!(object.open_for_append(&mut meta).unwrap(), 0);
    assert_eq!(commit_index(&mut meta) - before, 1, "a grant is one metadata entry");
    // chunk index of the last byte written: the object moves on lazily, when the next byte needs room
    let chunk_of = |total: u64| if total == 0 { 0 } else { (total - 1) / CHUNK };
    let mut expected: Vec<u8> = Vec::new();
    let mut total = 0u64;
    let mut piece_seed = 0u8;
    let mut append = |object: &mut LogObject, meta: &mut RemoteMetaClient, length: usize, expected: &mut Vec<u8>, total: &mut u64| -> u64 {
        piece_seed = piece_seed.wrapping_add(1);
        let piece: Vec<u8> = (0..length).map(|i| piece_seed.wrapping_add((i * 13) as u8)).collect();
        let before = commit_index(meta);
        let switches = chunk_of(*total + length as u64) - chunk_of(*total);
        // a lease past half its term is renewed by the append that finds it so: one entry, counted
        let renewal = u64::from(object.renew_due());
        assert_eq!(object.append(meta, &piece).unwrap(), *total + length as u64);
        let spent = commit_index(meta) - before;
        assert_eq!(spent, switches + renewal, "an append of {length} bytes at {total} crosses {switches} chunk boundaries");
        expected.extend_from_slice(&piece);
        *total += length as u64;
        spent
    };
    // inside one chunk: not one metadata entry for any of them
    for _ in 0..4 {
        assert_eq!(append(&mut object, &mut meta, 20_000, &mut expected, &mut total), 0);
    }
    // across one boundary, then across three at once, then a small one: one entry per boundary
    assert_eq!(append(&mut object, &mut meta, 200_000, &mut expected, &mut total), 1);
    assert_eq!(append(&mut object, &mut meta, 800_000, &mut expected, &mut total), 3);
    assert_eq!(append(&mut object, &mut meta, 1_000, &mut expected, &mut total), 0);

    // reads cost the metadata group nothing and see every byte, sealed chunks and live tail alike
    let before = commit_index(&mut meta);
    let mut reader = LogObjectReader::new("bucket", "wal-0", ReplicaClientOptions::default());
    assert_eq!(reader.length(&mut meta).unwrap(), total);
    assert_eq!(reader.read_range(&mut meta, 0, expected.len() + 10).unwrap(), expected);
    assert_eq!(reader.read_range(&mut meta, CHUNK - 100, 300).unwrap(), expected[(CHUNK - 100) as usize..(CHUNK + 200) as usize]);
    assert_eq!(commit_index(&mut meta), before, "reads are not metadata entries");

    // renewal: a lease renews at half its term with ONE entry; a batch of three objects with ONE
    let mut shorts: Vec<LogObject> = (0..3).map(|n| LogObject::new("bucket", &format!("wal-short-{n}"), options("gw-a", 6_000))).collect();
    for object in shorts.iter_mut() {
        object.open_for_append(&mut meta).unwrap();
        object.append(&mut meta, &[1u8; 100]).unwrap();
    }
    std::thread::sleep(Duration::from_millis(3_300));
    assert!(shorts.iter().all(|object| object.renew_due()));
    let before = commit_index(&mut meta);
    let mut handles: Vec<&mut LogObject> = shorts.iter_mut().collect();
    let lost = matrixobjectstore_rs::cluster::renew_leases(&mut meta, "gw-a", 6_000, &mut handles).unwrap();
    assert!(lost.is_empty(), "the metadata plane no longer holds these leases for this writer: {lost:?}");
    assert_eq!(commit_index(&mut meta) - before, 1, "the leases of three objects are renewed by one entry");
    std::thread::sleep(Duration::from_millis(3_300));
    let before = commit_index(&mut meta);
    shorts[0].append(&mut meta, &[2u8; 100]).unwrap();
    assert_eq!(commit_index(&mut meta) - before, 1, "an object renews its own lease with one entry");

    // the first object's lease is renewed by one entry before the slow part of the test, so a slow
    // machine cannot let it run out
    let before = commit_index(&mut meta);
    let mut handles: Vec<&mut LogObject> = vec![&mut object];
    let lost = matrixobjectstore_rs::cluster::renew_leases(&mut meta, "gw-a", 30_000, &mut handles).unwrap();
    assert!(lost.is_empty(), "{lost:?}");
    assert_eq!(commit_index(&mut meta) - before, 1, "one lease renewed by one entry");
    // a follower dies and comes back; then every voter is restarted
    let follower = (leader + 1) % 3;
    group.kill(follower);
    assert_eq!(append(&mut object, &mut meta, 50_000, &mut expected, &mut total), 0, "a follower down changes nothing for an append");
    group.start(follower);
    for index in 0..3 {
        group.kill(index);
    }
    for index in 0..3 {
        group.start(index);
    }
    let leader = group.leader();
    let mut meta = group.client(leader);
    // a second writer takes the object over once the first writer's lease has run out: it starts
    // at the length the nodes acknowledged
    std::thread::sleep(Duration::from_secs(1));
    let mut second = LogObject::new("bucket", "wal-0", options("gw-b", 30_000));
    let mut attempts = 0;
    let length = loop {
        match second.open_for_append(&mut meta) {
            Ok(length) => break length,
            Err(error) => {
                attempts += 1;
                assert!(attempts < 120, "the object was never released: {error}");
                std::thread::sleep(Duration::from_secs(1));
            }
        }
    };
    assert_eq!(length, total, "no acknowledged byte is lost and none is invented");
    let tail: Vec<u8> = (0..70_000usize).map(|i| (i * 3) as u8).collect();
    second.append(&mut meta, &tail).unwrap();
    expected.extend_from_slice(&tail);
    let mut reader = LogObjectReader::new("bucket", "wal-0", ReplicaClientOptions::default());
    assert_eq!(reader.read_range(&mut meta, 0, expected.len() + 1).unwrap(), expected);
    // the first writer is fenced: its next append is refused
    assert!(object.append(&mut meta, b"late").is_err());
}

#[cfg(feature = "replica-server")]
#[test]
fn the_reclaim_loop_frees_after_the_grace_survives_a_failed_open_and_refuses_while_page_objects_exist() {
    use matrixobjectstore_rs::chunk_reclaim::{live_pages_of, PageKey};
    use matrixobjectstore_rs::cluster::reclaim_loop::{ReclaimLoop, ReclaimSettings};
    use matrixobjectstore_rs::cluster::{
        ChunkGroupCatalog, ChunkGroupConfig, ChunkGroupIdentity, ObjectWriteOptions, ReplicaClientOptions,
    };
    use std::collections::BTreeSet;

    let root = std::env::temp_dir().join(node_regime::run_token("chunk-reclaim-loop"));
    std::fs::create_dir(&root).unwrap();
    let mut chunks = Vec::new();
    for index in 0..3 {
        let journal = root.join(format!("{index}.log"));
        chunks.push(processes::start(
            env!("CARGO_BIN_EXE_chunkserver"),
            &[
                ("MATRIXOBJECT_REPLICA_ADDR", "127.0.0.1:0"),
                ("MATRIXOBJECT_REPLICA_JOURNAL", journal.to_str().unwrap()),
            ],
        ));
    }
    let endpoints: Vec<_> = chunks.iter().map(|node| node.endpoint.clone()).collect();
    let members: Vec<_> = (1..=3).map(|id| format!("star-{id}")).collect();
    let config = ChunkGroupConfig {
        placement_id: "group-a".into(),
        failure_domain: "rack-a".into(),
        identity: Some(ChunkGroupIdentity {
            version: 3,
            group_id: 1,
            node_id: 1,
            capacity_bytes: 1 << 28,
            max_pages: 4096,
            voters: vec![1, 2, 3],
            peer_endpoints: Vec::new(),
            bootstrap_voters: Vec::new(),
            bootstrap_id: 0,
        }),
        api_endpoints: endpoints,
        member_ids: members.clone(),
    };
    let catalog = ChunkGroupCatalog::new(vec![config]).unwrap();
    let metadata = Group::with_catalog(None, None, Some(catalog.to_bytes().unwrap()));
    let meta_endpoint = metadata.nodes[metadata.leader()].as_ref().unwrap().endpoint.clone();
    let mut client = RemoteMetaClient::connect(
        &meta_endpoint,
        &MetaClientOptions {
            read_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(10),
            ..Default::default()
        },
    )
    .unwrap();
    for member in &members {
        client.heartbeat_placement_node(member, 0, 1 << 28, false, true, 100).unwrap();
    }
    let options = ObjectWriteOptions {
        page_bytes: 4096,
        target_groups: 1,
        replica: ReplicaClientOptions {
            read_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(10),
            ..Default::default()
        },
        ..Default::default()
    };
    let bytes = |length: usize, salt: usize| -> Vec<u8> { (0..length).map(|index| (index * 31 + salt) as u8).collect() };
    let stripe = 4 * 4096;
    let put = |client: &mut RemoteMetaClient, key: &str, salt: usize| {
        client.write_chunk_object("chunk", key, None, &bytes(3 * 4096, salt), &options, stripe).unwrap()
    };
    let pages_of = |manifest: &matrixobjectstore_rs::chunk_manifest::ChunkManifest| -> BTreeSet<PageKey> {
        live_pages_of([manifest])
    };
    let sequences = |client: &mut RemoteMetaClient| client.reclaim_inventory(&options).unwrap();
    let grace = 60_000u64;
    let settings = ReclaimSettings { grace_ms: grace, interval_ms: 1_000 };
    let opener = |fail_first: bool| {
        let endpoint = meta_endpoint.clone();
        let mut first = fail_first;
        move || {
            if first {
                first = false;
                return Err(matrixobjectstore_rs::ObjectError::InvalidArgument("the metadata plane is down".into()));
            }
            RemoteMetaClient::connect(
                &endpoint,
                &MetaClientOptions {
                    read_timeout: Duration::from_secs(10),
                    write_timeout: Duration::from_secs(10),
                    ..Default::default()
                },
            )
        }
    };

    let live = put(&mut client, "live", 1);
    let gone = put(&mut client, "gone", 2);
    let gone_pages = pages_of(&gone);
    client.delete_chunk_object("chunk", "gone").unwrap();

    // a round whose connection cannot be opened fails and the loop goes on: the next round works
    let mut sweeper = ReclaimLoop::new(opener(true), options.clone(), settings);
    assert!(sweeper.round(0).is_err(), "the first open fails");
    let first = sweeper.round(0).unwrap();
    assert_eq!((first.unnamed_pages, first.freed.len()), (gone_pages.len(), 0), "the first sight starts the clocks");
    assert!(sweeper.round(grace - 1).unwrap().freed.is_empty(), "nothing is freed before the grace");
    let freed: BTreeSet<PageKey> = sweeper.round(grace).unwrap().freed.into_iter().collect();
    assert_eq!(freed, gone_pages, "exactly the deleted object's pages are freed after the grace");
    assert_eq!(client.read_chunk_object("chunk", "live", &options).unwrap().1, bytes(3 * 4096, 1));
    let _ = &live;

    // a restart forgets what it had seen: the clocks start again, so a page is freed later, never earlier
    let late = put(&mut client, "late", 3);
    let late_pages = pages_of(&late);
    client.delete_chunk_object("chunk", "late").unwrap();
    let mut restarted = ReclaimLoop::new(opener(false), options.clone(), settings);
    assert!(restarted.round(10 * grace).unwrap().freed.is_empty(), "a restarted sweeper frees nothing on its first round");
    let freed: BTreeSet<PageKey> = restarted.round(11 * grace).unwrap().freed.into_iter().collect();
    assert_eq!(freed, late_pages);

    // while a page-granular object exists the sweep is refused and forgets what it saw
    let page_data = bytes(5000, 9);
    client.write_object("pages", "p", None, &page_data, &options).unwrap();
    let held = put(&mut client, "held", 4);
    let held_pages = pages_of(&held);
    client.delete_chunk_object("chunk", "held").unwrap();
    let mut careful = ReclaimLoop::new(opener(false), options.clone(), settings);
    let status = careful.status();
    // many rounds, each a full grace apart: with one live page object NOTHING is ever freed
    for round in 0..25u64 {
        let refused = careful.round((20 + round) * grace).expect_err("a page-granular object exists");
        assert!(refused.to_string().contains("1 page-granular objects"), "{refused}");
    }
    assert_eq!((status.refused_page_objects(), status.page_objects(), status.freed_pages()), (25, 1, 0));
    assert!(status.line().contains("reclaim_refused_page_objects=25"), "{}", status.line());
    for (page, sequence) in sequences(&mut client) {
        if held_pages.contains(&page) {
            assert_eq!(sequence, 1, "{page:?} must not be freed while a page-granular object exists");
        }
    }
    // once it is deleted the clocks start from the first round that is allowed
    client.delete_object("pages", "p", 1).unwrap();
    assert!(careful.round(50 * grace).unwrap().freed.is_empty(), "the clocks restart after a refusal");
    let freed: BTreeSet<PageKey> = careful.round(51 * grace).unwrap().freed.into_iter().collect();
    assert!(held_pages.is_subset(&freed), "{freed:?}");
    assert_eq!((status.refused_page_objects(), status.page_objects()), (25, 0), "no further refusal once it is gone");
    assert!(status.freed_pages() >= held_pages.len() as u64);
    assert_eq!(client.read_chunk_object("chunk", "live", &options).unwrap().1, bytes(3 * 4096, 1));
    let _ = std::fs::remove_dir_all(&root);
}

#[cfg(feature = "replica-server")]
#[test]
fn a_door_refuses_to_start_over_an_older_data_directory_and_starts_once_it_holds_none() {
    use matrixobjectstore_rs::cluster::reclaim_loop::refuse_older_data_directory;
    use matrixobjectstore_rs::cluster::{
        ChunkGroupCatalog, ChunkGroupConfig, ChunkGroupIdentity, ObjectWriteOptions, ReplicaClientOptions,
    };

    let root = std::env::temp_dir().join(node_regime::run_token("older-data-directory"));
    std::fs::create_dir(&root).unwrap();
    let mut chunks = Vec::new();
    for index in 0..3 {
        let journal = root.join(format!("{index}.log"));
        chunks.push(processes::start(
            env!("CARGO_BIN_EXE_chunkserver"),
            &[
                ("MATRIXOBJECT_REPLICA_ADDR", "127.0.0.1:0"),
                ("MATRIXOBJECT_REPLICA_JOURNAL", journal.to_str().unwrap()),
            ],
        ));
    }
    let endpoints: Vec<_> = chunks.iter().map(|node| node.endpoint.clone()).collect();
    let members: Vec<_> = (1..=3).map(|id| format!("star-{id}")).collect();
    let config = ChunkGroupConfig {
        placement_id: "group-a".into(),
        failure_domain: "rack-a".into(),
        identity: Some(ChunkGroupIdentity {
            version: 3,
            group_id: 1,
            node_id: 1,
            capacity_bytes: 1 << 28,
            max_pages: 4096,
            voters: vec![1, 2, 3],
            peer_endpoints: Vec::new(),
            bootstrap_voters: Vec::new(),
            bootstrap_id: 0,
        }),
        api_endpoints: endpoints,
        member_ids: members.clone(),
    };
    let catalog = ChunkGroupCatalog::new(vec![config]).unwrap();
    let metadata = Group::with_catalog(None, None, Some(catalog.to_bytes().unwrap()));
    let mut client = RemoteMetaClient::connect(
        &metadata.nodes[metadata.leader()].as_ref().unwrap().endpoint,
        &MetaClientOptions {
            read_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(10),
            ..Default::default()
        },
    )
    .unwrap();
    for member in &members {
        client.heartbeat_placement_node(member, 0, 1 << 28, false, true, 100).unwrap();
    }
    let options = ObjectWriteOptions {
        page_bytes: 4096,
        target_groups: 1,
        replica: ReplicaClientOptions {
            read_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(10),
            ..Default::default()
        },
        ..Default::default()
    };
    let bytes = |length: usize| -> Vec<u8> { (0..length).map(|index| (index * 31) as u8).collect() };

    // an empty cluster and one that holds chunk objects only start
    assert_eq!(refuse_older_data_directory(&mut client), Ok(()));
    client.write_chunk_object("chunk", "a", None, &bytes(5000), &options, 4 * 4096).unwrap();
    assert_eq!(refuse_older_data_directory(&mut client), Ok(()), "chunk objects are what this build stores");

    // a page-granular object (what an earlier build wrote) is refused by name, with the count
    client.write_object("pages", "old", None, &bytes(5000), &options).unwrap();
    let refused = refuse_older_data_directory(&mut client).expect_err("an older data directory must be refused");
    assert!(refused.contains("older data directory not supported"), "{refused}");
    assert!(refused.contains("1 page-granular objects"), "{refused}");
    assert!(refused.contains("data directories from earlier builds are not supported"), "{refused}");

    // a deleted page-granular object does not count: the directory holds nothing this build cannot read
    client.delete_object("pages", "old", 1).unwrap();
    assert_eq!(refuse_older_data_directory(&mut client), Ok(()));
    let _ = std::fs::remove_dir_all(&root);
}

