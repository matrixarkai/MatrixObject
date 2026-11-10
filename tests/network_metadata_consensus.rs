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
struct Group {
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
                if self.membership { "3000" } else { "700" },
            ),
            (
                "MATRIXOBJECT_METASERVER_RAFT_CHECKPOINT_BYTES",
                checkpoint_bytes.as_str(),
            ),
        ];
        if self.membership {
            environment.push(("MATRIXOBJECT_METASERVER_RAFT_BOOTSTRAP_VOTERS", "1,2,3"));
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
fn election_noops_checkpoint_and_replay_without_changing_metadata() {
    use matrixobjectstore_rs::{MatrixObjectStore, MetadataStateMachine, StoreOptions};
    use matrixraft::{ApplyRequest, LogId, StateMachine};
    let mut machine =
        MetadataStateMachine::new(MatrixObjectStore::new(StoreOptions::default()).unwrap());
    let before = machine.store().metadata_state_fingerprint();
    let request = ApplyRequest {
        group_id: 1,
        log_id: LogId { term: 1, index: 1 },
        payload: Vec::new(),
    };
    let response = machine.apply(request.clone()).unwrap();
    assert_eq!(response.response, vec![0]);
    assert_eq!(machine.store().metadata_state_fingerprint(), before);
    let snapshot = machine.snapshot().unwrap();
    assert_eq!(snapshot.meta.payload_bytes, snapshot.data.len() as u64);
    let mut restored =
        MetadataStateMachine::new(MatrixObjectStore::new(StoreOptions::default()).unwrap());
    restored.install_snapshot(snapshot).unwrap();
    assert_eq!(restored.apply(request).unwrap(), response);
    let next = ApplyRequest {
        group_id: 1,
        log_id: LogId { term: 2, index: 2 },
        payload: Vec::new(),
    };
    assert_eq!(restored.apply(next).unwrap().applied_index, 2);
    assert_eq!(restored.store().metadata_state_fingerprint(), before);
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
    assert_eq!(repaired_pages, first.extents.len() + appended.extents.len());

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
    assert!(client.resume_staged_object(&aborted, None, aborted_candidate, owned_bytes, &options).is_err());
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
    let mut set = RemoteReplicaSet::connect(&endpoints, 2, 2, &options).unwrap();
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
