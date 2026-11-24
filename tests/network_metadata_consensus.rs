// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#![cfg(all(target_os = "linux", feature = "metadata-consensus"))]
mod node_regime;
use matrixobjectstore_rs::cluster::{
    CommittedPlacement, MetaClientOptions, MetaNodeOptions, MetaNodeServer, MetaRaftOptions, RemoteMetaClient,
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
        // 0 means "the server's default interval": the server refuses 0 as a setting, and an empty value is the default
        let checkpoint_bytes = if self.checkpoint_bytes == 0 { String::new() } else { self.checkpoint_bytes.to_string() };
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

    // an append adds extents after the last one, and the whole object reads back verified
    let base = client.read_chunk_manifest("chunk", "few").unwrap();
    let tail_a = bytes(300, 11);
    let grown = client.append_chunk_object(&base, &tail_a, &options, stripe).unwrap();
    assert_eq!((grown.generation, grown.length, grown.extents.len()), (3, 5300, base.extents.len() + 1));
    assert_eq!(grown.extents[..base.extents.len()], base.extents[..], "the extents already there are untouched");
    // 4 pages per stripe: 16,434 bytes is two more extents
    let tail_b = bytes(4 * 4096 + 50, 12);
    let grown_again = client.append_chunk_object(&grown, &tail_b, &options, stripe).unwrap();
    assert_eq!((grown_again.generation, grown_again.length, grown_again.extents.len()), (4, 5300 + tail_b.len() as u64, base.extents.len() + 3));
    let whole: Vec<u8> = [second_data.as_slice(), tail_a.as_slice(), tail_b.as_slice()].concat();
    assert_eq!(client.read_chunk_object("chunk", "few", &options).unwrap().1, whole);
    for (offset, span) in [(4990usize, 30usize), (5290, 20), (5300, 4096), (whole.len() - 5, 5)] {
        let slice = client.read_chunk_object_range(&grown_again, offset as u64, span as u64, &options).unwrap();
        assert_eq!(slice, &whole[offset..offset + span], "range {offset}+{span} across the appended extents");
    }
    // an append decided from a stale manifest is refused before publication and changes nothing
    match client.append_chunk_object(&base, &tail_a, &options, stripe) {
        Err(ChunkWriteFailure::BeforePublication(ObjectError::PreconditionFailed(_))) => {}
        other => panic!("a stale append must be refused before publication: {other:?}"),
    }
    assert_eq!(client.read_chunk_manifest("chunk", "few").unwrap(), grown_again, "the refusal changed nothing");
    // an empty append and one with another page size are refused
    assert!(matches!(
        client.append_chunk_object(&grown_again, &[], &options, stripe),
        Err(ChunkWriteFailure::BeforePublication(ObjectError::InvalidArgument(_)))
    ));
    let other_pages = ObjectWriteOptions { page_bytes: 8192, ..options.clone() };
    assert!(matches!(
        client.append_chunk_object(&grown_again, &tail_a, &other_pages, stripe),
        Err(ChunkWriteFailure::BeforePublication(ObjectError::InvalidArgument(_)))
    ));
    // a manifest that already names the most extents refuses an append whole, before any page is written
    let full = matrixobjectstore_rs::chunk_object::build_manifest(
        "chunk",
        "full",
        1,
        4096,
        (0..matrixobjectstore_rs::chunk_manifest::MAX_CHUNK_EXTENTS as u64)
            .map(|index| matrixobjectstore_rs::chunk_manifest::ChunkExtent {
                group: "group-a".into(),
                volume_id: 0,
                chunk_id: index + 1,
                kind: matrixobjectstore_rs::chunk_manifest::CHUNK_KIND_PAGED,
                object_offset: index,
                chunk_offset: 0,
                length: 1,
                flags: matrixobjectstore_rs::chunk_manifest::CHUNK_FLAG_SEALED,
                digest: vec![7; 32],
                footer_crc: 0,
                footer_sha256: Vec::new(),
            })
            .collect(),
    )
    .unwrap();
    match client.append_chunk_object(&full, &tail_a, &options, stripe) {
        Err(ChunkWriteFailure::BeforePublication(ObjectError::InvalidArgument(message))) => {
            assert!(message.contains("append refused"), "{message}")
        }
        other => panic!("an append past the extent bound must be refused whole: {other:?}"),
    }
    // the fence republishes the same extents one generation later
    let fenced = client.republish_chunk_manifest(&grown_again).unwrap();
    assert_eq!((fenced.generation, fenced.length, &fenced.extents), (grown_again.generation + 1, grown_again.length, &grown_again.extents));
    assert_eq!(client.read_chunk_object("chunk", "few", &options).unwrap().1, whole);

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
        gateway.ensure_bucket("gw").unwrap();
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
        // an append lands at the end and the object reads back whole, verified
        let attempt = "00112233445566778899aabbccddeeff";
        let more = bytes(70_000, 5);
        let appended = call("PUT", "/gw/object", &[("x-amz-write-offset-bytes", "100"), ("x-amz-attempt-id", attempt)], &more);
        assert_eq!(appended.status, 200, "{}", String::from_utf8_lossy(&appended.body));
        let whole: Vec<u8> = [second.as_slice(), more.as_slice()].concat();
        assert_eq!(call("GET", "/gw/object", &[], &[]).body, whole);
        // one at the wrong offset is refused and changes nothing
        let wrong = call("PUT", "/gw/object", &[("x-amz-write-offset-bytes", "100")], b"late");
        assert!(wrong.status >= 400, "an append that would not land at the end must be refused, got {}", wrong.status);
        assert_eq!(call("GET", "/gw/object", &[], &[]).body, whole);

        // THE APPEND RESOLVE, END TO END. The attempt the gateway saw land is recognised as landed by its
        // own extents; a different attempt for the same range is only ever ambiguous; and a resolve at the
        // end of the object fences an attempt that never landed, after which the retry lands exactly once.
        let resolve = |id: &str, offset: usize, length: usize| {
            call("POST", &format!("/gw/object?append-resolve&attempt={id}&offset={offset}&length={length}"), &[], &[])
        };
        let landed = resolve(attempt, 100, more.len());
        assert_eq!((landed.status, String::from_utf8_lossy(&landed.body).trim().to_string()), (200, "landed".to_string()));
        let stranger = resolve("ffeeddccbbaa99887766554433221100", 100, more.len());
        assert_eq!((stranger.status, String::from_utf8_lossy(&stranger.body).trim().to_string()), (200, "ambiguous".to_string()));
        let end = 100 + more.len();
        let before_fence = call("HEAD", "/gw/object", &[], &[]);
        let etag_of = |response: &matrixobjectstore_rs::net::Response| {
            response.headers.iter().find(|(name, _)| name.eq_ignore_ascii_case("etag")).map(|(_, value)| value.clone()).unwrap_or_default()
        };
        let never = resolve("0123456789abcdef0123456789abcdef", end, 5);
        assert_eq!((never.status, String::from_utf8_lossy(&never.body).trim().to_string()), (200, "absent".to_string()));
        assert_ne!(etag_of(&call("HEAD", "/gw/object", &[], &[])), etag_of(&before_fence), "the fence is a new generation");
        assert_eq!(call("GET", "/gw/object", &[], &[]).body, whole, "the fence changed no byte");
        let retry = call("PUT", "/gw/object", &[("x-amz-write-offset-bytes", &end.to_string())], b"tail!");
        assert_eq!(retry.status, 200, "{}", String::from_utf8_lossy(&retry.body));
        let after: Vec<u8> = [whole.as_slice(), b"tail!".as_slice()].concat();
        assert_eq!(call("GET", "/gw/object", &[], &[]).body, after, "the retry landed exactly once");
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

/// STAR MODE, MEMBER FAILURE: a routed Star group needs every member to acknowledge a write, so one stopped
/// member refuses every attempt on that group. The write is retried after the unreachable member is reported
/// offline, and placement then chooses the other group; the same for an append.
#[cfg(feature = "replica-server")]
#[test]
fn a_chunk_write_and_append_switch_groups_when_a_member_of_the_chosen_group_stops() {
    use matrixobjectstore_rs::cluster::{
        ChunkGroupCatalog, ChunkGroupConfig, ChunkGroupIdentity, FailoverPolicy, ObjectWriteOptions, ReplicaClientOptions,
    };

    let root = std::env::temp_dir().join(node_regime::run_token("chunk-failover"));
    std::fs::create_dir(&root).unwrap();
    let mut chunks = Vec::new();
    for index in 0..6 {
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
    let group_config = |name: &str, group_id: u64, first: usize| ChunkGroupConfig {
        placement_id: name.into(),
        failure_domain: format!("rack-{name}"),
        identity: Some(ChunkGroupIdentity {
            version: 3,
            group_id,
            node_id: 1,
            capacity_bytes: 1 << 28,
            max_pages: 4096,
            voters: vec![1, 2, 3],
            peer_endpoints: Vec::new(),
            bootstrap_voters: Vec::new(),
            bootstrap_id: 0,
        }),
        api_endpoints: endpoints[first..first + 3].to_vec(),
        member_ids: (first + 1..=first + 3).map(|id| format!("star-{id}")).collect(),
    };
    let catalog = ChunkGroupCatalog::new(vec![group_config("group-a", 1, 0), group_config("group-b", 2, 3)]).unwrap();
    let metadata = Group::with_catalog(None, None, Some(catalog.to_bytes().unwrap()));
    let meta_endpoint = metadata.nodes[metadata.leader()].as_ref().unwrap().endpoint.clone();
    let meta_options = || MetaClientOptions {
        read_timeout: Duration::from_secs(10),
        write_timeout: Duration::from_secs(10),
        ..Default::default()
    };
    let mut client = RemoteMetaClient::connect(&meta_endpoint, &meta_options()).unwrap();
    // group-a is online and group-b is not: the first write can only be placed on group-a
    for id in 1..=3 {
        client.heartbeat_placement_node(&format!("star-{id}"), 0, 1 << 28, false, true, 100).unwrap();
    }
    for id in 4..=6 {
        client.heartbeat_placement_node(&format!("star-{id}"), 0, 1 << 28, false, false, 100).unwrap();
    }
    let options = ObjectWriteOptions {
        page_bytes: 4096,
        target_groups: 1,
        replica: ReplicaClientOptions {
            connect_timeout: Duration::from_secs(2),
            read_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(10),
            ..Default::default()
        },
        ..Default::default()
    };
    let bytes = |length: usize, salt: usize| -> Vec<u8> { (0..length).map(|index| (index * 31 + salt) as u8).collect() };
    let stripe = 4 * 4096;
    let before = client.write_chunk_object("fo", "before", None, &bytes(5000, 1), &options, stripe).unwrap();
    assert_eq!(before.extents[0].group, "group-a");

    // one member of group-a stops; group-b comes online while the write waits to be retried
    let stopped = chunks.remove(1);
    drop(stopped);
    let waker_endpoint = meta_endpoint.clone();
    let waker = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(150));
        let mut other = RemoteMetaClient::connect(&waker_endpoint, &meta_options()).unwrap();
        for id in 4..=6 {
            other.heartbeat_placement_node(&format!("star-{id}"), 0, 1 << 28, false, true, 200).unwrap();
        }
    });
    let policy = FailoverPolicy {
        initial_wait: Duration::from_millis(400),
        max_wait: Duration::from_secs(2),
        max_switches: 6,
        report_unreachable: true,
    };
    let data = bytes(9000, 2);
    let written = client
        .write_chunk_object_with_failover("fo", "after", None, &data, &options, stripe, &policy)
        .expect("the write must be acknowledged on the other group");
    waker.join().unwrap();
    assert!(written.switches >= 1, "the first attempt was on the group with a stopped member");
    assert_eq!(written.manifest.extents[0].group, "group-b");
    assert_eq!(client.read_chunk_object("fo", "after", &options).unwrap(), (written.manifest.clone(), data.clone()));

    // an append to that object stays on the healthy group and reads back whole
    let grown = client
        .append_chunk_object_with_failover(&written.manifest, b"+tail", &options, stripe, &policy)
        .expect("the append must be acknowledged");
    let whole: Vec<u8> = [data.as_slice(), b"+tail".as_slice()].concat();
    assert_eq!(client.read_chunk_object("fo", "after", &options).unwrap(), (grown.manifest, whole));
    let _ = std::fs::remove_dir_all(&root);
}

/// THE S3 SURFACE OVER CHUNK OBJECTS, END TO END: what the retired embedded door answered and the cluster gateway
/// still does (listing with prefix, delimiter and paging; conditional writes and reads; copy; batch delete; multipart;
/// ranges; delete of a bucket), against real metadata and chunk processes. The same scenarios run against a fake plane
/// in the gateway's unit tests; this runs them through `ChunkPlane` and the real replicas.
#[cfg(feature = "replica-server")]
#[test]
fn the_s3_door_over_chunk_objects_serves_listing_conditions_copy_batch_multipart_and_ranges() {
    use matrixobjectstore_rs::cluster::{
        ChunkGroupCatalog, ChunkGroupConfig, ChunkGroupIdentity, ObjectWriteOptions, ReplicaClientOptions,
    };
    use matrixobjectstore_rs::net::cluster_gateway::{ClusterGateway, GatewayOptions};
    use matrixobjectstore_rs::net::{Request, RequestHandler, Response};

    let root = std::env::temp_dir().join(node_regime::run_token("s3-over-chunks"));
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
    let meta_options = || MetaClientOptions {
        read_timeout: Duration::from_secs(10),
        write_timeout: Duration::from_secs(10),
        ..Default::default()
    };
    let mut client = RemoteMetaClient::connect(&meta_endpoint, &meta_options()).unwrap();
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
    let gateway_options = GatewayOptions { write: options.clone(), replica: options.replica.clone(), ..Default::default() };
    let gateway = ClusterGateway::over_chunk_objects(move || RemoteMetaClient::connect(&meta_endpoint, &meta_options()), gateway_options, "gw");
    gateway.ensure_bucket("gw").unwrap();
    let call = |method: &str, target: &str, headers: &[(&str, &str)], body: &[u8]| -> Response {
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
    let text = |response: &Response| String::from_utf8_lossy(&response.body).to_string();
    let header = |response: &Response, name: &str| -> Option<String> {
        response.headers.iter().find(|(n, _)| n.eq_ignore_ascii_case(name)).map(|(_, v)| v.clone())
    };
    let between = |body: &str, tag: &str| -> Option<String> {
        let (open, close) = (format!("<{tag}>"), format!("</{tag}>"));
        body.split_once(&open)?.1.split_once(&close).map(|(value, _)| value.to_string())
    };
    let all_between = |body: &str, tag: &str| -> Vec<String> {
        let (open, close) = (format!("<{tag}>"), format!("</{tag}>"));
        body.split(&open).skip(1).filter_map(|rest| rest.split_once(&close).map(|(v, _)| v.to_string())).collect()
    };
    let escape = |value: &str| -> String {
        value
            .bytes()
            .map(|byte| match byte {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => (byte as char).to_string(),
                _ => format!("%{byte:02X}"),
            })
            .collect()
    };

    // the health probe, the bucket verbs that need no cluster call
    assert_eq!(call("GET", "/healthz", &[], &[]).status, 200);

    // LISTING: prefix, delimiter and paging over real chunk objects
    for key in ["a/1", "a/2", "a/x/3", "b/1", "c", "d"] {
        assert_eq!(call("PUT", &format!("/gw/{key}"), &[], key.as_bytes()).status, 200, "{key}");
    }
    let top = text(&call("GET", "/gw?list-type=2&delimiter=%2F", &[], &[]));
    assert_eq!(all_between(&top, "Key"), ["c", "d"], "{top}");
    assert!(top.contains("<CommonPrefixes><Prefix>a/</Prefix></CommonPrefixes>") && top.contains("<CommonPrefixes><Prefix>b/</Prefix></CommonPrefixes>"), "{top}");
    let under = text(&call("GET", "/gw?list-type=2&prefix=a%2F&delimiter=%2F", &[], &[]));
    assert_eq!(all_between(&under, "Key"), ["a/1", "a/2"], "{under}");
    let mut seen = Vec::new();
    let mut token: Option<String> = None;
    for _ in 0..20 {
        let target = match &token {
            None => "/gw?list-type=2&delimiter=%2F&max-keys=2".to_string(),
            Some(token) => format!("/gw?list-type=2&delimiter=%2F&max-keys=2&continuation-token={}", escape(token)),
        };
        let page = text(&call("GET", &target, &[], &[]));
        seen.extend(all_between(&page, "Key"));
        seen.extend(page.split("<CommonPrefixes><Prefix>").skip(1).filter_map(|rest| rest.split_once("</Prefix>").map(|(v, _)| v.to_string())));
        if between(&page, "IsTruncated").as_deref() == Some("true") {
            token = Some(between(&page, "NextContinuationToken").expect("a truncated page names its token"));
        } else {
            break;
        }
    }
    seen.sort();
    assert_eq!(seen, ["a/", "b/", "c", "d"], "paging loses and repeats nothing");

    // CONDITIONS: create-only, compare-and-swap on the etag, and conditional reads
    let created = call("PUT", "/gw/cond", &[("if-none-match", "*")], b"one");
    assert_eq!(created.status, 200);
    assert_eq!(call("PUT", "/gw/cond", &[("if-none-match", "*")], b"again").status, 412, "create-only on an existing key");
    let etag = header(&created, "etag").expect("a write answers its etag");
    let swapped = call("PUT", "/gw/cond", &[("if-match", &etag)], b"two");
    assert_eq!(swapped.status, 200, "{}", text(&swapped));
    assert_eq!(call("PUT", "/gw/cond", &[("if-match", &etag)], b"stale").status, 412, "the old etag no longer matches");
    let current = header(&swapped, "etag").unwrap();
    assert_ne!(etag, current);
    assert_eq!(call("GET", "/gw/cond", &[("if-none-match", &current)], &[]).status, 304);
    assert_eq!(call("GET", "/gw/cond", &[("if-match", &etag)], &[]).status, 412);
    assert_eq!(call("GET", "/gw/cond", &[], &[]).body, b"two");

    // RANGES: closed, open-ended and suffix
    let body: Vec<u8> = (0..20_000u32).map(|index| (index % 251) as u8).collect();
    assert_eq!(call("PUT", "/gw/ranged", &[], &body).status, 200);
    for (spec, expected) in [("bytes=10-19", &body[10..20]), ("bytes=19990-", &body[19990..]), ("bytes=-5", &body[19995..]), ("bytes=4090-4101", &body[4090..4102])] {
        let reply = call("GET", "/gw/ranged", &[("range", spec)], &[]);
        assert_eq!((reply.status, reply.body.as_slice()), (206, expected), "{spec}");
    }

    // COPY moves the bytes, and the copy does not follow the source
    assert_eq!(call("PUT", "/gw/copied", &[("x-amz-copy-source", "/gw/ranged")], &[]).status, 200);
    assert_eq!(call("PUT", "/gw/ranged", &[], b"replaced").status, 200);
    assert_eq!(call("GET", "/gw/copied", &[], &[]).body, body);

    // MULTIPART: assembled whole, the parts leave nothing behind, an aborted upload leaves nothing
    let begun = call("POST", "/gw/big?uploads", &[], &[]);
    assert_eq!(begun.status, 200, "{}", text(&begun));
    let id = between(&text(&begun), "UploadId").unwrap();
    let two = call("PUT", &format!("/gw/big?partNumber=2&uploadId={id}"), &[], b"-world");
    let one = call("PUT", &format!("/gw/big?partNumber=1&uploadId={id}"), &[], b"hello");
    assert_eq!((one.status, two.status), (200, 200));
    assert_eq!(call("GET", "/gw/big", &[], &[]).status, 404, "nothing exists before completion");
    let listing = text(&call("GET", "/gw?list-type=2", &[], &[]));
    assert!(!listing.contains(".multipart"), "{listing}");
    let (e1, e2) = (header(&one, "etag").unwrap(), header(&two, "etag").unwrap());
    let complete = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{e1}</ETag></Part><Part><PartNumber>2</PartNumber><ETag>{e2}</ETag></Part></CompleteMultipartUpload>"
    );
    let done = call("POST", &format!("/gw/big?uploadId={id}"), &[], complete.as_bytes());
    assert_eq!(done.status, 200, "{}", text(&done));
    assert_eq!(call("GET", "/gw/big", &[], &[]).body, b"hello-world");
    let aborted_id = between(&text(&call("POST", "/gw/gone?uploads", &[], &[])), "UploadId").unwrap();
    assert_eq!(call("PUT", &format!("/gw/gone?partNumber=1&uploadId={aborted_id}"), &[], b"x").status, 200);
    assert_eq!(call("DELETE", &format!("/gw/gone?uploadId={aborted_id}"), &[], &[]).status, 204);
    assert_eq!(call("GET", "/gw/gone", &[], &[]).status, 404);

    // BATCH DELETE and DeleteBucket
    let batch = "<Delete><Object><Key>a/1</Key></Object><Object><Key>a/2</Key></Object><Object><Key>never-existed</Key></Object></Delete>";
    let deleted = call("POST", "/gw?delete", &[], batch.as_bytes());
    assert_eq!(deleted.status, 200, "{}", text(&deleted));
    assert_eq!(all_between(&text(&deleted), "Key").len(), 3, "{}", text(&deleted));
    assert_eq!(call("GET", "/gw/a/1", &[], &[]).status, 404);
    assert_eq!(call("DELETE", "/gw", &[], &[]).status, 409, "a bucket with objects in it is not deleted");
    let remaining = all_between(&text(&call("GET", "/gw?list-type=2", &[], &[])), "Key");
    for key in remaining {
        assert_eq!(call("DELETE", &format!("/gw/{key}"), &[], &[]).status, 204, "{key}");
    }
    assert_eq!(call("DELETE", "/gw", &[], &[]).status, 204);
    let _ = std::fs::remove_dir_all(&root);
}

/// BUCKETS ON THE CHUNK PATH, AGAINST REAL PROCESSES: a bucket is a replicated record that survives what an object
/// survives. Created, configured, listed, refused when it is missing and removed, through the S3 door over chunk objects.
#[cfg(feature = "replica-server")]
#[test]
fn the_s3_door_over_chunk_objects_creates_configures_lists_and_deletes_buckets() {
    use matrixobjectstore_rs::cluster::{
        ChunkGroupCatalog, ChunkGroupConfig, ChunkGroupIdentity, ObjectWriteOptions, ReplicaClientOptions,
    };
    use matrixobjectstore_rs::net::cluster_gateway::{ClusterGateway, GatewayOptions};
    use matrixobjectstore_rs::net::{Request, RequestHandler, Response};

    let root = std::env::temp_dir().join(node_regime::run_token("s3-buckets"));
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
    let meta_options = || MetaClientOptions {
        read_timeout: Duration::from_secs(10),
        write_timeout: Duration::from_secs(10),
        ..Default::default()
    };
    let mut client = RemoteMetaClient::connect(&meta_endpoint, &meta_options()).unwrap();
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
    let gateway_options = GatewayOptions { write: options.clone(), replica: options.replica.clone(), ..Default::default() };
    let open = {
        let endpoint = meta_endpoint.clone();
        move || RemoteMetaClient::connect(&endpoint, &meta_options())
    };
    let gateway = ClusterGateway::over_chunk_objects(open.clone(), gateway_options.clone(), "start");
    let call = |gateway: &ClusterGateway, method: &str, target: &str, headers: &[(&str, &str)], body: &[u8]| -> Response {
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
    let text = |response: &Response| String::from_utf8_lossy(&response.body).to_string();

    // nothing exists until it is created
    assert_eq!(call(&gateway, "PUT", "/photos/cat", &[], b"meow").status, 404);
    assert_eq!(call(&gateway, "HEAD", "/photos", &[], &[]).status, 404);
    assert!(gateway.ensure_bucket("start").unwrap());
    assert_eq!(call(&gateway, "PUT", "/photos", &[], &[]).status, 200);
    assert_eq!(call(&gateway, "PUT", "/photos", &[], &[]).status, 200, "creating one that exists succeeds");
    assert_eq!(call(&gateway, "PUT", "/photos/cat", &[], b"meow").status, 200);
    let listed = text(&call(&gateway, "GET", "/", &[], &[]));
    assert!(listed.contains("<Name>photos</Name>") && listed.contains("<Name>start</Name>"), "{listed}");

    // configuration is stored and read back
    let tagging = "<Tagging><TagSet><Tag><Key>team</Key><Value>storage</Value></Tag></TagSet></Tagging>";
    assert_eq!(call(&gateway, "PUT", "/photos?tagging", &[], tagging.as_bytes()).status, 200);
    let cors = "<CORSConfiguration><CORSRule><AllowedOrigin>*</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>";
    assert_eq!(call(&gateway, "PUT", "/photos?cors", &[], cors.as_bytes()).status, 200);
    // versioning is configured on a bucket that stays: deleting an object of a versioned bucket keeps a version, so
    // that bucket is not empty
    assert_eq!(call(&gateway, "PUT", "/archive", &[], &[]).status, 200);
    let on = "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>";
    assert_eq!(call(&gateway, "PUT", "/archive?versioning", &[], on.as_bytes()).status, 200);

    // ANOTHER GATEWAY on the same cluster sees the same buckets and the same configuration: the record is the cluster's, not the door's
    let other = ClusterGateway::over_chunk_objects(open, gateway_options, "start");
    assert_eq!(call(&other, "HEAD", "/photos", &[], &[]).status, 200);
    assert!(text(&call(&other, "GET", "/photos?tagging", &[], &[])).contains("<Key>team</Key>"));
    assert!(text(&call(&other, "GET", "/photos?cors", &[], &[])).contains("<AllowedOrigin>*</AllowedOrigin>"));
    assert!(text(&call(&other, "GET", "/archive?versioning", &[], &[])).contains("<Status>Enabled</Status>"));
    assert_eq!(call(&other, "GET", "/photos/cat", &[], &[]).body, b"meow");

    // a bucket with an object is not deleted; an empty one is, and is then missing everywhere
    assert_eq!(call(&gateway, "DELETE", "/photos", &[], &[]).status, 409);
    assert_eq!(call(&gateway, "DELETE", "/photos/cat", &[], &[]).status, 204);
    assert_eq!(call(&gateway, "DELETE", "/photos", &[], &[]).status, 204);
    std::thread::sleep(Duration::from_millis(2100));
    assert_eq!(call(&other, "HEAD", "/photos", &[], &[]).status, 404);
    assert_eq!(call(&other, "PUT", "/photos/cat", &[], b"x").status, 404);
    let _ = std::fs::remove_dir_all(&root);
}

/// ATTRIBUTES, OBJECT LOCK, VERSIONING, QUOTAS AND THE CHANGE FEED, AGAINST REAL PROCESSES. The unit tests run the rules
/// over an in-memory plane; this runs the three things only the chunk plane does for them: publish a manifest again with
/// other attributes, name the same pages under a second key (a kept version), and delete exactly the manifest a lock
/// check read, over three chunk servers and a metadata group, with the data read back from the pages.
#[cfg(feature = "replica-server")]
#[test]
fn the_s3_door_over_chunk_objects_keeps_attributes_locks_versions_quotas_and_a_change_feed() {
    use matrixobjectstore_rs::cluster::{
        ChunkGroupCatalog, ChunkGroupConfig, ChunkGroupIdentity, ObjectWriteOptions, ReplicaClientOptions,
    };
    use matrixobjectstore_rs::net::cluster_gateway::{ClusterGateway, GatewayOptions};
    use matrixobjectstore_rs::net::{Request, RequestHandler, Response};

    let root = std::env::temp_dir().join(node_regime::run_token("s3-features"));
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
    let meta_options = || MetaClientOptions {
        read_timeout: Duration::from_secs(10),
        write_timeout: Duration::from_secs(10),
        ..Default::default()
    };
    let mut client = RemoteMetaClient::connect(&meta_endpoint, &meta_options()).unwrap();
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
    let gateway_options = GatewayOptions { write: options.clone(), replica: options.replica.clone(), ..Default::default() };
    let open = {
        let endpoint = meta_endpoint.clone();
        move || RemoteMetaClient::connect(&endpoint, &meta_options())
    };
    let gateway = ClusterGateway::over_chunk_objects(open, gateway_options, "start");
    let call = |method: &str, target: &str, headers: &[(&str, &str)], body: &[u8]| -> Response {
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
    let text = |response: &Response| String::from_utf8_lossy(&response.body).to_string();
    let header = |response: &Response, name: &str| response.headers.iter().find(|(n, _)| n.eq_ignore_ascii_case(name)).map(|(_, v)| v.clone());
    let count = |body: &str, needle: &str| body.matches(needle).count();
    let lock = "<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>";
    let versioned = "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>";
    for bucket in ["docs", "vault", "hist", "small", "feed"] {
        assert_eq!(call("PUT", &format!("/{bucket}"), &[], &[]).status, 200, "{bucket}");
    }
    assert_eq!(call("PUT", "/vault?object-lock", &[], lock.as_bytes()).status, 200);
    assert_eq!(call("PUT", "/hist?versioning", &[], versioned.as_bytes()).status, 200);
    assert_eq!(call("PUT", "/small?quota", &[], b"<Quota><Bytes>10</Bytes></Quota>").status, 200);
    let on = "<ChangeFeedConfiguration><Status>Enabled</Status></ChangeFeedConfiguration>";
    assert_eq!(call("PUT", "/feed?changefeed", &[], on.as_bytes()).status, 200);

    // ATTRIBUTES: written with the object, republished without moving a byte, dropped by an overwrite
    let body: Vec<u8> = (0..9000u32).map(|i| (i % 251) as u8).collect();
    let put = call("PUT", "/docs/report", &[("content-type", "text/csv"), ("x-amz-meta-owner", "ana"), ("x-amz-tagging", "team=storage")], &body);
    assert_eq!(put.status, 200, "{}", text(&put));
    let head = call("HEAD", "/docs/report", &[], &[]);
    assert_eq!(header(&head, "content-type").as_deref(), Some("text/csv"));
    assert_eq!(header(&head, "x-amz-meta-owner").as_deref(), Some("ana"));
    assert!(header(&head, "last-modified").is_some());
    let retag = "<Tagging><TagSet><Tag><Key>team</Key><Value>data</Value></Tag><Tag><Key>tier</Key><Value>hot</Value></Tag></TagSet></Tagging>";
    assert_eq!(call("PUT", "/docs/report?tagging", &[], retag.as_bytes()).status, 200);
    let tags = text(&call("GET", "/docs/report?tagging", &[], &[]));
    assert!(tags.contains("<Key>tier</Key>") && tags.contains("<Value>data</Value>"), "{tags}");
    assert_eq!(call("GET", "/docs/report", &[], &[]).body, body, "the bytes are what was written");
    assert_eq!(header(&call("HEAD", "/docs/report", &[], &[]), "content-type").as_deref(), Some("text/csv"), "an attribute change keeps the rest");
    assert_eq!(call("PUT", "/docs/report", &[], b"plain").status, 200);
    assert!(header(&call("HEAD", "/docs/report", &[], &[]), "x-amz-meta-owner").is_none(), "an overwrite names its own attributes");

    // OBJECT LOCK: a governance retention refuses an overwrite and a delete, and a bypass deletes exactly what was checked
    let until = "2099-01-01T00:00:00Z";
    let held = call("PUT", "/vault/g", &[("x-amz-object-lock-mode", "GOVERNANCE"), ("x-amz-object-lock-retain-until-date", until)], &body);
    assert_eq!(held.status, 200, "{}", text(&held));
    assert_eq!(call("PUT", "/vault/g", &[], b"again").status, 403);
    assert_eq!(call("DELETE", "/vault/g", &[], &[]).status, 403);
    assert_eq!(call("GET", "/vault/g", &[], &[]).body, body, "a refused overwrite and delete changed nothing");
    assert_eq!(call("DELETE", "/vault/g", &[("x-amz-bypass-governance-retention", "true")], &[]).status, 204);
    assert_eq!(call("GET", "/vault/g", &[], &[]).status, 404);
    let compliance = "<Retention><Mode>COMPLIANCE</Mode><RetainUntilDate>2099-01-01T00:00:00Z</RetainUntilDate></Retention>";
    assert_eq!(call("PUT", "/vault/c", &[], b"c").status, 200);
    assert_eq!(call("PUT", "/vault/c?retention", &[], compliance.as_bytes()).status, 200);
    assert_eq!(call("DELETE", "/vault/c", &[("x-amz-bypass-governance-retention", "true")], &[]).status, 403, "compliance is not bypassed");

    // VERSIONING: the replaced manifest is kept under a hidden key that names the same pages
    let (first, second) = (vec![1u8; 5000], vec![2u8; 6000]);
    let v1 = header(&call("PUT", "/hist/page", &[("content-type", "text/plain")], &first), "x-amz-version-id").unwrap();
    let v2 = header(&call("PUT", "/hist/page", &[], &second), "x-amz-version-id").unwrap();
    let old = call("GET", &format!("/hist/page?versionId={v1}"), &[], &[]);
    assert_eq!((old.status, old.body.as_slice()), (200, first.as_slice()), "an old version is read from the pages it always had");
    assert_eq!(header(&old, "content-type").as_deref(), Some("text/plain"));
    assert_eq!(call("GET", "/hist/page", &[], &[]).body, second);
    let deleted = call("DELETE", "/hist/page", &[], &[]);
    assert_eq!(header(&deleted, "x-amz-delete-marker").as_deref(), Some("true"));
    assert_eq!(call("GET", "/hist/page", &[], &[]).status, 404);
    assert_eq!(call("GET", &format!("/hist/page?versionId={v2}"), &[], &[]).body, second);
    let listing = text(&call("GET", "/hist?versions", &[], &[]));
    assert_eq!(count(&listing, "<VersionId>"), 3, "{listing}");
    assert_eq!(call("PUT", "/hist/page", &[], b"three").status, 200);
    let current = header(&call("HEAD", "/hist/page", &[], &[]), "x-amz-version-id").unwrap();
    // removing the current version brings the latest kept one (the marker) back; removing v1 frees only its manifest
    assert_eq!(call("DELETE", &format!("/hist/page?versionId={current}"), &[], &[]).status, 204);
    assert_eq!(call("GET", "/hist/page", &[], &[]).status, 404);
    assert_eq!(call("DELETE", &format!("/hist/page?versionId={v1}"), &[], &[]).status, 204);
    assert_eq!(call("GET", &format!("/hist/page?versionId={v1}"), &[], &[]).status, 404);
    assert_eq!(call("GET", &format!("/hist/page?versionId={v2}"), &[], &[]).body, second, "the other version still reads");

    // QUOTA: the write that would pass it is refused and stores nothing
    assert_eq!(call("PUT", "/small/a", &[], b"123456").status, 200);
    let over = call("PUT", "/small/b", &[], b"123456");
    assert_eq!(over.status, 403, "{}", text(&over));
    assert!(text(&over).contains("QuotaExceeded"));
    assert_eq!(call("GET", "/small/b", &[], &[]).status, 404);

    // CHANGE FEED: what happened to the bucket, in order, read by cursor
    assert_eq!(call("PUT", "/feed/a", &[], b"one").status, 200);
    assert_eq!(call("PUT", "/feed/a", &[("x-amz-write-offset-bytes", "3")], b"two").status, 200);
    assert_eq!(call("DELETE", "/feed/a", &[], &[]).status, 204);
    let changes = text(&call("GET", "/feed?changes", &[], &[]));
    let operations: Vec<&str> = changes.split("<Operation>").skip(1).filter_map(|rest| rest.split_once("</Operation>").map(|(op, _)| op)).collect();
    assert_eq!(operations, ["put", "append", "delete"], "{changes}");
    let _ = std::fs::remove_dir_all(&root);
}

