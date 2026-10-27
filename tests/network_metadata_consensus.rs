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
    roster: Option<String>,
    heartbeat_ttl: Option<String>,
}
impl Group {
    fn new() -> Self {
        Self::with_roster(None)
    }
    fn with_roster(roster: Option<String>) -> Self {
        Self::with_health(roster, None)
    }
    fn with_health(roster: Option<String>, heartbeat_ttl: Option<String>) -> Self {
        let root = std::env::temp_dir().join(node_regime::run_token("metadata-peers"));
        std::fs::create_dir(&root).unwrap();
        let reserved: Vec<_> = (0..3)
            .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let peers = reserved
            .iter()
            .enumerate()
            .map(|(i, l)| (i as u64 + 1, l.local_addr().unwrap()))
            .collect();
        drop(reserved);
        let mut group = Self {
            nodes: vec![None, None, None],
            peers,
            root,
            roster,
            heartbeat_ttl,
        };
        for i in 0..3 {
            std::fs::create_dir(group.root.join(i.to_string())).unwrap();
            group.start(i);
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
        let path = self.root.join(i.to_string());
        let mut environment = vec![
            ("MATRIXOBJECT_METASERVER_ADDR", "127.0.0.1:0"),
            ("MATRIXOBJECT_METASERVER_RAFT_PEERS", &peers),
            ("MATRIXOBJECT_METASERVER_RAFT_NODE_ID", &id),
            (
                "MATRIXOBJECT_METASERVER_RAFT_STATE_DIR",
                path.to_str().unwrap(),
            ),
            ("MATRIXOBJECT_METASERVER_RAFT_TIMEOUT_MS", "700"),
        ];
        let roster = self.roster.as_ref().map(|roster| {
            let mut entries: Vec<_> = roster.split(',').collect();
            if i % 2 == 1 {
                entries.reverse();
            }
            entries.join(",")
        });
        if let Some(roster) = &roster {
            environment.push(("MATRIXOBJECT_METASERVER_DATA_ROSTER", roster));
        } else {
            environment.push(("MATRIXOBJECT_METASERVER_SEED_DATA_NODES", "3"));
        }
        if let Some(ttl) = &self.heartbeat_ttl {
            environment.push(("MATRIXOBJECT_METASERVER_HEARTBEAT_TTL_MS", ttl));
        }
        let node = processes::start(env!("CARGO_BIN_EXE_metaserver"), &environment);
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
    fn leader(&self) -> usize {
        let until = Instant::now() + Duration::from_secs(15);
        loop {
            for i in 0..3 {
                if self.nodes[i].is_some() && self.client(i).probe().into_result().is_ok() {
                    return i;
                }
            }
            assert!(
                Instant::now() < until,
                "no metadata leader committed a read barrier"
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
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_metaserver"))
        .env("MATRIXOBJECT_METASERVER_ADDR", "127.0.0.1:0")
        .env("MATRIXOBJECT_METASERVER_RAFT_NODE_ID", "1")
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

    let directory = std::env::temp_dir().join(node_regime::run_token("peer-storage"));
    std::fs::create_dir(&directory).unwrap();
    let reserved = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = reserved.local_addr().unwrap();
    drop(reserved);
    let options = MetaRaftOptions {
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
    assert_eq!(replicas.write_chunk(&page, 0, payload).unwrap(), 3);
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
fn chunk_processes_report_usage_follow_leaders_and_expire_without_manual_heartbeats() {
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
    let start = |i: usize, path: &std::path::Path| {
        processes::start(
            env!("CARGO_BIN_EXE_chunkserver"),
            &[
                ("MATRIXOBJECT_REPLICA_ADDR", &endpoints[i]),
                ("MATRIXOBJECT_REPLICA_JOURNAL", path.to_str().unwrap()),
                ("MATRIXOBJECT_REPLICA_CAPACITY_BYTES", "8192"),
                ("MATRIXOBJECT_CHUNK_NODE_ID", &format!("auto-{i}")),
                ("MATRIXOBJECT_CHUNK_METADATA_ENDPOINTS", &metadata),
                ("MATRIXOBJECT_CHUNK_HEARTBEAT_MS", "100"),
            ],
        )
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
