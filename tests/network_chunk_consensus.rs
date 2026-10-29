// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#![cfg(all(target_os = "linux", feature = "chunk-consensus"))]
#[allow(dead_code)]
mod node_regime;
use matrixobjectstore_rs::cluster::{
    ChunkRaftOptions, RemoteReplicaClient, ReplicaClientOptions, ReplicaNodeOptions,
    ReplicaNodeServer,
};
use matrixobjectstore_rs::{ChunkId, PageId};
use node_regime::processes::{self, NodeProcess};
use std::{
    collections::BTreeMap,
    net::{SocketAddr, TcpListener},
    path::PathBuf,
    time::{Duration, Instant},
};
struct Group {
    nodes: Vec<Option<NodeProcess>>,
    peers: BTreeMap<u64, SocketAddr>,
    root: PathBuf,
    capacity: u64,
    checkpoint_bytes: u64,
    group_id: u64,
    max_pages: u64,
    heartbeat_ms: &'static str,
    api_addresses: Vec<SocketAddr>,
    reservations: Vec<Option<(TcpListener, TcpListener)>>,
    report: Option<(String, Vec<SocketAddr>)>,
}
impl Group {
    fn new() -> Self {
        Self::with_capacity(65536)
    }
    fn with_capacity(capacity: u64) -> Self {
        let mut group = Self::configured(capacity);
        for i in 0..3 {
            group.start(i);
        }
        group
    }
    fn configured(capacity: u64) -> Self {
        let root = std::env::temp_dir().join(node_regime::run_token("chunk-peers"));
        std::fs::create_dir(&root).unwrap();
        // Hold all six distinct listeners until their process takes ownership.
        // API port selection must never consume a future voter's Raft endpoint.
        let reserved: Vec<_> = (0..3)
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
        let g = Self {
            nodes: vec![None, None, None],
            peers,
            root,
            capacity,
            checkpoint_bytes: 0,
            group_id: 1,
            max_pages: 64,
            heartbeat_ms: "100",
            api_addresses,
            reservations: reserved.into_iter().map(Some).collect(),
            report: None,
        };
        for i in 0..3 {
            std::fs::create_dir(g.root.join(i.to_string())).unwrap();
        }
        g
    }
    fn placement_id(&self) -> String {
        if self.group_id == 1 {
            "group-a".into()
        } else {
            format!("group-{}", self.group_id)
        }
    }
    fn member_id(&self, i: usize) -> String {
        if self.group_id == 1 {
            format!("chunk-{i}")
        } else {
            format!("chunk-{}-{i}", self.group_id)
        }
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
        let bind = self
            .report
            .as_ref()
            .map(|(_, a)| a[i].to_string())
            .unwrap_or_else(|| self.api_addresses[i].to_string());
        let node_id = self.member_id(i);
        let group_id = self.group_id.to_string();
        let max_pages = self.max_pages.to_string();
        let capacity = self.capacity.to_string();
        let checkpoint_bytes = self.checkpoint_bytes.to_string();
        let timeout = if self.capacity > 65536 { "5000" } else { "700" };
        let mut environment = vec![
            ("MATRIXOBJECT_REPLICA_ADDR", bind.as_str()),
            ("MATRIXOBJECT_REPLICA_CAPACITY_BYTES", capacity.as_str()),
            ("MATRIXOBJECT_REPLICA_MAX_PAGES", max_pages.as_str()),
            ("MATRIXOBJECT_CHUNK_RAFT_GROUP_ID", group_id.as_str()),
            ("MATRIXOBJECT_CHUNK_RAFT_PEERS", peers.as_str()),
            ("MATRIXOBJECT_CHUNK_RAFT_NODE_ID", id.as_str()),
            ("MATRIXOBJECT_CHUNK_RAFT_STATE_DIR", path.to_str().unwrap()),
            ("MATRIXOBJECT_CHUNK_RAFT_TIMEOUT_MS", timeout),
            (
                "MATRIXOBJECT_CHUNK_RAFT_CHECKPOINT_BYTES",
                checkpoint_bytes.as_str(),
            ),
        ];
        if let Some((metadata, _)) = &self.report {
            environment.extend([
                ("MATRIXOBJECT_CHUNK_NODE_ID", node_id.as_str()),
                ("MATRIXOBJECT_CHUNK_METADATA_ENDPOINTS", metadata.as_str()),
                ("MATRIXOBJECT_CHUNK_HEARTBEAT_MS", self.heartbeat_ms),
            ]);
        }
        drop(self.reservations[i].take());
        let node = processes::start(env!("CARGO_BIN_EXE_chunkserver"), &environment);
        assert_eq!(node.endpoint, bind);
        assert!(!self
            .peers
            .values()
            .any(|peer| peer.to_string() == node.endpoint));
        assert_ne!(node.pid, std::process::id());
        assert!(node.startup_line.contains("storage=raft"));
        self.nodes[i] = Some(node);
    }
    fn client(&self, i: usize) -> RemoteReplicaClient {
        RemoteReplicaClient::connect(
            &self.nodes[i].as_ref().unwrap().endpoint,
            &ReplicaClientOptions {
                connect_timeout: Duration::from_millis(200),
                read_timeout: Duration::from_secs(if self.capacity > 65536 { 10 } else { 2 }),
                write_timeout: Duration::from_secs(2),
                ..Default::default()
            },
        )
        .unwrap()
    }
    fn leader(&self) -> usize {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            for i in 0..3 {
                if self.nodes[i].is_some() && self.client(i).ping().is_ok() {
                    return i;
                }
            }
            assert!(
                Instant::now() < deadline,
                "no chunk leader committed a read barrier"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }
    fn kill(&mut self, i: usize) {
        let mut n = self.nodes[i].take().unwrap();
        n.child.kill().unwrap();
        assert!(processes::reap_within(&mut n.child, Duration::from_secs(3)).is_some());
        assert!(!processes::is_live(n.pid));
    }
}
impl Drop for Group {
    fn drop(&mut self) {
        if std::thread::panicking() {
            diagnostics(&mut self.nodes, &format!("chunk-group-{}", self.group_id));
        }
        self.nodes.clear();
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
fn diagnostics(nodes: &mut [Option<NodeProcess>], label: &str) {
    use std::io::Read;
    for (i, node) in nodes.iter_mut().enumerate() {
        if let Some(node) = node {
            let _ = node.child.kill();
            if processes::reap_within(&mut node.child, Duration::from_secs(3)).is_some() {
                if let Some(stderr) = node.child.stderr.take() {
                    let mut text = String::new();
                    let _ = stderr.take(4096).read_to_string(&mut text);
                    if !text.is_empty() {
                        eprintln!("{label} voter={i} endpoint={} {text}", node.endpoint);
                    }
                }
            }
        }
    }
}
fn page(i: u64) -> PageId {
    PageId {
        chunk_id: ChunkId::new(3, 7).unwrap(),
        page_index: i,
    }
}
#[test]
fn chunk_processes_commit_binary_outcomes_survive_leader_loss_and_total_restart() {
    let mut g = Group::new();
    let first = g.leader();
    let mut saved = Vec::new();
    for i in 0..16 {
        let p = page(i);
        let data = format!("chunk-{i:02}").into_bytes();
        let mut c = g.client(first);
        c.write_chunk(p.clone(), 2048, &data).unwrap();
        c.discard_chunk(&p, 0, 16).unwrap();
        saved.push((p.clone(), data, c.head_chunk(&p).unwrap()));
    }
    g.kill(first);
    let next = g.leader();
    assert_ne!(next, first);
    for (p, data, head) in &saved {
        let mut c = g.client(next);
        assert_eq!(
            &c.read_chunk(p, 2048, data.len() as u64).unwrap().data,
            data
        );
        assert_eq!(&c.head_chunk(p).unwrap(), head);
    }
    let other = (0..3).find(|i| *i != first && *i != next).unwrap();
    g.kill(other);
    assert!(
        g.client(next)
            .write_chunk(page(60), 0, b"uncertain")
            .is_err(),
        "minority write must not be acknowledged"
    );
    assert!(
        g.client(next).read_chunk(&page(0), 2048, 8).is_err(),
        "minority read must not be served"
    );
    g.kill(next);
    for i in 0..3 {
        let wal = std::fs::read(g.root.join(i.to_string()).join("chunk.peer.wal")).unwrap();
        assert!(wal.windows(4).any(|w| w == b"MOPO"));
        g.start(i);
    }
    let leader = g.leader();
    for (p, data, head) in &saved {
        let mut c = g.client(leader);
        assert_eq!(
            &c.read_chunk(p, 2048, data.len() as u64).unwrap().data,
            data
        );
        assert_eq!(&c.head_chunk(p).unwrap(), head);
    }
    g.client(leader)
        .write_chunk(page(61), 0, b"after recovery")
        .unwrap();
    assert_eq!(
        g.client(leader).read_chunk(&page(61), 0, 14).unwrap().data,
        b"after recovery"
    );
}
#[test]
fn chunk_raft_capacity_refusal_does_not_commit_partial_page_state() {
    let g = Group::new();
    let leader = g.leader();
    let p = page(0);
    let mut c = g.client(leader);
    c.write_chunk(p.clone(), 65530, b"fits").unwrap();
    let before = c.head_chunk(&p).unwrap();
    assert!(c.write_chunk(p.clone(), 65535, b"overflow").is_err());
    assert_eq!(c.head_chunk(&p).unwrap(), before);
    assert!(c.write_chunk(page(1), 0, b"too large").is_err());
    assert_eq!(c.ping().unwrap(), 1);
}
#[test]
fn chunk_peer_storage_lock_configuration_and_truncation_fail_closed() {
    let root = std::env::temp_dir().join(node_regime::run_token("chunk-peer-storage"));
    std::fs::create_dir(&root).unwrap();
    let reserved = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = reserved.local_addr().unwrap();
    drop(reserved);
    let options = ChunkRaftOptions {
        checkpoint_after_bytes: 0,
        node_id: 1,
        group_id: 8,
        peers: BTreeMap::from([(1, address)]),
        directory: root.clone(),
        request_timeout: Duration::from_millis(700),
    };
    let mut first = ReplicaNodeServer::bind("127.0.0.1:0", ReplicaNodeOptions::default()).unwrap();
    first.enable_consensus(options.clone()).unwrap();
    let mut second = ReplicaNodeServer::bind("127.0.0.1:0", ReplicaNodeOptions::default()).unwrap();
    assert!(second.enable_consensus(options.clone()).is_err());
    drop(first);
    let mut changed = ReplicaNodeServer::bind(
        "127.0.0.1:0",
        ReplicaNodeOptions {
            capacity_bytes: 1024,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(changed.enable_consensus(options.clone()).is_err());
    let wal = root.join("chunk.peer.wal");
    let original = std::fs::read(&wal).unwrap();
    std::fs::write(&wal, []).unwrap();
    assert!(second.enable_consensus(options.clone()).is_err());
    assert_eq!(std::fs::metadata(&wal).unwrap().len(), 0);
    std::fs::write(&wal, original).unwrap();
    second.enable_consensus(options).unwrap();
    drop(second);
    drop(changed);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn slow_chunk_peer_packets_do_not_prevent_shutdown() {
    use std::io::Write;
    let directory = std::env::temp_dir().join(node_regime::run_token("chunk-peer-shutdown"));
    std::fs::create_dir(&directory).unwrap();
    let socket = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = socket.local_addr().unwrap();
    let mut server = ReplicaNodeServer::bind("127.0.0.1:0", ReplicaNodeOptions::default()).unwrap();
    drop(socket);
    server
        .enable_consensus(ChunkRaftOptions {
            checkpoint_after_bytes: 0,
            node_id: 1,
            group_id: 4,
            peers: BTreeMap::from([(1, address)]),
            directory: directory.clone(),
            request_timeout: Duration::from_secs(1),
        })
        .unwrap();
    let stop = server.shutdown_handle();
    let worker = std::thread::spawn(move || server.serve().unwrap());
    let mut slow = std::net::TcpStream::connect(address).unwrap();
    slow.write_all(&[0]).unwrap();
    let started = Instant::now();
    stop.stop();
    worker.join().unwrap();
    assert!(started.elapsed() < Duration::from_secs(2));
    drop(slow);
    std::fs::remove_dir_all(directory).unwrap();
}
#[test]
fn partial_chunk_raft_settings_and_local_journal_combination_are_refused() {
    use std::process::{Command, Stdio};
    for env in [
        vec![("MATRIXOBJECT_CHUNK_RAFT_NODE_ID", "1")],
        vec![("MATRIXOBJECT_CHUNK_RAFT_PEERS", "1@127.0.0.1:18801")],
        vec![
            ("MATRIXOBJECT_CHUNK_RAFT_PEERS", "1@127.0.0.1:18801"),
            (
                "MATRIXOBJECT_REPLICA_JOURNAL",
                "/directory-that-does-not-exist/journal",
            ),
        ],
    ] {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_chunkserver"));
        for name in [
            "MATRIXOBJECT_CHUNK_RAFT_PEERS",
            "MATRIXOBJECT_CHUNK_RAFT_NODE_ID",
            "MATRIXOBJECT_CHUNK_RAFT_GROUP_ID",
            "MATRIXOBJECT_CHUNK_RAFT_STATE_DIR",
            "MATRIXOBJECT_CHUNK_RAFT_TIMEOUT_MS",
            "MATRIXOBJECT_REPLICA_JOURNAL",
        ] {
            cmd.env_remove(name);
        }
        let mut child = cmd
            .env("MATRIXOBJECT_REPLICA_ADDR", "127.0.0.1:0")
            .envs(env)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let until = Instant::now() + Duration::from_secs(3);
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                assert_eq!(status.code(), Some(2));
                break;
            }
            if Instant::now() >= until {
                let _ = child.kill();
                let _ = child.wait();
                panic!("invalid chunk Raft configuration started serving");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}
#[test]
fn chunk_voter_catalog_rejects_aliases_and_invalid_addresses_before_storage() {
    let directory = std::env::temp_dir().join(node_regime::run_token("chunk-peer-invalid"));
    std::fs::create_dir(&directory).unwrap();
    for peers in [
        BTreeMap::from([
            (1, "127.0.0.1:18801".parse().unwrap()),
            (2, "[::ffff:127.0.0.1]:18801".parse().unwrap()),
        ]),
        BTreeMap::from([(1, "0.0.0.0:18801".parse().unwrap())]),
        BTreeMap::from([(1, "224.0.0.1:18801".parse().unwrap())]),
        BTreeMap::from([(1, "[::ffff:224.0.0.1]:18801".parse().unwrap())]),
    ] {
        let mut server =
            ReplicaNodeServer::bind("127.0.0.1:0", ReplicaNodeOptions::default()).unwrap();
        assert!(server
            .enable_consensus(ChunkRaftOptions {
                checkpoint_after_bytes: 0,
                node_id: 1,
                group_id: 1,
                peers,
                directory: directory.clone(),
                request_timeout: Duration::from_secs(1)
            })
            .is_err());
        assert_eq!(std::fs::read_dir(&directory).unwrap().count(), 0);
    }
    std::fs::remove_dir_all(directory).unwrap();
}

#[cfg(feature = "meta-server")]
#[test]
fn metadata_reports_track_committed_chunk_bytes_on_every_voter() {
    use matrixobjectstore_rs::cluster::{
        DataNodeSeed, MetaClientOptions, MetaNodeOptions, MetaNodeServer, RemoteMetaClient,
    };
    let mut g = Group::configured(65536);
    let addresses = g.api_addresses.clone();
    let roster = addresses
        .iter()
        .enumerate()
        .map(|(i, a)| format!("chunk-{i}@{a}@rack-{i}"))
        .collect::<Vec<_>>()
        .join(",");
    let mut meta = MetaNodeServer::bind("127.0.0.1:0", MetaNodeOptions::default()).unwrap();
    meta.seed_data_roster(&DataNodeSeed::parse_roster(&roster).unwrap(), 65536)
        .unwrap();
    meta.enable_heartbeat_expiry(1500).unwrap();
    let endpoint = meta.local_addr().unwrap().to_string();
    let stop = meta.shutdown_handle();
    let worker = std::thread::spawn(move || meta.serve().unwrap());
    g.report = Some((endpoint.clone(), addresses));
    for i in 0..3 {
        g.start(i);
    }
    let leader = g.leader();
    g.client(leader).write_chunk(page(0), 8192, b"abc").unwrap();
    let wait = |expected_offline: Option<usize>, expected_used: u64| {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let mut c =
                RemoteMetaClient::connect(&endpoint, &MetaClientOptions::default()).unwrap();
            let nodes = c.list_placement_nodes().unwrap();
            if nodes.len() == 3
                && nodes.iter().all(|n| {
                    n.used_bytes
                        == if expected_offline.map(|i| format!("chunk-{i}"))
                            == Some(n.node_id.clone())
                        {
                            8195
                        } else {
                            expected_used
                        }
                        && n.online
                            != (expected_offline.map(|i| format!("chunk-{i}"))
                                == Some(n.node_id.clone()))
                })
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "metadata did not observe committed voter usage: {nodes:?}"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    };
    wait(None, 8195);
    let follower = (leader + 1) % 3;
    g.kill(follower);
    g.client(leader).write_chunk(page(1), 4096, b"new").unwrap();
    wait(Some(follower), 12294);
    g.start(follower);
    wait(None, 12294);
    drop(g);
    stop.stop();
    worker.join().unwrap();
}

#[test]
fn oversized_chunk_outcome_is_refused_without_poisoning_the_group() {
    let directory = std::env::temp_dir().join(node_regime::run_token("chunk-peer-large"));
    std::fs::create_dir(&directory).unwrap();
    let socket = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = socket.local_addr().unwrap();
    let mut server = ReplicaNodeServer::bind("127.0.0.1:0", ReplicaNodeOptions::default()).unwrap();
    drop(socket);
    server
        .enable_consensus(ChunkRaftOptions {
            checkpoint_after_bytes: 0,
            node_id: 1,
            group_id: 9,
            peers: BTreeMap::from([(1, address)]),
            directory: directory.clone(),
            request_timeout: Duration::from_secs(5),
        })
        .unwrap();
    let endpoint = server.local_addr().unwrap().to_string();
    let stop = server.shutdown_handle();
    let worker = std::thread::spawn(move || server.serve().unwrap());
    let mut client =
        RemoteReplicaClient::connect(&endpoint, &ReplicaClientOptions::default()).unwrap();
    let until = Instant::now() + Duration::from_secs(10);
    while client.ping().is_err() {
        assert!(Instant::now() < until);
        std::thread::sleep(Duration::from_millis(25));
    }
    let error = client
        .write_chunk(page(0), 0, &vec![1; 4 * 1024 * 1024])
        .unwrap_err();
    assert!(
        error.to_string().contains("peer proposal must contain"),
        "unexpected refusal: {error}"
    );
    assert_eq!(client.ping().unwrap(), 0);
    client.write_chunk(page(0), 0, b"small").unwrap();
    assert_eq!(client.read_chunk(&page(0), 0, 5).unwrap().data, b"small");
    drop(client);
    stop.stop();
    worker.join().unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn multi_megabyte_chunk_outcomes_replicate_over_tcp_and_survive_leader_loss() {
    let mut group = Group::with_capacity(8 * 1024 * 1024);
    let leader = group.leader();
    let data: Vec<u8> = (0..2 * 1024 * 1024)
        .map(|i| ((i % 251) + 1) as u8)
        .collect();
    group.client(leader).write_chunk(page(0), 0, &data).unwrap();
    let head = group.client(leader).head_chunk(&page(0)).unwrap();
    group.kill(leader);
    let next = group.leader();
    assert_eq!(
        group
            .client(next)
            .read_chunk(&page(0), 0, data.len() as u64)
            .unwrap()
            .data,
        data
    );
    assert_eq!(group.client(next).head_chunk(&page(0)).unwrap(), head);
    group
        .client(next)
        .write_chunk(page(0), 1024, b"updated")
        .unwrap();
    assert_eq!(
        group
            .client(next)
            .read_chunk(&page(0), 1024, 7)
            .unwrap()
            .data,
        b"updated"
    );
}

#[test]
fn chunk_catalog_finds_the_leader_and_moves_reads_after_process_loss() {
    let mut group = Group::new();
    let first = group.leader();
    let mut endpoints: Vec<_> = (0..3)
        .filter(|i| *i != first)
        .map(|i| group.nodes[i].as_ref().unwrap().endpoint.clone())
        .collect();
    endpoints.push(group.nodes[first].as_ref().unwrap().endpoint.clone());
    let mut client = RemoteReplicaClient::connect_raft_catalog(
        &endpoints,
        1,
        &ReplicaClientOptions {
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
    client.write_chunk(page(0), 0, b"routed").unwrap();
    assert_eq!(
        client.endpoint(),
        group.nodes[first].as_ref().unwrap().endpoint
    );
    assert_eq!(client.head_chunk(&page(0)).unwrap().write_sequence, 1);
    group.kill(first);
    let next = group.leader();
    assert_ne!(next, first);
    assert_eq!(client.read_chunk(&page(0), 0, 6).unwrap().data, b"routed");
    assert_eq!(
        client.endpoint(),
        group.nodes[next].as_ref().unwrap().endpoint
    );
    client.write_chunk(page(0), 6, b" again").unwrap();
    assert_eq!(client.head_chunk(&page(0)).unwrap().write_sequence, 2);
    assert_eq!(
        client.read_chunk(&page(0), 0, 12).unwrap().data,
        b"routed again"
    );
    let other = (0..3).find(|i| *i != first && *i != next).unwrap();
    group.kill(other);
    assert!(client.write_chunk(page(1), 0, b"uncertain").is_err());
    assert!(client.read_chunk(&page(0), 0, 12).is_err());
}

#[test]
fn chunk_catalog_rejects_another_group_with_the_same_numeric_id() {
    let first = Group::new();
    let leader = first.leader();
    let second = Group::new();
    let other = second.leader();
    let follower = (leader + 1) % 3;
    let mut source = first.client(follower);
    let description = source.chunk_group_identity().unwrap();
    assert_eq!(description.group_id, 1);
    assert_eq!(description.node_id, (follower + 1) as u64);
    let endpoints = vec![
        first.nodes[follower].as_ref().unwrap().endpoint.clone(),
        second.nodes[other].as_ref().unwrap().endpoint.clone(),
    ];
    let mut routed =
        RemoteReplicaClient::connect_raft_catalog(&endpoints, 1, &ReplicaClientOptions::default())
            .unwrap();
    assert!(routed.write_chunk(page(0), 0, b"misdirected").is_err());
    assert!(matches!(
        second.client(other).head_chunk(&page(0)),
        Err(matrixobjectstore_rs::ObjectError::NotFound(_))
    ));
    assert!(matches!(
        first.client(leader).head_chunk(&page(0)),
        Err(matrixobjectstore_rs::ObjectError::NotFound(_))
    ));
}

#[cfg(feature = "meta-server")]
#[test]
fn metadata_catalog_routes_chunk_group_through_leader_loss() {
    use matrixobjectstore_rs::cluster::{
        ChunkGroupCatalog, ChunkGroupConfig, ChunkGroupIdentity, MetaClientOptions,
        RemoteMetaClient,
    };
    let mut group = Group::configured(65536);
    let catalog = ChunkGroupCatalog::new(vec![ChunkGroupConfig {
        placement_id: "group-a".into(),
        failure_domain: "rack-a".into(),
        identity: Some(ChunkGroupIdentity {
            version: 1,
            group_id: 1,
            node_id: 1,
            capacity_bytes: 65536,
            max_pages: 64,

            voters: group.peers.keys().copied().collect(),
            peer_endpoints: group.peers.values().map(ToString::to_string).collect(),
        }),
        api_endpoints: group
            .api_addresses
            .iter()
            .map(ToString::to_string)
            .collect(),
        member_ids: (0..3).map(|i| format!("chunk-{i}")).collect(),
    }])
    .unwrap();
    let catalog_path = group.root.join("catalog.bin");
    std::fs::write(&catalog_path, catalog.to_bytes().unwrap()).unwrap();
    let mut meta = processes::start(
        env!("CARGO_BIN_EXE_metaserver"),
        &[
            ("MATRIXOBJECT_METASERVER_ADDR", "127.0.0.1:0"),
            (
                "MATRIXOBJECT_METASERVER_CHUNK_GROUPS_FILE",
                catalog_path.to_str().unwrap(),
            ),
            ("MATRIXOBJECT_METASERVER_HEARTBEAT_TTL_MS", "1500"),
        ],
    );
    group.report = Some((meta.endpoint.clone(), group.api_addresses.clone()));
    for i in 0..3 {
        group.start(i);
    }
    let leader = group.leader();
    let mut client =
        RemoteMetaClient::connect(&meta.endpoint, &MetaClientOptions::default()).unwrap();
    assert_eq!(
        client.read_chunk_group("group-a").unwrap(),
        catalog.get("group-a").unwrap().clone()
    );
    let mut chunk = client
        .connect_chunk_group("group-a", &ReplicaClientOptions::default())
        .unwrap();
    chunk.write_chunk(page(0), 0, b"catalog-data").unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let nodes = client.list_placement_nodes().unwrap();
        let logical = nodes.iter().find(|n| n.node_id == "group-a").unwrap();
        if logical.online && logical.used_bytes == 12 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "group health did not converge: {nodes:?}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    let plan = client
        .plan_object_placement("bucket", "key", 32, 1)
        .unwrap();
    assert_eq!(plan.node_ids, vec!["group-a"]);
    assert!(client
        .plan_object_placement("bucket", "key", 32, 2)
        .is_err());
    group.kill(leader);
    group.leader();
    assert_eq!(
        chunk.read_chunk(&page(0), 0, 12).unwrap().data,
        b"catalog-data"
    );
    chunk.write_chunk(page(0), 12, b"!").unwrap();
    assert_eq!(
        chunk.read_chunk(&page(0), 0, 13).unwrap().data,
        b"catalog-data!"
    );
    meta.child.kill().unwrap();
    assert!(processes::reap_within(&mut meta.child, Duration::from_secs(3)).is_some());
}

#[cfg(feature = "metadata-consensus")]
mod coupled_groups {
    use super::*;
    use matrixobjectstore_rs::cluster::{
        ChunkGroupCatalog, ChunkGroupConfig, ChunkGroupIdentity, CommittedPlacement,
        MetaClientOptions, RemoteMetaClient,
    };

    struct MetadataGroup {
        nodes: Vec<Option<NodeProcess>>,
        api: Vec<String>,
        peers: String,
        root: PathBuf,
        reservations: Vec<Option<(TcpListener, TcpListener)>>,
        multiple_groups: bool,
        checkpoint_bytes: u64,
    }
    impl MetadataGroup {
        fn new(chunk: &Group) -> Self {
            Self::for_groups(std::slice::from_ref(chunk))
        }
        fn for_groups(chunks: &[Group]) -> Self {
            let root = chunks[0].root.join("metadata");
            std::fs::create_dir(&root).unwrap();
            let sockets: Vec<_> = (0..3)
                .map(|_| {
                    (
                        TcpListener::bind("127.0.0.1:0").unwrap(),
                        TcpListener::bind("127.0.0.1:0").unwrap(),
                    )
                })
                .collect();
            let api = sockets
                .iter()
                .map(|(a, _)| a.local_addr().unwrap().to_string())
                .collect();
            let peers = sockets
                .iter()
                .enumerate()
                .map(|(i, (_, p))| format!("{}@{}", i + 1, p.local_addr().unwrap()))
                .collect::<Vec<_>>()
                .join(",");
            let catalog = ChunkGroupCatalog::new(
                chunks
                    .iter()
                    .map(|chunk| ChunkGroupConfig {
                        placement_id: chunk.placement_id(),
                        failure_domain: format!("rack-{}", chunk.group_id),
                        identity: Some(ChunkGroupIdentity {
                            version: 1,
                            group_id: chunk.group_id,
                            node_id: 1,
                            capacity_bytes: chunk.capacity,
                            max_pages: chunk.max_pages,
                            voters: chunk.peers.keys().copied().collect(),
                            peer_endpoints: chunk.peers.values().map(ToString::to_string).collect(),
                        }),
                        api_endpoints: chunk
                            .api_addresses
                            .iter()
                            .map(ToString::to_string)
                            .collect(),
                        member_ids: (0..3).map(|i| chunk.member_id(i)).collect(),
                    })
                    .collect(),
            )
            .unwrap();
            std::fs::write(root.join("catalog.bin"), catalog.to_bytes().unwrap()).unwrap();
            let mut group = Self {
                nodes: vec![None, None, None],
                api,
                peers,
                root,
                reservations: sockets.into_iter().map(Some).collect(),
                multiple_groups: chunks.len() > 1,
                checkpoint_bytes: chunks[0].checkpoint_bytes,
            };
            for i in 0..3 {
                std::fs::create_dir(group.root.join(i.to_string())).unwrap();
                group.start(i);
            }
            group
        }
        fn start(&mut self, i: usize) {
            let id = (i + 1).to_string();
            let state = self.root.join(i.to_string());
            let catalog = self.root.join("catalog.bin");
            let checkpoint_bytes = self.checkpoint_bytes.to_string();
            drop(self.reservations[i].take());
            let node = processes::start(
                env!("CARGO_BIN_EXE_metaserver"),
                &[
                    ("MATRIXOBJECT_METASERVER_ADDR", &self.api[i]),
                    ("MATRIXOBJECT_METASERVER_RAFT_PEERS", &self.peers),
                    ("MATRIXOBJECT_METASERVER_RAFT_NODE_ID", &id),
                    (
                        "MATRIXOBJECT_METASERVER_RAFT_CHECKPOINT_BYTES",
                        &checkpoint_bytes,
                    ),
                    (
                        "MATRIXOBJECT_METASERVER_RAFT_STATE_DIR",
                        state.to_str().unwrap(),
                    ),
                    (
                        "MATRIXOBJECT_METASERVER_RAFT_TIMEOUT_MS",
                        if self.multiple_groups { "5000" } else { "700" },
                    ),
                    (
                        "MATRIXOBJECT_METASERVER_CHUNK_GROUPS_FILE",
                        catalog.to_str().unwrap(),
                    ),
                    (
                        "MATRIXOBJECT_METASERVER_HEARTBEAT_TTL_MS",
                        if self.multiple_groups {
                            "10000"
                        } else {
                            "1500"
                        },
                    ),
                ],
            );
            assert_eq!(node.endpoint, self.api[i]);
            assert!(node.startup_line.contains("storage=raft"));
            self.nodes[i] = Some(node);
        }
        fn ready(&self) -> RemoteMetaClient {
            let deadline = Instant::now() + Duration::from_secs(20);
            loop {
                let options = MetaClientOptions {
                    connect_timeout: Duration::from_millis(200),
                    read_timeout: Duration::from_secs(2),
                    write_timeout: Duration::from_secs(2),
                    ..Default::default()
                };
                if let Ok(mut client) = RemoteMetaClient::connect_catalog(&self.api, &options) {
                    if client.probe().into_result().is_ok() {
                        return client;
                    }
                }
                assert!(
                    Instant::now() < deadline,
                    "metadata quorum did not become readable"
                );
                std::thread::sleep(Duration::from_millis(50));
            }
        }
        fn kill(&mut self, i: usize) {
            let mut node = self.nodes[i].take().unwrap();
            node.child.kill().unwrap();
            assert!(processes::reap_within(&mut node.child, Duration::from_secs(3)).is_some());
        }
    }
    impl Drop for MetadataGroup {
        fn drop(&mut self) {
            if std::thread::panicking() {
                diagnostics(&mut self.nodes, "metadata");
            }
            self.nodes.clear();
        }
    }
    fn health(client: &mut RemoteMetaClient, online: bool, used: u64, committed: u64) {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let nodes = client.list_placement_nodes().unwrap();
            let group = nodes.iter().find(|n| n.node_id == "group-a").unwrap();
            if group.online == online
                && group.used_bytes == used
                && group.committed_bytes == committed
            {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "group health did not converge: {nodes:?}"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }
    fn store(
        client: &mut RemoteMetaClient,
        chunk: &mut RemoteReplicaClient,
        n: u64,
    ) -> CommittedPlacement {
        let data = vec![n as u8; 256];
        chunk.write_chunk(page(n), 0, &data).unwrap();
        let plan = client
            .plan_object_placement("bucket", &format!("object-{n}"), 256, 1)
            .unwrap();
        assert_eq!(plan.node_ids, vec!["group-a"]);
        client
            .commit_object_placement(
                CommittedPlacement {
                    bucket: plan.bucket,
                    key: plan.key,
                    length: 256,
                    node_ids: plan.node_ids,
                    version: 1,
                    update_time_ms: n + 1,
                },
                &plan.plan_checksum,
            )
            .unwrap()
    }
    fn verify(
        client: &mut RemoteMetaClient,
        chunk: &mut RemoteReplicaClient,
        placements: &[CommittedPlacement],
    ) {
        for (n, p) in placements.iter().enumerate() {
            assert_eq!(client.object_placement(&p.bucket, &p.key).unwrap(), *p);
            assert_eq!(
                chunk.read_chunk(&page(n as u64), 0, 256).unwrap().data,
                vec![n as u8; 256]
            );
            assert_eq!(chunk.head_chunk(&page(n as u64)).unwrap().write_sequence, 1);
        }
    }
    #[test]
    fn metadata_and_chunk_quorums_recover_together_and_refuse_minority_operations() {
        let mut chunks = Group::configured(65536);
        let mut metadata = MetadataGroup::new(&chunks);
        chunks.report = Some((metadata.api.join(","), chunks.api_addresses.clone()));
        for i in 0..3 {
            chunks.start(i);
        }
        let chunk_leader = chunks.leader();
        let mut client = metadata.ready();
        health(&mut client, true, 0, 0);
        let meta_leader = metadata
            .api
            .iter()
            .position(|a| a == client.endpoint())
            .unwrap();
        let mut chunk = client
            .connect_chunk_group("group-a", &ReplicaClientOptions::default())
            .unwrap();
        let mut placements: Vec<_> = (0..32).map(|n| store(&mut client, &mut chunk, n)).collect();
        health(&mut client, true, 8192, 8192);
        metadata.kill(meta_leader);
        chunks.kill(chunk_leader);
        metadata.ready();
        chunks.leader();
        verify(&mut client, &mut chunk, &placements);
        placements.push(store(&mut client, &mut chunk, 32));
        for i in 0..3 {
            if chunks.nodes[i].is_some() {
                chunks.kill(i);
            }
            if metadata.nodes[i].is_some() {
                metadata.kill(i);
            }
        }
        for i in 0..3 {
            metadata.start(i);
        }
        for i in 0..3 {
            chunks.start(i);
        }
        client = metadata.ready();
        chunks.leader();
        health(&mut client, true, 8448, 8448);
        chunk = client
            .connect_chunk_group("group-a", &ReplicaClientOptions::default())
            .unwrap();
        verify(&mut client, &mut chunk, &placements);
        let leader = chunks.leader();
        for i in 0..3 {
            if i != leader {
                chunks.kill(i);
            }
        }
        assert!(chunk.write_chunk(page(33), 0, b"minority").is_err());
        assert!(chunk.read_chunk(&page(0), 0, 256).is_err());
        health(&mut client, false, 8448, 8448);
        assert!(client
            .plan_object_placement("bucket", "unavailable", 1, 1)
            .is_err());
        let current = metadata
            .api
            .iter()
            .position(|a| a == client.endpoint())
            .unwrap();
        for i in 0..3 {
            if i != current {
                metadata.kill(i);
            }
        }
        assert!(client.object_placement("bucket", "object-0").is_err());
    }
    #[test]
    fn conditional_data_and_metadata_updates_survive_conflicts_failover_and_restart() {
        let mut chunks = Group::configured(65536);
        chunks.checkpoint_bytes = 1024;
        let mut metadata = MetadataGroup::new(&chunks);
        chunks.report = Some((metadata.api.join(","), chunks.api_addresses.clone()));
        for id in 0..3 {
            chunks.start(id);
        }
        let mut meta = metadata.ready();
        chunks.leader();
        health(&mut meta, true, 0, 0);
        let mut data = meta
            .connect_chunk_group("group-a", &chunk_options())
            .unwrap();
        data.write_chunk_if_sequence(page(700), 0, b"init", 0)
            .unwrap();
        assert!(data
            .write_chunk_if_sequence(page(700), 0, b"oops", 0)
            .is_err());
        assert_eq!(data.head_chunk(&page(700)).unwrap().write_sequence, 1);
        let plan = meta
            .plan_object_placement("conditional", "object", 4, 1)
            .unwrap();
        let first = CommittedPlacement {
            bucket: plan.bucket,
            key: plan.key,
            length: 4,
            node_ids: plan.node_ids,
            version: 1,
            update_time_ms: 1,
        };
        meta.commit_object_placement_if_current(first.clone(), None, &plan.plan_checksum)
            .unwrap();
        assert!(meta
            .commit_object_placement_if_current(first.clone(), None, &plan.plan_checksum)
            .is_err());
        let barrier = std::sync::Barrier::new(2);
        let leader = chunks.leader();
        let clients = vec![chunks.client(leader), chunks.client(leader)];
        let results = std::thread::scope(|scope| {
            let handles: Vec<_> = clients
                .into_iter()
                .enumerate()
                .map(|(id, mut client)| {
                    let barrier = &barrier;
                    scope.spawn(move || {
                        barrier.wait();
                        (
                            id as u8 + 2,
                            client.write_chunk_if_sequence(page(700), 0, &[id as u8 + 2; 4], 1),
                        )
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert_eq!(results.iter().filter(|(_, r)| r.is_ok()).count(), 1);
        let winner = results.iter().find(|(_, r)| r.is_ok()).unwrap().0;
        assert_eq!(
            data.read_chunk(&page(700), 0, 4).unwrap().data,
            vec![winner; 4]
        );
        assert_eq!(data.head_chunk(&page(700)).unwrap().write_sequence, 2);
        let clients = vec![metadata.ready(), metadata.ready()];
        let results = std::thread::scope(|scope| {
            let handles: Vec<_> = clients
                .into_iter()
                .enumerate()
                .map(|(id, mut client)| {
                    let barrier = &barrier;
                    let expected = first.clone();
                    let checksum = plan.plan_checksum.clone();
                    scope.spawn(move || {
                        let mut next = expected.clone();
                        next.version = 2;
                        next.update_time_ms = id as u64 + 2;
                        barrier.wait();
                        client.commit_object_placement_if_current(next, Some(expected), &checksum)
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
        let mut current = results.into_iter().find_map(Result::ok).unwrap();
        assert_eq!(
            meta.object_placement("conditional", "object").unwrap(),
            current
        );
        // Compare the entire record, including fields other than its version.
        let mut wrong = current.clone();
        wrong.length += 1;
        let mut next = current.clone();
        next.version = 3;
        assert!(meta
            .commit_object_placement_if_current(next.clone(), Some(wrong), &plan.plan_checksum)
            .is_err());
        let metadata_leader = metadata
            .api
            .iter()
            .position(|a| a == meta.endpoint())
            .unwrap();
        metadata.kill(metadata_leader);
        let chunk_leader = chunks.leader();
        chunks.kill(chunk_leader);
        meta = metadata.ready();
        chunks.leader();
        data = meta
            .connect_chunk_group("group-a", &chunk_options())
            .unwrap();
        assert_eq!(
            data.read_chunk(&page(700), 0, 4).unwrap().data,
            vec![winner; 4]
        );
        assert!(data
            .write_chunk_if_sequence(page(700), 0, b"late", 1)
            .is_err());
        assert!(meta
            .commit_object_placement_if_current(next.clone(), Some(first), &plan.plan_checksum)
            .is_err());
        for version in 3..=20 {
            data.write_chunk_if_sequence(page(700), 0, &[version as u8; 4], version - 1)
                .unwrap();
            next = current.clone();
            next.version = version;
            next.update_time_ms = version + 10;
            current = meta
                .commit_object_placement_if_current(next, Some(current), &plan.plan_checksum)
                .unwrap();
        }
        health(&mut meta, true, 4, 4);
        let meta_leader = metadata
            .api
            .iter()
            .position(|a| a == meta.endpoint())
            .unwrap();
        node_regime::checkpoints::wait_for_checkpoint(
            &metadata
                .root
                .join(meta_leader.to_string())
                .join("metadata.peer.wal"),
            2,
        );
        node_regime::checkpoints::wait_for_checkpoint(
            &chunks
                .root
                .join(chunks.leader().to_string())
                .join("chunk.peer.wal"),
            2,
        );
        for id in 0..3 {
            if chunks.nodes[id].is_some() {
                chunks.kill(id);
            }
            if metadata.nodes[id].is_some() {
                metadata.kill(id);
            }
        }
        for id in 0..3 {
            metadata.start(id);
            chunks.start(id);
        }
        meta = metadata.ready();
        chunks.leader();
        data = meta
            .connect_chunk_group("group-a", &chunk_options())
            .unwrap();
        assert_eq!(
            meta.object_placement("conditional", "object").unwrap(),
            current
        );
        assert_eq!(data.read_chunk(&page(700), 0, 4).unwrap().data, vec![20; 4]);
        assert_eq!(data.head_chunk(&page(700)).unwrap().write_sequence, 20);
        assert!(data
            .write_chunk_if_sequence(page(700), 0, b"late", 19)
            .is_err());
        health(&mut meta, true, 4, 4);
    }

    fn load_data(n: u64) -> Vec<u8> {
        let mut bytes: Vec<u8> = (0..1024)
            .map(|i| (n.wrapping_mul(31).wrapping_add(i) % 251) as u8)
            .collect();
        bytes[..8].copy_from_slice(&n.to_be_bytes());
        bytes
    }
    fn load_options() -> MetaClientOptions {
        MetaClientOptions {
            connect_timeout: Duration::from_secs(1),
            read_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(5),
            ..Default::default()
        }
    }
    fn chunk_options() -> ReplicaClientOptions {
        ReplicaClientOptions {
            connect_timeout: Duration::from_secs(1),
            read_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(5),
            ..Default::default()
        }
    }
    fn load_batch(
        api: &[String],
        start: u64,
        end: u64,
        workers: usize,
    ) -> Vec<(u64, CommittedPlacement)> {
        let begin = Instant::now();
        let results = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..workers)
                .map(|worker| {
                    scope.spawn(move || {
                        let mut meta =
                            RemoteMetaClient::connect_catalog(api, &load_options()).unwrap();
                        let mut clients = BTreeMap::new();
                        let mut saved = Vec::new();
                        let mut latencies = Vec::new();
                        let mut stages = [0u128; 4];
                        for n in ((start + worker as u64)..end).step_by(workers) {
                            assert!(
                                begin.elapsed() < Duration::from_secs(180),
                                "bounded workload exceeded deadline after {} writes in worker {worker}; plan/connect/write/commit_us={stages:?}", saved.len()
                            );
                            let started = Instant::now();
                            let plan = meta
                                .plan_object_placement("load", &format!("object-{n}"), 1024, 1)
                                .unwrap();
                            stages[0] += started.elapsed().as_micros();
                            let phase = Instant::now();
                            assert_eq!(plan.node_ids.len(), 1);
                            let group = plan.node_ids[0].clone();
                            let chunk = clients.entry(group.clone()).or_insert_with(|| {
                                meta.connect_chunk_group(&group, &chunk_options()).unwrap()
                            });
                            stages[1] += phase.elapsed().as_micros();
                            let phase = Instant::now();
                            // No mutation retry: a failure after admission remains uncertain.
                            chunk.write_chunk(page(n), 0, &load_data(n)).unwrap();
                            stages[2] += phase.elapsed().as_micros();
                            let phase = Instant::now();
                            let placement = meta
                                .commit_object_placement(
                                    CommittedPlacement {
                                        bucket: plan.bucket,
                                        key: plan.key,
                                        length: 1024,
                                        node_ids: plan.node_ids,
                                        version: 1,
                                        update_time_ms: n + 1,
                                    },
                                    &plan.plan_checksum,
                                )
                                .unwrap();
                            stages[3] += phase.elapsed().as_micros();
                            saved.push((n, placement));
                            latencies.push(started.elapsed().as_micros());
                            if saved.len() % 32 == 0 {
                                println!("MULTIGROUP_STAGE worker={worker} completed={} plan_us={} connect_us={} write_us={} commit_us={}", saved.len(), stages[0], stages[1], stages[2], stages[3]);
                            }
                        }
                        (saved, latencies)
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().unwrap())
                .collect::<Vec<_>>()
        });
        let elapsed = begin.elapsed();
        let mut saved = Vec::new();
        let mut latencies = Vec::new();
        for (records, timings) in results {
            saved.extend(records);
            latencies.extend(timings);
        }
        saved.sort_by_key(|(n, _)| *n);
        latencies.sort_unstable();
        assert_eq!(saved.len() as u64, end - start);
        println!("MULTIGROUP writes={} workers={} elapsed_ms={} operations_per_second={:.1} p50_us={} p95_us={} p99_us={}", saved.len(), workers, elapsed.as_millis(), saved.len() as f64 / elapsed.as_secs_f64(), latencies[latencies.len()/2], latencies[latencies.len()*95/100], latencies[latencies.len()*99/100]);
        saved
    }
    fn group_usage(api: &[String], groups: &[Group], saved: &[(u64, CommittedPlacement)]) {
        let mut meta = RemoteMetaClient::connect_catalog(api, &load_options()).unwrap();
        let expected: BTreeMap<_, _> = groups
            .iter()
            .map(|g| {
                let id = g.placement_id();
                let bytes = saved
                    .iter()
                    .filter(|(_, p)| p.node_ids == [id.clone()])
                    .count() as u64
                    * 1024;
                (id, bytes)
            })
            .collect();
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let nodes = meta.list_placement_nodes().unwrap();
            assert_eq!(nodes.len(), groups.len() * 4);
            if groups.iter().all(|g| {
                let id = g.placement_id();
                let node = nodes.iter().find(|n| n.node_id == id).unwrap();
                node.online
                    && node.data_eligible
                    && node.used_bytes == expected[&id]
                    && node.committed_bytes == expected[&id]
                    && node.capacity_bytes == g.capacity
            }) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "multi-group accounting did not converge: {nodes:?}"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    }
    fn verify_load(api: &[String], saved: &[(u64, CommittedPlacement)]) {
        let mut meta = RemoteMetaClient::connect_catalog(api, &load_options()).unwrap();
        let mut clients = BTreeMap::new();
        for (n, placement) in saved {
            assert_eq!(
                meta.object_placement(&placement.bucket, &placement.key)
                    .unwrap(),
                *placement
            );
            let id = &placement.node_ids[0];
            let chunk = clients
                .entry(id.clone())
                .or_insert_with(|| meta.connect_chunk_group(id, &chunk_options()).unwrap());
            assert_eq!(
                chunk.read_chunk(&page(*n), 0, 1024).unwrap().data,
                load_data(*n)
            );
            assert_eq!(chunk.head_chunk(&page(*n)).unwrap().write_sequence, 1);
        }
    }
    fn multiple_group_load(group_count: usize, writes: u64, workers: usize) {
        assert!((2..=16).contains(&group_count));
        assert!(writes > 0 && writes <= 1024);
        let mut groups: Vec<_> = (0..group_count)
            .map(|i| {
                let mut g = Group::configured(64 * 1024 * 1024);
                g.group_id = i as u64 + 1;
                g.max_pages = 2048;
                g.checkpoint_bytes = 16 * 1024;
                g.heartbeat_ms = "1000";
                g
            })
            .collect();
        // Every API and peer socket stays reserved until its process starts.
        let mut metadata = MetadataGroup::for_groups(&groups);
        for group in &mut groups {
            group.report = Some((metadata.api.join(","), group.api_addresses.clone()));
            for i in 0..3 {
                group.start(i);
            }
        }
        metadata.ready();
        for group in &groups {
            group.leader();
        }
        group_usage(&metadata.api, &groups, &[]);
        let mut saved = load_batch(&metadata.api, 0, writes, workers);
        group_usage(&metadata.api, &groups, &saved);
        for group in &groups {
            assert!(
                saved
                    .iter()
                    .any(|(_, p)| p.node_ids == [group.placement_id()]),
                "every group must carry data"
            );
        }
        let leader = metadata.ready().endpoint().to_string();
        let leader = metadata.api.iter().position(|a| *a == leader).unwrap();
        node_regime::checkpoints::wait_for_checkpoint(
            &metadata
                .root
                .join(leader.to_string())
                .join("metadata.peer.wal"),
            2,
        );
        for group in &groups {
            let leader = group.leader();
            node_regime::checkpoints::wait_for_checkpoint(
                &group.root.join(leader.to_string()).join("chunk.peer.wal"),
                2,
            );
        }
        metadata.kill(leader);
        for group in &mut groups {
            let leader = group.leader();
            group.kill(leader);
        }
        metadata.ready();
        for group in &groups {
            group.leader();
        }
        verify_load(&metadata.api, &saved);
        saved.extend(load_batch(
            &metadata.api,
            writes,
            writes + writes / 4,
            workers,
        ));
        group_usage(&metadata.api, &groups, &saved);
        for group in &mut groups {
            for i in 0..3 {
                if group.nodes[i].is_some() {
                    group.kill(i);
                }
            }
        }
        for i in 0..3 {
            if metadata.nodes[i].is_some() {
                metadata.kill(i);
            }
        }
        for i in 0..3 {
            metadata.start(i);
        }
        for group in &mut groups {
            for i in 0..3 {
                group.start(i);
            }
        }
        metadata.ready();
        for group in &groups {
            group.leader();
        }
        group_usage(&metadata.api, &groups, &saved);
        verify_load(&metadata.api, &saved);
        println!(
            "MULTIGROUP verified groups={group_count} processes={} recovered_objects={}",
            group_count * 3 + 3,
            saved.len()
        );
    }
    #[test]
    fn concurrent_groups_preserve_placement_and_data_through_failover_and_restart() {
        multiple_group_load(3, 96, 6);
    }
    #[test]
    #[ignore = "bounded 51-process local load and recovery measurement"]
    fn sixteen_groups_preserve_concurrent_writes_through_recovery() {
        multiple_group_load(16, 1024, 8);
    }
}

#[test]
fn chunk_quorum_reads_leave_the_wal_unchanged_and_refuse_a_minority() {
    let mut group = Group::new();
    let leader = group.leader();
    let mut client = group.client(leader);
    client.write_chunk(page(0), 0, b"committed bytes").unwrap();
    let head = client.head_chunk(&page(0)).unwrap();
    let path = group.root.join(leader.to_string()).join("chunk.peer.wal");
    let before = std::fs::read(&path).unwrap();
    for _ in 0..32 {
        assert_eq!(
            client.read_chunk(&page(0), 0, 15).unwrap().data,
            b"committed bytes"
        );
        assert_eq!(client.head_chunk(&page(0)).unwrap(), head);
        assert_eq!(client.ping().unwrap(), 1);
    }
    assert_eq!(std::fs::read(&path).unwrap(), before);
    // A write following confirmed reads still becomes visible through consensus.
    client.write_chunk(page(0), 0, b"updated bytes!!").unwrap();
    assert_eq!(
        client.read_chunk(&page(0), 0, 15).unwrap().data,
        b"updated bytes!!"
    );
    for i in 0..3 {
        if i != leader {
            group.kill(i);
        }
    }
    assert!(client.read_chunk(&page(0), 0, 15).is_err());
    assert!(group.client(leader).ping().is_err());
}

#[test]
fn chunk_checkpoints_reclaim_history_transfer_large_state_and_recover_processes() {
    let mut group = Group::configured(32 * 1024 * 1024);
    group.checkpoint_bytes = 2 * 1024 * 1024;
    for id in 0..3 {
        group.start(id);
    }
    let leader = group.leader();
    let lagging = (leader + 1) % 3;
    let other = (leader + 2) % 3;
    group.kill(lagging);
    let mut client = group.client(leader);
    let mut expected = Vec::new();
    for id in 0..9u64 {
        let bytes = vec![id as u8 + 1; 1024 * 1024];
        client.write_chunk(page(id), 0, &bytes).unwrap();
        expected.push(bytes);
    }
    for value in 20..36u8 {
        expected[0].fill(value);
        client.write_chunk(page(0), 0, &expected[0]).unwrap();
    }
    client.discard_chunk(&page(1), 0, 128).unwrap();
    let heads: Vec<_> = (0..9)
        .map(|id| client.head_chunk(&page(id)).unwrap())
        .collect();
    let wal = group.root.join(leader.to_string()).join("chunk.peer.wal");
    node_regime::checkpoints::wait_for_checkpoint(&wal, 4);
    assert!(
        std::fs::metadata(&wal).unwrap().len() < 20 * 1024 * 1024,
        "overwritten history was not reclaimed"
    );
    group.start(lagging);
    node_regime::checkpoints::wait_for_checkpoint(
        &group.root.join(lagging.to_string()).join("chunk.peer.wal"),
        4,
    );
    // This write can commit only with the repaired voter after the other stops.
    group.kill(other);
    // Publishing a snapshot does not promise the old leader retains its term.
    // Confirm a read quorum on the two remaining voters before issuing a write.
    drop(client);
    let active = group.leader();
    let mut client = group.client(active);
    client
        .write_chunk(page(20), 0, b"after snapshot transfer")
        .unwrap();
    drop(client);
    group.kill(leader);
    group.kill(lagging);
    for id in 0..3 {
        group.start(id);
    }
    let recovered = group.leader();
    let mut client = group.client(recovered);
    for id in 0..9 {
        let offset = if id == 1 { 128 } else { 0 };
        assert_eq!(
            client
                .read_chunk(
                    &page(id as u64),
                    offset as u64,
                    (expected[id].len() - offset) as u64
                )
                .unwrap()
                .data,
            expected[id][offset..]
        );
        assert_eq!(client.head_chunk(&page(id as u64)).unwrap(), heads[id]);
    }
    assert_eq!(
        client.read_chunk(&page(20), 0, 23).unwrap().data,
        b"after snapshot transfer"
    );
    // Recovered payload accounting still enforces the original capacity.
    let too_large = vec![0; 1024 * 1024];
    for id in 30..52 {
        client.write_chunk(page(id), 0, &too_large).unwrap();
    }
    assert!(client.write_chunk(page(60), 0, &too_large).is_err());
}

#[test]
#[ignore = "272 MiB three-process recovery; run explicitly in release mode"]
fn chunk_streamed_checkpoints_recover_large_process_state() {
    let mut group = Group::configured(300 * 1024 * 1024);
    group.max_pages = 300;
    group.checkpoint_bytes = 128 * 1024 * 1024;
    for i in 0..3 {
        group.start(i);
    }
    let leader = group.leader();
    let lagging = (leader + 1) % 3;
    let other = (leader + 2) % 3;
    group.kill(lagging);
    let mut client = group.client(leader);
    for index in 0..272u64 {
        client
            .write_chunk(page(index), 0, &vec![(index % 251) as u8; 1024 * 1024])
            .unwrap();
    }
    let payload_dir = group
        .root
        .join(leader.to_string())
        .join("chunk.peer.wal.payloads");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let largest = std::fs::read_dir(&payload_dir)
            .unwrap()
            .map(|e| e.unwrap().metadata().unwrap().len())
            .max()
            .unwrap_or(0);
        if largest > 256 * 1024 * 1024 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "no checkpoint larger than 256 MiB was published"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    group.start(lagging);
    node_regime::checkpoints::wait_for_checkpoint(
        &group.root.join(lagging.to_string()).join("chunk.peer.wal"),
        4,
    );
    group.kill(other);
    drop(client);
    let active = group.leader();
    let mut client = group.client(active);
    client
        .write_chunk(page(280), 0, b"after large transfer")
        .unwrap();
    drop(client);
    group.kill(leader);
    group.kill(lagging);
    for i in 0..3 {
        group.start(i);
    }
    let recovered = group.leader();
    let mut client = group.client(recovered);
    for index in 0..272u64 {
        let response = client.read_chunk(&page(index), 0, 1024 * 1024).unwrap();
        assert_eq!(response.data, vec![(index % 251) as u8; 1024 * 1024]);
    }
    assert_eq!(
        client.read_chunk(&page(280), 0, 20).unwrap().data,
        b"after large transfer"
    );
}
