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
            api_addresses,
            reservations: reserved.into_iter().map(Some).collect(),
            report: None,
        };
        for i in 0..3 {
            std::fs::create_dir(g.root.join(i.to_string())).unwrap();
        }
        g
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
        let node_id = format!("chunk-{i}");
        let capacity = self.capacity.to_string();
        let timeout = if self.capacity > 65536 { "5000" } else { "700" };
        let mut environment = vec![
            ("MATRIXOBJECT_REPLICA_ADDR", bind.as_str()),
            ("MATRIXOBJECT_REPLICA_CAPACITY_BYTES", capacity.as_str()),
            ("MATRIXOBJECT_REPLICA_MAX_PAGES", "64"),
            ("MATRIXOBJECT_CHUNK_RAFT_PEERS", peers.as_str()),
            ("MATRIXOBJECT_CHUNK_RAFT_NODE_ID", id.as_str()),
            ("MATRIXOBJECT_CHUNK_RAFT_STATE_DIR", path.to_str().unwrap()),
            ("MATRIXOBJECT_CHUNK_RAFT_TIMEOUT_MS", timeout),
        ];
        if let Some((metadata, _)) = &self.report {
            environment.extend([
                ("MATRIXOBJECT_CHUNK_NODE_ID", node_id.as_str()),
                ("MATRIXOBJECT_CHUNK_METADATA_ENDPOINTS", metadata.as_str()),
                ("MATRIXOBJECT_CHUNK_HEARTBEAT_MS", "100"),
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
        self.nodes.clear();
        let _ = std::fs::remove_dir_all(&self.root);
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
