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
fn api_tls_config(root: Option<&PathBuf>) -> Option<matrixobjectstore_rs::cluster::ApiTlsConfig> {
    root.map(|root| matrixobjectstore_rs::cluster::ApiTlsConfig::load(
        &matrixobjectstore_rs::cluster::ApiTlsOptions {
            ca_certificate: root.join("ca.pem"),
            certificate: root.join("peer.pem"),
            private_key: root.join("peer.key"),
        },
    ).unwrap())
}
fn api_tls_environment(root: Option<&PathBuf>) -> Vec<(&'static str, String)> {
    let Some(root) = root else { return Vec::new() };
    [
        ("MATRIXOBJECT_API_TLS_CA_CERT", "ca.pem"),
        ("MATRIXOBJECT_API_TLS_CERT", "peer.pem"),
        ("MATRIXOBJECT_API_TLS_KEY", "peer.key"),
        ("MATRIXOBJECT_CLIENT_TLS_CA_CERT", "ca.pem"),
        ("MATRIXOBJECT_CLIENT_TLS_CERT", "peer.pem"),
        ("MATRIXOBJECT_CLIENT_TLS_KEY", "peer.key"),
    ].into_iter().map(|(name, file)| (name, root.join(file).to_str().unwrap().to_owned())).collect()
}
struct Group {
    membership: bool,
    peer_tls: Option<PathBuf>,
    api_tls: Option<PathBuf>,
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
        Self::configured_voters(capacity, 3)
    }
    fn configured_voters(capacity: u64, voters: usize) -> Self {
        assert!([3, 4, 5, 7].contains(&voters));
        let root = std::env::temp_dir().join(node_regime::run_token("chunk-peers"));
        std::fs::create_dir(&root).unwrap();
        // Hold all distinct listeners until their process takes ownership.
        // API port selection must never consume a future voter's Raft endpoint.
        let reserved: Vec<_> = (0..voters)
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
            membership: false,
            peer_tls: None,
            api_tls: None,
            nodes: (0..voters).map(|_| None).collect(),
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
        for i in 0..voters {
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
        let placement_id = self.placement_id();
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
        if self.membership {
            environment.push(("MATRIXOBJECT_CHUNK_RAFT_BOOTSTRAP_VOTERS", "1,2,3"));
        }
        if let Some((metadata, _)) = &self.report {
            if self.membership { environment.push(("MATRIXOBJECT_CHUNK_PLACEMENT_ID", placement_id.as_str())); }
            environment.extend([
                ("MATRIXOBJECT_CHUNK_NODE_ID", node_id.as_str()),
                ("MATRIXOBJECT_CHUNK_METADATA_ENDPOINTS", metadata.as_str()),
                ("MATRIXOBJECT_CHUNK_HEARTBEAT_MS", self.heartbeat_ms),
            ]);
        }
        let tls_paths = self.peer_tls.as_ref().map(|root| {
            [
                root.join("ca.pem"),
                root.join(format!("peer-{i}.pem")),
                root.join(format!("peer-{i}.key")),
            ]
        });
        if let Some(paths) = &tls_paths {
            environment.extend([
                (
                    "MATRIXOBJECT_CHUNK_RAFT_TLS_CA_CERT",
                    paths[0].to_str().unwrap(),
                ),
                (
                    "MATRIXOBJECT_CHUNK_RAFT_TLS_CERT",
                    paths[1].to_str().unwrap(),
                ),
                (
                    "MATRIXOBJECT_CHUNK_RAFT_TLS_KEY",
                    paths[2].to_str().unwrap(),
                ),
            ]);
        }
        let api_environment = api_tls_environment(self.api_tls.as_ref());
        environment.extend(api_environment.iter().map(|(name, value)| (*name, value.as_str())));
        // A concurrent child can inherit another group's reservations until exec.
        // Keep release and startup together; membership operations remain concurrent.
        static STARTUP: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _startup = STARTUP.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
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
                tls: api_tls_config(self.api_tls.as_ref()),
                write_timeout: Duration::from_secs(2),
                ..Default::default()
            },
        )
        .unwrap()
    }
    fn leader(&self) -> usize {
        let deadline = Instant::now() + Duration::from_secs(15);
        let mut last = vec![String::new(); self.nodes.len()];
        loop {
            for (i, reply) in last.iter_mut().enumerate() {
                if self.nodes[i].is_some() {
                    match self.client(i).ping() {
                        Ok(_) => return i,
                        Err(error) => *reply = error.to_string(),
                    }
                } else {
                    *reply = "process stopped".into();
                }
            }
            assert!(
                Instant::now() < deadline,
                "no chunk leader committed a read barrier; last replies: {last:?}"
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
        bootstrap_voters: None,
        peer_tls: None,
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
            bootstrap_voters: None,
            peer_tls: None,
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
        vec![("MATRIXOBJECT_CHUNK_RAFT_BOOTSTRAP_VOTERS", "1,2,3")],
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
                bootstrap_voters: None,
                peer_tls: None,
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
            bootstrap_voters: None,
            peer_tls: None,
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
    let state = client.read_chunk_state(&page(0)).unwrap();
    assert!(client.install_chunk_state(&page(0), &state, None, "raft-refusal").is_err());

    group.kill(first);
    let next = group.leader();
    assert_ne!(next, first);
    assert_eq!(client.read_chunk_state(&page(0)).unwrap().data.as_slice(), b"routed");
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
            bootstrap_voters: Vec::new(),
            bootstrap_id: 0,
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
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        match chunk.write_chunk_if_sequence(page(0), 0, b"catalog-data", 0) {
            Ok(()) => break,
            Err(error) => {
                assert!(
                    Instant::now() < deadline,
                    "catalog creation did not settle: {error}"
                );
                chunk = client
                    .connect_chunk_group("group-a", &ReplicaClientOptions::default())
                    .unwrap();
                if let Ok(read) = chunk.read_chunk(&page(0), 0, 12) {
                    assert_eq!(read.data, b"catalog-data");
                    assert_eq!(read.write_sequence, 1);
                    break;
                }
            }
        }
    }
    assert_eq!(chunk.head_chunk(&page(0)).unwrap().write_sequence, 1);
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
    // A deadline can lose the reply after admission. Keep the same condition
    // across attempts, then prove the full result and sequence through readback.
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let outcome = chunk.write_chunk_if_sequence(page(0), 12, b"!", 1);
        if let Ok(state) = chunk.read_chunk_state(&page(0)) {
            if state.write_sequence == 2 {
                assert_eq!(state.data.as_slice(), b"catalog-data!");
                assert!(state.discarded_ranges.is_empty());
                break;
            }
            assert_eq!(state.write_sequence, 1);
            assert_eq!(state.data.as_slice(), b"catalog-data");
        }
        assert!(
            Instant::now() < deadline,
            "conditional catalog update did not settle: {outcome:?}"
        );
        chunk = client
            .connect_chunk_group("group-a", &ReplicaClientOptions::default())
            .unwrap();
        std::thread::sleep(Duration::from_millis(25));
    }
    assert!(chunk.write_chunk_if_sequence(page(0), 12, b"!", 1).is_err());
    let state = chunk.read_chunk_state(&page(0)).unwrap();
    assert_eq!(state.write_sequence, 2);
    assert_eq!(state.data.as_slice(), b"catalog-data!");
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

    mod fenced_heartbeats { include!("node_regime/fenced_chunk_heartbeat_processes.rs"); }
    mod dynamic_groups { include!("node_regime/dynamic_chunk_group_scale.rs"); }

    struct MetadataGroup {
        peer_tls: Option<PathBuf>,
        api_tls: Option<PathBuf>,
        nodes: Vec<Option<NodeProcess>>,
        api: Vec<String>,
        peers: String,
        root: PathBuf,
        reservations: Vec<Option<(TcpListener, TcpListener)>>,
        multiple_groups: bool,
        checkpoint_bytes: u64,
        request_threads: usize,
    }
    impl MetadataGroup {
        fn new(chunk: &Group) -> Self {
            Self::for_groups(std::slice::from_ref(chunk))
        }
        fn for_groups(chunks: &[Group]) -> Self {
            Self::for_groups_with_threads(chunks, 2)
        }
        fn for_groups_with_threads(chunks: &[Group], request_threads: usize) -> Self {
            Self::for_groups_with_voters(chunks, request_threads, 3)
        }
        fn for_groups_with_voters(chunks: &[Group], request_threads: usize, voters: usize) -> Self {
            assert!([3, 5, 7].contains(&voters));
            assert!((1..=32).contains(&request_threads));
            let root = chunks[0].root.join("metadata");
            std::fs::create_dir(&root).unwrap();
            let sockets: Vec<_> = (0..voters)
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
            let mut health = BTreeMap::new();
            let groups =
                chunks
                    .iter()
                    .map(|chunk| {
                        let mut config = ChunkGroupConfig {
                        placement_id: chunk.placement_id(),
                        failure_domain: format!("rack-{}", chunk.group_id),
                        identity: Some(ChunkGroupIdentity {
                            bootstrap_voters: Vec::new(),
                            bootstrap_id: 0,
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
                        member_ids: (0..chunk.nodes.len()).map(|i| chunk.member_id(i)).collect(),
                        };
                        if chunk.membership {
                            use matrixobjectstore_rs::cluster::{ChunkMembershipFence, ChunkMembershipHealth, MembershipRequest};
                            let mut client = chunk.client(chunk.leader());
                            let identity = client.chunk_group_identity().unwrap();
                            let status = client.chunk_membership(MembershipRequest {version: 1, bootstrap_id: identity.bootstrap_id, operation: 0, node_id: 0}).unwrap();
                            let fence = ChunkMembershipFence::from_status(&identity, &status).unwrap();
                            health.insert(chunk.placement_id(), ChunkMembershipHealth::new(fence));
                            config.identity = Some(identity);
                        }
                        config
                    })
                    .collect();
            let catalog = ChunkGroupCatalog::with_membership_health(groups, health).unwrap();
            std::fs::write(root.join("catalog.bin"), catalog.to_bytes().unwrap()).unwrap();
            let mut group = Self {
                peer_tls: chunks[0].peer_tls.clone(),
                api_tls: chunks[0].api_tls.clone(),
                nodes: (0..voters).map(|_| None).collect(),
                api,
                peers,
                root,
                reservations: sockets.into_iter().map(Some).collect(),
                multiple_groups: chunks.len() > 1,
                checkpoint_bytes: chunks[0].checkpoint_bytes,
                request_threads,
            };
            for i in 0..voters {
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
            let request_threads = self.request_threads.to_string();
            let mut environment: Vec<(&str, &str)> = vec![
                ("MATRIXOBJECT_METASERVER_ADDR", &self.api[i]),
                ("MATRIXOBJECT_METASERVER_THREADS", &request_threads),
                ("MATRIXOBJECT_METASERVER_RAFT_PEERS", &self.peers),
                ("MATRIXOBJECT_METASERVER_RAFT_NODE_ID", &id),
                ("MATRIXOBJECT_METASERVER_RAFT_CHECKPOINT_BYTES", &checkpoint_bytes),
                ("MATRIXOBJECT_METASERVER_RAFT_STATE_DIR", state.to_str().unwrap()),
                ("MATRIXOBJECT_METASERVER_RAFT_TIMEOUT_MS", if self.multiple_groups { "5000" } else { "700" }),
                ("MATRIXOBJECT_METASERVER_CHUNK_GROUPS_FILE", catalog.to_str().unwrap()),
                ("MATRIXOBJECT_METASERVER_HEARTBEAT_TTL_MS", if self.multiple_groups { "10000" } else { "1500" }),
            ];
            let tls_paths = self.peer_tls.as_ref().map(|root| [
                root.join("ca.pem"),
                root.join(format!("peer-{i}.pem")),
                root.join(format!("peer-{i}.key")),
            ]);
            if let Some(paths) = &tls_paths {
                environment.extend([
                    ("MATRIXOBJECT_METASERVER_RAFT_TLS_CA_CERT", paths[0].to_str().unwrap()),
                    ("MATRIXOBJECT_METASERVER_RAFT_TLS_CERT", paths[1].to_str().unwrap()),
                    ("MATRIXOBJECT_METASERVER_RAFT_TLS_KEY", paths[2].to_str().unwrap()),
                ]);
            }
            let api_environment = api_tls_environment(self.api_tls.as_ref());
            environment.extend(api_environment.iter().map(|(name, value)| (*name, value.as_str())));
            drop(self.reservations[i].take());
            let node = processes::start(env!("CARGO_BIN_EXE_metaserver"), &environment);
            assert_eq!(node.endpoint, self.api[i]);
            assert!(node.startup_line.contains("storage=raft"));
            assert!(node.startup_line.contains(&format!("threads={request_threads} ")));
            self.nodes[i] = Some(node);
        }
        fn ready(&self) -> RemoteMetaClient {
            let deadline = Instant::now() + Duration::from_secs(20);
            loop {
                let options = MetaClientOptions {
                    connect_timeout: Duration::from_millis(200),
                    read_timeout: Duration::from_secs(2),
                    write_timeout: Duration::from_secs(2),
                    tls: api_tls_config(self.api_tls.as_ref()),
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
            // This is an eventual-state read; a transient refusal does not extend its deadline.
            let observed = match client.list_placement_nodes() {
                Ok(nodes) => {
                    let group = nodes.iter().find(|n| n.node_id == "group-a").unwrap();
                    if group.online == online
                        && group.used_bytes == used
                        && group.committed_bytes == committed
                    {
                        return;
                    }
                    format!("{nodes:?}")
                }
                Err(error) => format!("read failed: {error}"),
            };
            assert!(
                Instant::now() < deadline,
                "group health did not converge: {observed}"
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
    fn health_convergence_wait_survives_temporary_metadata_quorum_loss() {
        let mut chunks = Group::configured(65536);
        let mut metadata = MetadataGroup::new(&chunks);
        chunks.report = Some((metadata.api.join(","), chunks.api_addresses.clone()));
        for i in 0..3 {
            chunks.start(i);
        }
        chunks.leader();
        let mut client = metadata.ready();
        health(&mut client, true, 0, 0);
        let leader = metadata.api.iter().position(|a| a == client.endpoint()).unwrap();
        let stopped: Vec<_> = (0..3).filter(|i| *i != leader).collect();
        for &i in &stopped {
            metadata.kill(i);
        }
        // Prove the read cannot succeed before asking the convergence waiter to recover.
        assert!(client.list_placement_nodes().is_err());
        let waiting = std::thread::spawn(move || health(&mut client, true, 0, 0));
        // The configured consensus deadline is 700 ms; force another refused read.
        std::thread::sleep(Duration::from_secs(2));
        let finished_without_quorum = waiting.is_finished();
        for i in stopped {
            metadata.start(i);
        }
        waiting.join().expect("health convergence must survive a temporary read failure");
        assert!(!finished_without_quorum, "health wait returned without metadata quorum");
        metadata.ready();
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

    #[test]
    fn binary_manifests_bind_immutable_data_through_failover_and_restart() {
        use matrixobjectstore_rs::object_manifest::{
            ManifestOutcome, ObjectExtent, ObjectManifest,
        };
        use matrixobjectstore_rs::ObjectError;
        fn checksum(bytes: &[u8]) -> u64 {
            bytes.iter().fold(0xcbf29ce484222325u64, |h, b| {
                (h ^ u64::from(*b)).wrapping_mul(0x100000001b3)
            })
        }
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
        let mut current = None;
        for generation in 1..=16 {
            let bytes = vec![generation as u8; 4];
            data.write_chunk_if_sequence(page(800 + generation), 0, &bytes, 0)
                .unwrap();
            let manifest = ObjectManifest {
                format_version: 1,
                bucket: "manifests".into(),
                key: "object".into(),
                generation,
                length: 4,
                deleted: false,
                extents: vec![ObjectExtent {
                    group: "group-a".into(),
                    volume_id: 3,
                    chunk_id: 7,
                    page_index: 800 + generation,
                    object_offset: 0,
                    page_offset: 0,
                    length: 4,
                    write_sequence: 1,
                    data_checksum: checksum(&bytes),
                }],
            };
            let outcome = ManifestOutcome::decide(current.clone(), manifest.clone()).unwrap();
            if generation == 1 {
                let mut wrong = manifest.clone();
                wrong.extents[0].group = "unknown-group".into();
                assert!(meta
                    .publish_manifest(ManifestOutcome::decide(None, wrong).unwrap(), 1)
                    .is_err());
            }
            assert_eq!(
                meta.publish_manifest(outcome, generation).unwrap(),
                manifest
            );
            current = Some(manifest);
        }
        let current = current.unwrap();
        assert_eq!(
            meta.read_object("manifests", "object", 4, &chunk_options())
                .unwrap(),
            (current.clone(), vec![16; 4])
        );
        assert!(matches!(
            meta.read_object("manifests", "object", 3, &chunk_options()),
            Err(ObjectError::PreconditionFailed(_))
        ));
        // Verify assembly with page offsets and multiple extents, including a late
        // invalid extent: a failed read must never expose a successful prefix.
        let mut composite = current.clone();
        composite.key = "composite".into();
        composite.generation = 1;
        composite.length = 4;
        composite.extents[0].page_offset = 1;
        composite.extents[0].length = 2;
        composite.extents[0].data_checksum = checksum(&[16; 2]);
        let mut second = composite.extents[0].clone();
        second.page_index = 815;
        second.object_offset = 2;
        second.data_checksum = checksum(&[15; 2]);
        composite.extents.push(second);
        meta.publish_manifest(
            ManifestOutcome::decide(None, composite.clone()).unwrap(),
            20,
        )
        .unwrap();
        assert_eq!(
            meta.read_object("manifests", "composite", 4, &chunk_options())
                .unwrap()
                .1,
            vec![16, 16, 15, 15]
        );
        let mut invalid = composite.clone();
        invalid.key = "invalid-checksum".into();
        invalid.extents[1].data_checksum ^= 1;
        meta.publish_manifest(ManifestOutcome::decide(None, invalid).unwrap(), 21)
            .unwrap();
        assert!(matches!(
            meta.read_object("manifests", "invalid-checksum", 4, &chunk_options()),
            Err(ObjectError::Corruption(_))
        ));
        let mut invalid = composite;
        invalid.key = "invalid-sequence".into();
        invalid.extents[1].write_sequence += 1;
        meta.publish_manifest(ManifestOutcome::decide(None, invalid).unwrap(), 22)
            .unwrap();
        assert!(matches!(
            meta.read_object("manifests", "invalid-sequence", 4, &chunk_options()),
            Err(ObjectError::Corruption(_))
        ));
        assert_eq!(meta.read_manifest("manifests", "object").unwrap(), current);
        health(&mut meta, true, 64, 16);
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
        metadata.kill(meta_leader);
        chunks.kill(chunks.leader());
        meta = metadata.ready();
        chunks.leader();
        assert_eq!(meta.read_manifest("manifests", "object").unwrap(), current);
        assert_eq!(
            meta.read_object("manifests", "object", 4, &chunk_options())
                .unwrap(),
            (current.clone(), vec![16; 4])
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
        let restored = meta.read_manifest("manifests", "object").unwrap();
        assert_eq!(restored, current);
        assert_eq!(
            meta.read_object("manifests", "object", 4, &chunk_options())
                .unwrap(),
            (current.clone(), vec![16; 4])
        );
        for generation in 1..=16 {
            let read = data.read_chunk(&page(800 + generation), 0, 4).unwrap();
            assert_eq!(read.data, vec![generation as u8; 4]);
            assert_eq!(
                data.head_chunk(&page(800 + generation))
                    .unwrap()
                    .write_sequence,
                1
            );
        }
        assert_eq!(
            checksum(&data.read_chunk(&page(816), 0, 4).unwrap().data),
            restored.extents[0].data_checksum
        );
        data.write_chunk_if_sequence(page(815), 0, &[15; 4], 1)
            .unwrap();
        assert!(matches!(
            meta.read_object("manifests", "composite", 4, &chunk_options()),
            Err(ObjectError::Corruption(_))
        ));
        let mut deleted = restored.clone();
        deleted.generation += 1;
        deleted.length = 0;
        deleted.extents.clear();
        deleted.deleted = true;
        meta.publish_manifest(
            ManifestOutcome::decide(Some(restored.clone()), deleted.clone()).unwrap(),
            30,
        )
        .unwrap();
        assert_eq!(meta.read_manifest("manifests", "object").unwrap(), deleted);
        assert!(matches!(
            meta.read_object("manifests", "object", 4, &chunk_options()),
            Err(ObjectError::NotFound(_))
        ));
        assert!(meta
            .publish_manifest(
                ManifestOutcome::decide(Some(restored), deleted.clone()).unwrap(),
                30
            )
            .is_err());
        // Exercise empty manifests alongside the deletion before another restart.
        for generation in 1..=16 {
            let empty = ObjectManifest {
                format_version: 1,
                bucket: "empty".into(),
                key: format!("key-{generation}"),
                generation: 1,
                length: 0,
                extents: vec![],
                deleted: false,
            };
            meta.publish_manifest(
                ManifestOutcome::decide(None, empty).unwrap(),
                40 + generation,
            )
            .unwrap();
        }
        for id in 0..3 {
            metadata.kill(id);
        }
        for id in 0..3 {
            metadata.start(id);
        }
        meta = metadata.ready();
        assert_eq!(meta.read_manifest("manifests", "object").unwrap(), deleted);
        assert!(matches!(
            meta.read_object("manifests", "object", 4, &chunk_options()),
            Err(ObjectError::NotFound(_))
        ));
        assert!(meta
            .read_object("empty", "key-1", 0, &chunk_options())
            .unwrap()
            .1
            .is_empty());
        assert!(matches!(
            meta.read_object("empty", "missing", 0, &chunk_options()),
            Err(ObjectError::NotFound(_))
        ));
        let leader = metadata
            .api
            .iter()
            .position(|a| a == meta.endpoint())
            .unwrap();
        for id in 0..3 {
            if id != leader {
                metadata.kill(id);
            }
        }
        assert!(meta.read_manifest("manifests", "object").is_err());
    }

    #[test]
    fn allocated_pages_span_data_groups_and_stay_unique_after_metadata_recovery() {
        use matrixobjectstore_rs::object_manifest::{
            ManifestOutcome, ObjectExtent, ObjectManifest,
        };
        let mut groups: Vec<_> = (1..=2)
            .map(|id| {
                let mut group = Group::configured(1024 * 1024);
                group.group_id = id;
                group.checkpoint_bytes = 1024;
                group
            })
            .collect();
        let mut metadata = MetadataGroup::for_groups(&groups);
        for group in &mut groups {
            group.report = Some((metadata.api.join(","), group.api_addresses.clone()));
            for id in 0..3 {
                group.start(id);
            }
        }
        let mut meta = metadata.ready();
        for group in &groups {
            group.leader();
        }
        group_usage(&metadata.api, &groups, &[]);
        let allocation = meta.allocate_object_pages(2).unwrap();
        let mut extents = Vec::new();
        for (index, group) in groups.iter().enumerate() {
            let page = allocation.page(index as u32).unwrap();
            let bytes = vec![index as u8 + 1; 4];
            let mut data = meta
                .connect_chunk_group(&group.placement_id(), &chunk_options())
                .unwrap();
            data.write_chunk_if_sequence(page.clone(), 0, &bytes, 0)
                .unwrap();
            assert!(data
                .write_chunk_if_sequence(page.clone(), 0, b"late", 0)
                .is_err());
            assert_eq!(data.read_chunk(&page, 0, 4).unwrap().data, bytes);
            extents.push(ObjectExtent {
                group: group.placement_id(),
                volume_id: page.chunk_id.volume_id(),
                chunk_id: page.chunk_id.id(),
                page_index: page.page_index,
                object_offset: index as u64 * 4,
                page_offset: 0,
                length: 4,
                write_sequence: 1,
                data_checksum: bytes.iter().fold(0xcbf29ce484222325u64, |h, b| {
                    (h ^ u64::from(*b)).wrapping_mul(0x100000001b3)
                }),
            });
        }
        let manifest = ObjectManifest {
            format_version: 1,
            bucket: "allocated".into(),
            key: "object".into(),
            generation: 1,
            length: 8,
            extents,
            deleted: false,
        };
        meta.publish_manifest(ManifestOutcome::decide(None, manifest.clone()).unwrap(), 10)
            .unwrap();
        assert_eq!(
            meta.read_object("allocated", "object", 8, &chunk_options())
                .unwrap(),
            (manifest.clone(), vec![1, 1, 1, 1, 2, 2, 2, 2])
        );
        // Independent clients race allocations through the same committed sequence.
        let mut threads = Vec::new();
        for _ in 0..4 {
            let endpoints = metadata.api.clone();
            threads.push(std::thread::spawn(move || {
                let mut client =
                    RemoteMetaClient::connect_catalog(&endpoints, &MetaClientOptions::default())
                        .unwrap();
                (0..8)
                    .map(|_| client.allocate_object_pages(2).unwrap().allocation_id)
                    .collect::<Vec<_>>()
            }));
        }
        let ids: std::collections::BTreeSet<_> = threads
            .into_iter()
            .flat_map(|t| t.join().unwrap())
            .collect();
        assert_eq!(ids.len(), 32);
        assert!(ids.iter().all(|id| *id > allocation.allocation_id));
        let mut last = *ids.last().unwrap();
        let leader = metadata
            .api
            .iter()
            .position(|a| a == meta.endpoint())
            .unwrap();
        node_regime::checkpoints::wait_for_checkpoint(
            &metadata
                .root
                .join(leader.to_string())
                .join("metadata.peer.wal"),
            2,
        );
        metadata.kill(leader);
        for group in &mut groups {
            group.kill(group.leader());
        }
        meta = metadata.ready();
        for group in &groups {
            group.leader();
        }
        let next = meta.allocate_object_pages(2).unwrap();
        assert!(next.allocation_id > last);
        last = next.allocation_id;
        assert_eq!(
            meta.read_object("allocated", "object", 8, &chunk_options())
                .unwrap()
                .1,
            vec![1, 1, 1, 1, 2, 2, 2, 2]
        );
        for id in 0..3 {
            if metadata.nodes[id].is_some() {
                metadata.kill(id);
            }
            for group in &mut groups {
                if group.nodes[id].is_some() {
                    group.kill(id);
                }
            }
        }
        for id in 0..3 {
            metadata.start(id);
            for group in &mut groups {
                group.start(id);
            }
        }
        meta = metadata.ready();
        for group in &groups {
            group.leader();
        }
        assert!(meta.allocate_object_pages(2).unwrap().allocation_id > last);
        assert_eq!(
            meta.read_object("allocated", "object", 8, &chunk_options())
                .unwrap(),
            (manifest, vec![1, 1, 1, 1, 2, 2, 2, 2])
        );
        let leader = metadata
            .api
            .iter()
            .position(|a| a == meta.endpoint())
            .unwrap();
        for id in 0..3 {
            if id != leader {
                metadata.kill(id);
            }
        }
        assert!(meta.allocate_object_pages(1).is_err());
    }

    #[test]
    fn object_writer_publishes_verified_pages_across_groups_and_recovery() {
        object_writer_recovery(4);
    }
    #[test]
    fn serial_object_writer_preserves_verified_recovery() {
        object_writer_recovery(1);
    }
    fn object_writer_recovery(max_parallel_groups: usize) {
        use matrixobjectstore_rs::cluster::{ObjectWriteFailure, ObjectWriteOptions};
        let mut groups: Vec<_> = (1..=2)
            .map(|id| {
                let mut group = Group::configured(1024 * 1024);
                group.group_id = id;
                group.checkpoint_bytes = 1024;
                group
            })
            .collect();
        let mut metadata = MetadataGroup::for_groups(&groups);
        for group in &mut groups {
            group.report = Some((metadata.api.join(","), group.api_addresses.clone()));
            for id in 0..3 {
                group.start(id);
            }
        }
        let mut meta = metadata.ready();
        for group in &groups {
            group.leader();
        }
        group_usage(&metadata.api, &groups, &[]);
        let options = ObjectWriteOptions {
            page_bytes: 4,
            target_groups: 2,
            max_parallel_groups,
            replica: ReplicaClientOptions {
                deadline: Some(Instant::now() + Duration::from_secs(60)),
                ..chunk_options()
            },
            ..Default::default()
        };
        let first = meta
            .write_object("written", "object", None, b"abcdefghijklmnopq", &options)
            .unwrap();
        assert_eq!(first.extents.len(), 5);
        assert_ne!(first.extents[0].group, first.extents[1].group);
        assert_eq!(
            meta.read_object("written", "object", 100, &options.replica)
                .unwrap(),
            (first.clone(), b"abcdefghijklmnopq".to_vec())
        );
        let second = meta
            .write_object(
                "written",
                "object",
                Some(first.clone()),
                b"new bytes",
                &options,
            )
            .unwrap();
        assert_eq!(second.generation, 2);
        assert!(matches!(
            meta.write_object("written", "object", Some(first.clone()), b"stale", &options),
            Err(ObjectWriteFailure::BeforePublication { staged: None, .. })
        ));
        // Old generations stay readable from their retained pages.
        let old = &first.extents[0];
        let mut data = meta
            .connect_chunk_group(&old.group, &options.replica)
            .unwrap();
        let page = matrixobjectstore_rs::PageId {
            chunk_id: matrixobjectstore_rs::ChunkId::new(old.volume_id, old.chunk_id).unwrap(),
            page_index: old.page_index,
        };
        assert_eq!(data.read_chunk(&page, 0, old.length).unwrap().data, b"abcd");
        let empty = meta
            .write_object("written", "empty", None, b"", &options)
            .unwrap();
        assert!(empty.extents.is_empty());
        let mut deleted = second.clone();
        deleted.generation += 1;
        deleted.deleted = true;
        deleted.length = 0;
        deleted.extents.clear();
        meta.publish_manifest(
            matrixobjectstore_rs::object_manifest::ManifestOutcome::decide(
                Some(second),
                deleted.clone(),
            )
            .unwrap(),
            30,
        )
        .unwrap();
        assert!(matches!(
            meta.write_object("written", "object", None, b"old absent writer", &options),
            Err(ObjectWriteFailure::BeforePublication { .. })
        ));
        let recreated = meta
            .write_object("written", "object", Some(deleted), b"recreated", &options)
            .unwrap();
        assert_eq!(recreated.generation, 4);
        let leader = metadata
            .api
            .iter()
            .position(|a| a == meta.endpoint())
            .unwrap();
        metadata.kill(leader);
        for group in &mut groups {
            group.kill(group.leader());
        }
        meta = metadata.ready();
        for group in &groups {
            group.leader();
        }
        let updated = meta
            .write_object(
                "written",
                "object",
                Some(recreated),
                b"after failover",
                &options,
            )
            .unwrap();
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let mut racers = Vec::new();
        for payload in [b"racer-a".to_vec(), b"racer-b".to_vec()] {
            let endpoints = metadata.api.clone();
            let expected = updated.clone();
            let options = options.clone();
            let barrier = barrier.clone();
            racers.push(std::thread::spawn(move || {
                let mut client =
                    RemoteMetaClient::connect_catalog(&endpoints, &MetaClientOptions::default())
                        .unwrap();
                barrier.wait();
                let result =
                    client.write_object("written", "object", Some(expected), &payload, &options);
                (result, payload)
            }));
        }
        let mut winners = Vec::new();
        for racer in racers {
            let (result, payload) = racer.join().unwrap();
            match result {
                Ok(manifest) => winners.push((manifest, payload)),
                Err(
                    ObjectWriteFailure::BeforePublication { .. }
                    | ObjectWriteFailure::PublicationUnconfirmed { .. },
                ) => {}
            }
        }
        assert_eq!(winners.len(), 1);
        let winning = winners.pop().unwrap();

        for id in 0..3 {
            if metadata.nodes[id].is_some() {
                metadata.kill(id);
            }
            for group in &mut groups {
                if group.nodes[id].is_some() {
                    group.kill(id);
                }
            }
        }
        for id in 0..3 {
            metadata.start(id);
            for group in &mut groups {
                group.start(id);
            }
        }
        meta = metadata.ready();
        for group in &groups {
            group.leader();
        }
        assert_eq!(
            meta.read_object("written", "object", 100, &options.replica)
                .unwrap(),
            winning
        );
    }

    #[test]
    fn maximum_default_object_survives_group_failover_and_total_restart() {
        use matrixobjectstore_rs::cluster::{ObjectWriteFailure, ObjectWriteOptions};
        let mut groups: Vec<_> = (1..=2)
            .map(|id| {
                let mut group = Group::configured(128 * 1024 * 1024);
                group.group_id = id;
                group.max_pages = 128;
                group.checkpoint_bytes = 16 * 1024 * 1024;
                group.heartbeat_ms = "1000";
                group
            })
            .collect();
        let mut metadata = MetadataGroup::for_groups_with_threads(&groups, 16);
        for group in &mut groups {
            group.report = Some((metadata.api.join(","), group.api_addresses.clone()));
            for id in 0..3 {
                group.start(id);
            }
        }
        let mut meta = metadata.ready();
        for group in &groups {
            group.leader();
        }
        group_usage(&metadata.api, &groups, &[]);
        let options = ObjectWriteOptions {
            target_groups: 2,
            replica: chunk_options(),
            ..Default::default()
        };
        assert_eq!(options.max_object_bytes, 64 * 1024 * 1024);
        let data: Vec<u8> = (0..options.max_object_bytes)
            .map(|i| ((i * 31 + i / 1024) % 251) as u8)
            .collect();
        let started = Instant::now();
        let manifest = meta
            .write_object("large", "object", None, &data, &options)
            .unwrap();
        assert_eq!(manifest.extents.len(), 64);
        assert_eq!(manifest.length, data.len() as u64);
        for group in &groups {
            assert_eq!(
                manifest
                    .extents
                    .iter()
                    .filter(|e| e.group == group.placement_id())
                    .count(),
                32
            );
        }
        assert!(manifest
            .extents
            .iter()
            .all(|e| e.length == 1024 * 1024 && e.write_sequence == 1));
        println!(
            "LARGE_OBJECT write_bytes={} elapsed_ms={}",
            data.len(),
            started.elapsed().as_millis()
        );
        let verify = |meta: &mut RemoteMetaClient| {
            let (actual, bytes) = meta
                .read_object("large", "object", data.len(), &chunk_options())
                .unwrap();
            assert_eq!(actual, manifest);
            assert!(bytes == data, "large object payload differs");
        };
        verify(&mut meta);
        assert!(meta
            .read_object("large", "object", data.len() - 1, &chunk_options())
            .is_err());
        let mut oversized = data.clone();
        oversized.push(0);
        assert!(matches!(
            meta.write_object("large", "too-large", None, &oversized, &options),
            Err(ObjectWriteFailure::BeforePublication { staged: None, .. })
        ));
        drop(oversized);
        assert!(matches!(
            meta.read_manifest("large", "too-large"),
            Err(matrixobjectstore_rs::ObjectError::NotFound(_))
        ));
        for group in &groups {
            let leader = group.leader();
            node_regime::checkpoints::wait_for_checkpoint(
                &group.root.join(leader.to_string()).join("chunk.peer.wal"),
                2,
            );
        }
        let leader = metadata
            .api
            .iter()
            .position(|a| a == meta.endpoint())
            .unwrap();
        metadata.kill(leader);
        for group in &mut groups {
            group.kill(group.leader());
        }
        meta = metadata.ready();
        for group in &groups {
            group.leader();
        }
        verify(&mut meta);
        // Quorums that survived leader loss must still accept fresh publication.
        let later = meta
            .write_object("large", "later", None, b"after leader loss", &options)
            .unwrap();
        for group in &mut groups {
            for id in 0..3 {
                if group.nodes[id].is_some() {
                    group.kill(id);
                }
            }
        }
        for id in 0..3 {
            if metadata.nodes[id].is_some() {
                metadata.kill(id);
            }
        }
        for id in 0..3 {
            metadata.start(id);
            for group in &mut groups {
                group.start(id);
            }
        }
        meta = metadata.ready();
        for group in &groups {
            group.leader();
        }
        verify(&mut meta);
        let (actual, bytes) = meta
            .read_object("large", "later", 64, &chunk_options())
            .unwrap();
        assert_eq!(actual, later);
        assert_eq!(bytes, b"after leader loss");
        println!(
            "LARGE_OBJECT recovered_bytes={} processes=9 groups=2",
            data.len()
        );
    }
    #[cfg(target_os = "linux")]
    #[test]
    fn suspended_leaders_rejoin_without_serving_stale_objects() {
        use matrixobjectstore_rs::cluster::ObjectWriteOptions;
        fn signal(node: &NodeProcess, value: &str) {
            assert!(std::process::Command::new("kill")
                .args([value, &node.pid.to_string()])
                .status()
                .unwrap()
                .success());
        }
        fn suspend(node: NodeProcess) -> NodeProcess {
            signal(&node, "-STOP");
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                let status = std::fs::read_to_string(format!("/proc/{}/status", node.pid)).unwrap();
                if status
                    .lines()
                    .any(|line| line.starts_with("State:") && line.contains("T (stopped)"))
                {
                    break;
                }
                assert!(Instant::now() < deadline, "child did not suspend");
                std::thread::sleep(Duration::from_millis(5));
            }
            node
        }
        let mut groups: Vec<_> = (1..=2)
            .map(|id| {
                let mut group = Group::configured(1024 * 1024);
                group.group_id = id;
                group.checkpoint_bytes = 1024;
                group
            })
            .collect();
        let mut metadata = MetadataGroup::for_groups_with_threads(&groups, 16);
        for group in &mut groups {
            group.report = Some((metadata.api.join(","), group.api_addresses.clone()));
            for id in 0..3 {
                group.start(id);
            }
            group.leader();
        }
        let mut meta = metadata.ready();
        group_usage(&metadata.api, &groups, &[]);
        let options = ObjectWriteOptions {
            page_bytes: 4,
            target_groups: 2,
            replica: ReplicaClientOptions {
                read_timeout: Duration::from_secs(2),
                ..chunk_options()
            },
            ..Default::default()
        };
        let first = meta
            .write_object("suspended", "object", None, b"abcdefgh", &options)
            .unwrap();
        let old_meta = metadata
            .api
            .iter()
            .position(|e| e == meta.endpoint())
            .unwrap();
        let old_data: Vec<_> = groups.iter().map(Group::leader).collect();
        let suspended_meta = suspend(metadata.nodes[old_meta].take().unwrap());
        let suspended_data: Vec<_> = groups
            .iter_mut()
            .zip(&old_data)
            .map(|(group, id)| suspend(group.nodes[*id].take().unwrap()))
            .collect();
        let quick_meta = MetaClientOptions {
            read_timeout: Duration::from_millis(200),
            ..load_options()
        };
        let quick_data = ReplicaClientOptions {
            read_timeout: Duration::from_millis(200),
            ..chunk_options()
        };
        assert!(
            RemoteMetaClient::connect(&metadata.api[old_meta], &quick_meta)
                .unwrap()
                .read_manifest("suspended", "object")
                .is_err()
        );
        for (group, id) in groups.iter().zip(&old_data) {
            assert!(RemoteReplicaClient::connect(
                &group.api_addresses[*id].to_string(),
                &quick_data
            )
            .unwrap()
            .ping()
            .is_err());
            group.leader();
        }
        meta = metadata.ready();
        let second = meta
            .write_object(
                "suspended",
                "object",
                Some(first.clone()),
                b"ijklmnop",
                &options,
            )
            .unwrap();
        assert_eq!(
            meta.read_object("suspended", "object", 8, &options.replica)
                .unwrap(),
            (second.clone(), b"ijklmnop".to_vec())
        );
        // These requests can remain queued in the suspended voters' sockets.
        // Their replies time out, so they are uncertain rather than retried.
        // Once resumed, stale conditions must not overwrite the newer state.
        let mut stale = first.clone();
        stale.generation += 1;
        let stale_outcome =
            matrixobjectstore_rs::object_manifest::ManifestOutcome::decide(Some(first), stale)
                .unwrap();
        assert!(
            RemoteMetaClient::connect(&metadata.api[old_meta], &quick_meta)
                .unwrap()
                .publish_manifest(stale_outcome, 0)
                .is_err()
        );
        for (group, id) in groups.iter().zip(&old_data) {
            let extent = second
                .extents
                .iter()
                .find(|e| e.group == group.placement_id())
                .unwrap();
            let page_id = matrixobjectstore_rs::PageId {
                chunk_id: matrixobjectstore_rs::ChunkId::new(extent.volume_id, extent.chunk_id)
                    .unwrap(),
                page_index: extent.page_index,
            };
            assert!(RemoteReplicaClient::connect(
                &group.api_addresses[*id].to_string(),
                &quick_data
            )
            .unwrap()
            .write_chunk_if_sequence(page_id, 0, b"zzzz", 0)
            .is_err());
        }
        signal(&suspended_meta, "-CONT");
        metadata.nodes[old_meta] = Some(suspended_meta);
        for ((group, id), node) in groups.iter_mut().zip(&old_data).zip(suspended_data) {
            signal(&node, "-CONT");
            group.nodes[*id] = Some(node);
        }
        // A resumed voter may refuse while catching up. Any successful quorum
        // read must reflect the write acknowledged while that voter was frozen.
        for _ in 0..8 {
            let mut old = RemoteMetaClient::connect(&metadata.api[old_meta], &quick_meta).unwrap();
            match old.read_manifest("suspended", "object") {
                Ok(value) => assert_eq!(value, second),
                Err(matrixobjectstore_rs::ObjectError::NotFound(_)) => {
                    panic!("resumed metadata returned stale absence")
                }
                Err(_) => {}
            }
            for (group, id) in groups.iter().zip(&old_data) {
                let extent = second
                    .extents
                    .iter()
                    .find(|e| e.group == group.placement_id())
                    .unwrap();
                let page_id = matrixobjectstore_rs::PageId {
                    chunk_id: matrixobjectstore_rs::ChunkId::new(extent.volume_id, extent.chunk_id)
                        .unwrap(),
                    page_index: extent.page_index,
                };
                let mut old = RemoteReplicaClient::connect(
                    &group.api_addresses[*id].to_string(),
                    &quick_data,
                )
                .unwrap();
                match old.read_chunk(&page_id, 0, extent.length) {
                    Ok(read) => {
                        assert_eq!(read.write_sequence, 1);
                        assert_eq!(
                            read.data,
                            b"ijklmnop"[extent.object_offset as usize
                                ..(extent.object_offset + extent.length) as usize]
                        );
                    }
                    Err(matrixobjectstore_rs::ObjectError::NotFound(_)) => {
                        panic!("resumed data voter returned stale absence")
                    }
                    Err(_) => {}
                }
            }
        }
        // Remove another voter so progress now requires each resumed process.
        metadata.kill((old_meta + 1) % 3);
        for (group, id) in groups.iter_mut().zip(&old_data) {
            group.kill((*id + 1) % 3);
            group.leader();
        }
        meta = metadata.ready();
        assert_eq!(
            meta.read_object("suspended", "object", 8, &options.replica)
                .unwrap(),
            (second.clone(), b"ijklmnop".to_vec())
        );
        // A recovered data quorum can serve reads before its next capacity
        // report makes the group eligible for a new placement. Observe that
        // readiness explicitly; do not retry the object mutation.
        let placement_deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let observed = meta.list_placement_nodes();
            if observed.as_ref().is_ok_and(|nodes| {
                groups.iter().all(|group| {
                    nodes.iter().any(|node| {
                        node.node_id == group.placement_id()
                            && node.online
                            && node.data_eligible
                    })
                })
            }) {
                break;
            }
            assert!(
                Instant::now() < placement_deadline,
                "resumed groups did not become eligible for placement: {observed:?}"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
        let third = meta
            .write_object("suspended", "object", Some(second), b"qrstuvwx", &options)
            .unwrap();
        for id in 0..3 {
            if metadata.nodes[id].is_some() {
                metadata.kill(id);
            }
            for group in &mut groups {
                if group.nodes[id].is_some() {
                    group.kill(id);
                }
            }
        }
        for id in 0..3 {
            metadata.start(id);
            for group in &mut groups {
                group.start(id);
            }
        }
        for group in &groups {
            group.leader();
        }
        assert_eq!(
            metadata
                .ready()
                .read_object("suspended", "object", 8, &options.replica)
                .unwrap(),
            (third, b"qrstuvwx".to_vec())
        );
    }

    #[test]
    fn object_writer_data_refusal_never_publishes_partial_objects() {
        use matrixobjectstore_rs::cluster::{ObjectWriteFailure, ObjectWriteOptions};
        let mut chunks = Group::configured(65536);
        chunks.max_pages = 1;
        let metadata = MetadataGroup::new(&chunks);
        chunks.report = Some((metadata.api.join(","), chunks.api_addresses.clone()));
        for id in 0..3 {
            chunks.start(id);
        }
        let mut meta = metadata.ready();
        chunks.leader();
        health(&mut meta, true, 0, 0);
        let options = ObjectWriteOptions {
            page_bytes: 4,
            replica: chunk_options(),
            ..Default::default()
        };
        let error = meta
            .write_object("written", "partial", None, b"abcdefgh", &options)
            .unwrap_err();
        let ObjectWriteFailure::BeforePublication {
            staged: Some(manifest),
            ..
        } = error
        else {
            panic!("partial write must remain unpublished");
        };
        assert_eq!(manifest.extents.len(), 2);
        assert!(matches!(
            meta.read_manifest("written", "partial"),
            Err(matrixobjectstore_rs::ObjectError::NotFound(_))
        ));
        let mut data = meta
            .connect_chunk_group("group-a", &chunk_options())
            .unwrap();
        assert_eq!(data.ping().unwrap(), 1);
        let extent = &manifest.extents[0];
        let page = matrixobjectstore_rs::PageId {
            chunk_id: matrixobjectstore_rs::ChunkId::new(extent.volume_id, extent.chunk_id)
                .unwrap(),
            page_index: extent.page_index,
        };
        assert_eq!(data.read_chunk(&page, 0, 4).unwrap().data, b"abcd");
    }

    #[test]
    fn concurrent_group_refusal_drains_siblings_without_publishing() {
        use matrixobjectstore_rs::cluster::{ObjectWriteFailure, ObjectWriteOptions};
        let mut groups: Vec<_> = (1..=2).map(|id| {
            let mut group = Group::configured(65536);
            group.group_id = id;
            group.max_pages = if id == 1 { 1 } else { 16 };
            group
        }).collect();
        let metadata = MetadataGroup::for_groups(&groups);
        for group in &mut groups {
            group.report = Some((metadata.api.join(","), group.api_addresses.clone()));
            for id in 0..3 { group.start(id); }
            group.leader();
        }
        let mut meta = metadata.ready();
        group_usage(&metadata.api, &groups, &[]);
        let bytes = b"abcdefghijklmnop";
        let error = meta.write_object("written", "partial-groups", None, bytes, &ObjectWriteOptions {
            page_bytes: 4, target_groups: 2, max_parallel_groups: 2,
            replica: ReplicaClientOptions {
                deadline: Some(Instant::now() + Duration::from_secs(10)),
                ..chunk_options()
            },
            ..Default::default()
        }).unwrap_err();
        let ObjectWriteFailure::BeforePublication { staged: Some(manifest), .. } = error else {
            panic!("group refusal must keep the object unpublished");
        };
        assert_eq!(manifest.extents.len(), 4);
        assert!(matches!(meta.read_manifest("written", "partial-groups"), Err(matrixobjectstore_rs::ObjectError::NotFound(_))));
        let healthy = groups[1].placement_id();
        let mut data = meta.connect_chunk_group(&healthy, &chunk_options()).unwrap();
        // All started siblings finish before failure is returned, including their verification.
        assert_eq!(data.ping().unwrap(), 2);
        for extent in manifest.extents.iter().filter(|extent| extent.group == healthy) {
            let page = matrixobjectstore_rs::PageId {
                chunk_id: matrixobjectstore_rs::ChunkId::new(extent.volume_id, extent.chunk_id).unwrap(),
                page_index: extent.page_index,
            };
            let read = data.read_chunk(&page, 0, extent.length).unwrap();
            assert_eq!(read.write_sequence, 1);
            let start = extent.object_offset as usize;
            assert_eq!(read.data, bytes[start..start + extent.length as usize]);
        }
    }

    fn load_data(n: u64) -> Vec<u8> {
        load_data_sized(n, 1024)
    }
    fn load_data_sized(n: u64, bytes: u64) -> Vec<u8> {
        assert!((8..=4 * 1024 * 1024).contains(&bytes));
        let mut bytes: Vec<u8> = (0..bytes)
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
    fn load_batch_with_groups(
        api: &[String],
        start: u64,
        end: u64,
        workers: usize,
        objects: bool,
        target_groups: u32,
    ) -> Vec<(u64, CommittedPlacement)> {
        load_batch_sized(&LoadEndpoint { api, tls: None }, start, end, workers, objects, target_groups, 1024)
    }
    struct LoadEndpoint<'a> {
        api: &'a [String],
        tls: Option<matrixobjectstore_rs::cluster::ApiTlsConfig>,
    }
    impl LoadEndpoint<'_> {
        fn metadata_options(&self) -> MetaClientOptions {
            MetaClientOptions { tls: self.tls.clone(), ..load_options() }
        }
        fn replica_options(&self) -> ReplicaClientOptions {
            ReplicaClientOptions { tls: self.tls.clone(), ..chunk_options() }
        }
    }
    fn load_batch_sized(
        endpoint: &LoadEndpoint<'_>,
        start: u64,
        end: u64,
        workers: usize,
        objects: bool,
        target_groups: u32,
        object_bytes: u64,
    ) -> Vec<(u64, CommittedPlacement)> {
        assert!(objects || object_bytes == 1024);
        assert_eq!(object_bytes % 4, 0);
        let begin = Instant::now();
        let results = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..workers)
                .map(|worker| {
                    scope.spawn(move || {
                        let mut meta =
                            RemoteMetaClient::connect_catalog(endpoint.api, &endpoint.metadata_options()).unwrap();
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
                            if objects {
                                let options = matrixobjectstore_rs::cluster::ObjectWriteOptions {
                                    page_bytes: (object_bytes / 4) as usize, update_time_ms: n + 1, target_groups,
                                    replica: ReplicaClientOptions { deadline: Some(started + Duration::from_secs(10)), ..endpoint.replica_options() }, ..Default::default()
                                };
                                let manifest = meta.write_object("load", &format!("object-{n}"), None, &load_data_sized(n, object_bytes), &options).unwrap();
                                assert_eq!(manifest.extents.len(), 4);
                                let distinct = manifest.extents.iter().map(|e| &e.group).collect::<std::collections::BTreeSet<_>>();
                                assert_eq!(distinct.len(), target_groups as usize);
                                for group in distinct {
                                    assert_eq!(manifest.extents.iter().filter(|e| &e.group == group).count(), 4 / target_groups as usize);
                                }
                                if target_groups == 2 {
                                    assert_eq!(manifest.extents[0].group, manifest.extents[2].group);
                                    assert_eq!(manifest.extents[1].group, manifest.extents[3].group);
                                    assert_ne!(manifest.extents[0].group, manifest.extents[1].group);
                                }
                                let node_ids = manifest.extents.iter().map(|e| e.group.clone()).collect::<std::collections::BTreeSet<_>>().into_iter().collect();
                                saved.push((n, CommittedPlacement { bucket: manifest.bucket, key: manifest.key, length: manifest.length, node_ids, version: manifest.generation, update_time_ms: n + 1 }));
                                latencies.push(started.elapsed().as_micros());
                                if saved.len() % 32 == 0 { println!("OBJECT_WRITER worker={worker} completed={}", saved.len()); }
                                continue;
                            }
                            let plan = meta
                                .plan_object_placement("load", &format!("object-{n}"), 1024, 1)
                                .unwrap();
                            stages[0] += started.elapsed().as_micros();
                            let phase = Instant::now();
                            assert_eq!(plan.node_ids.len(), 1);
                            let group = plan.node_ids[0].clone();
                            let chunk = clients.entry(group.clone()).or_insert_with(|| {
                                meta.connect_chunk_group(&group, &endpoint.replica_options()).unwrap()
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
        println!("MULTIGROUP objects={objects} object_bytes={object_bytes} writes={} workers={} elapsed_ms={} operations_per_second={:.1} p50_us={} p95_us={} p99_us={}", saved.len(), workers, elapsed.as_millis(), saved.len() as f64 / elapsed.as_secs_f64(), latencies[latencies.len()/2], latencies[latencies.len()*95/100], latencies[latencies.len()*99/100]);
        saved
    }
    fn group_usage(api: &[String], groups: &[Group], saved: &[(u64, CommittedPlacement)]) {
        let mut options = load_options();
        options.tls = api_tls_config(groups[0].api_tls.as_ref());
        let mut meta = RemoteMetaClient::connect_catalog(api, &options).unwrap();
        let expected: BTreeMap<_, _> = groups
            .iter()
            .map(|g| {
                let id = g.placement_id();
                let mut physical = 0;
                let mut committed = 0;
                for (_, placement) in saved.iter().filter(|(_, p)| p.node_ids.contains(&id)) {
                    assert_eq!(placement.length % placement.node_ids.len() as u64, 0);
                    physical += placement.length / placement.node_ids.len() as u64;
                    committed += placement.length;
                }
                (id, (physical, committed))
            })
            .collect();
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let nodes = meta.list_placement_nodes().unwrap();
            assert_eq!(nodes.len(), groups.iter().map(|g| g.nodes.len() + 1).sum::<usize>());
            if groups.iter().all(|g| {
                let id = g.placement_id();
                let node = nodes.iter().find(|n| n.node_id == id).unwrap();
                node.online
                    && node.data_eligible
                    && node.used_bytes == expected[&id].0
                    && node.committed_bytes == expected[&id].1
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
    fn verify_load(api: &[String], saved: &[(u64, CommittedPlacement)], objects: bool) {
        verify_load_with_transport(&LoadEndpoint { api, tls: None }, saved, objects);
    }
    fn verify_load_with_transport(endpoint: &LoadEndpoint<'_>, saved: &[(u64, CommittedPlacement)], objects: bool) {
        if objects {
            let began = Instant::now();
            let mut samples = std::thread::scope(|scope| {
                let handles: Vec<_> = (0..8)
                    .map(|worker| {
                        scope.spawn(move || {
                            let mut samples = Vec::new();
                            let mut meta =
                                RemoteMetaClient::connect_catalog(endpoint.api, &endpoint.metadata_options()).unwrap();
                            for (index, (n, placement)) in
                                saved.iter().enumerate().skip(worker).step_by(8)
                            {
                                assert!(
                                    began.elapsed() < Duration::from_secs(180),
                                    "object read workload exceeded deadline"
                                );
                                let started = Instant::now();
                                let (manifest, bytes) = meta
                                    .read_object(
                                        &placement.bucket,
                                        &placement.key,
                                        usize::try_from(placement.length).unwrap(),
                                        &ReplicaClientOptions { deadline: Some(started + Duration::from_secs(10)), ..endpoint.replica_options() },
                                    )
                                    .unwrap();
                                samples.push(started.elapsed().as_micros());
                                assert_eq!(bytes, load_data_sized(*n, placement.length));
                                assert_eq!(manifest.generation, placement.version);
                                assert_eq!(manifest.length, placement.length);
                                assert_eq!(manifest.extents.len(), 4);
                                assert!(manifest.extents.iter().all(|e| e.write_sequence == 1));
                                let actual: Vec<_> = manifest
                                    .extents
                                    .iter()
                                    .map(|e| e.group.clone())
                                    .collect::<std::collections::BTreeSet<_>>()
                                    .into_iter()
                                    .collect();
                                assert_eq!(actual, placement.node_ids);
                                for group in &actual {
                                    assert_eq!(
                                        manifest
                                            .extents
                                            .iter()
                                            .filter(|e| &e.group == group)
                                            .count(),
                                        4 / actual.len()
                                    );
                                }
                                assert_eq!(
                                    meta.object_placement(&placement.bucket, &placement.key)
                                        .unwrap(),
                                    *placement
                                );
                                if index % 128 == 0 {
                                    println!("OBJECT_READ worker={worker} index={index}");
                                }
                            }
                            samples
                        })
                    })
                    .collect();
                handles.into_iter().flat_map(|handle| handle.join().unwrap()).collect::<Vec<_>>()
            });
            assert_eq!(samples.len(), saved.len());
            samples.sort_unstable();
            let elapsed = began.elapsed();
            let bytes: u64 = saved.iter().map(|(_, placement)| placement.length).sum();
            println!(
                "OBJECT_READ verified={} elapsed_ms={} verified_mib_s={:.2} p50_us={} p95_us={} p99_us={}",
                saved.len(), elapsed.as_millis(), bytes as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64(),
                samples[samples.len()/2], samples[samples.len()*95/100], samples[samples.len()*99/100]
            );
            return;
        }
        let mut meta = RemoteMetaClient::connect_catalog(endpoint.api, &endpoint.metadata_options()).unwrap();
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
                .or_insert_with(|| meta.connect_chunk_group(id, &endpoint.replica_options()).unwrap());
            assert_eq!(
                chunk.read_chunk(&page(*n), 0, 1024).unwrap().data,
                load_data(*n)
            );
            assert_eq!(chunk.head_chunk(&page(*n)).unwrap().write_sequence, 1);
        }
    }
    #[test]
    fn unavailable_data_group_is_isolated_and_recovers_without_losing_objects() {
        unavailable_group_objects(1);
    }
    #[test]
    fn unavailable_group_preserves_striped_objects_and_other_group_writes() {
        unavailable_group_objects(2);
    }
    fn unavailable_group_objects(target_groups: u32) {
        let mut groups: Vec<_> = (0..3)
            .map(|index| {
                let mut group = Group::configured(64 * 1024 * 1024);
                group.group_id = index + 1;
                group.max_pages = 2048;
                group.checkpoint_bytes = 16 * 1024;
                group.heartbeat_ms = "1000";
                group
            })
            .collect();
        let mut metadata = MetadataGroup::for_groups_with_threads(&groups, 16);
        for group in &mut groups {
            group.report = Some((metadata.api.join(","), group.api_addresses.clone()));
            for id in 0..3 {
                group.start(id);
            }
            group.leader();
        }
        metadata.ready();
        group_usage(&metadata.api, &groups, &[]);
        let mut saved = load_batch_with_groups(&metadata.api, 0, 48, 4, true, target_groups);
        group_usage(&metadata.api, &groups, &saved);
        for group in &groups {
            assert!(saved
                .iter()
                .any(|(_, p)| p.node_ids.contains(&group.placement_id())));
        }
        let unavailable = groups[0].placement_id();
        let isolated = saved
            .iter()
            .find(|(_, p)| p.node_ids.contains(&unavailable))
            .unwrap()
            .0;
        let mut meta = metadata.ready();
        let original = meta
            .read_manifest("load", &format!("object-{isolated}"))
            .unwrap();
        groups[0].kill(0);
        groups[0].kill(1);
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let nodes = meta.list_placement_nodes().unwrap();
            let down = nodes.iter().find(|n| n.node_id == unavailable).unwrap();
            if !down.online {
                for group in &groups[1..] {
                    assert!(
                        nodes
                            .iter()
                            .find(|n| n.node_id == group.placement_id())
                            .unwrap()
                            .online
                    );
                }
                break;
            }
            assert!(
                Instant::now() < deadline,
                "minority group remained eligible: {nodes:?}"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
        if target_groups == 2 {
            let leader = metadata.ready().endpoint().to_string();
            let leader = metadata
                .api
                .iter()
                .position(|endpoint| endpoint == &leader)
                .unwrap();
            metadata.kill(leader);
            metadata.ready();
            // The existing catalog client must follow leadership while one data
            // group remains unavailable; its immutable manifest must survive.
            assert_eq!(
                meta.read_manifest("load", &format!("object-{isolated}"))
                    .unwrap(),
                original
            );
            assert!(
                !meta
                    .list_placement_nodes()
                    .unwrap()
                    .iter()
                    .find(|n| n.node_id == unavailable)
                    .unwrap()
                    .online
            );
        }
        // Metadata remains readable, but one surviving data voter cannot serve
        // an object from this group or acknowledge a replacement page.
        let short = ReplicaClientOptions {
            connect_timeout: Duration::from_millis(200),
            read_timeout: Duration::from_millis(500),
            write_timeout: Duration::from_millis(500),
            ..Default::default()
        };
        assert!(meta
            .read_object("load", &format!("object-{isolated}"), 1024, &short)
            .is_err());
        assert_eq!(
            meta.read_manifest("load", &format!("object-{isolated}"))
                .unwrap(),
            original
        );
        let mut minority = meta.connect_chunk_group(&unavailable, &short).unwrap();
        let extent = original
            .extents
            .iter()
            .find(|e| e.group == unavailable)
            .unwrap();
        let existing_page = matrixobjectstore_rs::PageId {
            chunk_id: matrixobjectstore_rs::ChunkId::new(extent.volume_id, extent.chunk_id)
                .unwrap(),
            page_index: extent.page_index,
        };
        // Even if this timed-out request finishes after quorum restoration, the
        // expected-zero condition cannot overwrite an already published page.
        assert!(minority
            .write_chunk_if_sequence(existing_page, 0, b"uncommitted", 0)
            .is_err());
        let available: Vec<_> = saved
            .iter()
            .filter(|(_, p)| !p.node_ids.contains(&unavailable))
            .cloned()
            .collect();
        assert!(!available.is_empty());
        verify_load(&metadata.api, &available, true);
        let newer = load_batch_with_groups(&metadata.api, 48, 72, 4, true, target_groups);
        assert!(newer
            .iter()
            .all(|(_, p)| !p.node_ids.contains(&unavailable)));
        verify_load(&metadata.api, &newer, true);
        saved.extend(newer);
        // Restore only one voter first: the restored quorum must catch up and
        // make original objects readable while the third voter remains stopped.
        groups[0].start(0);
        groups[0].leader();
        group_usage(&metadata.api, &groups, &saved);
        verify_load(&metadata.api, &saved, true);
        groups[0].start(1);
        group_usage(&metadata.api, &groups, &saved);
        assert_eq!(
            meta.read_manifest("load", &format!("object-{isolated}"))
                .unwrap(),
            original
        );
    }

    #[test]
    fn external_probe_verifies_striped_objects_after_leader_loss_and_restart() {
        probe_recovery(false);
    }

    #[test]
    #[ignore = "requires a private network namespace; use tools/network_partition_test.sh"]
    fn live_peer_partitions_preserve_objects_and_recover() {
        node_regime::network::require_isolated();
        probe_recovery(true);
    }

    #[cfg(feature = "peer-tls")]
    #[test]
    #[ignore = "requires a private network namespace; use tools/network_partition_test.sh"]
    fn authenticated_live_peer_partitions_preserve_objects_and_recover() {
        node_regime::network::require_isolated();
        let certificates = node_regime::tls::TlsFiles::new();
        for i in 0..3 {
            certificates.issue(&format!("peer-{i}"), "127.0.0.1");
        }
        probe_recovery_with_transport(true, Some(certificates.root.clone()));
    }

    fn probe_recovery(partition: bool) {
        probe_recovery_with_transport(partition, None);
    }
    #[cfg(feature = "peer-tls")]
    #[test]
    fn authenticated_api_and_peer_objects_recover_after_restart() {
        let certificates = node_regime::tls::TlsFiles::new();
        for i in 0..3 {
            certificates.issue(&format!("peer-{i}"), "127.0.0.1");
        }
        probe_recovery_with_security(false, Some(certificates.root.clone()), true);
    }

    #[cfg(feature = "peer-tls")]
    #[test]
    #[ignore = "requires a private network namespace; use tools/network_partition_test.sh"]
    fn authenticated_api_and_peer_partitions_preserve_objects() {
        node_regime::network::require_isolated();
        let certificates = node_regime::tls::TlsFiles::new();
        for i in 0..3 {
            certificates.issue(&format!("peer-{i}"), "127.0.0.1");
        }
        probe_recovery_with_security(true, Some(certificates.root.clone()), true);
    }

    fn probe_recovery_with_transport(partition: bool, peer_tls: Option<PathBuf>) {
        probe_recovery_with_security(partition, peer_tls, false);
    }
    fn probe_recovery_with_security(partition: bool, peer_tls: Option<PathBuf>, api_tls: bool) {
        let mut groups: Vec<_> = (1..=2)
            .map(|id| {
                let mut group = Group::configured(4 * 1024 * 1024);
                group.peer_tls = peer_tls.clone();
                group.api_tls = if api_tls { peer_tls.clone() } else { None };
                group.group_id = id;
                group.max_pages = 1024;
                group.checkpoint_bytes = 16 * 1024;
                group.heartbeat_ms = "1000";
                group
            })
            .collect();
        let mut metadata = MetadataGroup::for_groups_with_threads(&groups, 8);
        for group in &mut groups {
            group.report = Some((metadata.api.join(","), group.api_addresses.clone()));
            for i in 0..3 {
                group.start(i);
            }
            group.leader();
        }
        let group_id = metadata
            .ready()
            .metadata_group_identity()
            .unwrap()
            .group_id
            .to_string();
        group_usage(&metadata.api, &groups, &[]);
        let endpoints = metadata.api.join(",");
        let api_environment = api_tls_environment(metadata.api_tls.as_ref());
        let replica_options = ReplicaClientOptions { tls: api_tls_config(metadata.api_tls.as_ref()), ..Default::default() };
        let run = |mode: &str, bytes: &str, name: &str| {
            std::process::Command::new(env!("CARGO_BIN_EXE_distributed_probe"))
                .envs(api_environment.iter().map(|(name, value)| (*name, value)))
                .args([
                    mode,
                    "--metadata",
                    &endpoints,
                    "--group",
                    &group_id,
                    "--bucket",
                    "probe-bucket",
                    "--run",
                    name,
                    "--objects",
                    "12",
                    "--bytes",
                    bytes,
                    "--workers",
                    "3",
                    "--groups",
                    "2",
                    "--page-bytes",
                    "256",
                ])
                .output()
                .unwrap()
        };
        let check = |output: std::process::Output| {
            assert!(
                output.status.success(),
                "stdout={} stderr={}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            let text = String::from_utf8(output.stdout).unwrap();
            assert!(
                text.contains("phase=verify completed=12 requested=12"),
                "{text}"
            );
            assert!(!text.contains("failed=true"), "{text}");
        };
        check(run("write", "1025", "recovery"));
        let (_, first) = metadata
            .ready()
            .read_object(
                "probe-bucket",
                "probe/recovery/00000000",
                1025,
                &replica_options,
            )
            .unwrap();
        // Keep workload version 1 reproducible across client upgrades.
        assert_eq!(
            &first[..16],
            &[48, 4, 40, 164, 67, 125, 127, 187, 26, 250, 23, 127, 169, 118, 248, 113]
        );
        assert!(!run("write", "1025", "recovery").status.success());
        assert!(!run("verify", "1024", "recovery").status.success());
        check(run("verify", "1025", "recovery"));
        if partition {
            let leaders: Vec<_> = groups.iter().map(Group::leader).collect();
            let endpoint = metadata.ready().endpoint().to_owned();
            let meta_leader = metadata
                .api
                .iter()
                .position(|api| api == &endpoint)
                .unwrap();
            let mut addresses: Vec<_> = groups
                .iter()
                .zip(&leaders)
                .map(|(group, leader)| group.peers[&(*leader as u64 + 1)])
                .collect();
            addresses.push(
                metadata
                    .peers
                    .split(',')
                    .nth(meta_leader)
                    .unwrap()
                    .split_once('@')
                    .unwrap()
                    .1
                    .parse()
                    .unwrap(),
            );
            let mut blocked = node_regime::network::BlockPeers::new(&addresses);
            for (group, isolated) in groups.iter().zip(&leaders) {
                let until = Instant::now() + Duration::from_secs(30);
                while group.leader() == *isolated {
                    assert!(
                        Instant::now() < until,
                        "isolated data leader still serves quorum reads"
                    );
                    std::thread::sleep(Duration::from_millis(50));
                }
                assert!(group.client(*isolated).ping().is_err());
            }
            let until = Instant::now() + Duration::from_secs(30);
            while metadata.ready().endpoint() == endpoint {
                assert!(
                    Instant::now() < until,
                    "isolated metadata leader still serves quorum reads"
                );
                std::thread::sleep(Duration::from_millis(50));
            }
            let mut isolated =
                RemoteMetaClient::connect(&endpoint, &MetaClientOptions { tls: api_tls_config(metadata.api_tls.as_ref()), ..Default::default() }).unwrap();
            assert!(isolated
                .read_manifest("probe-bucket", "probe/recovery/00000000")
                .is_err());
            check(run("verify", "1025", "recovery"));
            check(run("write", "1025", "partition"));
            for group in &mut groups {
                for node in &mut group.nodes {
                    assert!(node.as_mut().unwrap().child.try_wait().unwrap().is_none());
                }
            }
            for node in &mut metadata.nodes {
                assert!(node.as_mut().unwrap().child.try_wait().unwrap().is_none());
            }
            blocked.assert_dropped();
            blocked.heal();
            // Remove the new leaders, so the healed voters must join the next write quorums.
            for (group, isolated) in groups.iter_mut().zip(&leaders) {
                let leader = group.leader();
                assert_ne!(leader, *isolated);
                group.kill(leader);
            }
            let leader = metadata.ready().endpoint().to_owned();
            let leader = metadata.api.iter().position(|api| api == &leader).unwrap();
            assert_ne!(leader, meta_leader);
            metadata.kill(leader);
            metadata.ready();
            for group in &groups {
                group.leader();
            }
            check(run("write", "1025", "healed"));
        } else {
            for group in &mut groups {
                let leader = group.leader();
                group.kill(leader);
            }
            let leader = metadata.ready().endpoint().to_owned();
            let leader = metadata.api.iter().position(|api| api == &leader).unwrap();
            metadata.kill(leader);
        }
        metadata.ready();
        for group in &groups {
            group.leader();
        }
        check(run("verify", "1025", "recovery"));
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
        metadata.ready();
        for group in &mut groups {
            for i in 0..3 {
                group.start(i);
            }
            group.leader();
        }
        check(run("verify", "1025", "recovery"));
        if partition {
            check(run("verify", "1025", "partition"));
            check(run("verify", "1025", "healed"));
        }
    }

    fn multiple_group_load(group_count: usize, writes: u64, workers: usize, objects: bool) {
        multiple_group_load_with_groups(group_count, writes, workers, objects, 1);
    }
    fn multiple_group_load_with_groups(
        group_count: usize,
        writes: u64,
        workers: usize,
        objects: bool,
        target_groups: u32,
    ) {
        multiple_group_load_sized(group_count, writes, workers, objects, target_groups, 1024);
    }
    fn multiple_group_load_sized(
        group_count: usize,
        writes: u64,
        workers: usize,
        objects: bool,
        target_groups: u32,
        object_bytes: u64,
    ) {
        multiple_group_load_with_transport(
            group_count,
            writes,
            workers,
            objects,
            target_groups,
            object_bytes,
            VoterSetup { peer_tls: None, api_tls: false, voters: 3 },
        );
    }
    struct VoterSetup {
        peer_tls: Option<PathBuf>,
        api_tls: bool,
        voters: usize,
    }
    fn multiple_group_load_with_transport(
        group_count: usize,
        writes: u64,
        workers: usize,
        objects: bool,
        target_groups: u32,
        object_bytes: u64,
        setup: VoterSetup,
    ) {
        let VoterSetup { peer_tls, api_tls, voters } = setup;
        assert!(!api_tls || peer_tls.is_some());
        assert!((8..=4 * 1024 * 1024).contains(&object_bytes));
        assert_eq!(object_bytes % 4, 0);
        assert!(objects || object_bytes == 1024);
        assert!(target_groups == 1 || (objects && matches!(target_groups, 2 | 4)));
        assert!(target_groups as usize <= group_count);
        assert!((2..=16).contains(&group_count));
        assert!(writes > 0 && writes <= 1024);
        let mut groups: Vec<_> = (0..group_count)
            .map(|i| {
                let mut g = Group::configured_voters(64 * 1024 * 1024, voters);
                g.peer_tls = peer_tls.clone();
                g.api_tls = if api_tls { peer_tls.clone() } else { None };
                g.group_id = i as u64 + 1;
                g.max_pages = 2048;
                g.checkpoint_bytes = 16 * 1024;
                g.heartbeat_ms = "1000";
                g
            })
            .collect();
        // Every API and peer socket stays reserved until its process starts.
        let request_threads = if objects { 16 } else { 2 };
        let mut metadata = MetadataGroup::for_groups_with_voters(&groups, request_threads, voters);
        for group in &mut groups {
            group.report = Some((metadata.api.join(","), group.api_addresses.clone()));
            for i in 0..voters {
                group.start(i);
            }
        }
        metadata.ready();
        for group in &groups {
            group.leader();
        }
        group_usage(&metadata.api, &groups, &[]);
        let tls = api_tls_config(metadata.api_tls.as_ref());
        let mut saved = load_batch_sized(
            &LoadEndpoint { api: &metadata.api, tls: tls.clone() },
            0,
            writes,
            workers,
            objects,
            target_groups,
            object_bytes,
        );
        group_usage(&metadata.api, &groups, &saved);
        for group in &groups {
            assert!(
                saved
                    .iter()
                    .any(|(_, p)| p.node_ids.contains(&group.placement_id())),
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
        for _ in 1..voters / 2 {
            let follower = metadata.nodes.iter().position(Option::is_some).unwrap();
            metadata.kill(follower);
        }
        for group in &mut groups {
            let leader = group.leader();
            group.kill(leader);
            for _ in 1..voters / 2 {
                let follower = group.nodes.iter().position(Option::is_some).unwrap();
                group.kill(follower);
            }
            assert_eq!(group.nodes.iter().filter(|n| n.is_some()).count(), voters / 2 + 1);
        }
        assert_eq!(metadata.nodes.iter().filter(|n| n.is_some()).count(), voters / 2 + 1);
        metadata.ready();
        for group in &groups {
            group.leader();
        }
        verify_load_with_transport(&LoadEndpoint { api: &metadata.api, tls: tls.clone() }, &saved, objects);
        saved.extend(load_batch_sized(
            &LoadEndpoint { api: &metadata.api, tls: tls.clone() },
            writes,
            writes + writes / 4,
            workers,
            objects,
            target_groups,
            object_bytes,
        ));
        group_usage(&metadata.api, &groups, &saved);
        for group in &mut groups {
            for i in 0..voters {
                if group.nodes[i].is_some() {
                    group.kill(i);
                }
            }
        }
        for i in 0..voters {
            if metadata.nodes[i].is_some() {
                metadata.kill(i);
            }
        }
        for i in 0..voters {
            metadata.start(i);
        }
        for group in &mut groups {
            for i in 0..voters {
                group.start(i);
            }
        }
        metadata.ready();
        for group in &groups {
            group.leader();
        }
        group_usage(&metadata.api, &groups, &saved);
        verify_load_with_transport(&LoadEndpoint { api: &metadata.api, tls: tls.clone() }, &saved, objects);
        if peer_tls.is_some() {
            for root in std::iter::once(&metadata.root).chain(groups.iter().map(|g| &g.root)) {
                for i in 0..voters {
                    assert_eq!(
                        std::fs::read(root.join(i.to_string()).join("peer.transport")).unwrap(),
                        b"tls-v1\n"
                    );
                    if api_tls {
                        assert_eq!(
                            std::fs::read(root.join(i.to_string()).join("api.transport")).unwrap(),
                            b"tls-v1\n"
                        );
                    }
                }
            }
        }
        println!(
            "MULTIGROUP verified api_tls={api_tls} objects={objects} object_bytes={object_bytes} groups={group_count} groups_per_object={target_groups} metadata_threads={request_threads} processes={} recovered_objects={}",
            (group_count + 1) * voters,
            saved.len()
        );
    }
    #[test]
    fn concurrent_groups_preserve_placement_and_data_through_failover_and_restart() {
        multiple_group_load(3, 96, 6, false);
    }
    #[test]
    #[ignore = "bounded 51-process local load and recovery measurement"]
    fn sixteen_groups_preserve_concurrent_writes_through_recovery() {
        multiple_group_load(16, 1024, 8, false);
    }
    #[test]
    fn object_writer_load_preserves_manifests_and_pages_through_recovery() {
        multiple_group_load(3, 96, 6, true);
    }
    #[test]
    fn striped_objects_preserve_pages_across_group_failover_and_restart() {
        multiple_group_load_with_groups(3, 96, 6, true, 2);
    }
    #[cfg(feature = "peer-tls")]
    fn api_tls_load(groups: usize, writes: u64, bytes: u64) {
        let certificates = node_regime::tls::TlsFiles::new();
        for i in 0..3 {
            certificates.issue(&format!("peer-{i}"), "127.0.0.1");
        }
        multiple_group_load_with_transport(
            groups, writes, 4, true, 4, bytes,
            VoterSetup { peer_tls: Some(certificates.root.clone()), api_tls: true, voters: 3 },
        );
    }
    #[cfg(feature = "peer-tls")]
    #[test]
    fn encrypted_api_load_recovers_four_way_objects() {
        api_tls_load(4, 24, 64 * 1024);
    }
    #[cfg(feature = "peer-tls")]
    #[test]
    #[ignore = "51-process API and Raft TLS load; run explicitly in WSL"]
    fn encrypted_api_sixteen_groups_recover_four_way_objects() {
        api_tls_load(16, 128, 1024 * 1024);
    }
    #[cfg(feature = "peer-tls")]
    fn tls_payload_recovery(groups: usize, writes: u64, bytes: u64) {
        tls_payload_recovery_with_width(groups, writes, bytes, 2);
    }
    #[cfg(feature = "peer-tls")]
    fn tls_payload_recovery_with_width(groups: usize, writes: u64, bytes: u64, target_groups: u32) {
        let certificates = node_regime::tls::TlsFiles::new();
        for i in 0..3 {
            certificates.issue(&format!("peer-{i}"), "127.0.0.1");
        }
        multiple_group_load_with_transport(
            groups,
            writes,
            4,
            true,
            target_groups,
            bytes,
            VoterSetup { peer_tls: Some(certificates.root.clone()), api_tls: false, voters: 3 },
        );
    }
    #[cfg(feature = "peer-tls")]
    #[test]
    fn larger_authenticated_quorums_recover_after_maximum_voter_loss() {
        let certificates = node_regime::tls::TlsFiles::new();
        for i in 0..7 {
            certificates.issue(&format!("peer-{i}"), "127.0.0.1");
        }
        for voters in [5, 7] {
            println!("AUTHENTICATED_VOTERS voters={voters} failures_per_group={}", voters / 2);
            multiple_group_load_with_transport(
                3, 24, 4, true, 2, 64 * 1024,
                VoterSetup { peer_tls: Some(certificates.root.clone()), api_tls: false, voters },
            );
        }
    }
    #[cfg(feature = "peer-tls")]
    #[test]
    fn authenticated_peer_quorums_recover_striped_payloads_and_capacity() {
        tls_payload_recovery(3, 24, 64 * 1024);
    }
    #[cfg(feature = "peer-tls")]
    #[test]
    #[ignore = "51-process mutually authenticated peer recovery; run explicitly in WSL"]
    fn authenticated_sixteen_groups_recover_one_mib_objects() {
        tls_payload_recovery(16, 128, 1024 * 1024);
    }
    #[cfg(feature = "peer-tls")]
    #[test]
    fn four_way_authenticated_objects_recover_after_leader_loss_and_restart() {
        tls_payload_recovery_with_width(4, 24, 64 * 1024, 4);
    }
    #[cfg(feature = "peer-tls")]
    #[test]
    #[ignore = "51-process four-way authenticated object recovery; run explicitly in WSL"]
    fn authenticated_sixteen_groups_recover_four_way_objects() {
        tls_payload_recovery_with_width(16, 128, 1024 * 1024, 4);
    }
    #[test]
    fn striped_payloads_recover_with_exact_bytes_and_capacity_accounting() {
        multiple_group_load_sized(3, 24, 4, true, 2, 64 * 1024);
    }
    #[test]
    #[ignore = "bounded 51-process one-MiB striped-object recovery measurement"]
    fn sixteen_groups_recover_one_mib_striped_objects() {
        multiple_group_load_sized(16, 128, 8, true, 2, 1024 * 1024);
    }
    #[test]
    #[ignore = "bounded 51-process striped-object load and recovery measurement"]
    fn sixteen_groups_preserve_striped_objects_through_recovery() {
        multiple_group_load_with_groups(16, 1024, 8, true, 2);
    }
    #[test]
    #[ignore = "bounded 51-process complete-object load and recovery measurement"]
    fn sixteen_groups_preserve_object_writes_and_reads_through_recovery() {
        multiple_group_load(16, 1024, 8, true);
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
    std::thread::scope(|scope| {
        let mut workers = Vec::new();
        for _ in 0..8 {
            let mut reader = group.client(leader);
            let expected = head.clone();
            workers.push(scope.spawn(move || {
                for _ in 0..8 {
                    assert_eq!(
                        reader.read_chunk(&page(0), 0, 15).unwrap().data,
                        b"committed bytes"
                    );
                    assert_eq!(reader.head_chunk(&page(0)).unwrap(), expected);
                    assert_eq!(reader.ping().unwrap(), 1);
                }
            }));
        }
        for worker in workers {
            worker.join().unwrap();
        }
    });
    let transferred = client.read_chunk_state(&page(0)).unwrap();
    assert_eq!(transferred.data.as_slice(), b"committed bytes");
    assert_eq!(transferred.write_sequence, head.write_sequence);
    assert_eq!(std::fs::read(&path).unwrap(), before);
    // A write following confirmed reads still becomes visible through consensus.
    client.write_chunk(page(0), 0, b"updated bytes!!").unwrap();
    assert_eq!(
        client.read_chunk(&page(0), 0, 15).unwrap().data,
        b"updated bytes!!"
    );
    let updated = client.head_chunk(&page(0)).unwrap();
    assert!(client.read_chunk_state_range(&page(0), &head.state_checksum, 0, 15).is_err());
    for i in 0..3 {
        if i != leader {
            group.kill(i);
        }
    }
    assert!(client.read_chunk(&page(0), 0, 15).is_err());
    assert!(group.client(leader).read_chunk_state_range(&page(0), &updated.state_checksum, 0, 15).is_err());
    assert!(group.client(leader).ping().is_err());
}

fn create_checkpoint_page(group: &Group, client: &mut RemoteReplicaClient, index: u64, data: &[u8]) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        // Conditional creation and readback resolve an uncertain response
        // even when checkpoint installation changes the current leader.
        match client.write_chunk_if_sequence(page(index), 0, data, 0) {
            Ok(()) => break,
            Err(error) => {
                assert!(
                    Instant::now() < deadline,
                    "page {index} did not settle: {error}"
                );
                eprintln!("resolving uncertain page {index} creation: {error}");
                *client = group.client(group.leader());
                if let Ok(read) = client.read_chunk(&page(index), 0, data.len() as u64) {
                    assert_eq!(read.write_sequence, 1);
                    assert_eq!(read.data, data);
                    break;
                }
            }
        }
    }
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
    create_checkpoint_page(&group, &mut client, 20, b"after snapshot transfer");
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
    let started = Instant::now();
    let phase = |name: &str| {
        eprintln!(
            "LARGE_CHUNK phase={name} elapsed_ms={}",
            started.elapsed().as_millis()
        )
    };
    phase("start");
    let mut group = Group::configured(300 * 1024 * 1024);
    group.max_pages = 300;
    group.checkpoint_bytes = 128 * 1024 * 1024;
    for i in 0..3 {
        group.start(i);
    }
    phase("initial-quorum");
    let leader = group.leader();
    phase("write-pages");
    let lagging = (leader + 1) % 3;
    let other = (leader + 2) % 3;
    group.kill(lagging);
    let mut client = group.client(leader);
    for index in 0..272u64 {
        create_checkpoint_page(
            &group,
            &mut client,
            index,
            &vec![(index % 251) as u8; 1024 * 1024],
        );
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
    phase("start-lagging-voter");
    group.start(lagging);
    node_regime::checkpoints::wait_for_checkpoint(
        &group.root.join(lagging.to_string()).join("chunk.peer.wal"),
        4,
    );
    phase("checkpoint-received");
    group.kill(other);
    drop(client);
    phase("recovered-voter-quorum");
    let active = group.leader();
    phase("write-after-transfer");
    let mut client = group.client(active);
    create_checkpoint_page(&group, &mut client, 280, b"after large transfer");
    drop(client);
    group.kill(leader);
    group.kill(lagging);
    for i in 0..3 {
        group.start(i);
    }
    phase("full-restart-quorum");
    let recovered = group.leader();
    phase("verify-pages");
    let mut client = group.client(recovered);
    for index in 0..272u64 {
        let response = client.read_chunk(&page(index), 0, 1024 * 1024).unwrap();
        assert_eq!(response.write_sequence, 1);
        assert_eq!(response.data, vec![(index % 251) as u8; 1024 * 1024]);
    }
    assert_eq!(client.head_chunk(&page(280)).unwrap().write_sequence, 1);
    assert_eq!(
        client.read_chunk(&page(280), 0, 20).unwrap().data,
        b"after large transfer"
    );
}

#[test]
fn conditional_discards_replicate_and_recover_without_accepting_stale_sequences() {
    let mut group = Group::configured(65536);
    group.checkpoint_bytes = 1024;
    for id in 0..3 {
        group.start(id);
    }
    let mut c = group.client(group.leader());
    c.write_chunk_if_sequence(page(90), 0, b"abcdefgh", 0)
        .unwrap();
    c.discard_chunk_if_sequence(&page(90), 2, 3, 1).unwrap();
    let expected = c.head_chunk(&page(90)).unwrap();
    assert_eq!(expected.write_sequence, 2);
    assert!(c.discard_chunk_if_sequence(&page(90), 0, 1, 1).is_err());
    assert_eq!(c.head_chunk(&page(90)).unwrap(), expected);
    let leader = group.leader();
    group.kill(leader);
    let mut c = group.client(group.leader());
    assert_eq!(c.head_chunk(&page(90)).unwrap(), expected);
    assert!(c.read_chunk(&page(90), 2, 3).is_err());
    c.discard_chunk_if_sequence(&page(90), 5, 1, 2).unwrap();
    let recovered = c.head_chunk(&page(90)).unwrap();
    for id in 0..3 {
        if group.nodes[id].is_some() {
            group.kill(id);
        }
    }
    for id in 0..3 {
        group.start(id);
    }
    let mut c = group.client(group.leader());
    assert_eq!(c.head_chunk(&page(90)).unwrap(), recovered);
    assert!(c.discard_chunk_if_sequence(&page(90), 0, 1, 2).is_err());
    assert_eq!(c.read_chunk(&page(90), 0, 2).unwrap().data, b"ab");
    assert!(c.read_chunk(&page(90), 5, 1).is_err());
    c.write_chunk_if_sequence(page(90), 2, b"CDEF", 3).unwrap();
    assert_eq!(c.read_chunk(&page(90), 0, 8).unwrap().data, b"abCDEFgh");
}

#[test]
fn chunk_membership_replaces_lost_storage_with_a_distinct_process() {
    use matrixobjectstore_rs::cluster::{MembershipOperation as Op, MembershipRequest};
    let mut group = Group::configured_voters(65536, 4);
    group.membership = true;
    group.checkpoint_bytes = 1024;
    for i in 0..3 {
        group.start(i);
    }
    let first = group.leader();
    let identity = group.client(first).chunk_group_identity().unwrap();
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
    for i in 0..16 {
        let p = page(i);
        let data = vec![i as u8; 1024];
        let mut client = group.client(first);
        client.write_chunk(p.clone(), 0, &data).unwrap();
        client.discard_chunk(&p, 128, 32).unwrap();
        saved.push((
            p.clone(),
            client.read_chunk_state(&p).unwrap(),
            client.head_chunk(&p).unwrap(),
        ));
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    while !group
        .root
        .join(first.to_string())
        .join("chunk.peer.wal.payloads")
        .is_dir()
    {
        assert!(Instant::now() < deadline, "checkpoint was not captured");
        std::thread::sleep(Duration::from_millis(20));
    }
    group.kill(first);
    let leader = group.leader();
    let mut client = group.client(leader);
    let mut wrong = command(Op::Read, 0);
    wrong.bootstrap_id ^= 1;
    assert!(client.chunk_membership(wrong).is_err());
    assert_eq!(
        client
            .chunk_membership(command(Op::AddLearner, 4))
            .unwrap()
            .learners,
        vec![4]
    );
    assert!(client.chunk_membership(command(Op::Promote, 4)).is_err());
    group.start(3);
    let newcomer = group.client(3).chunk_group_identity().unwrap();
    assert_eq!(newcomer.bootstrap_id, identity.bootstrap_id);
    assert!(group.client(3).ping().is_err());
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut writer = group.client(leader);
    let barrier = std::sync::Barrier::new(2);
    let (joint, concurrent_pages) = std::thread::scope(|scope| {
        let writing = scope.spawn(|| {
            barrier.wait();
            let mut pages = Vec::new();
            for i in 100..132 {
                let p = page(i);
                let bytes = vec![i as u8; 128];
                writer.write_chunk_if_sequence(p.clone(), 0, &bytes, 0).unwrap();
                let state = writer.read_chunk_state(&p).unwrap();
                let head = writer.head_chunk(&p).unwrap();
                assert_eq!(head.write_sequence, 1);
                assert_eq!(writer.read_chunk(&p, 0, 128).unwrap().data, bytes);
                pages.push((p, state, head));
            }
            pages
        });
        barrier.wait();
        let joint = loop {
            match client.chunk_membership(command(Op::Promote, 4)) {
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
        (joint, writing.join().unwrap())
    });
    saved.extend(concurrent_pages);
    assert_eq!(saved.len(), 48);
    assert_eq!(joint.voters, vec![1, 2, 3, 4]);
    assert_eq!(joint.outgoing_voters, vec![1, 2, 3]);
    client.chunk_membership(command(Op::LeaveJoint, 0)).unwrap();
    client
        .chunk_membership(command(Op::Remove, first as u64 + 1))
        .unwrap();
    let status = client.chunk_membership(command(Op::LeaveJoint, 0)).unwrap();
    let expected: Vec<_> = (1..=4).filter(|id| *id != first as u64 + 1).collect();
    assert_eq!(status.voters, expected);
    assert!(status.outgoing_voters.is_empty());
    group.kill(leader);
    let serving = group.leader();
    let mut client = group.client(serving);
    for (p, state, head) in &saved {
        assert_eq!(client.read_chunk_state(p).unwrap(), *state);
        assert_eq!(client.head_chunk(p).unwrap(), *head);
    }
    client.write_chunk(page(40), 0, b"new quorum").unwrap();
    for i in 0..4 {
        if group.nodes[i].is_some() {
            group.kill(i);
        }
    }
    for i in 0..4 {
        group.start(i);
    }
    let serving = group.leader();
    assert_ne!(serving, first);
    let mut client = group.client(serving);
    assert_eq!(
        client
            .chunk_membership(command(Op::Read, 0))
            .unwrap()
            .voters,
        expected
    );
    assert_eq!(
        client.read_chunk(&page(40), 0, 10).unwrap().data,
        b"new quorum"
    );
    for (p, state, head) in &saved {
        assert_eq!(client.read_chunk_state(p).unwrap(), *state);
        assert_eq!(client.head_chunk(p).unwrap(), *head);
    }
    assert!(group
        .client(first)
        .write_chunk(page(41), 0, b"removed")
        .is_err());
}

#[test]
fn chunk_membership_requires_quorum_and_removes_the_serving_leader() {
    use matrixobjectstore_rs::cluster::{MembershipOperation as Op, MembershipRequest};
    let mut group = Group::configured_voters(65536, 4);
    group.membership = true;
    for i in 0..3 {
        group.start(i);
    }
    let first = group.leader();
    let mut client = group.client(first);
    let identity = client.chunk_group_identity().unwrap();
    let command = |op: Op, id| MembershipRequest {
        version: 1,
        bootstrap_id: identity.bootstrap_id,
        operation: op as i32,
        node_id: id,
    };
    for i in 0..3 {
        if i != first {
            group.kill(i);
        }
    }
    assert!(client.chunk_membership(command(Op::AddLearner, 4)).is_err());
    assert!(client.chunk_membership(command(Op::Read, 0)).is_err());
    for i in 0..3 {
        if i != first {
            group.start(i);
        }
    }
    let leader = group.leader();
    let mut client = group.client(leader);
    let status = client.chunk_membership(command(Op::Read, 0)).unwrap();
    assert_eq!(status.voters, vec![1, 2, 3]);
    assert!(status.learners.is_empty() || status.learners == vec![4]);
    if status.learners.is_empty() {
        client.chunk_membership(command(Op::AddLearner, 4)).unwrap();
    }
    assert!(client
        .chunk_membership(command(Op::Remove, 4))
        .unwrap()
        .learners
        .is_empty());
    client
        .chunk_membership(command(Op::Remove, leader as u64 + 1))
        .unwrap();
    let status = client.chunk_membership(command(Op::LeaveJoint, 0)).unwrap();
    assert!(!status.voters.contains(&(leader as u64 + 1)));
    assert!(client.write_chunk(page(0), 0, b"removed").is_err());
    let next = group.leader();
    assert_ne!(next, leader);
    group
        .client(next)
        .write_chunk(page(0), 0, b"remaining quorum")
        .unwrap();
}
