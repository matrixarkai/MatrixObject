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
}
impl Group {
    fn new() -> Self {
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
        let node = processes::start(
            env!("CARGO_BIN_EXE_metaserver"),
            &[
                ("MATRIXOBJECT_METASERVER_ADDR", "127.0.0.1:0"),
                ("MATRIXOBJECT_METASERVER_SEED_DATA_NODES", "3"),
                ("MATRIXOBJECT_METASERVER_RAFT_PEERS", &peers),
                ("MATRIXOBJECT_METASERVER_RAFT_NODE_ID", &id),
                (
                    "MATRIXOBJECT_METASERVER_RAFT_STATE_DIR",
                    path.to_str().unwrap(),
                ),
                ("MATRIXOBJECT_METASERVER_RAFT_TIMEOUT_MS", "700"),
            ],
        );
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
    let mut client = group.client(first);
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
