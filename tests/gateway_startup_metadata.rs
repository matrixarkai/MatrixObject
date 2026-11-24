// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#![cfg(all(target_os = "linux", feature = "metadata-consensus", feature = "http-server"))]
mod node_regime;
use node_regime::processes::{self, NodeProcess};
use matrixobjectstore_rs::cluster::{ChunkGroupCatalog, ChunkGroupConfig, ChunkGroupIdentity, MetaClientOptions, RemoteMetaClient};
use std::{
    io::{BufRead, BufReader, Read, Write}, net::{TcpListener, TcpStream},
    process::{Command, Stdio}, sync::{mpsc, Arc, Mutex}, time::{Duration, Instant},
};

enum Scenario { DelayedQuorumAndData, NoQuorum, NoData }

fn startup_case(scenario: Scenario) {
    let token = node_regime::run_token("gateway-startup");
    let root = std::env::temp_dir().join(&token);
    std::fs::create_dir(&root).unwrap();
    let mut reserved: Vec<_> = (0..3).map(|_| Some((
        TcpListener::bind("127.0.0.1:0").unwrap(), TcpListener::bind("127.0.0.1:0").unwrap(),
    ))).collect();
    let apis: Vec<_> = reserved.iter().map(|p| p.as_ref().unwrap().0.local_addr().unwrap().to_string()).collect();
    let peers = reserved.iter().enumerate().map(|(i,p)| format!("{}@{}", i+1, p.as_ref().unwrap().1.local_addr().unwrap())).collect::<Vec<_>>().join(",");
    let mut data_ports: Vec<_> = (0..3).map(|_| Some(TcpListener::bind("127.0.0.1:0").unwrap())).collect();
    let data_apis: Vec<_> = data_ports.iter().map(|p| p.as_ref().unwrap().local_addr().unwrap().to_string()).collect();
    let catalog = ChunkGroupCatalog::new(vec![ChunkGroupConfig {
        placement_id: "startup-group".into(), failure_domain: "local".into(),
        identity: Some(ChunkGroupIdentity { version: 3, group_id: 1, node_id: 1,
            capacity_bytes: 67_108_864, max_pages: 1024, voters: vec![1,2,3],
            peer_endpoints: Vec::new(), bootstrap_voters: Vec::new(), bootstrap_id: 0 }),
        api_endpoints: data_apis.clone(), member_ids: vec!["startup-1".into(), "startup-2".into(), "startup-3".into()],
    }]).unwrap();
    let catalog_path = root.join("catalog.bin");
    std::fs::write(&catalog_path, catalog.to_bytes().unwrap()).unwrap();
    // Unstarted voters refuse TCP instead of accepting an unserved backlog.
    drop(reserved[1].take()); drop(reserved[2].take());
    let mut start = |i: usize| {
        std::fs::create_dir_all(root.join(i.to_string())).unwrap(); drop(reserved[i].take());
        processes::start(env!("CARGO_BIN_EXE_metaserver"), &[
            ("MATRIXOBJECT_METASERVER_ADDR", &apis[i]), ("MATRIXOBJECT_METASERVER_THREADS", "2"),
            ("MATRIXOBJECT_METASERVER_RAFT_PEERS", &peers),
            ("MATRIXOBJECT_METASERVER_RAFT_NODE_ID", &(i+1).to_string()),
            ("MATRIXOBJECT_METASERVER_RAFT_STATE_DIR", root.join(i.to_string()).to_str().unwrap()),
            ("MATRIXOBJECT_METASERVER_RAFT_TIMEOUT_MS", "200"),
            ("MATRIXOBJECT_METASERVER_CHUNK_GROUPS_FILE", catalog_path.to_str().unwrap()),
            (node_regime::METASERVER_TOKEN_VAR, &token),
        ])
    };
    let mut nodes = vec![start(0)];
    if matches!(scenario, Scenario::NoData) {
        nodes.push(start(1)); nodes.push(start(2));
        let options = MetaClientOptions { deadline: Some(Instant::now()+Duration::from_secs(5)), ..Default::default() };
        RemoteMetaClient::connect_raft_catalog(&apis, 1, &options).unwrap().list_placement_nodes().unwrap();
    }
    let listener = TcpListener::bind("127.0.0.1:0").unwrap(); let addr = listener.local_addr().unwrap(); drop(listener);
    let timeout_ms = if matches!(scenario, Scenario::DelayedQuorumAndData) { 10000 } else { 1500 };
    let binary = std::env::var_os("MATRIXOBJECT_STARTUP_TEST_GATEWAY_BIN")
        .unwrap_or_else(|| env!("CARGO_BIN_EXE_object_server").into());
    let mut command = Command::new(binary);
    command.env_clear().envs(std::env::vars().filter(|(k,_)| !k.starts_with("MATRIXOBJECT_")));
    let child = command.env("MATRIXOBJECT_HTTP_ADDR", addr.to_string())
        .env("MATRIXOBJECT_CLUSTER_META", apis.join(",")).env("MATRIXOBJECT_CLUSTER_META_GROUP", "1")
        .env("MATRIXOBJECT_CLUSTER_TIMEOUT_MS", timeout_ms.to_string()).env("MATRIXOBJECT_THREADS", "2")
        .env("MATRIXOBJECT_BUCKET", "temporalstore").stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();
    let mut door = NodeProcess { pid: child.id(), child, endpoint: addr.to_string(), startup_line: String::new() };
    let (send, receive) = mpsc::channel(); let stdout = door.child.stdout.take().unwrap();
    let reader = std::thread::spawn(move || {
        let mut line = String::new(); let mut output = BufReader::new(stdout);
        output.read_line(&mut line).unwrap(); let _ = send.send(line);
        std::io::copy(&mut output, &mut std::io::sink()).unwrap();
    });
    let errors = Arc::new(Mutex::new(String::new())); let captured = Arc::clone(&errors);
    let stderr = door.child.stderr.take().unwrap(); let (waiting, waited) = mpsc::channel();
    let mut error_reader = Some(std::thread::spawn(move || {
        for line in BufReader::new(stderr).lines() {
            let line = line.unwrap();
            if line.contains("waiting for bucket") { let _ = waiting.send(()); }
            let mut text = captured.lock().unwrap();
            if text.len() < 65536 { text.push_str(&line); text.push('\n'); }
        }
    }));
    let diagnostics = || errors.lock().unwrap().clone();
    let mut data = Vec::new();
    if matches!(scenario, Scenario::DelayedQuorumAndData) {
        std::thread::sleep(Duration::from_secs(2));
        assert!(door.child.try_wait().unwrap().is_none(), "door abandoned the election: {}", diagnostics());
        assert!(TcpStream::connect_timeout(&addr, Duration::from_millis(100)).is_err(), "door listened without metadata quorum");
        nodes.push(start(1)); nodes.push(start(2));
        waited.recv_timeout(Duration::from_secs(12)).unwrap_or_else(|err| panic!("bucket readiness was not awaited: {err}; {}", diagnostics()));
        assert!(door.child.try_wait().unwrap().is_none(), "door abandoned data startup: {}", diagnostics());
        assert!(TcpStream::connect_timeout(&addr, Duration::from_millis(100)).is_err(), "door listened before bucket data was stored");
        for i in 0..3 {
            drop(data_ports[i].take()); let dir = root.join(format!("data-{i}")); std::fs::create_dir(&dir).unwrap();
            data.push(processes::start(env!("CARGO_BIN_EXE_chunkserver"), &[
                ("MATRIXOBJECT_REPLICA_ADDR", &data_apis[i]),
                ("MATRIXOBJECT_REPLICA_JOURNAL", dir.join("journal.wal").to_str().unwrap()),
                ("MATRIXOBJECT_REPLICA_CAPACITY_BYTES", "67108864"), ("MATRIXOBJECT_REPLICA_MAX_PAGES", "1024"),
                ("MATRIXOBJECT_CHUNK_NODE_ID", &format!("startup-{}", i+1)),
                ("MATRIXOBJECT_CHUNK_METADATA_ENDPOINTS", &apis.join(",")),
                (node_regime::REPLICA_TOKEN_VAR, &token),
            ]));
        }
        let line = receive.recv_timeout(Duration::from_secs(12)).unwrap();
        assert!(line.contains("object_server listening on"), "{line:?}; status={:?}; {}", door.child.try_wait().unwrap(), diagnostics());
        assert!(line.contains(&format!("pid={}", door.pid)), "{line}"); let startup_observed = Instant::now();
        for after_deadline in [false, true] {
            if after_deadline { std::thread::sleep(Duration::from_millis(10200).saturating_sub(startup_observed.elapsed())); }
            let mut stream = TcpStream::connect(addr).unwrap(); stream.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
            stream.write_all(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n").unwrap();
            let mut response = String::new(); stream.read_to_string(&mut response).unwrap();
            assert!(response.starts_with("HTTP/1.1 200"), "{response}; {}", diagnostics());
        }
    } else {
        if matches!(scenario, Scenario::NoData) {
            waited.recv_timeout(Duration::from_secs(3)).unwrap_or_else(|err| panic!("bucket failure was not observed: {err}; {}", diagnostics()));
        }
        assert_eq!(processes::reap_within(&mut door.child, Duration::from_secs(5)), Some(1), "{}", diagnostics());
        assert!(receive.recv_timeout(Duration::from_secs(1)).unwrap().is_empty());
        error_reader.take().unwrap().join().unwrap();
        let message = diagnostics();
        assert!(message.contains(if matches!(scenario, Scenario::NoData) { "bucket" } else { "metadata" }), "{message}");
        assert!(TcpStream::connect_timeout(&addr, Duration::from_millis(100)).is_err());
    }
    drop(door); reader.join().unwrap();
    if let Some(reader) = error_reader { reader.join().unwrap(); }
    drop(data); drop(nodes);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn a_gateway_waits_for_metadata_election_before_listening() { startup_case(Scenario::DelayedQuorumAndData); }
#[test]
fn a_gateway_without_metadata_quorum_stops_at_its_startup_deadline() { startup_case(Scenario::NoQuorum); }
#[test]
fn a_gateway_without_bucket_data_stops_at_its_startup_deadline() { startup_case(Scenario::NoData); }
