// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use crate::cluster::native_object_client::{NativeObjectClient, NativeObjectError};
use crate::cluster::{
    AppendIdentity, ChunkGroupCatalog, ChunkGroupConfig, ChunkGroupIdentity, MetaClientOptions,
    MetaRaftOptions, RemoteMetaClient, ReplicaClientOptions, ReplicaNodeOptions, ReplicaNodeServer,
};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
const HELPER: &str = "cluster::meta_server::native_object_path_tests::native_object_process_node";
static NEXT: AtomicU64 = AtomicU64::new(0);

#[test]
#[ignore = "owned child process entry"]
fn native_object_process_node() {
    let Ok(role) = std::env::var("MATRIXOBJECT_NATIVE_PROCESS_ROLE") else {
        return;
    };
    let root = PathBuf::from(std::env::var("MATRIXOBJECT_NATIVE_PROCESS_ROOT").unwrap());
    std::fs::create_dir_all(&root).unwrap();
    let address = std::env::var("MATRIXOBJECT_NATIVE_PROCESS_ADDRESS").unwrap();
    if role == "data" {
        let server = ReplicaNodeServer::bind_with_journal(
            &address,
            ReplicaNodeOptions {
                publication_writes: true,
                max_payload_bytes: 64 * 1024,
                ..Default::default()
            },
            &root.join("journal"),
        )
        .unwrap();
        println!("NATIVE_READY {}", server.local_addr().unwrap());
        std::io::stdout().flush().unwrap();
        server.serve().unwrap();
        return;
    }
    assert_eq!(role, "meta");
    let endpoints: Vec<String> = std::env::var("MATRIXOBJECT_NATIVE_PROCESS_DATA")
        .unwrap()
        .split(',')
        .map(str::to_owned)
        .collect();
    let group = ChunkGroupConfig {
        placement_id: "native-group".into(),
        failure_domain: "local".into(),
        identity: Some(ChunkGroupIdentity {
            version: 3,
            group_id: 11,
            node_id: 1,
            voters: vec![1, 2, 3],
            capacity_bytes: 1 << 30,
            max_pages: 1024,
            peer_endpoints: Vec::new(),
            bootstrap_id: 0,
            bootstrap_voters: Vec::new(),
        }),
        api_endpoints: endpoints,
        member_ids: vec!["native-1".into(), "native-2".into(), "native-3".into()],
    };
    let mut server = MetaNodeServer::bind(
        &address,
        MetaNodeOptions {
            max_payload_bytes: 2 * 1024 * 1024,
            ..Default::default()
        },
    )
    .unwrap();
    server
        .seed_chunk_groups(ChunkGroupCatalog::new(vec![group]).unwrap())
        .unwrap();
    {
        // Exercise the guarded protocol without advertising it to ordinary operators.
        let mut guard = server.meta.lock().unwrap();
        guard.service.cluster_features.raised = crate::cluster_features::FEATURE_NATIVE_OBJECT_STATE
            | crate::cluster_features::FEATURE_UNIFIED_LOG_WRITERS;
        guard.service.placement_commit_index = 1;
        guard.service.next_commit_index = 2;
        guard.service.invalidate_snapshot_export_cache();
    }
    let peers = std::env::var("MATRIXOBJECT_NATIVE_PROCESS_PEERS")
        .unwrap()
        .split(',')
        .enumerate()
        .map(|(i, address)| (i as u64 + 1, address.parse().unwrap()))
        .collect();
    let directory = root.join("raft");
    std::fs::create_dir_all(&directory).unwrap();
    server
        .enable_raft(MetaRaftOptions {
            node_id: std::env::var("MATRIXOBJECT_NATIVE_PROCESS_ID")
                .unwrap()
                .parse()
                .unwrap(),
            group_id: 71,
            peers,
            directory,
            request_timeout: Duration::from_secs(3),
            checkpoint_after_bytes: 1024,
            bootstrap_voters: None,
            peer_tls: None,
            wal_zero_fill_bytes: None,
        })
        .unwrap();
    println!("NATIVE_READY {}", server.local_addr().unwrap());
    std::io::stdout().flush().unwrap();
    server.serve().unwrap();
}
struct ServerProcess {
    child: Child,
    address: String,
    root: PathBuf,
    role: String,
    id: usize,
    data: String,
    peers: String,
}
impl ServerProcess {
    fn start(root: &Path, role: &str, id: usize, address: &str, data: &str, peers: &str) -> Self {
        std::fs::create_dir_all(root).unwrap();
        let log = root.join("process.stderr");
        let stderr = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log)
            .unwrap();
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                HELPER,
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("MATRIXOBJECT_NATIVE_PROCESS_ROLE", role)
            .env("MATRIXOBJECT_NATIVE_PROCESS_ROOT", root)
            .env("MATRIXOBJECT_NATIVE_PROCESS_ADDRESS", address)
            .env("MATRIXOBJECT_NATIVE_PROCESS_ID", id.to_string())
            .env("MATRIXOBJECT_NATIVE_PROCESS_DATA", data)
            .env("MATRIXOBJECT_NATIVE_PROCESS_PEERS", peers)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(stderr)
            .spawn()
            .unwrap();
        let output = child.stdout.take().unwrap();
        let mut result = Self {
            child,
            address: String::new(),
            root: root.into(),
            role: role.into(),
            id,
            data: data.into(),
            peers: peers.into(),
        };
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(output).lines() {
                let Ok(line) = line else { break };
                if let Some((_, address)) = line.split_once("NATIVE_READY ") {
                    let _ = tx.send(address.trim().to_owned());
                    return;
                }
            }
        });
        match rx.recv_timeout(Duration::from_secs(15)) {
            Ok(address) => result.address = address,
            Err(error) => panic!(
                "{role} {id} did not start: {error}; {}",
                std::fs::read_to_string(log).unwrap_or_default()
            ),
        };
        result
    }
    fn halt(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
    fn restart(&mut self) {
        self.halt();
        *self = Self::start(
            &self.root,
            &self.role,
            self.id,
            &self.address,
            &self.data,
            &self.peers,
        );
    }
}
impl Drop for ServerProcess {
    fn drop(&mut self) {
        self.halt();
    }
}
pub(crate) struct Cluster {
    root: PathBuf,
    data: Vec<ServerProcess>,
    meta: Vec<ServerProcess>,
    proxies: Vec<crate::cluster::chunk_log_tests::FaultProxy>,
}
impl Cluster {
    pub(crate) fn new() -> Self {
        Self::with_append_gate(None)
    }
    fn with_append_gate(
        gate: Option<std::sync::Arc<(std::sync::Mutex<usize>, std::sync::Condvar)>>,
    ) -> Self {
        let root = std::env::temp_dir().join(format!(
            "native-object-processes-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&root).unwrap();
        let mut result = Self {
            root,
            data: Vec::new(),
            meta: Vec::new(),
            proxies: Vec::new(),
        };
        for i in 1..=3 {
            result.data.push(ServerProcess::start(
                &result.root.join(format!("data-{i}")),
                "data",
                i,
                "127.0.0.1:0",
                "",
                "",
            ));
        }
        let data = if let Some(gate) = gate {
            result.proxies = result
                .data
                .iter()
                .map(|node| {
                    crate::cluster::chunk_log_tests::FaultProxy::with_operation_gate(
                        node.address.clone(),
                        0,
                        false,
                        Some(std::sync::Arc::clone(&gate)),
                        crate::cluster::log_op::OP_APPEND,
                    )
                })
                .collect();
            result
                .proxies
                .iter()
                .map(|proxy| proxy.addr.as_str())
                .collect::<Vec<_>>()
                .join(",")
        } else {
            result
                .data
                .iter()
                .map(|node| node.address.as_str())
                .collect::<Vec<_>>()
                .join(",")
        };
        let reservations: Vec<_> = (0..3)
            .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let peers = reservations
            .iter()
            .map(|p| p.local_addr().unwrap().to_string())
            .collect::<Vec<_>>()
            .join(",");
        drop(reservations);
        for i in 1..=3 {
            result.meta.push(ServerProcess::start(
                &result.root.join(format!("meta-{i}")),
                "meta",
                i,
                "127.0.0.1:0",
                &data,
                &peers,
            ));
        }
        let pids: std::collections::HashSet<_> = result
            .data
            .iter()
            .chain(&result.meta)
            .map(|p| p.child.id())
            .collect();
        assert_eq!(pids.len(), 6);
        result
    }
    pub(crate) fn client(&self) -> NativeObjectClient {
        let options = MetaClientOptions {
            read_timeout: Duration::from_secs(10),
            connect_timeout: Duration::from_secs(2),
            ..Default::default()
        };
        let endpoints: Vec<_> = self.meta.iter().map(|p| p.address.clone()).collect();
        let until = Instant::now() + Duration::from_secs(20);
        loop {
            match RemoteMetaClient::connect_raft_catalog(&endpoints, 71, &options) {
                Ok(meta) => {
                    let mut client = NativeObjectClient::new(
                        meta,
                        ReplicaClientOptions {
                            max_payload_bytes: 64 * 1024,
                            read_timeout: Duration::from_secs(2),
                            connect_timeout: Duration::from_millis(300),
                            ..Default::default()
                        },
                    );
                    if client.metadata_position().is_ok() {
                        return client;
                    }
                }
                Err(_) => {}
            };
            assert!(
                Instant::now() < until,
                "metadata group did not become ready"
            );
            std::thread::sleep(Duration::from_millis(30));
        }
    }
}
impl Drop for Cluster {
    fn drop(&mut self) {
        self.meta.clear();
        self.proxies.clear();
        self.data.clear();
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

#[test]
fn native_object_stages_pins_and_data_only_appends_survive_six_process_restarts() {
    let mut cluster = Cluster::new();
    let mut client = cluster.client();
    let mut stage = client
        .begin(
            "bucket",
            "native-log",
            "writer",
            [9; 32],
            2 * 96 * 1024 + 4,
            None,
            120_000,
        )
        .unwrap();
    let parts = [vec![17; 96 * 1024], vec![31; 96 * 1024], b"init".to_vec()];
    let mut expected = Vec::new();
    for (index, body) in parts.iter().enumerate() {
        let owned = client
            .allocate(&mut stage, "native-group", body.len() as u64)
            .unwrap();
        client
            .write_chunk(&stage, &owned, body, [100 + index as u8; 16])
            .unwrap();
        expected.extend_from_slice(body);
    }
    let recovered = client
        .recover_stage("bucket", "native-log", "writer", [9; 32])
        .unwrap();
    assert_eq!(recovered.revision, stage.revision);
    assert_eq!(recovered.chunks, stage.chunks);
    let published = client.publish(&stage, true).unwrap();
    let incarnation = published
        .manifest
        .initialized_log
        .as_ref()
        .unwrap()
        .object_incarnation
        .as_slice()
        .try_into()
        .unwrap();
    let mut writer = client.writer(&published).unwrap();
    let before = client.metadata_position().unwrap();
    for index in 1..=64 {
        let body = vec![index as u8; 4096];
        let identity = AppendIdentity {
            object_incarnation: incarnation,
            attempt_id: [index as u8; 16],
            request_offset: writer.length(),
            request_length: body.len() as u64,
        };
        assert_eq!(
            writer.append(&body, identity).unwrap(),
            expected.len() as u64 + body.len() as u64
        );
        assert!(writer.pending_identity().is_none());
        expected.extend_from_slice(&body);
    }
    assert_eq!(
        client.metadata_position().unwrap(),
        before,
        "steady appends committed metadata"
    );
    cluster.data[2].halt();
    let body = vec![80; 4096];
    let identity = AppendIdentity {
        object_incarnation: incarnation,
        attempt_id: [80; 16],
        request_offset: writer.length(),
        request_length: body.len() as u64,
    };
    assert!(
        matches!(
            writer.append(&body, identity),
            Err(NativeObjectError::Unknown(_))
        ),
        "missing third replica did not retain an unknown outcome"
    );
    assert_eq!(writer.pending_identity(), Some(identity));
    cluster.data[2].restart();
    assert_eq!(
        writer.append(&body, identity).unwrap(),
        expected.len() as u64 + body.len() as u64
    );
    expected.extend_from_slice(&body);
    assert_eq!(client.metadata_position().unwrap(), before);
    let last = published.manifest.extents.last().unwrap();
    let binding = published
        .manifest
        .initialized_log
        .as_ref()
        .unwrap()
        .physical_binding(&published.manifest, published.manifest.extents.len() - 1)
        .unwrap();
    let physical = writer.length() - last.object_offset;
    let foreign = vec![81; 4096];
    for (member, node) in cluster.data.iter().enumerate() {
        let mut direct = crate::cluster::RemoteReplicaClient::connect(
            &node.address,
            &ReplicaClientOptions::default(),
        )
        .unwrap();
        direct
            .log_call(
                crate::cluster::LogOpRequest::append_published(
                    binding.volume_id,
                    binding.chunk_id,
                    binding.generation,
                    binding.writer_epoch,
                    physical,
                    foreign.clone(),
                    [200; 16],
                    physical + foreign.len() as u64,
                    binding.initializations[member],
                )
                .for_storage(
                    binding.storage_incarnations[member],
                    binding.initializations[member],
                ),
            )
            .unwrap();
    }
    let own = AppendIdentity {
        object_incarnation: incarnation,
        attempt_id: [81; 16],
        request_offset: writer.length(),
        request_length: foreign.len() as u64,
    };
    assert!(
        matches!(
            writer.append(&foreign, own),
            Err(NativeObjectError::Unknown(_))
        ),
        "equal foreign bytes acknowledged the wrong original attempt"
    );
    assert_eq!(writer.pending_identity(), Some(own));
    expected.extend_from_slice(&foreign);
    let pin = client.pin("bucket", "native-log", 30_000).unwrap();
    assert_eq!(client.length(&pin).unwrap(), expected.len() as u64);
    let bytes = client.read_range(&pin, 0, expected.len()).unwrap();
    assert!(
        bytes == expected,
        "native range differed from original writes"
    );
    client.close_pin(&pin).unwrap();
    assert!(
        client.read_range(&pin, 0, 1).is_err(),
        "released pin still admitted reads"
    );
    drop(writer);
    drop(client);
    for process in &mut cluster.meta {
        process.halt();
    }
    for process in &mut cluster.data {
        process.halt();
    }
    for process in &mut cluster.data {
        process.restart();
    }
    for process in &mut cluster.meta {
        process.restart();
    }
    let mut client = cluster.client();
    let pin = client.pin("bucket", "native-log", 30_000).unwrap();
    assert_eq!(client.length(&pin).unwrap(), expected.len() as u64);
    assert!(
        client.read_range(&pin, 0, expected.len()).unwrap() == expected,
        "restarted native object bytes differed"
    );
    client.close_pin(&pin).unwrap();
    println!("NATIVE_OBJECT_PROCESS_EVIDENCE metadata=3 data=3 restarts=6 steady_appends=64 steady_metadata_commits=0 exact_bytes={} original_pending_retry=passed released_pin=refused",expected.len());
}

#[test]
fn native_stage_prefixes_are_sealed_before_more_chunks_are_written() {
    let mut cluster = Cluster::new();
    let mut client = cluster.client();
    let chunk_bytes = 96 * 1024;
    let count = 65;
    let mut stage = client
        .begin(
            "bucket",
            "bounded-stage",
            "writer",
            [11; 32],
            (count * chunk_bytes + 4) as u64,
            None,
            900_000,
        )
        .unwrap();
    let mut expected = Vec::new();
    for index in 0..count {
        let body = vec![(index + 1) as u8; chunk_bytes];
        let owned = client
            .allocate(&mut stage, "native-group", body.len() as u64)
            .unwrap();
        let attempt = (index as u128 + 1).to_be_bytes();
        client.write_chunk(&stage, &owned, &body, attempt).unwrap();
        if index == 0 {
            for process in &mut cluster.data {
                process.restart();
            }
            client.write_chunk(&stage, &owned, &body, attempt).unwrap();
            assert!(
                matches!(
                    client.write_chunk(&stage, &owned, &body, [200; 16]),
                    Err(NativeObjectError::Data(
                        crate::cluster::ChunkLogError::AttemptMismatch { .. }
                    ))
                ),
                "equal bytes acknowledged a different original stage write"
            );
            let mut altered = body.clone();
            altered[0] ^= 1;
            assert!(
                matches!(
                    client.write_chunk(&stage, &owned, &altered, attempt),
                    Err(NativeObjectError::Data(
                        crate::cluster::ChunkLogError::AttemptMismatch { .. }
                    ))
                ),
                "the original attempt accepted a changed sealed payload"
            );
        }
        expected.extend_from_slice(&body);
    }
    let owned = client.allocate(&mut stage, "native-group", 4).unwrap();
    client
        .write_chunk(&stage, &owned, b"init", [100; 16])
        .unwrap();
    let extent = owned.extent.as_ref().unwrap();
    let binding = owned.initialization.as_ref().unwrap();
    for (member, node) in cluster.data.iter().enumerate() {
        let mut direct = crate::cluster::RemoteReplicaClient::connect(
            &node.address,
            &ReplicaClientOptions::default(),
        )
        .unwrap();
        let reply = direct
            .log_call(
                crate::cluster::LogOpRequest::publication_probe(
                    extent.volume_id,
                    extent.chunk_id,
                    binding.chunk_generation,
                )
                .for_storage(
                    binding.replicas[member]
                        .storage_incarnation
                        .as_slice()
                        .try_into()
                        .unwrap(),
                    binding.replicas[member]
                        .initialization
                        .as_slice()
                        .try_into()
                        .unwrap(),
                ),
            )
            .unwrap();
        assert!(!reply.sealed, "the final appendable tail was sealed early");
        assert_eq!(reply.published_length, 4);
    }
    expected.extend_from_slice(b"init");
    let published = client.publish(&stage, true).unwrap();
    assert!(published.manifest.extents[..count]
        .iter()
        .all(|e| e.flags & crate::chunk_manifest::CHUNK_FLAG_SEALED != 0));
    let incarnation = published
        .manifest
        .initialized_log
        .as_ref()
        .unwrap()
        .object_incarnation
        .as_slice()
        .try_into()
        .unwrap();
    let mut writer = client.writer(&published).unwrap();
    let before = client.metadata_position().unwrap();
    let append = vec![77; 4096];
    let identity = AppendIdentity {
        object_incarnation: incarnation,
        attempt_id: [201; 16],
        request_offset: writer.length(),
        request_length: append.len() as u64,
    };
    assert_eq!(
        writer.append(&append, identity).unwrap(),
        expected.len() as u64 + append.len() as u64
    );
    expected.extend_from_slice(&append);
    assert_eq!(client.metadata_position().unwrap(), before);
    let pin = client.pin("bucket", "bounded-stage", 120_000).unwrap();
    assert_eq!(client.length(&pin).unwrap(), expected.len() as u64);
    assert_eq!(
        client.read_range(&pin, 0, expected.len()).unwrap(),
        expected
    );
    client.close_pin(&pin).unwrap();
    println!("NATIVE_BOUNDED_STAGE_EVIDENCE metadata=3 data=3 prefix_chunks={count} data_restarts=3 original_sealed_retry=passed foreign_attempt=refused altered_payload=refused open_tail=appendable exact_bytes={} steady_metadata_commits=0", expected.len());
}

#[test]
fn native_staged_write_reaches_every_member_before_waiting_for_any_acknowledgment() {
    let gate = std::sync::Arc::new((std::sync::Mutex::new(0usize), std::sync::Condvar::new()));
    let cluster = Cluster::with_append_gate(Some(std::sync::Arc::clone(&gate)));
    let mut client = cluster.client();
    let body = vec![53; 8192];
    let mut stage = client
        .begin(
            "bucket",
            "parallel-stage",
            "writer",
            [12; 32],
            8196,
            None,
            120_000,
        )
        .unwrap();
    let owned = client.allocate(&mut stage, "native-group", 8192).unwrap();
    let before = client.metadata_position().unwrap();
    let result = client.write_chunk(&stage, &owned, &body, [91; 16]);
    assert!(
        result.is_ok(),
        "staged write failed: {result:?}; members reached={}",
        *gate.0.lock().unwrap()
    );
    assert_eq!(*gate.0.lock().unwrap(), 3);
    assert_eq!(client.metadata_position().unwrap(), before);
    client.write_chunk(&stage, &owned, &body, [91; 16]).unwrap();
    assert!(matches!(
        client.write_chunk(&stage, &owned, &body, [92; 16]),
        Err(NativeObjectError::Data(
            crate::cluster::ChunkLogError::AttemptMismatch { .. }
        ))
    ));
    assert_eq!(
        *gate.0.lock().unwrap(),
        3,
        "sealed retries sent another append"
    );
    for proxy in &cluster.proxies {
        assert_eq!(
            proxy
                .operations
                .lock()
                .unwrap()
                .iter()
                .filter(|op| **op == crate::cluster::log_op::OP_APPEND)
                .count(),
            1
        );
    }
    let tail = client.allocate(&mut stage, "native-group", 4).unwrap();
    client
        .write_chunk(&stage, &tail, b"init", [93; 16])
        .unwrap();
    client.publish(&stage, true).unwrap();
    let pin = client.pin("bucket", "parallel-stage", 30_000).unwrap();
    let mut expected = body;
    expected.extend_from_slice(b"init");
    assert_eq!(
        client.read_range(&pin, 0, expected.len()).unwrap(),
        expected
    );
    client.close_pin(&pin).unwrap();
    println!("NATIVE_STAGE_FANOUT_EVIDENCE metadata=3 data=3 required_acks=3 prefix_metadata_commits=0 sealed_retry=passed foreign_attempt=refused exact_bytes=8196");
}

#[test]
fn native_writer_renewal_keeps_original_attempts_across_process_restarts() {
    let mut cluster = Cluster::new();
    let mut client = cluster.client();
    let mut stage = client
        .begin("bucket", "renewed-log", "writer", [13; 32], 4, None, 3_000)
        .unwrap();
    let owned = client.allocate(&mut stage, "native-group", 4).unwrap();
    client
        .write_chunk(&stage, &owned, b"init", [14; 16])
        .unwrap();
    // The locally mutable stage receipt cannot extend the leader's actual grant.
    stage.start.expires_at_ms = u64::MAX;
    let published = client.publish(&stage, true).unwrap();
    let incarnation = published
        .manifest
        .initialized_log
        .as_ref()
        .unwrap()
        .object_incarnation
        .as_slice()
        .try_into()
        .unwrap();
    let mut writer = client.writer(&published).unwrap();
    let mut old_writer = client.writer(&published).unwrap();
    assert!(old_writer.lease_remaining() <= Duration::from_millis(3_000));
    let before = client.metadata_position().unwrap();
    client.renew_writer(&mut writer, 30_000).unwrap();
    assert_eq!(client.metadata_position().unwrap(), before + 1);
    assert!(writer.lease_remaining() > Duration::from_secs(20));
    std::thread::sleep(old_writer.lease_remaining() + Duration::from_millis(10));
    let identity = AppendIdentity {
        object_incarnation: incarnation,
        attempt_id: [15; 16],
        request_offset: 4,
        request_length: 4,
    };
    assert!(matches!(
        old_writer.append(b"late", identity),
        Err(NativeObjectError::Unknown(_))
    ));
    assert!(client.renew_writer(&mut old_writer, 60_000).is_err());
    assert_eq!(
        client.metadata_position().unwrap(),
        before + 1,
        "expired handle performed a renewal"
    );
    cluster.data[2].halt();
    assert!(matches!(
        writer.append(b"next", identity),
        Err(NativeObjectError::Unknown(_))
    ));
    assert_eq!(writer.pending_identity(), Some(identity));
    client.renew_writer(&mut writer, 60_000).unwrap();
    assert_eq!(
        writer.pending_identity(),
        Some(identity),
        "renewal replaced the original append"
    );
    assert_eq!(client.metadata_position().unwrap(), before + 2);
    for node in &mut cluster.meta {
        node.restart();
    }
    let until = Instant::now() + Duration::from_secs(20);
    loop {
        if client.metadata_position().is_ok() {
            break;
        }
        assert!(
            Instant::now() < until,
            "original client did not reconnect after metadata restart"
        );
        std::thread::sleep(Duration::from_millis(30));
    }
    client.renew_writer(&mut writer, 120_000).unwrap();
    assert_eq!(
        client.metadata_position().unwrap(),
        before + 3,
        "renewed lease was not recovered"
    );
    cluster.data[2].restart();
    let append_position = client.metadata_position().unwrap();
    assert_eq!(writer.append(b"next", identity).unwrap(), 8);
    assert!(writer.pending_identity().is_none());
    assert_eq!(
        client.metadata_position().unwrap(),
        append_position,
        "data append committed metadata"
    );
    for node in &mut cluster.data {
        node.restart();
    }
    let pinned = client.pin("bucket", "renewed-log", 30_000).unwrap();
    assert_eq!(client.length(&pinned).unwrap(), 8);
    assert_eq!(client.read_range(&pinned, 0, 8).unwrap(), b"initnext");
    client.close_pin(&pinned).unwrap();
}
