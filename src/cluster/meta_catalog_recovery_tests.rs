// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::cluster::{MetaClientOptions, MetaRaftOptions, RemoteMetaClient};
use crate::object_manifest::{ManifestOutcome, ObjectManifest};
use crate::placement_outcome::PlacementOutcome;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::thread::JoinHandle;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

struct Directory(PathBuf);
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
struct Running {
    address: String,
    stop: MetaNodeShutdown,
    thread: Option<JoinHandle<std::io::Result<()>>>,
}
impl Drop for Running {
    fn drop(&mut self) {
        self.stop.stop();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
fn start(
    root: &Path,
    id: u64,
    peers: &BTreeMap<u64, SocketAddr>,
    seed: Option<&MatrixMetaService>,
) -> Running {
    let directory = root.join(id.to_string());
    std::fs::create_dir_all(&directory).unwrap();
    let mut server = MetaNodeServer::bind(
        "127.0.0.1:0",
        MetaNodeOptions {
            worker_threads: 16,
            ..MetaNodeOptions::default()
        },
    )
    .unwrap();
    if let Some(seed) = seed {
        server.meta.lock().unwrap().service = seed.clone();
    }
    server
        .enable_raft(MetaRaftOptions {
            node_id: id,
            group_id: 81,
            peers: peers.clone(),
            directory,
            request_timeout: Duration::from_secs(5),
            checkpoint_after_bytes: 4096,
        })
        .unwrap();
    Running {
        address: server.local_addr().unwrap().to_string(),
        stop: server.shutdown_handle(),
        thread: Some(std::thread::spawn(move || server.serve())),
    }
}
fn client(nodes: &[Option<Running>]) -> RemoteMetaClient {
    let endpoints: Vec<_> = nodes.iter().flatten().map(|n| n.address.clone()).collect();
    let options = MetaClientOptions {
        connect_timeout: Duration::from_millis(200),
        read_timeout: Duration::from_secs(10),
        write_timeout: Duration::from_secs(5),
        ..MetaClientOptions::default()
    };
    let until = Instant::now() + Duration::from_secs(60);
    loop {
        if let Ok(mut client) = RemoteMetaClient::connect_catalog(&endpoints, &options) {
            if client.read_manifest("capacity", "object-099999").is_ok() {
                return client;
            }
        }
        assert!(
            Instant::now() < until,
            "metadata quorum did not become ready"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}
fn publish(nodes: &[Option<Running>], index: usize, generation: u64) {
    let key = format!("object-{index:06}");
    let mut connection = client(nodes);
    let before = connection.read_manifest("capacity", &key).unwrap();
    let mut after = before.clone();
    after.generation = generation;
    after.deleted = generation == 2;
    let outcome = ManifestOutcome::decide(Some(before), after.clone()).unwrap();
    if connection.publish_manifest(outcome, generation).is_err() {
        // An uncertain response is resolved by reading, never by resending.
        let until = Instant::now() + Duration::from_secs(60);
        loop {
            if client(nodes).read_manifest("capacity", &key).unwrap() == after {
                break;
            }
            assert!(Instant::now() < until, "publication was not confirmed");
            std::thread::sleep(Duration::from_millis(100));
        }
    }
    assert_eq!(
        client(nodes).read_manifest("capacity", &key).unwrap(),
        after
    );
}
fn checkpoint_index(path: &Path) -> Option<u64> {
    use prost::Message;
    use std::io::Read;
    #[derive(Clone, PartialEq, Message)]
    struct Batch {
        #[prost(uint32, tag = "1")]
        version: u32,
        #[prost(bytes = "vec", tag = "7")]
        snapshot: Vec<u8>,
    }
    let mut file = std::fs::File::open(path).ok()?;
    let mut header = [0; 24];
    file.read_exact(&mut header).ok()?;
    if &header[..4] != b"MRPW" {
        return None;
    }
    let length = u32::from_be_bytes(header[4..8].try_into().unwrap()) as usize;
    if length > 8 * 1024 * 1024 {
        return None;
    }
    let mut bytes = vec![0; length];
    file.read_exact(&mut bytes).ok()?;
    let batch = Batch::decode(bytes.as_slice()).ok()?;
    if batch.version != 4 {
        return None;
    }
    let snapshot = raft::eraftpb::Snapshot::decode(batch.snapshot.as_slice()).ok()?;
    Some(snapshot.get_metadata().index)
}

#[test]
fn full_catalog_tcp_servers_checkpoint_rejoin_and_restart() {
    let root = Directory(std::env::temp_dir().join(format!(
            "metadata-catalog-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        )));
    let mut seed = MatrixMetaService::new();
    for index in 0..100_000 {
        let value = ObjectManifest {
            format_version: 1,
            bucket: "capacity".into(),
            key: format!("object-{index:06}"),
            generation: 1,
            length: 0,
            extents: vec![],
            deleted: false,
        };
        PlacementOutcome::publish_manifest(&seed, ManifestOutcome::decide(None, value).unwrap(), 0)
            .unwrap()
            .apply(&mut seed)
            .unwrap();
    }
    let reservations: Vec<_> = (0..3)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let peers: BTreeMap<_, _> = reservations
        .iter()
        .enumerate()
        .map(|(i, l)| (i as u64 + 1, l.local_addr().unwrap()))
        .collect();
    let mut nodes: Vec<Option<Running>> = Vec::new();
    for (i, reservation) in reservations.into_iter().enumerate() {
        drop(reservation);
        nodes.push(Some(start(&root.0, i as u64 + 1, &peers, Some(&seed))));
    }
    drop(seed);
    let connection = client(&nodes);
    let leader = nodes
        .iter()
        .position(|n| n.as_ref().unwrap().address == connection.endpoint())
        .unwrap();
    drop(connection);
    let lagging = (leader + 1) % 3;
    drop(nodes[lagging].take());
    for index in 0..64 {
        publish(&nodes, index, 2);
    }
    let until = Instant::now() + Duration::from_secs(60);
    loop {
        if nodes
            .iter()
            .enumerate()
            .filter(|(_, n)| n.is_some())
            .all(|(i, _)| {
                checkpoint_index(&root.0.join((i + 1).to_string()).join("metadata.peer.wal"))
                    .is_some_and(|index| index > 2)
            })
        {
            break;
        }
        assert!(
            Instant::now() < until,
            "live quorum did not compact its checkpoint"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    nodes[lagging] = Some(start(&root.0, lagging as u64 + 1, &peers, None));
    // Removing the other follower makes every successful operation require the recovered voter.
    let other = (0..3).find(|i| *i != leader && *i != lagging).unwrap();
    drop(nodes[other].take());
    publish(&nodes, 0, 3);
    for node in &mut nodes {
        drop(node.take());
    }
    for (i, node) in nodes.iter_mut().enumerate() {
        *node = Some(start(&root.0, i as u64 + 1, &peers, None));
    }
    let mut connection = client(&nodes);
    assert!(matches!(
        connection.probe().into_result().unwrap(),
        MetaReply::Pong {
            placement_count: 100_000,
            ..
        }
    ));
    for index in [0, 1, 31, 63, 50_000, 99_999] {
        let value = connection
            .read_manifest("capacity", &format!("object-{index:06}"))
            .unwrap();
        assert_eq!(
            value.generation,
            if index == 0 {
                3
            } else if index < 64 {
                2
            } else {
                1
            }
        );
        assert_eq!(value.deleted, index > 0 && index < 64);
    }
}
