// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#![cfg(all(target_os = "linux", feature = "replica-server"))]

mod node_regime;
use matrixobjectstore_rs::cluster::{
    RemoteReplicaClient, ReplicaClientOptions, ReplicaNodeOptions, ReplicaNodeServer,
};
use matrixobjectstore_rs::{ChunkId, PageId};
use node_regime::processes;
use std::path::{Path, PathBuf};

struct JournalDir(PathBuf);
impl JournalDir {
    fn new() -> Self {
        let path = std::env::temp_dir().join(node_regime::run_token("replica-restart"));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn file(&self) -> PathBuf {
        self.0.join("replica.log")
    }
}
impl Drop for JournalDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn page(index: u64) -> PageId {
    PageId {
        chunk_id: ChunkId::new(3, 7).unwrap(),
        page_index: index,
    }
}
fn start(path: &Path) -> processes::NodeProcess {
    let node = processes::start(
        env!("CARGO_BIN_EXE_chunkserver"),
        &[
            ("MATRIXOBJECT_REPLICA_ADDR", "127.0.0.1:0"),
            ("MATRIXOBJECT_REPLICA_JOURNAL", path.to_str().unwrap()),
        ],
    );
    assert_ne!(node.pid, std::process::id());
    assert!(node.startup_line.contains("storage=journal"));
    node
}
fn client(node: &processes::NodeProcess) -> RemoteReplicaClient {
    RemoteReplicaClient::connect(&node.endpoint, &ReplicaClientOptions::default()).unwrap()
}

#[test]
fn acknowledged_writes_and_discards_survive_process_kills() {
    let dir = JournalDir::new();
    let node = start(&dir.file());
    let mut c = client(&node);
    c.write_chunk(page(1), 0, b"abcdefghijklmnop").unwrap();
    c.write_chunk(page(1), 12, b"WXYZ").unwrap();
    c.write_chunk(page(2), 0, b"another page").unwrap();
    c.discard_chunk(&page(1), 4, 4).unwrap();
    let head = c.head_chunk(&page(1)).unwrap();
    let bytes_before_refusal = std::fs::metadata(dir.file()).unwrap().len();
    assert!(c.discard_chunk(&page(1), 100, 1).is_err());
    assert_eq!(
        std::fs::metadata(dir.file()).unwrap().len(),
        bytes_before_refusal
    );
    drop(node); // NodeProcess kills and reaps this child, without graceful shutdown.
    drop(c);

    let node = start(&dir.file());
    let mut c = client(&node);
    assert_eq!(c.head_chunk(&page(1)).unwrap(), head);
    assert_eq!(c.read_chunk(&page(1), 0, 4).unwrap().data, b"abcd");
    assert_eq!(c.read_chunk(&page(1), 12, 4).unwrap().data, b"WXYZ");
    assert!(c.read_chunk(&page(1), 4, 4).is_err());
    assert_eq!(c.read_chunk(&page(2), 0, 12).unwrap().data, b"another page");
    assert!(c.head_chunk(&page(3)).is_err());
    c.write_chunk(page(1), 4, b"rest").unwrap();
    let final_head = c.head_chunk(&page(1)).unwrap();
    drop(node);
    drop(c);

    let node = start(&dir.file());
    let mut c = client(&node);
    assert_eq!(c.head_chunk(&page(1)).unwrap(), final_head);
    assert_eq!(
        c.read_chunk(&page(1), 0, 16).unwrap().data,
        b"abcdrestijklWXYZ"
    );
}

#[test]
fn a_live_journal_cannot_be_opened_by_a_second_server() {
    let dir = JournalDir::new();
    let node = start(&dir.file());
    let mut c = client(&node);
    c.write_chunk(page(1), 0, b"held").unwrap();
    let before = std::fs::read(dir.file()).unwrap();
    assert!(ReplicaNodeServer::bind_with_journal(
        "127.0.0.1:0",
        ReplicaNodeOptions::default(),
        &dir.file()
    )
    .is_err());
    assert_eq!(std::fs::read(dir.file()).unwrap(), before);
    assert_eq!(c.read_chunk(&page(1), 0, 4).unwrap().data, b"held");
    drop(node);
    // A killed process releases its operating-system lock without a stale lock file.
    let restarted = start(&dir.file());
    assert_eq!(
        client(&restarted).read_chunk(&page(1), 0, 4).unwrap().data,
        b"held"
    );
}

#[test]
fn a_damaged_committed_record_prevents_startup() {
    let dir = JournalDir::new();
    let node = start(&dir.file());
    client(&node)
        .write_chunk(page(1), 0, b"acknowledged")
        .unwrap();
    drop(node);
    let mut bytes = std::fs::read(dir.file()).unwrap();
    *bytes.last_mut().unwrap() ^= 1;
    std::fs::write(dir.file(), &bytes).unwrap();
    assert!(ReplicaNodeServer::bind_with_journal(
        "127.0.0.1:0",
        ReplicaNodeOptions::default(),
        &dir.file()
    )
    .is_err());
    assert_eq!(std::fs::read(dir.file()).unwrap(), bytes);
}

#[test]
fn a_three_node_quorum_recovers_after_every_process_is_killed() {
    use matrixobjectstore_rs::cluster::RemoteReplicaSet;
    let dir = JournalDir::new();
    let paths: Vec<_> = (0..3)
        .map(|index| dir.0.join(format!("node-{index}.log")))
        .collect();
    let nodes: Vec<_> = paths.iter().map(|path| start(path)).collect();
    let pids: std::collections::BTreeSet<_> = nodes.iter().map(|node| node.pid).collect();
    assert_eq!(pids.len(), 3);
    let endpoints: Vec<_> = nodes.iter().map(|node| node.endpoint.clone()).collect();
    let mut set =
        RemoteReplicaSet::connect(&endpoints, 2, 2, &ReplicaClientOptions::default()).unwrap();
    for index in 0..24 {
        let data = vec![index as u8; 1024];
        assert_eq!(set.write_chunk(&page(index), 0, &data).unwrap(), 3);
    }
    drop(nodes);
    drop(set);
    let mut nodes: Vec<_> = paths.iter().map(|path| start(path)).collect();
    let endpoints: Vec<_> = nodes.iter().map(|node| node.endpoint.clone()).collect();
    let mut set =
        RemoteReplicaSet::connect(&endpoints, 2, 2, &ReplicaClientOptions::default()).unwrap();
    assert_eq!(set.ping_all(), vec![Some(24); 3]);
    // Recovery must also preserve enough independent copies to survive another loss.
    drop(nodes.pop().unwrap());
    for index in 0..24 {
        assert_eq!(
            set.read_chunk(&page(index), 0, 1024).unwrap().data,
            vec![index as u8; 1024]
        );
    }
    assert!(set.read_chunk(&page(24), 0, 1024).is_err());
}
