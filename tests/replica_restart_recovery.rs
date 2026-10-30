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

#[test]
fn recovery_enforces_storage_limits_without_changing_the_journal() {
    let dir = JournalDir::new();
    let node = processes::start(env!("CARGO_BIN_EXE_chunkserver"), &[
        ("MATRIXOBJECT_REPLICA_ADDR", "127.0.0.1:0"),
        ("MATRIXOBJECT_REPLICA_JOURNAL", dir.file().to_str().unwrap()),
        ("MATRIXOBJECT_REPLICA_CAPACITY_BYTES", "4096"),
        ("MATRIXOBJECT_REPLICA_MAX_PAGES", "2"),
    ]);
    let mut c = client(&node);
    c.write_chunk(page(1), 4095, b"x").unwrap();
    c.write_chunk(page(2), 0, b"").unwrap();
    let saved = std::fs::read(dir.file()).unwrap();
    assert!(c.write_chunk(page(1), 4096, b"y").is_err());
    assert!(c.write_chunk(page(3), 0, b"").is_err());
    assert_eq!(std::fs::read(dir.file()).unwrap(), saved);
    drop(node);
    drop(c);
    for (capacity_bytes, max_pages) in [(4095, 2), (4096, 1)] {
        let options = ReplicaNodeOptions { capacity_bytes, max_pages, ..Default::default() };
        assert!(ReplicaNodeServer::bind_with_journal("127.0.0.1:0", options, &dir.file()).is_err());
        assert_eq!(std::fs::read(dir.file()).unwrap(), saved);
    }
    let options = ReplicaNodeOptions { capacity_bytes: 4096, max_pages: 2, ..Default::default() };
    let server = ReplicaNodeServer::bind_with_journal("127.0.0.1:0", options, &dir.file()).unwrap();
    let endpoint = server.local_addr().unwrap().to_string();
    let stop = server.shutdown_handle();
    let thread = std::thread::spawn(move || server.serve().unwrap());
    let mut c = RemoteReplicaClient::connect(&endpoint, &ReplicaClientOptions::default()).unwrap();
    assert_eq!(c.read_chunk(&page(1), 4095, 1).unwrap().data, b"x");
    assert_eq!(c.ping().unwrap(), 2);
    stop.stop();
    thread.join().unwrap();
    // Shutdown also releases the journal lock while the client is still alive.
    drop(ReplicaNodeServer::bind_with_journal("127.0.0.1:0", ReplicaNodeOptions::default(), &dir.file()).unwrap());
}

#[test]
fn conditional_chunk_writes_persist_and_refuse_stale_sequences() {
    let dir = JournalDir::new();
    let node = start(&dir.file());
    let mut c = client(&node);
    assert!(c.write_chunk_if_sequence(page(8), 0, b"lost", 1).is_err());
    assert!(c.head_chunk(&page(8)).is_err());
    c.write_chunk_if_sequence(page(8), 0, b"first", 0).unwrap();
    let before = std::fs::read(dir.file()).unwrap();
    assert!(c.write_chunk_if_sequence(page(8), 0, b"wrong", 0).is_err());
    assert_eq!(std::fs::read(dir.file()).unwrap(), before);
    c.write_chunk_if_sequence(page(8), 0, b"later", 1).unwrap();
    drop(c);
    drop(node);
    let node = start(&dir.file());
    let mut c = client(&node);
    assert_eq!(c.read_chunk(&page(8), 0, 5).unwrap().data, b"later");
    assert_eq!(c.head_chunk(&page(8)).unwrap().write_sequence, 2);
    assert!(c.write_chunk_if_sequence(page(8), 0, b"stale", 1).is_err());
    c.write_chunk_if_sequence(page(8), 0, b"final", 2).unwrap();
    assert_eq!(c.read_chunk(&page(8), 0, 5).unwrap().data, b"final");
}

#[test]
fn star_writes_exclude_divergent_page_state_and_require_a_matching_quorum() {
    use matrixobjectstore_rs::cluster::RemoteReplicaSet;
    let dir = JournalDir::new();
    let nodes: Vec<_> = (0..3)
        .map(|i| start(&dir.0.join(format!("fenced-{i}.log"))))
        .collect();
    let endpoints: Vec<_> = nodes.iter().map(|node| node.endpoint.clone()).collect();
    // Equal sequence numbers are insufficient: the first replica contains different bytes.
    for (i, node) in nodes.iter().enumerate() {
        client(node)
            .write_chunk(page(1), 0, if i == 0 { b"wrong" } else { b"right" })
            .unwrap();
    }
    let rejected_before = client(&nodes[0]).head_chunk(&page(1)).unwrap();
    let mut set =
        RemoteReplicaSet::connect(&endpoints, 2, 2, &ReplicaClientOptions::default()).unwrap();
    assert_eq!(set.write_chunk(&page(1), 0, b"R").unwrap(), 2);
    assert_eq!(
        client(&nodes[0]).head_chunk(&page(1)).unwrap(),
        rejected_before
    );
    assert_eq!(
        client(&nodes[0]).read_chunk(&page(1), 0, 5).unwrap().data,
        b"wrong"
    );
    assert_eq!(set.read_chunk(&page(1), 0, 5).unwrap().data, b"Right");
    // A stricter writer must refuse before changing even the matching replicas.
    let before: Vec<_> = nodes
        .iter()
        .map(|node| client(node).head_chunk(&page(1)).unwrap())
        .collect();
    let mut strict =
        RemoteReplicaSet::connect(&endpoints, 3, 2, &ReplicaClientOptions::default()).unwrap();
    assert!(strict.write_chunk(&page(1), 0, b"bad").is_err());
    for (node, expected) in nodes.iter().zip(before) {
        assert_eq!(client(node).head_chunk(&page(1)).unwrap(), expected);
    }
    // A missing-page quorum may create a page without overwriting a minority's existing page.
    client(&nodes[0]).write_chunk(page(2), 0, b"keep").unwrap();
    assert_eq!(set.write_chunk(&page(2), 0, b"new").unwrap(), 2);
    assert_eq!(
        client(&nodes[0]).read_chunk(&page(2), 0, 4).unwrap().data,
        b"keep"
    );
    assert_eq!(set.read_chunk(&page(2), 0, 3).unwrap().data, b"new");
    assert!(strict.discard_chunk(&page(1), 1, 1).is_err());
    assert_eq!(set.discard_chunk(&page(1), 1, 1).unwrap(), 2);
    assert_eq!(
        client(&nodes[0]).head_chunk(&page(1)).unwrap(),
        rejected_before
    );
    assert!(set.read_chunk(&page(1), 1, 1).is_err());
    assert_eq!(
        client(&nodes[0]).read_chunk(&page(1), 0, 5).unwrap().data,
        b"wrong"
    );
}

#[test]
fn conditional_discards_survive_journal_recovery_and_refuse_stale_sequences() {
    let dir = JournalDir::new();
    let node = start(&dir.file());
    let mut c = client(&node);
    c.write_chunk(page(9), 0, b"abcdefgh").unwrap();
    c.discard_chunk_if_sequence(&page(9), 2, 3, 1).unwrap();
    let expected = c.head_chunk(&page(9)).unwrap();
    assert_eq!(expected.write_sequence, 2);
    let before = std::fs::metadata(dir.file()).unwrap().len();
    for sequence in [0, 1, u64::MAX] {
        assert!(c
            .discard_chunk_if_sequence(&page(9), 0, 2, sequence)
            .is_err());
    }
    assert_eq!(std::fs::metadata(dir.file()).unwrap().len(), before);
    assert_eq!(c.head_chunk(&page(9)).unwrap(), expected);
    drop(c);
    drop(node);
    let node = start(&dir.file());
    let mut c = client(&node);
    assert_eq!(c.head_chunk(&page(9)).unwrap(), expected);
    assert!(c.read_chunk(&page(9), 2, 3).is_err());
    assert_eq!(c.read_chunk(&page(9), 5, 3).unwrap().data, b"fgh");
    c.write_chunk_if_sequence(page(9), 2, b"CDE", 2).unwrap();
    assert!(c.discard_chunk_if_sequence(&page(9), 0, 1, 2).is_err());
    assert_eq!(c.read_chunk(&page(9), 0, 8).unwrap().data, b"abCDEfgh");
}

#[test]
fn star_set_reconnects_recovered_members_but_excludes_missed_writes() {
    use matrixobjectstore_rs::cluster::RemoteReplicaSet;
    let dir = JournalDir::new();
    let paths: Vec<_> = (0..3)
        .map(|i| dir.0.join(format!("rejoin-{i}.log")))
        .collect();
    let mut nodes: Vec<_> = paths.iter().map(|path| Some(start(path))).collect();
    let endpoints: Vec<_> = nodes
        .iter()
        .map(|n| n.as_ref().unwrap().endpoint.clone())
        .collect();
    let restart = |index: usize| {
        processes::start(
            env!("CARGO_BIN_EXE_chunkserver"),
            &[
                ("MATRIXOBJECT_REPLICA_ADDR", endpoints[index].as_str()),
                (
                    "MATRIXOBJECT_REPLICA_JOURNAL",
                    paths[index].to_str().unwrap(),
                ),
            ],
        )
    };
    let options = ReplicaClientOptions {
        connect_timeout: std::time::Duration::from_millis(200),
        ..Default::default()
    };
    let mut set = RemoteReplicaSet::connect(&endpoints, 2, 2, &options).unwrap();
    assert_eq!(set.write_chunk(&page(10), 0, b"first").unwrap(), 3);
    drop(nodes[0].take());
    assert_eq!(set.ping_all(), vec![None, Some(1), Some(1)]);
    nodes[0] = Some(restart(0));
    assert_eq!(set.ping_all(), vec![Some(1); 3]);
    assert_eq!(set.endpoints(), endpoints);
    assert_eq!(set.write_chunk(&page(10), 0, b"again").unwrap(), 3);
    // This time the stopped member misses a successful quorum update.
    drop(nodes[0].take());
    assert_eq!(set.ping_all(), vec![None, Some(1), Some(1)]);
    assert_eq!(set.write_chunk(&page(10), 0, b"newer").unwrap(), 2);
    nodes[0] = Some(restart(0));
    assert_eq!(set.ping_all(), vec![Some(1); 3]);
    assert_eq!(set.write_chunk(&page(10), 0, b"final").unwrap(), 2);
    assert_eq!(set.read_chunk(&page(10), 0, 5).unwrap().data, b"final");
    let mut stale = client(nodes[0].as_ref().unwrap());
    assert_eq!(stale.read_chunk(&page(10), 0, 5).unwrap().data, b"again");
    assert_eq!(stale.head_chunk(&page(10)).unwrap().write_sequence, 2);
    assert_eq!(set.discard_chunk(&page(10), 0, 1).unwrap(), 2);
    assert_eq!(stale.read_chunk(&page(10), 0, 5).unwrap().data, b"again");
    use matrixobjectstore_rs::cluster::PageRepairStatus;
    let report = set.repair_chunk(&page(10)).unwrap();
    assert_eq!(
        report.members,
        vec![
            PageRepairStatus::Installed,
            PageRepairStatus::Current,
            PageRepairStatus::Current
        ]
    );
    assert_eq!(stale.head_chunk(&page(10)).unwrap(), report.source);
    assert!(stale.read_chunk(&page(10), 0, 1).is_err());
    assert_eq!(
        stale.read_chunk_state(&page(10)).unwrap().data.as_slice(),
        b"final"
    );
    assert_eq!(set.write_chunk(&page(10), 0, b"fixed").unwrap(), 3);
    let expected = set.head_chunk(&page(10)).unwrap();
    drop(stale);
    drop(set);
    for node in &mut nodes {
        drop(node.take());
    }
    for (i, node) in nodes.iter_mut().enumerate() {
        *node = Some(restart(i));
    }
    let mut set = RemoteReplicaSet::connect(&endpoints, 2, 2, &options).unwrap();
    assert_eq!(
        set.repair_chunk(&page(10)).unwrap().members,
        vec![PageRepairStatus::Current; 3]
    );
    for node in &nodes {
        let mut c = client(node.as_ref().unwrap());
        assert_eq!(c.head_chunk(&page(10)).unwrap(), expected);
        assert_eq!(c.read_chunk(&page(10), 0, 5).unwrap().data, b"fixed");
    }
}


#[test]
fn star_journal_checkpoints_recover_quorum_pages_and_later_mutations() {
    use matrixobjectstore_rs::cluster::RemoteReplicaSet;
    let directories: Vec<_> = (0..3).map(|_| JournalDir::new()).collect();
    let start_compacting = |path: &Path| {
        processes::start(
            env!("CARGO_BIN_EXE_chunkserver"),
            &[
                ("MATRIXOBJECT_REPLICA_ADDR", "127.0.0.1:0"),
                ("MATRIXOBJECT_REPLICA_JOURNAL", path.to_str().unwrap()),
                ("MATRIXOBJECT_REPLICA_CHECKPOINT_BYTES", "4096"),
            ],
        )
    };
    let nodes: Vec<_> = directories
        .iter()
        .map(|d| start_compacting(&d.file()))
        .collect();
    let endpoints: Vec<_> = nodes.iter().map(|n| n.endpoint.clone()).collect();
    let mut set =
        RemoteReplicaSet::connect(&endpoints, 2, 2, &ReplicaClientOptions::default()).unwrap();
    for value in 1..=20 {
        assert_eq!(set.write_chunk(&page(1), 0, &[value; 1024]).unwrap(), 3);
    }
    assert_eq!(set.discard_chunk(&page(1), 100, 10).unwrap(), 3);
    let expected = set.head_chunk(&page(1)).unwrap();
    for directory in &directories {
        let bytes = std::fs::read(directory.file()).unwrap();
        assert!(bytes.starts_with(b"MOSJ"));
        assert!(
            bytes.len() < 10 * 1024,
            "journal did not reclaim overwrite history"
        );
    }
    drop(set);
    drop(nodes);
    let nodes: Vec<_> = directories.iter().map(|d| start(&d.file())).collect();
    let endpoints: Vec<_> = nodes.iter().map(|n| n.endpoint.clone()).collect();
    let mut set =
        RemoteReplicaSet::connect(&endpoints, 2, 2, &ReplicaClientOptions::default()).unwrap();
    assert_eq!(set.head_chunk(&page(1)).unwrap(), expected);
    assert_eq!(
        set.read_chunk(&page(1), 0, 100).unwrap().data,
        vec![20; 100]
    );
    assert!(set.read_chunk(&page(1), 100, 10).is_err());
    assert_eq!(set.write_chunk(&page(1), 100, &[77; 10]).unwrap(), 3);
    let expected = set.head_chunk(&page(1)).unwrap();
    drop(set);
    drop(nodes);
    for directory in &directories {
        let node = start(&directory.file());
        let mut connection = client(&node);
        assert_eq!(connection.head_chunk(&page(1)).unwrap(), expected);
        assert_eq!(
            connection.read_chunk(&page(1), 100, 10).unwrap().data,
            vec![77; 10]
        );
    }
}

#[test]
fn failed_journal_checkpoint_fences_serving_and_recovers_the_durable_outcome() {
    let directory = JournalDir::new();
    let path = directory.file();
    let mut pending = path.as_os_str().to_owned();
    pending.push(".pending");
    let pending = PathBuf::from(pending);
    std::fs::create_dir(&pending).unwrap();
    let node = processes::start(
        env!("CARGO_BIN_EXE_chunkserver"),
        &[
            ("MATRIXOBJECT_REPLICA_ADDR", "127.0.0.1:0"),
            ("MATRIXOBJECT_REPLICA_JOURNAL", path.to_str().unwrap()),
            ("MATRIXOBJECT_REPLICA_CHECKPOINT_BYTES", "1024"),
        ],
    );
    let mut connection = client(&node);
    assert!(connection.write_chunk(page(1), 0, &[8; 1024]).is_err());
    assert!(connection.ping().is_err());
    drop(connection);
    drop(node);
    std::fs::remove_dir(pending).unwrap();
    let node = start(&path);
    let mut connection = client(&node);
    let result = connection.read_chunk(&page(1), 0, 1024).unwrap();
    assert_eq!(result.data, vec![8; 1024]);
    assert_eq!(result.write_sequence, 1);
}

#[test]
#[ignore = "64 MiB process interruption during checkpoint streaming; run explicitly in release mode"]
fn interrupted_journal_checkpoint_preserves_synced_outcomes_and_reclaims_on_retry() {
    use std::time::{Duration, Instant};
    let directory = JournalDir::new();
    let path = directory.file();
    let start_compacting = || {
        processes::start(
            env!("CARGO_BIN_EXE_chunkserver"),
            &[
                ("MATRIXOBJECT_REPLICA_ADDR", "127.0.0.1:0"),
                ("MATRIXOBJECT_REPLICA_JOURNAL", path.to_str().unwrap()),
                ("MATRIXOBJECT_REPLICA_CHECKPOINT_BYTES", "134217728"),
            ],
        )
    };
    let node = start_compacting();
    let mut connection = client(&node);
    let first = vec![1; 1024 * 1024];
    let second = vec![2; 1024 * 1024];
    for index in 0..64 {
        connection.write_chunk(page(index), 0, &first).unwrap();
    }
    for index in 0..63 {
        connection.write_chunk(page(index), 0, &second).unwrap();
    }
    let writer = std::thread::spawn(move || connection.write_chunk(page(63), 0, &second));
    let mut pending = path.as_os_str().to_owned();
    pending.push(".pending");
    let pending = PathBuf::from(pending);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if std::fs::metadata(&pending).is_ok_and(|m| m.len() > 32 && m.len() < 64 * 1024 * 1024) {
            break;
        }
        assert!(Instant::now() < deadline, "no partial checkpoint observed");
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(std::process::Command::new("kill")
        .args(["-STOP", &node.pid.to_string()])
        .status()
        .unwrap()
        .success());
    let history_bytes = std::fs::metadata(&path).unwrap().len();
    let incomplete = std::fs::metadata(&pending).unwrap().len();
    assert!(
        incomplete > 32 && incomplete < 64 * 1024 * 1024,
        "checkpoint finished before interruption"
    );
    drop(node);
    assert!(
        writer.join().unwrap().is_err(),
        "interrupted operation cannot acknowledge"
    );
    // Seeing a candidate proves its triggering outcome was already synced.
    let node = start(&path);
    let mut connection = client(&node);
    for index in 0..64 {
        let value = connection.read_chunk(&page(index), 0, 1024 * 1024).unwrap();
        assert_eq!(value.write_sequence, 2);
        assert_eq!(value.data, vec![2; 1024 * 1024]);
    }
    assert_eq!(std::fs::metadata(&pending).unwrap().len(), incomplete);
    drop(connection);
    drop(node);
    let node = start_compacting();
    let mut connection = client(&node);
    connection.write_chunk(page(0), 0, &[3]).unwrap();
    assert!(!pending.exists());
    let checkpoint_bytes = std::fs::metadata(&path).unwrap().len();
    // The complete image includes redundancy bytes and service metadata.
    assert!(checkpoint_bytes < history_bytes,
        "checkpoint {checkpoint_bytes} did not reclaim journal {history_bytes}");
    drop(connection);
    drop(node);
    let node = start(&path);
    let mut connection = client(&node);
    assert_eq!(connection.read_chunk(&page(0), 0, 1).unwrap().data, vec![3]);
    assert_eq!(connection.head_chunk(&page(0)).unwrap().write_sequence, 3);
    assert_eq!(
        connection
            .read_chunk(&page(63), 0, 1024 * 1024)
            .unwrap()
            .data,
        vec![2; 1024 * 1024]
    );
    println!("killed checkpoint at {incomplete} bytes; recovered 64 pages; journal {history_bytes} -> checkpoint {checkpoint_bytes} bytes");
}


#[test]
fn staged_page_install_is_fenced_atomic_and_durable() {
    use matrixobjectstore_rs::cluster::state_install::StateInstall;
    let dir = JournalDir::new();
    let node = start(&dir.file());
    let mut c = client(&node);
    let mut source = matrixobjectstore_rs::MatrixReplicaService::new();
    source.write_chunk(page(0), 0, &vec![17; 17000]).unwrap();
    source.discard_chunk(&page(0), 3000, 4000).unwrap();
    let state = source.read_chunk_state(&page(0)).unwrap();
    let head = source.head_chunk(&page(0)).unwrap();
    let begin = StateInstall::begin(
        "partial".into(),
        &page(0),
        None,
        &head,
        state.data.len() as u64,
    );
    c.stage_chunk_state(begin.clone()).unwrap();
    assert!(c.head_chunk(&page(0)).is_err());
    assert!(c.stage_chunk_state(begin.clone()).is_err());
    let mut part = StateInstall::command(2, "partial".into());
    part.data = state.data[..100].to_vec();
    c.stage_chunk_state(part.clone()).unwrap();
    assert!(c.stage_chunk_state(part).is_err());
    assert!(c
        .stage_chunk_state(StateInstall::command(4, "other".into()))
        .is_err());
    drop(c);
    drop(node);
    let node = start(&dir.file());
    let mut c = client(&node);
    assert!(c.head_chunk(&page(0)).is_err());
    assert!(c
        .stage_chunk_state(StateInstall::command(3, "partial".into()))
        .is_err());
    c.install_chunk_state(&page(0), &state, None, "complete")
        .unwrap();
    assert_eq!(c.head_chunk(&page(0)).unwrap(), head);
    assert_eq!(c.read_chunk_state(&page(0)).unwrap().data, state.data);
    assert!(c.read_chunk(&page(0), 3000, 1).is_err());
    drop(c);
    drop(node);
    let node = start(&dir.file());
    let mut c = client(&node);
    assert_eq!(c.head_chunk(&page(0)).unwrap(), head);
    let begin = StateInstall::begin(
        "racing".into(),
        &page(0),
        Some(&head.state_checksum),
        &head,
        state.data.len() as u64,
    );
    c.stage_chunk_state(begin).unwrap();
    let mut data = StateInstall::command(2, "racing".into());
    data.data = state.data.as_ref().clone();
    c.stage_chunk_state(data).unwrap();
    c.write_chunk(page(0), 16000, b"new").unwrap();
    assert!(c
        .stage_chunk_state(StateInstall::command(3, "racing".into()))
        .is_err());
    let changed = c.head_chunk(&page(0)).unwrap();
    assert_eq!(changed.write_sequence, 3);
    assert_eq!(c.read_chunk(&page(0), 16000, 3).unwrap().data, b"new");
    // Even with the current destination checksum, an older source cannot replace it.
    assert!(c
        .install_chunk_state(&page(0), &state, Some(&changed.state_checksum), "old")
        .is_err());
    let mut source = matrixobjectstore_rs::MatrixReplicaService::new();
    source.write_chunk(page(1), 0, b"valid").unwrap();
    let h = source.head_chunk(&page(1)).unwrap();
    c.stage_chunk_state(StateInstall::begin("corrupt".into(), &page(1), None, &h, 5))
        .unwrap();
    let mut data = StateInstall::command(2, "corrupt".into());
    data.data = b"wrong".to_vec();
    c.stage_chunk_state(data).unwrap();
    assert!(c
        .stage_chunk_state(StateInstall::command(3, "corrupt".into()))
        .is_err());
    assert!(c.head_chunk(&page(1)).is_err());
    drop(c);
    drop(node);
    let node = start(&dir.file());
    let mut c = client(&node);
    assert_eq!(c.head_chunk(&page(0)).unwrap(), changed);
    assert!(c.head_chunk(&page(1)).is_err());
}



#[test]
fn staged_install_disk_failure_fences_node_and_keeps_old_checkpoint() {
    let dir = JournalDir::new();
    let node = start(&dir.file());
    let mut c = client(&node);
    c.write_chunk(page(0), 0, b"old").unwrap();
    let old = c.head_chunk(&page(0)).unwrap();
    let mut source = matrixobjectstore_rs::MatrixReplicaService::new();
    source.write_chunk(page(0), 0, b"old").unwrap();
    source.write_chunk(page(0), 0, b"new").unwrap();
    let state = source.read_chunk_state(&page(0)).unwrap();
    let pending = dir.0.join("replica.log.pending");
    std::fs::create_dir(&pending).unwrap();
    assert!(c
        .install_chunk_state(&page(0), &state, Some(&old.state_checksum), "disk-failure")
        .is_err());
    assert!(c.head_chunk(&page(0)).is_err());
    assert!(c.write_chunk(page(0), 0, b"unsafe").is_err());
    drop(c);
    drop(node);
    std::fs::remove_dir(pending).unwrap();
    let node = start(&dir.file());
    let mut c = client(&node);
    assert_eq!(c.head_chunk(&page(0)).unwrap(), old);
    assert_eq!(c.read_chunk(&page(0), 0, 3).unwrap().data, b"old");
}

#[test]
#[ignore = "256 MiB staged installation and journal recovery; run explicitly in release mode"]
fn staged_install_recovers_a_full_256_mib_page() {
    let dir = JournalDir::new();
    let start_large = || {
        processes::start(
            env!("CARGO_BIN_EXE_chunkserver"),
            &[
                ("MATRIXOBJECT_REPLICA_ADDR", "127.0.0.1:0"),
                ("MATRIXOBJECT_REPLICA_JOURNAL", dir.file().to_str().unwrap()),
                ("MATRIXOBJECT_REPLICA_CAPACITY_BYTES", "314572800"),
            ],
        )
    };
    let node = start_large();
    let mut c = RemoteReplicaClient::connect(
        &node.endpoint,
        &ReplicaClientOptions {
            read_timeout: std::time::Duration::from_secs(60),
            write_timeout: std::time::Duration::from_secs(60),
            ..Default::default()
        },
    )
    .unwrap();
    let mut source = matrixobjectstore_rs::MatrixReplicaService::new();
    source
        .write_chunk(page(0), 0, &vec![31; 256 * 1024 * 1024])
        .unwrap();
    source.discard_chunk(&page(0), 1024 * 1024 - 3, 9).unwrap();
    let state = source.read_chunk_state(&page(0)).unwrap();
    let expected = source.head_chunk(&page(0)).unwrap();
    c.install_chunk_state(&page(0), &state, None, "large")
        .unwrap();
    assert_eq!(c.head_chunk(&page(0)).unwrap(), expected);
    drop(c);
    drop(node);
    let node = start_large();
    let mut c = client(&node);
    assert_eq!(c.head_chunk(&page(0)).unwrap(), expected);
    let recovered = c.read_chunk_state(&page(0)).unwrap();
    assert_eq!(recovered.data.len(), 256 * 1024 * 1024);
    assert!(recovered.data.iter().all(|b| *b == 31));
    assert_eq!(recovered.discarded_ranges, state.discarded_ranges);
    println!(
        "installed and recovered {} bytes through bounded frames",
        recovered.data.len()
    );
}


#[test]
fn staged_install_rechecks_capacity_and_releases_aborted_buffers() {
    use matrixobjectstore_rs::cluster::state_install::StateInstall;
    let node = processes::start(
        env!("CARGO_BIN_EXE_chunkserver"),
        &[
            ("MATRIXOBJECT_REPLICA_ADDR", "127.0.0.1:0"),
            ("MATRIXOBJECT_REPLICA_CAPACITY_BYTES", "8"),
            ("MATRIXOBJECT_REPLICA_MAX_PAGES", "2"),
        ],
    );
    let mut c = client(&node);
    let mut source = matrixobjectstore_rs::MatrixReplicaService::new();
    source.write_chunk(page(0), 0, b"123456").unwrap();
    let head = source.head_chunk(&page(0)).unwrap();
    c.stage_chunk_state(StateInstall::begin(
        "capacity".into(),
        &page(0),
        None,
        &head,
        6,
    ))
    .unwrap();
    let mut data = StateInstall::command(2, "capacity".into());
    data.data = b"123456".to_vec();
    c.stage_chunk_state(data).unwrap();
    c.write_chunk(page(1), 0, b"live").unwrap();
    assert!(c
        .stage_chunk_state(StateInstall::command(3, "capacity".into()))
        .is_err());
    assert!(c.head_chunk(&page(0)).is_err());
    assert_eq!(c.read_chunk(&page(1), 0, 4).unwrap().data, b"live");
    assert!(c
        .stage_chunk_state(StateInstall::begin(
            "too-big".into(),
            &page(0),
            None,
            &head,
            6
        ))
        .is_err());
    let mut source = matrixobjectstore_rs::MatrixReplicaService::new();
    source.write_chunk(page(0), 0, b"fits").unwrap();
    let head = source.head_chunk(&page(0)).unwrap();
    c.stage_chunk_state(StateInstall::begin(
        "abort".into(),
        &page(0),
        None,
        &head,
        4,
    ))
    .unwrap();
    c.stage_chunk_state(StateInstall::command(4, "abort".into()))
        .unwrap();
    c.install_chunk_state(
        &page(0),
        &source.read_chunk_state(&page(0)).unwrap(),
        None,
        "fits",
    )
    .unwrap();
    assert_eq!(c.read_chunk(&page(0), 0, 4).unwrap().data, b"fits");
    assert!(c
        .stage_chunk_state(StateInstall::begin(
            "page-limit".into(),
            &page(2),
            None,
            &head,
            4
        ))
        .is_err());
}


#[test]
fn star_repair_requires_write_strength_and_preserves_newer_destinations() {
    use matrixobjectstore_rs::cluster::{PageRepairStatus, RemoteReplicaSet};
    let dir = JournalDir::new();
    let paths: Vec<_> = (0..3)
        .map(|i| dir.0.join(format!("repair-{i}.log")))
        .collect();
    let nodes: Vec<_> = paths.iter().map(|path| start(path)).collect();
    let endpoints: Vec<_> = nodes.iter().map(|node| node.endpoint.clone()).collect();
    for (i, node) in nodes.iter().enumerate() {
        client(node)
            .write_chunk(page(0), 0, if i == 0 { b"wrong" } else { b"right" })
            .unwrap();
    }
    let before: Vec<_> = paths.iter().map(|p| std::fs::read(p).unwrap()).collect();
    let mut strict =
        RemoteReplicaSet::connect(&endpoints, 3, 1, &ReplicaClientOptions::default()).unwrap();
    assert!(strict.repair_chunk(&page(0)).is_err());
    for (p, bytes) in paths.iter().zip(before) {
        assert_eq!(std::fs::read(p).unwrap(), bytes);
    }
    let mut set =
        RemoteReplicaSet::connect(&endpoints, 2, 2, &ReplicaClientOptions::default()).unwrap();
    assert_eq!(
        set.repair_chunk(&page(0)).unwrap().members,
        vec![
            PageRepairStatus::Installed,
            PageRepairStatus::Current,
            PageRepairStatus::Current
        ]
    );
    client(&nodes[0]).write_chunk(page(0), 0, b"newer").unwrap();
    let newer = client(&nodes[0]).head_chunk(&page(0)).unwrap();
    let report = set.repair_chunk(&page(0)).unwrap();
    assert!(matches!(report.members[0], PageRepairStatus::Deferred(_)));
    assert_eq!(client(&nodes[0]).head_chunk(&page(0)).unwrap(), newer);
    for node in &nodes[1..] {
        client(node).write_chunk(page(1), 0, b"create").unwrap();
    }
    assert_eq!(
        set.repair_chunk(&page(1)).unwrap().members,
        vec![
            PageRepairStatus::Installed,
            PageRepairStatus::Current,
            PageRepairStatus::Current
        ]
    );
    assert_eq!(
        client(&nodes[0]).read_chunk(&page(1), 0, 6).unwrap().data,
        b"create"
    );
    assert!(set.repair_chunk(&page(99)).is_err());
}