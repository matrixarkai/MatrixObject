// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#![cfg(all(target_os = "linux", feature = "meta-server"))]

mod node_regime;
use matrixobjectstore_rs::cluster::{
    CommittedPlacement, MetaClientOptions, MetaNodeOptions, MetaNodeServer, RemoteMetaClient,
};
use node_regime::processes;
use std::path::{Path, PathBuf};

struct StateDir(PathBuf);
impl StateDir {
    fn new() -> Self {
        let path = std::env::temp_dir().join(node_regime::run_token("meta-restart"));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn snapshot(&self) -> PathBuf {
        self.0.join("metadata.snapshot")
    }
}
impl Drop for StateDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn start(path: &Path, count: &str) -> processes::NodeProcess {
    let node = processes::start(
        env!("CARGO_BIN_EXE_metaserver"),
        &[
            ("MATRIXOBJECT_METASERVER_ADDR", "127.0.0.1:0"),
            ("MATRIXOBJECT_METASERVER_STATE_DIR", path.to_str().unwrap()),
            ("MATRIXOBJECT_METASERVER_SEED_DATA_NODES", count),
        ],
    );
    assert_ne!(node.pid, std::process::id());
    assert!(node.startup_line.contains("storage=snapshot"));
    node
}
fn client(node: &processes::NodeProcess) -> RemoteMetaClient {
    RemoteMetaClient::connect(&node.endpoint, &MetaClientOptions::default()).unwrap()
}

#[test]
fn placements_roster_and_commit_index_survive_process_kills() {
    let dir = StateDir::new();
    let node = start(&dir.0, "3");
    let mut c = client(&node);
    let plan = c
        .plan_object_placement("bucket", "object", 1024, 2)
        .unwrap();
    let placement = CommittedPlacement {
        bucket: plan.bucket,
        key: plan.key,
        length: plan.length,
        node_ids: plan.node_ids,
        version: 1,
        update_time_ms: 10,
    };
    let committed = c
        .commit_object_placement(placement.clone(), &plan.plan_checksum)
        .unwrap();
    c.heartbeat_placement_node("data-0", 10, 1 << 30, true, true, 50)
        .unwrap();
    let roster = c.list_placement_nodes().unwrap();
    let status = c.probe().into_result().unwrap();
    let saved = std::fs::read(dir.snapshot()).unwrap();
    assert!(c.commit_object_placement(placement, "invalid").is_err());
    assert_eq!(std::fs::read(dir.snapshot()).unwrap(), saved);
    drop(node);
    drop(c);
    // A changed bootstrap count must not reset the recovered roster or node health.
    let node = start(&dir.0, "7");
    let mut c = client(&node);
    assert_eq!(c.list_placement_nodes().unwrap(), roster);
    assert_eq!(c.probe().into_result().unwrap(), status);
    assert_eq!(c.object_placement("bucket", "object").unwrap(), committed);
    assert!(c.object_placement("bucket", "missing").is_err());
    assert_eq!(c.expire_placement_node_heartbeats(10, 100).unwrap(), 3);
    drop(node);
    let node = start(&dir.0, "0");
    let mut c = client(&node);
    assert!(c
        .list_placement_nodes()
        .unwrap()
        .iter()
        .all(|node| !node.online));
    assert!(c.plan_object_placement("bucket", "new", 1, 1).is_err());
    assert_eq!(c.object_placement("bucket", "object").unwrap(), committed);
}

#[test]
fn directory_lock_and_corruption_prevent_unsafe_startup() {
    let dir = StateDir::new();
    let node = start(&dir.0, "3");
    assert!(
        MetaNodeServer::bind_with_snapshot("127.0.0.1:0", MetaNodeOptions::default(), &dir.0)
            .is_err()
    );
    drop(node);
    let mut bytes = std::fs::read(dir.snapshot()).unwrap();
    *bytes.last_mut().unwrap() ^= 1;
    std::fs::write(dir.snapshot(), &bytes).unwrap();
    assert!(
        MetaNodeServer::bind_with_snapshot("127.0.0.1:0", MetaNodeOptions::default(), &dir.0)
            .is_err()
    );
    assert_eq!(std::fs::read(dir.snapshot()).unwrap(), bytes);
    std::fs::remove_file(dir.snapshot()).unwrap();
    assert!(
        MetaNodeServer::bind_with_snapshot("127.0.0.1:0", MetaNodeOptions::default(), &dir.0)
            .is_err()
    );
    assert!(!dir.snapshot().exists());
}

#[test]
fn interrupted_snapshot_and_bootstrap_do_not_replace_committed_state() {
    let dir = StateDir::new();
    drop(
        MetaNodeServer::bind_with_snapshot("127.0.0.1:0", MetaNodeOptions::default(), &dir.0)
            .unwrap(),
    );
    assert!(!dir.snapshot().exists());
    let node = start(&dir.0, "3");
    let roster = client(&node).list_placement_nodes().unwrap();
    drop(node);
    std::fs::write(dir.0.join("metadata.pending"), b"partial snapshot").unwrap();
    let node = start(&dir.0, "1");
    assert_eq!(client(&node).list_placement_nodes().unwrap(), roster);
}

#[test]
fn failed_snapshot_write_blocks_requests_and_recovers_old_state() {
    let dir = StateDir::new();
    let node = start(&dir.0, "3");
    let mut c = client(&node);
    let roster = c.list_placement_nodes().unwrap();
    let saved = std::fs::read(dir.snapshot()).unwrap();
    // A directory cannot be opened as the pending snapshot file.
    std::fs::create_dir(dir.0.join("metadata.pending")).unwrap();
    assert!(c
        .heartbeat_placement_node("data-0", 10, 1 << 30, true, true, 50)
        .is_err());
    assert!(c.list_placement_nodes().is_err());
    assert!(c.probe().into_result().is_err());
    assert_eq!(std::fs::read(dir.snapshot()).unwrap(), saved);
    drop(node);
    std::fs::remove_dir(dir.0.join("metadata.pending")).unwrap();
    let node = start(&dir.0, "3");
    assert_eq!(client(&node).list_placement_nodes().unwrap(), roster);
}

#[test]
fn restored_metadata_leases_remain_in_later_snapshots() {
    use matrixobjectstore_rs::{MatrixMetaService, MetaSnapshot};
    let dir = StateDir::new();
    let node = start(&dir.0, "3");
    drop(node);
    let snapshot = MetaSnapshot::from_bytes(&std::fs::read(dir.snapshot()).unwrap()).unwrap();
    let mut service = MatrixMetaService::new();
    service.install_placement_roster(&snapshot).unwrap();
    service.install_snapshot(snapshot).unwrap();
    let lease = service.acquire_lease("namespace", "owner", 1000, 100).unwrap();
    let original = service.export_snapshot();
    std::fs::write(dir.snapshot(), original.to_bytes().unwrap()).unwrap();
    let node = start(&dir.0, "0");
    client(&node).heartbeat_placement_node("data-0", 20, 1 << 30, false, true, 200).unwrap();
    let saved = MetaSnapshot::from_bytes(&std::fs::read(dir.snapshot()).unwrap()).unwrap();
    assert_eq!(saved.leases.get("namespace"), Some(&lease));
    assert_eq!(saved.next_commit_index, original.next_commit_index);
    assert_eq!(saved.placement_nodes.len(), 3);
}
