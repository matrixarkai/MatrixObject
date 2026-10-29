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
    let bootstrap = std::fs::read(dir.snapshot()).unwrap();
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
    assert_eq!(saved, bootstrap);
    assert!(std::fs::read(dir.0.join("metadata.outcomes")).unwrap().starts_with(b"MOMJ"));
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
fn failed_outcome_append_recovers_last_durable_state() {
    let dir = StateDir::new();
    let node = start(&dir.0, "3");
    let mut c = client(&node);
    let roster = c.list_placement_nodes().unwrap();
    let saved = std::fs::read(dir.snapshot()).unwrap();
    // Limit only this disposable child so its next durable append cannot complete.
    assert!(std::process::Command::new("prlimit")
        .args(["--pid", &node.pid.to_string(), "--fsize=1:1"])
        .status().unwrap().success());
    assert!(c
        .heartbeat_placement_node("data-0", 10, 1 << 30, true, true, 50)
        .is_err());
    assert!(c.list_placement_nodes().is_err());
    assert!(c.probe().into_result().is_err());
    assert_eq!(std::fs::read(dir.snapshot()).unwrap(), saved);
    drop(node);
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

#[test]
fn missing_outcome_journal_is_not_treated_as_empty_history() {
    let dir=StateDir::new();
    let node=start(&dir.0,"3");
    drop(node);
    // A snapshot-only installation upgrades once, then requires its outcome journal.
    std::fs::write(dir.0.join("metadata.lock"), b"1").unwrap();
    std::fs::remove_file(dir.0.join("metadata.outcomes")).unwrap();
    let node=start(&dir.0,"3");
    assert_eq!(std::fs::read(dir.0.join("metadata.lock")).unwrap(), b"2");
    client(&node).heartbeat_placement_node("data-0",10,1<<30,false,true,20).unwrap();
    drop(node);
    std::fs::remove_file(dir.0.join("metadata.outcomes")).unwrap();
    std::fs::write(dir.0.join("metadata.lock"), b"3").unwrap();
    assert!(MetaNodeServer::bind_with_snapshot("127.0.0.1:0",MetaNodeOptions::default(),&dir.0).is_err());
    std::fs::write(dir.0.join("metadata.lock"), b"2").unwrap();
    assert!(MetaNodeServer::bind_with_snapshot("127.0.0.1:0",MetaNodeOptions::default(),&dir.0).is_err());
}

#[test]
fn placement_journal_append_size_does_not_grow_with_catalog() {
    let dir=StateDir::new();
    let node=start(&dir.0,"3");
    let bootstrap=std::fs::read(dir.snapshot()).unwrap();
    let mut c=client(&node);
    let journal=dir.0.join("metadata.outcomes");
    let mut largest=0;
    for index in 0..256 {
        let key=format!("item-{index:04}");
        let plan=c.plan_object_placement("bucket",&key,64,2).unwrap();
        let before=std::fs::metadata(&journal).unwrap().len();
        c.commit_object_placement(CommittedPlacement {
            bucket:plan.bucket,key:plan.key,length:plan.length,node_ids:plan.node_ids,version:1,update_time_ms:10,
        },&plan.plan_checksum).unwrap();
        let appended=std::fs::metadata(&journal).unwrap().len()-before;
        largest=largest.max(appended);
        assert!(appended<1024,"append {index} wrote {appended} bytes");
    }
    assert!(largest>0);
    assert_eq!(std::fs::read(dir.snapshot()).unwrap(),bootstrap);
    let committed=c.object_placement("bucket","item-0255").unwrap();
    let status=c.probe().into_result().unwrap();
    drop(c); drop(node);
    let node=start(&dir.0,"0");
    let mut c=client(&node);
    assert_eq!(c.object_placement("bucket","item-0255").unwrap(),committed);
    assert_eq!(c.probe().into_result().unwrap(),status);
}

#[test]
fn explicit_roster_and_binary_heartbeats_survive_changed_bootstrap() {
    let dir = StateDir::new();
    let start = |roster: &str| {
        processes::start(
            env!("CARGO_BIN_EXE_metaserver"),
            &[
                ("MATRIXOBJECT_METASERVER_ADDR", "127.0.0.1:0"),
                ("MATRIXOBJECT_METASERVER_STATE_DIR", dir.0.to_str().unwrap()),
                ("MATRIXOBJECT_METASERVER_DATA_ROSTER", roster),
                ("MATRIXOBJECT_METASERVER_SEED_CAPACITY_BYTES", "1024"),
            ],
        )
    };
    let node = start("node-b@127.0.0.1:18502@rack-b,node-a@127.0.0.1:18501@rack-a");
    let mut c = client(&node);
    assert!(c
        .list_placement_nodes()
        .unwrap()
        .iter()
        .all(|node| !node.online));
    c.heartbeat_placement_node("node-a", 11, 1024, false, true, 10)
        .unwrap();
    let saved = c.list_placement_nodes().unwrap();
    drop(c);
    drop(node);
    assert_eq!(
        &std::fs::read(dir.0.join("metadata.outcomes")).unwrap()[..4],
        b"MOMJ"
    );
    let node = start("different-node@127.0.0.1:19999@new-rack");
    let mut c = client(&node);
    assert_eq!(c.list_placement_nodes().unwrap(), saved);
    assert_eq!(
        c.plan_object_placement("bucket", "object", 1, 1)
            .unwrap()
            .node_ids,
        vec!["node-a"]
    );
}


#[test]
fn expiry_journal_only_carries_changed_nodes_and_skips_empty_passes() {
    let dir = StateDir::new();
    let server =
        MetaNodeServer::bind_with_snapshot("127.0.0.1:0", MetaNodeOptions::default(), &dir.0)
            .unwrap();
    let roster = (0..1024)
        .map(|i| format!("node-{i}@127.0.0.1:{}@rack", 20000 + i))
        .collect::<Vec<_>>()
        .join(",");
    server
        .seed_data_roster(
            &matrixobjectstore_rs::cluster::DataNodeSeed::parse_roster(&roster).unwrap(),
            1024,
        )
        .unwrap();
    let endpoint = server.local_addr().unwrap().to_string();
    let stop = server.shutdown_handle();
    let worker = std::thread::spawn(move || server.serve().unwrap());
    let mut c = RemoteMetaClient::connect(&endpoint, &MetaClientOptions::default()).unwrap();
    c.heartbeat_chunk_node("node-0", "127.0.0.1:20000", 1, 1024, true, 100)
        .unwrap();
    let journal = dir.0.join("metadata.outcomes");
    let before = std::fs::metadata(&journal).unwrap().len();
    assert_eq!(c.expire_placement_node_heartbeats(1000, 1200).unwrap(), 1);
    let after = std::fs::metadata(&journal).unwrap().len();
    assert!(
        after > before && after - before < 1024,
        "one expiry copied an entire roster: {} bytes",
        after - before
    );
    assert_eq!(c.expire_placement_node_heartbeats(1000, 1300).unwrap(), 0);
    assert_eq!(std::fs::metadata(&journal).unwrap().len(), after);
    drop(c);
    stop.stop();
    worker.join().unwrap();
    let node = start(&dir.0, "0");
    let mut c = client(&node);
    let nodes = c.list_placement_nodes().unwrap();
    assert_eq!(nodes.len(), 1024);
    assert!(nodes.iter().all(|n| !n.online));
    assert_eq!(
        nodes
            .iter()
            .find(|n| n.node_id == "node-0")
            .unwrap()
            .used_bytes,
        1
    );
}

#[test]
fn conditional_placement_commits_persist_and_refuse_stale_records() {
    let dir = StateDir::new();
    let node = start(&dir.0, "3");
    let mut c = client(&node);
    let plan = c
        .plan_object_placement("conditional", "key", 10, 1)
        .unwrap();
    let first = CommittedPlacement {
        bucket: plan.bucket,
        key: plan.key,
        length: 10,
        node_ids: plan.node_ids,
        version: 1,
        update_time_ms: 1,
    };
    c.commit_object_placement_if_current(first.clone(), None, &plan.plan_checksum)
        .unwrap();
    let bytes = std::fs::read(dir.0.join("metadata.outcomes")).unwrap();
    let mut next = first.clone();
    next.version = 2;
    next.update_time_ms = 2;
    assert!(c
        .commit_object_placement_if_current(next.clone(), None, &plan.plan_checksum)
        .is_err());
    assert_eq!(
        std::fs::read(dir.0.join("metadata.outcomes")).unwrap(),
        bytes
    );
    c.commit_object_placement_if_current(next.clone(), Some(first.clone()), &plan.plan_checksum)
        .unwrap();
    drop(c);
    drop(node);
    let node = start(&dir.0, "0");
    let mut c = client(&node);
    assert_eq!(c.object_placement("conditional", "key").unwrap(), next);
    let mut last = next.clone();
    last.version = 3;
    last.update_time_ms = 3;
    assert!(c
        .commit_object_placement_if_current(last.clone(), Some(first), &plan.plan_checksum)
        .is_err());
    c.commit_object_placement_if_current(last.clone(), Some(next), &plan.plan_checksum)
        .unwrap();
    assert_eq!(c.object_placement("conditional", "key").unwrap(), last);
    assert_eq!(
        c.list_placement_nodes()
            .unwrap()
            .iter()
            .map(|n| n.committed_bytes)
            .sum::<u64>(),
        10
    );
}

#[test]
fn binary_manifest_publication_survives_local_process_restart() {
    use matrixobjectstore_rs::object_manifest::{ManifestOutcome, ObjectManifest};
    let dir = StateDir::new();
    let node = start(&dir.0, "3");
    let mut c = client(&node);
    let first = ObjectManifest {
        format_version: 1,
        bucket: "bucket".into(),
        key: "empty".into(),
        generation: 1,
        length: 0,
        extents: vec![],
        deleted: false,
    };
    assert_eq!(
        c.publish_manifest(ManifestOutcome::decide(None, first.clone()).unwrap(), 1)
            .unwrap(),
        first
    );
    assert_eq!(c.read_manifest("bucket", "empty").unwrap(), first);
    let mut deleted = first.clone();
    deleted.generation = 2;
    deleted.deleted = true;
    c.publish_manifest(
        ManifestOutcome::decide(Some(first.clone()), deleted.clone()).unwrap(),
        2,
    )
    .unwrap();
    drop(c);
    drop(node);
    let node = start(&dir.0, "5");
    let mut c = client(&node);
    assert_eq!(c.read_manifest("bucket", "empty").unwrap(), deleted);
    assert!(c
        .publish_manifest(ManifestOutcome::decide(None, first).unwrap(), 3)
        .is_err());
    let mut recreated = deleted.clone();
    recreated.generation = 3;
    recreated.deleted = false;
    c.publish_manifest(
        ManifestOutcome::decide(Some(deleted), recreated.clone()).unwrap(),
        3,
    )
    .unwrap();
    assert_eq!(c.read_manifest("bucket", "empty").unwrap(), recreated);
}
