// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#![cfg(all(target_os = "linux", feature = "replica-server"))]
use matrixobjectstore_rs::{
    chunk_namespace_fence::{ImmutablePage, NamespaceFence},
    cluster::{
        state_install::StateInstall, PageRepairStatus, RemoteReplicaClient, RemoteReplicaSet,
        ReplicaClientOptions, ReplicaNodeOptions, ReplicaNodeServer, ReplicaNodeShutdown,
    },
    ChunkId, MatrixReplicaService, PageId,
};
use std::{
    path::{Path, PathBuf},
    thread,
};

struct Node {
    endpoint: String,
    stop: ReplicaNodeShutdown,
    task: Option<thread::JoinHandle<std::io::Result<()>>>,
}
impl Drop for Node {
    fn drop(&mut self) {
        self.stop.stop();
        if let Some(task) = self.task.take() {
            let _ = task.join();
        }
    }
}
fn start(path: &Path) -> Node {
    let mut server =
        ReplicaNodeServer::bind_with_journal("127.0.0.1:0", ReplicaNodeOptions::default(), path)
            .unwrap();
    server.enable_journal_checkpoints(1024).unwrap();
    let endpoint = server.local_addr().unwrap().to_string();
    let stop = server.shutdown_handle();
    Node {
        endpoint,
        stop,
        task: Some(thread::spawn(move || server.serve())),
    }
}
fn client(node: &Node) -> RemoteReplicaClient {
    RemoteReplicaClient::connect(&node.endpoint, &ReplicaClientOptions::default()).unwrap()
}

#[test]
fn star_namespace_fences_block_stale_writes_and_installation_across_checkpoint_restart_and_repair()
{
    let root = std::env::temp_dir().join(format!(
        "namespace-fence-star-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&root).unwrap();
    struct Cleanup(PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Cleanup(root.clone());
    let paths: Vec<_> = (0..3)
        .map(|index| root.join(format!("node-{index}.journal")))
        .collect();
    let mut nodes: Vec<_> = paths.iter().map(|path| start(path)).collect();
    let page = PageId {
        chunk_id: ChunkId::new(1, 70).unwrap(),
        page_index: 0,
    };
    let late = PageId {
        page_index: 1,
        ..page.clone()
    };
    for node in &nodes[..2] {
        client(node)
            .write_chunk(page.clone(), 0, b"immutable")
            .unwrap();
    }
    let state = client(&nodes[0]).read_chunk_state(&page).unwrap();
    let fence = NamespaceFence {
        version: 1,
        volume_id: 1,
        chunk_id: 70,
        generation: 1,
        pages: vec![ImmutablePage::from_state(0, &state).unwrap()],
    };
    // Stage bytes before closure; commit must recheck the now-closed namespace.
    let mut pending = client(&nodes[2]);
    let mut staged_source = MatrixReplicaService::new();
    staged_source
        .write_chunk(late.clone(), 0, b"immutable")
        .unwrap();
    let head = staged_source.head_chunk(&late).unwrap();
    pending
        .stage_chunk_state(StateInstall::begin(
            "pending".into(),
            &late,
            None,
            &head,
            state.data.len() as u64,
        ))
        .unwrap();
    let mut append = StateInstall::command(2, "pending".into());
    append.data = state.data.as_ref().clone();
    pending.stage_chunk_state(append).unwrap();
    for node in &nodes {
        assert_eq!(client(node).fence_namespace(fence.clone()).unwrap(), fence);
    }
    assert!(pending
        .stage_chunk_state(StateInstall::command(3, "pending".into()))
        .is_err());
    assert!(pending.read_chunk_state(&late).is_err());
    drop(pending);
    for node in &nodes {
        let mut replica = client(node);
        assert!(replica.write_chunk(page.clone(), 0, b"stale").is_err());
        assert!(replica
            .write_chunk_if_sequence(late.clone(), 0, b"late", 0)
            .is_err());
        assert!(replica
            .write_chunk_if_sequence_with_digest(late.clone(), 0, b"late", 0)
            .is_err());
        assert!(replica.discard_chunk(&page, 0, 1).is_err());
        assert!(replica.discard_chunk_if_sequence(&page, 0, 1, 1).is_err());
    }
    let endpoints = nodes
        .iter()
        .map(|node| node.endpoint.clone())
        .collect::<Vec<_>>();
    let mut set =
        RemoteReplicaSet::connect(&endpoints, 2, 2, &ReplicaClientOptions::default()).unwrap();
    let repaired = set.repair_inventory_batch(None, 4).unwrap();
    assert_eq!(repaired.pages.len(), 1);
    assert_eq!(
        repaired.pages[0]
            .result
            .as_ref()
            .unwrap()
            .members
            .iter()
            .filter(|status| matches!(status, PageRepairStatus::Installed))
            .count(),
        1
    );
    drop(set);
    // Force standalone journal replacement after the control decision.
    let other = PageId {
        chunk_id: ChunkId::new(1, 71).unwrap(),
        page_index: 0,
    };
    for node in &nodes {
        client(node)
            .write_chunk(other.clone(), 0, &vec![7; 4096])
            .unwrap();
    }
    nodes.clear();
    nodes = paths.iter().map(|path| start(path)).collect();
    for node in &nodes {
        let mut replica = client(node);
        assert_eq!(replica.read_namespace_fence(&page.chunk_id).unwrap(), fence);
        assert_eq!(replica.read_chunk(&page, 0, 9).unwrap().data, b"immutable");
        assert!(replica.write_chunk(late.clone(), 0, b"late").is_err());
        let retired = NamespaceFence {
            generation: 2,
            pages: Vec::new(),
            ..fence.clone()
        };
        replica.fence_namespace(retired.clone()).unwrap();
        let expected = replica.head_chunk(&page).unwrap().state_checksum;
        assert!(replica
            .install_chunk_state(&page, &state, Some(&expected), "retired")
            .is_err());
        assert!(replica
            .fence_namespace(NamespaceFence {
                generation: 3,
                ..fence.clone()
            })
            .is_err());
        assert_eq!(
            replica.read_namespace_fence(&page.chunk_id).unwrap(),
            retired
        );
    }
    nodes.clear();
    nodes = paths.iter().map(|path| start(path)).collect();
    for node in &nodes {
        let mut replica = client(node);
        assert!(replica
            .read_namespace_fence(&page.chunk_id)
            .unwrap()
            .pages
            .is_empty());
        assert!(replica.write_chunk(late.clone(), 0, b"resurrect").is_err());
    }
}
