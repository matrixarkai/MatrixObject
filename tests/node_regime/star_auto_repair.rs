// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use matrixobjectstore_rs::cluster::{
    ChunkGroupCatalog, ChunkGroupConfig, ChunkGroupIdentity, RemoteReplicaClient, RemoteReplicaSet,
    ReplicaClientOptions,
};
use matrixobjectstore_rs::{ChunkId, PageId};
use std::process::{Child, Command, Stdio};

struct RepairWorker(Child);
impl Drop for RepairWorker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn worker_repairs_later_pages_past_a_blocked_page_and_follows_metadata_failover() {
    exercise(3, 2);
}

#[test]
#[ignore = "local multi-process repair scale run"]
fn worker_repairs_256_pages_across_metadata_failover_and_chunk_restart() {
    exercise(256, 16);
}

fn exercise(pages: u64, batch_pages: usize) {
    let reservations: Vec<_> = (0..3)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let endpoints: Vec<_> = reservations
        .iter()
        .map(|listener| listener.local_addr().unwrap().to_string())
        .collect();
    let catalog = ChunkGroupCatalog::new(vec![ChunkGroupConfig {
        placement_id: "repair-group".into(),
        failure_domain: "rack-a".into(),
        identity: Some(ChunkGroupIdentity {
            version: 3,
            group_id: 1,
            node_id: 1,
            capacity_bytes: 1 << 28,
            max_pages: 4096,
            voters: vec![1, 2, 3],
            peer_endpoints: Vec::new(),
            bootstrap_voters: Vec::new(),
            bootstrap_id: 0,
        }),
        api_endpoints: endpoints.clone(),
        member_ids: (1..=3).map(|index| format!("repair-{index}")).collect(),
    }])
    .unwrap();
    let mut metadata = Group::with_catalog(None, None, Some(catalog.to_bytes().unwrap()));
    let journals: Vec<_> = (0..3)
        .map(|index| metadata.root.join(format!("repair-{index}.log")))
        .collect();
    drop(reservations);
    let start = |index: usize| {
        processes::start(
            env!("CARGO_BIN_EXE_chunkserver"),
            &[
                ("MATRIXOBJECT_REPLICA_ADDR", &endpoints[index]),
                (
                    "MATRIXOBJECT_REPLICA_JOURNAL",
                    journals[index].to_str().unwrap(),
                ),
            ],
        )
    };
    let mut chunks: Vec<_> = (0..3).map(start).collect();
    let page = |index| PageId {
        chunk_id: ChunkId::new(1, 77).unwrap(),
        page_index: index,
    };
    let options = ReplicaClientOptions::default();
    let mut sources: Vec<_> = endpoints[..2]
        .iter()
        .map(|endpoint| RemoteReplicaClient::connect(endpoint, &options).unwrap())
        .collect();
    for index in 0..pages {
        if index == 1 {
            continue;
        }
        for source in &mut sources {
            source
                .write_chunk(page(index), 0, &[index as u8; 32])
                .unwrap();
        }
    }
    sources[0].write_chunk(page(1), 0, &[1; 32]).unwrap();
    drop(sources);

    let metadata_endpoints = metadata
        .api_addresses
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let mut worker = RepairWorker(
        Command::new(env!("CARGO_BIN_EXE_star_repaird"))
            .env("MATRIXOBJECT_REPAIR_METADATA_ENDPOINTS", metadata_endpoints)
            .env("MATRIXOBJECT_REPAIR_GROUP", "repair-group")
            .env("MATRIXOBJECT_REPAIR_BATCH_PAGES", batch_pages.to_string())
            .env("MATRIXOBJECT_REPAIR_INTERVAL_MS", "100")
            .env("MATRIXOBJECT_REPAIR_SWEEP_INTERVAL_MS", "1000")
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let mut wait_for = |index: u64| {
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            assert!(
                worker.0.try_wait().unwrap().is_none(),
                "repair worker stopped"
            );
            if let Ok(mut destination) = RemoteReplicaClient::connect(&endpoints[2], &options) {
                if let Ok(read) = destination.read_chunk(&page(index), 0, 32) {
                    assert_eq!(read.data, vec![index as u8; 32]);
                    break;
                }
            }
            assert!(Instant::now() < deadline, "page {index} was not repaired");
            std::thread::sleep(Duration::from_millis(25));
        }
    };
    wait_for(pages - 1);
    let leader = metadata.leader();
    metadata.kill(leader);
    RemoteReplicaClient::connect(&endpoints[1], &options)
        .unwrap()
        .write_chunk(page(1), 0, &[1; 32])
        .unwrap();
    wait_for(1);
    let mut writers = RemoteReplicaSet::connect(&endpoints, 2, 2, &options).unwrap();
    drop(chunks.pop());
    assert_eq!(
        writers
            .write_chunks_if_sequence(&[(page(pages), 0, &[pages as u8; 32], 0)])
            .unwrap(),
        vec![2]
    );
    chunks.push(start(2));
    wait_for(pages);
    drop(writers);
    drop(worker);
    chunks.clear();
    chunks = (0..3).map(start).collect();
    for chunk in &chunks {
        let mut client = RemoteReplicaClient::connect(&chunk.endpoint, &options).unwrap();
        for index in 0..=pages {
            assert_eq!(
                client.read_chunk(&page(index), 0, 32).unwrap().data,
                vec![index as u8; 32]
            );
        }
    }
    println!(
        "STAR_AUTO_REPAIR pages={pages} verified_copies={}",
        (pages + 1) * 3
    );
}
