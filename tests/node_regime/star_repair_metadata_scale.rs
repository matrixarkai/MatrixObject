// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use matrixobjectstore_rs::cluster::{
    PageRepairStatus, RemoteReplicaClient, RemoteReplicaSet, ReplicaClientOptions,
};
use matrixobjectstore_rs::{ChunkId, PageId};

fn exercise(pages: u64) {
    let reservations: Vec<_> = (0..5)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let endpoints: Vec<_> = reservations
        .iter()
        .map(|s| s.local_addr().unwrap().to_string())
        .collect();
    let roster = endpoints
        .iter()
        .enumerate()
        .map(|(i, e)| format!("repair-{i}@{e}@rack-{i}"))
        .collect::<Vec<_>>()
        .join(",");
    let mut metadata =
        Group::with_catalog_checkpoints(Some(roster), Some("60000".into()), None, 4096);
    let addresses = metadata
        .api_addresses
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let paths: Vec<_> = (0..5)
        .map(|i| metadata.root.join(format!("star-{i}.log")))
        .collect();
    let start = |i: usize, maintenance: bool| {
        processes::start(
            env!("CARGO_BIN_EXE_chunkserver"),
            &[
                ("MATRIXOBJECT_REPLICA_ADDR", &endpoints[i]),
                ("MATRIXOBJECT_REPLICA_JOURNAL", paths[i].to_str().unwrap()),
                ("MATRIXOBJECT_REPLICA_CAPACITY_BYTES", "8388608"),
                ("MATRIXOBJECT_REPLICA_MAX_PAGES", "1024"),
                ("MATRIXOBJECT_REPLICA_CHECKPOINT_BYTES", "16384"),
                (
                    "MATRIXOBJECT_REPLICA_MAINTENANCE",
                    if maintenance { "true" } else { "false" },
                ),
                ("MATRIXOBJECT_CHUNK_NODE_ID", &format!("repair-{i}")),
                ("MATRIXOBJECT_CHUNK_METADATA_ENDPOINTS", &addresses),
                ("MATRIXOBJECT_CHUNK_HEARTBEAT_MS", "1000"),
            ],
        )
    };
    drop(reservations);
    let mut nodes: Vec<_> = (0..5).map(|i| Some(start(i, false))).collect();
    let options = ReplicaClientOptions::default();
    let page = |i| PageId {
        chunk_id: ChunkId::new(1, 401).unwrap(),
        page_index: i,
    };
    let payload = |i: u64, phase: u8| {
        let mut bytes = vec![phase; 1024];
        bytes[..8].copy_from_slice(&i.to_le_bytes());
        bytes
    };
    let mut set = RemoteReplicaSet::connect(&endpoints, 3, 3, &options).unwrap();
    for i in 0..pages {
        assert_eq!(set.write_chunk(&page(i), 0, &payload(i, 1)).unwrap(), 5);
    }
    for node in &mut nodes[..2] {
        drop(node.take());
    }
    for i in 0..pages {
        assert_eq!(set.write_chunk(&page(i), 0, &payload(i, 2)).unwrap(), 3);
        assert_eq!(set.discard_chunk(&page(i), 32, 16).unwrap(), 3);
    }
    for (i, node) in nodes.iter_mut().enumerate().take(2) {
        *node = Some(start(i, true));
    }
    let wait_health = |catalog: &mut RemoteMetaClient, maintenance: bool| {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if let Ok(roster) = catalog.list_placement_nodes() {
                if roster.len() == 5
                    && roster.iter().all(|n| {
                        n.used_bytes == pages * 1024
                            && n.online
                                == (!maintenance
                                    || !["repair-0", "repair-1"].contains(&n.node_id.as_str()))
                    })
                {
                    return;
                }
            }
            assert!(
                Instant::now() < deadline,
                "star repair health did not converge"
            );
            std::thread::sleep(Duration::from_millis(25));
        }
    };
    let mut catalog = metadata.client(metadata.leader());
    wait_health(&mut catalog, true);
    let mut after = None;
    let mut repaired = 0;
    let mut batches = 0;
    loop {
        let batch = set.repair_inventory_batch(after.as_ref(), 7).unwrap();
        assert!(!batch.blocked);
        assert!(!batch.pages.is_empty());
        assert!(batch.pages.len() <= 7);
        for entry in &batch.pages {
            assert_eq!(entry.page, page(repaired));
            let report = entry.result.as_ref().unwrap();
            assert_eq!(report.source.write_sequence, 3);
            assert_eq!(
                report.members,
                vec![
                    PageRepairStatus::Installed,
                    PageRepairStatus::Installed,
                    PageRepairStatus::Current,
                    PageRepairStatus::Current,
                    PageRepairStatus::Current
                ]
            );
            repaired += 1;
        }
        assert_eq!(batch.resume_after, Some(page(repaired - 1)));
        after = batch.resume_after;
        batches += 1;
        if batches == 1 {
            // The traversal has work remaining when its metadata leader disappears.
            assert!(batch.has_more);
            metadata.kill(metadata.leader());
            catalog = metadata.client(metadata.leader());
        }
        wait_health(&mut catalog, true);
        let placement = catalog
            .plan_object_placement("repair", &format!("batch-{batches}"), 1, 3)
            .unwrap();
        assert_eq!(placement.node_ids.len(), 3);
        assert!(placement
            .node_ids
            .iter()
            .all(|n| n != "repair-0" && n != "repair-1"));
        assert!(catalog
            .plan_object_placement("repair", "excluded", 1, 4)
            .is_err());
        if !batch.has_more {
            break;
        }
    }
    assert_eq!(repaired, pages);
    drop(set);
    drop(catalog);
    for node in &mut nodes {
        drop(node.take());
    }
    for i in 0..3 {
        if metadata.nodes[i].is_some() {
            metadata.kill(i);
        }
    }
    for i in 0..3 {
        metadata.start(i);
    }
    catalog = metadata.client(metadata.leader());
    // No reporter is running: retained exclusion must come from durable metadata.
    wait_health(&mut catalog, true);
    assert!(catalog
        .plan_object_placement("repair", "recovered-exclusion", 1, 4)
        .is_err());
    for (i, node) in nodes.iter_mut().enumerate() {
        *node = Some(start(i, i < 2));
    }
    wait_health(&mut catalog, true);
    for (index, endpoint) in endpoints.iter().enumerate() {
        let mut client = RemoteReplicaClient::connect(endpoint, &options).unwrap();
        for i in 0..pages {
            let state = client.read_chunk_state(&page(i)).unwrap();
            assert_eq!(state.write_sequence, 3);
            assert_eq!(state.data.as_slice(), payload(i, 2));
            assert!(client.read_chunk(&page(i), 32, 1).is_err());
            assert_eq!(
                client.read_chunk(&page(i), 64, 32).unwrap().data,
                payload(i, 2)[64..96]
            );
        }
        if index < 2 {
            assert!(client.write_chunk(page(0), 0, b"held").is_err());
        }
    }
    // Explicit resumption is separate from successful repair and restart.
    for i in 0..2 {
        drop(nodes[i].take());
        nodes[i] = Some(start(i, false));
    }
    wait_health(&mut catalog, false);
    let mut set = RemoteReplicaSet::connect(&endpoints, 3, 3, &options).unwrap();
    for i in 0..pages {
        assert_eq!(set.write_chunk(&page(i), 64, b"resumed").unwrap(), 5);
    }
    assert_eq!(
        catalog
            .plan_object_placement("repair", "all-members", 1, 5)
            .unwrap()
            .node_ids
            .len(),
        5
    );
    println!(
        "STAR_REPAIR pages={pages} batches={batches} replicas=5 metadata=3 verified_copies={}",
        pages * 5
    );
}

#[test]
fn five_star_replicas_repair_batches_with_metadata_failover_and_restart() {
    exercise(32);
}

#[test]
#[ignore = "explicit local distributed scale run"]
fn five_star_replicas_repair_256_pages_with_metadata_failover_and_restart() {
    exercise(256);
}
