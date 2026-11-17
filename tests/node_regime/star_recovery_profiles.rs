// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use matrixobjectstore_rs::cluster::{PageRepairStatus, RemoteReplicaSet};

#[test]
fn star_repair_and_append_work_across_local_recovery_profiles() {
    let mut fixture = Fixture::new();
    let target = PageId {
        chunk_id: ChunkId::new(987654, 1).unwrap(),
        page_index: 0,
    };
    let options = ReplicaClientOptions::default();
    let mut set = RemoteReplicaSet::connect(&fixture.chunk_endpoints, 3, 1, &options).unwrap();
    assert_eq!(set.write_chunk(&target, 0, b"prefix").unwrap(), 3);
    drop(set);

    for id in [0, 1] {
        fixture.chunks[id].child.kill().unwrap();
        assert!(
            processes::reap_within(&mut fixture.chunks[id].child, Duration::from_secs(3)).is_some()
        );
    }
    // Restore one healthy member with local redundancy. Replace another member
    // using a fresh fixture-owned journal while retaining its old journal.
    fixture.chunks[0] =
        start_chunk_with_recovery(&fixture.chunk_endpoints[0], &fixture.journals[0], "xor");
    assert!(fixture.chunks[0]
        .startup_line
        .contains("local_recovery=xor"));
    fixture.journals[1] = fixture.directory.0.join("replacement.journal");
    fixture.chunks[1] = start_chunk(&fixture.chunk_endpoints[1], &fixture.journals[1]);
    let mut repair = RemoteReplicaSet::connect(&fixture.chunk_endpoints, 2, 2, &options).unwrap();
    let report = repair.repair_chunk(&target).unwrap();
    assert_eq!(report.members.len(), 3);
    assert!(report.members.iter().all(|status| matches!(
        status,
        PageRepairStatus::Current | PageRepairStatus::Installed
    )));
    drop(repair);

    let mut set = RemoteReplicaSet::connect(&fixture.chunk_endpoints, 3, 1, &options).unwrap();
    assert_eq!(set.write_chunk(&target, 6, b"suffix").unwrap(), 3);
    drop(set);
    for endpoint in &fixture.chunk_endpoints {
        let mut client = RemoteReplicaClient::connect(endpoint, &options).unwrap();
        assert_eq!(
            client.read_chunk(&target, 0, 12).unwrap().data,
            b"prefixsuffix"
        );
    }
    fixture.restart();
    for node in &fixture.chunks {
        assert!(node.startup_line.contains("local_recovery=peers"));
        let mut client = RemoteReplicaClient::connect(&node.endpoint, &options).unwrap();
        assert_eq!(
            client.read_chunk(&target, 0, 12).unwrap().data,
            b"prefixsuffix"
        );
    }
}
