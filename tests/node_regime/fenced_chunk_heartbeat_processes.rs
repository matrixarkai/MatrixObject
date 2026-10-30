// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use matrixobjectstore_rs::cluster::{
    ChunkMembershipFence, MembershipOperation as Op, MembershipRequest,
    MembershipStatus,
};

fn advance(
    client: &mut RemoteMetaClient,
    identity: &ChunkGroupIdentity,
    fence: &mut ChunkMembershipFence,
    status: MembershipStatus,
) {
    let expected = fence.advance(identity, &status).unwrap();
    *fence = client.refresh_chunk_membership("group-a", &Default::default()).unwrap();
    assert!(fence.applied_floor() >= expected.applied_floor());
    assert_eq!(fence.voters(), expected.voters());
    assert_eq!(fence.outgoing_voters(), expected.outgoing_voters());
    assert_eq!(fence.learners(), expected.learners());
    assert_eq!(client.read_chunk_route("group-a").unwrap().membership_fence().unwrap().unwrap(), *fence);

}
fn member_health(client: &mut RemoteMetaClient, id: &str, used: u64) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let nodes = client.list_placement_nodes().unwrap();
        if nodes
            .iter()
            .any(|n| n.node_id == id && n.online && n.used_bytes == used)
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "member {id} report did not converge: {nodes:?}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn automatic_fenced_reports_follow_chunk_replacement_and_coupled_restart() {
    let mut chunks = Group::configured_voters(65536, 4);
    chunks.membership = true;
    chunks.checkpoint_bytes = 1024;
    for i in 0..3 {
        chunks.start(i);
    }
    let mut metadata = MetadataGroup::new(&chunks);
    let initial =
        ChunkGroupCatalog::from_bytes(&std::fs::read(metadata.root.join("catalog.bin")).unwrap())
            .unwrap();
    let identity = initial.get("group-a").unwrap().identity.clone().unwrap();
    let mut fence = initial
        .membership_health("group-a")
        .unwrap()
        .unwrap()
        .fence()
        .clone();
    let command = |op: Op, id| MembershipRequest {
        version: 1,
        bootstrap_id: identity.bootstrap_id,
        operation: op as i32,
        node_id: id,
    };
    chunks.report = Some((metadata.api.join(","), chunks.api_addresses.clone()));
    for i in 0..3 {
        chunks.kill(i);
    }
    for i in 0..3 {
        chunks.start(i);
    }
    let mut client = metadata.ready();
    health(&mut client, true, 0, 0);
    for i in 0..3 {
        member_health(&mut client, &chunks.member_id(i), 0);
    }
    let first = chunks.leader();
    let mut writer = client.connect_chunk_group("group-a", &Default::default()).unwrap();
    let mut placements = Vec::new();
    for i in 0..16 {
        placements.push(store(&mut client, &mut writer, i));
    }
    health(&mut client, true, 4096, 4096);
    for i in 0..3 {
        member_health(&mut client, &chunks.member_id(i), 4096);
    }
    drop(writer);
    chunks.kill(first);
    let metadata_leader = metadata
        .api
        .iter()
        .position(|a| a == client.endpoint())
        .unwrap();
    drop(client);
    metadata.kill(metadata_leader);
    let mut client = metadata.ready();
    let leader = chunks.leader();
    let mut writer = client.connect_chunk_group("group-a", &Default::default()).unwrap();
    let status = writer.chunk_membership(command(Op::AddLearner, 4)).unwrap();
    advance(&mut client, &identity, &mut fence, status);
    chunks.start(3);
    member_health(&mut client, &chunks.member_id(3), 4096);
    health(&mut client, true, 4096, 4096);
    // One remaining old voter plus a caught-up learner cannot make a majority.
    let other = (0..3).find(|i| *i != first && *i != leader).unwrap();
    chunks.kill(other);
    health(&mut client, false, 4096, 4096);
    chunks.start(other);
    // Wait for the restored quorum to commit a read before issuing membership writes.
    chunks.leader();
    drop(writer);
    let mut writer = client.connect_chunk_group("group-a", &Default::default()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    let status = loop {
        match writer.chunk_membership(command(Op::Promote, 4)) {
            Ok(status) => break status,
            Err(error) if error.to_string().contains("caught-up learner") => {
                assert!(
                    Instant::now() < deadline,
                    "learner did not catch up: {error}"
                );
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(error) => panic!("promotion failed: {error}"),
        }
    };
    assert!(!status.outgoing_voters.is_empty());
    advance(&mut client, &identity, &mut fence, status);
    health(&mut client, true, 4096, 4096);
    for (op, id) in [
        (Op::LeaveJoint, 0),
        (Op::Remove, first as u64 + 1),
        (Op::LeaveJoint, 0),
    ] {
        let status = writer.chunk_membership(command(op, id)).unwrap();
        advance(&mut client, &identity, &mut fence, status);
        health(&mut client, true, 4096, 4096);
    }
    assert_eq!(fence.voters().len(), 3);
    assert!(!fence.voters().contains(&(first as u64 + 1)));
    verify(&mut client, &mut writer, &placements);
    drop(client);
    drop(writer);
    for i in 0..4 {
        if chunks.nodes[i].is_some() {
            chunks.kill(i);
        }
    }
    for i in 0..3 {
        if metadata.nodes[i].is_some() {
            metadata.kill(i);
        }
    }
    for i in 0..3 {
        metadata.start(i);
    }
    for i in 0..4 {
        chunks.start(i);
    }
    let mut client = metadata.ready();
    health(&mut client, true, 4096, 4096);
    let leader = chunks.leader();
    assert_ne!(leader, first);
    let mut writer = client.connect_chunk_group("group-a", &Default::default()).unwrap();
    verify(&mut client, &mut writer, &placements);
    let recovered = client.read_chunk_route("group-a").unwrap();
    assert_eq!(recovered.membership_fence().unwrap().unwrap(), fence);
    assert!(!recovered.active_members().unwrap().iter().any(|(_, id)| *id == first as u64 + 1));
    let status = writer.chunk_membership(command(Op::Read, 0)).unwrap();
    assert_eq!(status.voters, fence.voters());
    // A restarted removed member cannot restore health after two active voters stop.
    let active: Vec<_> = fence.voters().iter().map(|id| *id as usize - 1).collect();
    for i in &active[..2] {
        chunks.kill(*i);
    }
    health(&mut client, false, 4096, 4096);
}
