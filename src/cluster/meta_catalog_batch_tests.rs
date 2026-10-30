// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use crate::cluster::{
    ChunkCatalogCommand, ChunkMembershipFence, ChunkMembershipHealth, MembershipStatus,
};

fn dynamic_fixture() -> (MatrixMetaService, ChunkGroupCatalog) {
    let (mut service, fixed) = fixture(2, 1);
    let mut health = BTreeMap::new();
    let configurations = fixed
        .groups()
        .map(|g| {
            let mut group = g.clone();
            let identity = group.identity.as_mut().unwrap();
            identity.version = 2;
            identity.bootstrap_id = 91;
            identity.bootstrap_voters = vec![1, 2, 3];
            let fence = ChunkMembershipFence::from_status(identity, &status(1)).unwrap();
            health.insert(
                group.placement_id.clone(),
                ChunkMembershipHealth::new(fence),
            );
            for id in group
                .member_ids
                .iter()
                .chain(std::iter::once(&group.placement_id))
            {
                let node = service.nodes.get_mut(id).unwrap();
                node.online = false;
                node.last_heartbeat_ms = 0;
            }
            group
        })
        .collect();
    service.invalidate_snapshot_export_cache();
    (
        service,
        ChunkGroupCatalog::with_membership_health(configurations, health).unwrap(),
    )
}
fn status(id: u64) -> MembershipStatus {
    MembershipStatus {
        version: 1,
        node_id: id,
        term: 2,
        applied_index: 10,
        voters: vec![1, 2, 3],
        outgoing_voters: vec![],
        learners: vec![],
    }
}
fn report(groups: &ChunkGroupCatalog, id: u64) -> MetaRequest {
    let group = groups.get("group-0").unwrap();
    MetaRequest::ChunkCatalog(ChunkCatalogCommand {
        version: 1,
        operation: 2,
        placement_id: "group-0".into(),
        expected_fence: vec![],
        bootstrap_id: 91,
        status: Some(status(id)),
        member_id: group.member_ids[id as usize - 1].clone(),
        expected_endpoint: group.api_endpoints[id as usize - 1].clone(),
        used_bytes: id * 100,
        capacity_bytes: 1 << 30,
        online: true,
    })
}
#[test]
fn catalog_batch_preserves_live_state_until_apply_and_replays_same_group_prefix() {
    let (files, mut peer) = batch_peer();
    let (mut service, groups) = dynamic_fixture();
    let saved = state(&service);
    let saved_groups = groups.to_bytes().unwrap();
    let mut bad = report(&groups, 3);
    if let MetaRequest::ChunkCatalog(c) = &mut bad {
        c.bootstrap_id += 1;
    }
    let mut requests = VecDeque::new();
    let mut receivers = Vec::new();
    for operation in [report(&groups, 1), bad, report(&groups, 2)] {
        let (request, rx) = queued(operation, Instant::now() + Duration::from_secs(30));
        requests.push_back(request);
        receivers.push(rx);
    }
    let (barrier, _read) = queued(
        MetaRequest::ListRootNodes,
        Instant::now() + Duration::from_secs(30),
    );
    requests.push_back(barrier);
    let (later, _later) = queued(report(&groups, 3), Instant::now() + Duration::from_secs(30));
    requests.push_back(later);
    let mut pending = VecDeque::new();
    propose_catalog_batch(
        &mut peer,
        &mut service,
        &groups,
        65536,
        &mut requests,
        &mut pending,
    )
    .unwrap();
    assert_eq!(pending.len(), 3);
    assert_eq!(requests.len(), 2);
    assert!(matches!(
        requests.front().unwrap().request,
        MetaRequest::ListRootNodes
    ));
    assert_eq!(state(&service), saved);
    assert_eq!(groups.to_bytes().unwrap(), saved_groups);
    assert!(receivers
        .iter()
        .all(|rx| matches!(rx.try_recv(), Err(mpsc::TryRecvError::Empty))));
    peer.process_ready(|id, bytes| {
        apply_committed(&mut service, &groups, 65536, &mut pending, id, bytes)
    })
    .unwrap();
    assert!(pending.is_empty());
    for (index, rx) in receivers.into_iter().enumerate() {
        assert_eq!(rx.try_recv().unwrap().is_ok(), index != 1);
    }
    assert!(service.nodes["group-0"].online);
    assert_eq!(service.nodes["group-0"].used_bytes, 200);
    assert_eq!(service.nodes["member-0-0"].used_bytes, 100);
    assert_eq!(service.nodes["member-0-1"].used_bytes, 200);
    assert!(!service.nodes["group-1"].online);
    let expected = state(&service);
    let expected_groups = groups.to_bytes().unwrap();
    drop(peer);
    let mut reopened = DurablePeer::open(PeerOptions {
        node_id: 1,
        group_id: 19,
        voters: vec![1],
        wal_path: files.0.join("peer.wal"),
    })
    .unwrap();
    let (mut restored, restored_groups) = dynamic_fixture();
    reopened
        .process_ready(|id, bytes| {
            apply_committed(
                &mut restored,
                &restored_groups,
                65536,
                &mut pending,
                id,
                bytes,
            )
        })
        .unwrap();
    assert_eq!(state(&restored), expected);
    assert_eq!(restored_groups.to_bytes().unwrap(), expected_groups);
}
#[test]
fn rejected_catalog_batch_restores_health_roster_and_all_replies() {
    let (_files, mut peer) = batch_peer();
    let (mut service, groups) = dynamic_fixture();
    peer.propose(vec![0; 4 * 1024 * 1024]).unwrap();
    let saved = state(&service);
    let health = groups.to_bytes().unwrap();
    let mut requests = VecDeque::new();
    let mut receivers = Vec::new();
    for id in [1, 2, 3] {
        let (r, rx) = queued(
            report(&groups, id),
            Instant::now() + Duration::from_secs(30),
        );
        requests.push_back(r);
        receivers.push(rx);
    }
    let mut pending = VecDeque::new();
    propose_catalog_batch(
        &mut peer,
        &mut service,
        &groups,
        65536,
        &mut requests,
        &mut pending,
    )
    .unwrap();
    assert!(requests.is_empty());
    assert!(pending.is_empty());
    assert_eq!(state(&service), saved);
    assert_eq!(groups.to_bytes().unwrap(), health);
    for rx in receivers {
        assert!(rx.try_recv().unwrap().is_err());
    }
}
#[test]
fn catalog_batch_bounds_requests_and_skips_expired_commands() {
    let (_files, mut peer) = batch_peer();
    let (mut service, groups) = dynamic_fixture();
    let saved = state(&service);
    let health = groups.to_bytes().unwrap();
    let mut requests = VecDeque::new();
    let mut receivers = Vec::new();
    let (expired, expired_rx) = queued(report(&groups, 1), Instant::now());
    requests.push_back(expired);
    for _ in 0..PROPOSALS_PER_BATCH {
        let (r, rx) = queued(report(&groups, 2), Instant::now() + Duration::from_secs(30));
        requests.push_back(r);
        receivers.push(rx);
    }
    let mut pending = VecDeque::new();
    propose_catalog_batch(
        &mut peer,
        &mut service,
        &groups,
        65536,
        &mut requests,
        &mut pending,
    )
    .unwrap();
    assert!(expired_rx.try_recv().unwrap().is_err());
    assert_eq!(pending.len(), PROPOSALS_PER_BATCH - 1);
    assert_eq!(requests.len(), 1);
    assert_eq!(state(&service), saved);
    assert_eq!(groups.to_bytes().unwrap(), health);
    assert!(receivers
        .iter()
        .all(|rx| matches!(rx.try_recv(), Err(mpsc::TryRecvError::Empty))));
}

#[test]
fn interleaved_catalog_groups_prepare_independent_health_and_restore_roster() {
    let (_files, mut peer) = batch_peer();
    let (mut service, groups) = dynamic_fixture();
    let before = state(&service);
    let health = groups.to_bytes().unwrap();
    let mut requests = VecDeque::new();
    let mut receivers = Vec::new();
    for member in [1, 2] {
        for placement in ["group-0", "group-1"] {
            let MetaRequest::ChunkCatalog(mut command) = report(&groups, member) else {
                unreachable!()
            };
            let group = groups.get(placement).unwrap();
            command.placement_id = placement.into();
            command.member_id = group.member_ids[member as usize - 1].clone();
            command.expected_endpoint = group.api_endpoints[member as usize - 1].clone();
            let (request, receiver) = queued(
                MetaRequest::ChunkCatalog(command),
                Instant::now() + Duration::from_secs(30),
            );
            requests.push_back(request);
            receivers.push(receiver);
        }
    }
    let mut pending = VecDeque::new();
    propose_catalog_batch(
        &mut peer,
        &mut service,
        &groups,
        65536,
        &mut requests,
        &mut pending,
    )
    .unwrap();
    assert_eq!(pending.len(), 4);
    assert!(requests.is_empty());
    assert_eq!(state(&service), before);
    assert_eq!(groups.to_bytes().unwrap(), health);
    peer.process_ready(|id, bytes| {
        apply_committed(&mut service, &groups, 65536, &mut pending, id, bytes)
    })
    .unwrap();
    for receiver in receivers {
        assert!(receiver.try_recv().unwrap().is_ok());
    }
    for placement in ["group-0", "group-1"] {
        assert!(service.nodes[placement].online);
        assert_eq!(service.nodes[placement].used_bytes, 200);
    }
}
