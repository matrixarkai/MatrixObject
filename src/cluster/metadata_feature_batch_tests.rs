// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use crate::cluster::FeaturesRequest;
use crate::cluster_features::{FEATURE_COMPACT_APPEND, FEATURE_RESERVE_NEXT};

const BOTH: u64 = FEATURE_COMPACT_APPEND | FEATURE_RESERVE_NEXT;

#[test]
fn metadata_feature_batch_waits_for_commit_and_replays_in_order() {
    let (files, mut peer) = batch_peer();
    let (mut service, groups) = fixture(1, 0);
    let saved = service.export_snapshot().to_bytes().unwrap();
    let saved_features = service.cluster_features.clone();
    let saved_index = service.placement_commit_index;
    let mut requests = VecDeque::new();
    let mut receivers = Vec::new();
    for command in [
        FeaturesRequest::declare(1, BOTH),
        FeaturesRequest::raise(BOTH, vec![1]),
    ] {
        let (request, rx) = queued(
            MetaRequest::Features(command),
            Instant::now() + Duration::from_secs(30),
        );
        requests.push_back(request);
        receivers.push(rx);
    }
    let mut pending = VecDeque::new();
    propose_batch(
        &mut peer,
        &mut service,
        &groups,
        65536,
        &mut requests,
        &mut pending,
    )
    .unwrap();
    assert!(requests.is_empty());
    assert_eq!(pending.len(), 2);
    assert_eq!(
        service.cluster_features, saved_features,
        "prepared feature state must remain invisible until commit"
    );
    assert_eq!(service.export_snapshot().to_bytes().unwrap(), saved);
    assert!(receivers
        .iter()
        .all(|rx| matches!(rx.try_recv(), Err(mpsc::TryRecvError::Empty))));
    peer.process_ready(|id, bytes| {
        apply_committed(&mut service, &groups, 65536, &mut pending, id, bytes)
    })
    .unwrap();
    assert!(pending.is_empty());
    for (index, rx) in receivers.into_iter().enumerate() {
        let MetaReply::Features(table) = rx.try_recv().unwrap().unwrap() else {
            panic!("missing feature reply")
        };
        let features = table.to_features().unwrap();
        assert_eq!(features.declared.get(&1), Some(&BOTH));
        assert_eq!(features.raised, if index == 0 { 0 } else { BOTH });
    }
    assert!(service.cluster_features.has(BOTH));
    assert_eq!(service.placement_commit_index, saved_index + 2);
    let expected = service.export_snapshot().to_bytes().unwrap();
    drop(peer);
    let mut reopened = DurablePeer::open(PeerOptions {
        node_id: 1,
        group_id: 19,
        voters: vec![1],
        wal_path: files.0.join("peer.wal"),
    })
    .unwrap();
    let (mut restored, _) = fixture(1, 0);
    reopened
        .process_ready(|id, bytes| {
            apply_committed(&mut restored, &groups, 65536, &mut pending, id, bytes)
        })
        .unwrap();
    assert_eq!(restored.export_snapshot().to_bytes().unwrap(), expected);
}

#[test]
fn metadata_feature_batch_rejection_restores_declarations_and_raised_bits() {
    let (_files, mut peer) = batch_peer();
    let (mut service, groups) = fixture(1, 0);
    let declaration = super::super::super::features_commands::prepare(
        &service,
        &FeaturesRequest::declare(1, BOTH),
    )
    .unwrap()
    .unwrap();
    declaration.apply(&mut service).unwrap();
    let saved = service.export_snapshot().to_bytes().unwrap();
    let saved_features = service.cluster_features.clone();
    peer.propose(vec![0; 4 * 1024 * 1024]).unwrap();
    let mut requests = VecDeque::new();
    let mut receivers = Vec::new();
    for command in [
        FeaturesRequest::raise(BOTH, vec![1]),
        FeaturesRequest::declare(2, BOTH),
    ] {
        let (request, rx) = queued(
            MetaRequest::Features(command),
            Instant::now() + Duration::from_secs(30),
        );
        requests.push_back(request);
        receivers.push(rx);
    }
    let mut pending = VecDeque::new();
    propose_batch(
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
    assert_eq!(
        service.cluster_features, saved_features,
        "refused proposals must not change live features"
    );
    assert_eq!(service.export_snapshot().to_bytes().unwrap(), saved);
    assert!(receivers
        .into_iter()
        .all(|rx| rx.try_recv().unwrap().is_err()));
}
