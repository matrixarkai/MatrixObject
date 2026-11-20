// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use crate::cluster::FeaturesRequest;
use crate::cluster_features::{FEATURE_COMPACT_APPEND, FEATURE_RESERVE_NEXT};

const BOTH: u64 = FEATURE_COMPACT_APPEND | FEATURE_RESERVE_NEXT;

fn refuse_feature_command(
    service: &mut MatrixMetaService,
    groups: &ChunkGroupCatalog,
    command: FeaturesRequest,
) {
    let (_files, mut peer) = batch_peer();
    assert_eq!(peer.membership().voters, vec![1]);
    let saved = service.export_snapshot().to_bytes().unwrap();
    let (request, receiver) = queued(
        MetaRequest::Features(command),
        Instant::now() + Duration::from_secs(30),
    );
    let mut requests = VecDeque::from([request]);
    let mut pending = VecDeque::new();
    propose_batch(
        &mut peer,
        service,
        groups,
        65536,
        &mut requests,
        &mut pending,
    )
    .unwrap();
    assert_eq!(service.export_snapshot().to_bytes().unwrap(), saved);
    peer.process_ready(|id, bytes| {
        apply_committed(service, groups, 65536, &mut pending, id, bytes)
    })
    .unwrap();
    assert!(
        matches!(
            receiver.try_recv().unwrap(),
            Err(ObjectError::PreconditionFailed(_))
        ),
        "feature admission must use the actual journal readers"
    );
    assert_eq!(service.export_snapshot().to_bytes().unwrap(), saved);
}

#[test]
fn metadata_feature_voter_batch_rejects_a_caller_selected_group() {
    let (mut service, groups) = fixture(1, 0);
    service.cluster_features.declared.insert(1, BOTH);
    service.cluster_features.declared.insert(2, BOTH);
    refuse_feature_command(&mut service, &groups, FeaturesRequest::raise(BOTH, vec![2]));
}

#[test]
fn metadata_feature_voter_batch_rejects_an_active_reader_downgrade() {
    let (mut service, groups) = fixture(1, 0);
    service.cluster_features.raised = BOTH;
    service.cluster_features.declared.insert(1, BOTH);
    refuse_feature_command(
        &mut service,
        &groups,
        FeaturesRequest::declare(1, FEATURE_COMPACT_APPEND),
    );
}

#[test]
fn metadata_feature_voter_batch_checks_the_speculative_feature_prefix() {
    let (_files, mut peer) = batch_peer();
    let (mut service, groups) = fixture(1, 0);
    let saved = service.export_snapshot().to_bytes().unwrap();
    let mut requests = VecDeque::new();
    let mut receivers = Vec::new();
    for command in [
        FeaturesRequest::declare(1, BOTH),
        FeaturesRequest::raise(BOTH, vec![1]),
        FeaturesRequest::declare(1, FEATURE_COMPACT_APPEND),
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
    assert_eq!(service.export_snapshot().to_bytes().unwrap(), saved);
    peer.process_ready(|id, bytes| {
        apply_committed(&mut service, &groups, 65536, &mut pending, id, bytes)
    })
    .unwrap();
    assert!(receivers[0].try_recv().unwrap().is_ok());
    assert!(receivers[1].try_recv().unwrap().is_ok());
    assert!(matches!(
        receivers[2].try_recv().unwrap(),
        Err(ObjectError::PreconditionFailed(_))
    ));
    assert_eq!(service.cluster_features.raised, BOTH);
    assert_eq!(service.cluster_features.declared.get(&1), Some(&BOTH));
}
