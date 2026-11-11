// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[test]
fn compact_selected_grants_restore_all_speculative_handles_before_commit() {
    use crate::manifest_retention::ManifestRetentionRequest;
    use crate::object_manifest::{ManifestOutcome, ObjectManifest};
    let (_files, mut peer) = batch_peer();
    let (mut service, groups) = fixture(1, 0);
    let manifest = ObjectManifest {
        format_version: 2,
        bucket: "bucket".into(),
        key: "reader".into(),
        generation: 1,
        ..Default::default()
    };
    PlacementOutcome::publish_manifest(
        &service,
        ManifestOutcome::decide(None, manifest.clone()).unwrap(),
        0,
    )
    .unwrap()
    .apply(&mut service)
    .unwrap();
    let command = ManifestRetentionRequest {
        version: 3,
        action: 8,
        bucket: manifest.bucket.clone(),
        key: manifest.key.clone(),
        generation: 1,
        ttl_ms: 60_000,
        manifest_sha256: crate::read_session_outcome::manifest_sha256(&manifest).unwrap(),
        ..Default::default()
    };
    let before = service.export_snapshot().to_bytes().unwrap();
    let first_id = service.placement_commit_index + 1;
    let mut requests = VecDeque::new();
    let mut receivers = Vec::new();
    let mut pending = VecDeque::new();
    for _ in 0..2 {
        let (request, receiver) = queued(
            MetaRequest::ManifestRetention(command.clone()),
            Instant::now() + Duration::from_secs(10),
        );
        requests.push_back(request);
        receivers.push(receiver);
    }
    propose_batch(
        &mut peer,
        &mut service,
        &groups,
        8192,
        &mut requests,
        &mut pending,
    )
    .unwrap();
    assert_eq!(service.export_snapshot().to_bytes().unwrap(), before);
    assert!(service
        .object_manifests
        .retained_manifest(first_id)
        .is_none());
    assert!(service
        .object_manifests
        .retained_manifest(first_id + 1)
        .is_none());
    assert!(receivers
        .iter()
        .all(|rx| matches!(rx.try_recv(), Err(mpsc::TryRecvError::Empty))));
    peer.process_ready(|id, bytes| {
        apply_committed(&mut service, &groups, 8192, &mut pending, id, bytes)
    })
    .unwrap();
    for (offset, receiver) in receivers.into_iter().enumerate() {
        let MetaReply::ManifestRetention(reply) = receiver.try_recv().unwrap().unwrap() else {
            panic!()
        };
        assert_eq!(reply.version, 4);
        assert_eq!(reply.retention_id, first_id + offset as u64);
        assert_eq!(reply.manifest_sha256, command.manifest_sha256);
        assert!(reply.retained.is_none());
        assert_eq!(
            service
                .object_manifests
                .retained_manifest(reply.retention_id)
                .unwrap()
                .manifest
                .as_ref(),
            Some(&manifest)
        );
    }
    assert_eq!(service.placement_commit_index, first_id + 1);
}
