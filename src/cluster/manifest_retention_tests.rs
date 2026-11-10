// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use crate::manifest_retention::{ManifestRetentionRequest, MAX_RETAINED_MANIFESTS};
use crate::object_manifest::{ManifestOutcome, ObjectManifest};

#[test]
fn retention_outcomes_bound_capacity_preserve_versions_and_never_reuse_handles() {
    let mut service = MatrixMetaService::new();
    let live = ObjectManifest {
        format_version: 2,
        bucket: "bucket".into(),
        key: "key".into(),
        generation: 1,
        length: 0,
        extents: Vec::new(),
        deleted: false,
    };
    service
        .object_manifests
        .apply(&ManifestOutcome::decide(None, live.clone()).unwrap())
        .unwrap();
    let request = ManifestRetentionRequest {
        version: 1,
        action: 1,
        bucket: "bucket".into(),
        key: "key".into(),
        generation: 1,
        ..Default::default()
    };
    let original = service.export_snapshot().to_bytes().unwrap();
    assert!(decide_retention(&service, &request, 1).is_err());
    assert_eq!(service.export_snapshot().to_bytes().unwrap(), original);
    let mut handles = Vec::new();
    for _ in 0..MAX_RETAINED_MANIFESTS {
        let (outcome, _) = decide_retention(&service, &request, usize::MAX).unwrap();
        let id = outcome.after_index;
        crate::placement_outcome::PlacementOutcome::from_payload(&outcome.to_payload().unwrap())
            .unwrap()
            .apply(&mut service)
            .unwrap();
        handles.push(id);
    }
    let full = service.export_snapshot().to_bytes().unwrap();
    assert!(matches!(
        decide_retention(&service, &request, usize::MAX),
        Err(ObjectError::PreconditionFailed(_))
    ));
    assert_eq!(service.export_snapshot().to_bytes().unwrap(), full);
    let release = ManifestRetentionRequest {
        version: 1,
        action: 2,
        retention_id: handles[0],
        ..Default::default()
    };
    let (outcome, _) = decide_retention(&service, &release, usize::MAX).unwrap();
    outcome.apply(&mut service).unwrap();
    let (new, _) = decide_retention(&service, &request, usize::MAX).unwrap();
    assert!(new.after_index > *handles.last().unwrap());
    let newest = new.after_index;
    new.apply(&mut service).unwrap();
    let (duplicate, _) = decide_retention(&service, &release, usize::MAX).unwrap();
    assert!(duplicate.retention.is_none());
    duplicate.apply(&mut service).unwrap();
    assert!(service.object_manifests.retained_manifest(newest).is_some());

    let mut deleted = live.clone();
    deleted.generation = 2;
    deleted.deleted = true;
    service
        .object_manifests
        .apply(&ManifestOutcome::decide(Some(live.clone()), deleted).unwrap())
        .unwrap();
    assert!(decide_retention(&service, &request, usize::MAX).is_err());
    assert_eq!(
        service
            .object_manifests
            .retained_manifest(newest)
            .unwrap()
            .manifest,
        Some(live)
    );
    let bytes = service.export_snapshot().to_bytes().unwrap();
    let restored = crate::MetaSnapshot::from_bytes(&bytes).unwrap();
    assert_eq!(restored.object_manifests, service.object_manifests);

    let mut forged = new;
    forged
        .retention
        .as_mut()
        .unwrap()
        .after
        .as_mut()
        .unwrap()
        .retention_id = handles[0];
    assert!(forged.to_payload().is_err());
}
