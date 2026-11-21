// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use crate::object_manifest::{ManifestOutcome, ObjectManifest};
use crate::placement_outcome::PlacementOutcome;

fn publish(service: &mut MatrixMetaService, key: &str, deleted: bool) {
    let id = ObjectId::new("capture", key);
    let before = service
        .object_manifests
        .get(&id)
        .map(|value| value.to_manifest());
    let after = ObjectManifest {
        format_version: 2,
        bucket: "capture".into(),
        key: key.into(),
        generation: before.as_ref().map_or(1, |value| value.generation + 1),
        deleted,
        ..Default::default()
    };
    let outcome = ManifestOutcome::decide(before, after).unwrap();
    PlacementOutcome::publish_manifest(service, outcome, 1)
        .unwrap()
        .apply(service)
        .unwrap();
}

fn seal(capture: MetaCheckpointCapture) -> MetaSnapshot {
    let mut snapshot = capture.into_snapshot();
    snapshot.checksum = snapshot.checksum_material_hash();
    snapshot
}

#[test]
fn capture_shares_the_placement_root_as_the_catalog_grows() {
    let mut service = MatrixMetaService::new();
    let mut next = 0;
    for count in [1, 512, 8192] {
        while next < count {
            publish(&mut service, &format!("object-{next:08}"), false);
            next += 1;
        }
        let expected = service.export_snapshot();
        let capture = service.capture_checkpoint_snapshot();
        assert!(capture.snapshot.placements.is_empty());
        assert!(capture.placements.ptr_eq(&service.placements));
        assert_eq!(capture.placements.len(), count);
        let actual = seal(capture);
        actual.validate().unwrap();
        assert_eq!(actual.to_bytes().unwrap(), expected.to_bytes().unwrap());
    }
}

#[test]
fn a_checkpoint_keeps_the_captured_manifest_and_placement_versions() {
    let mut service = MatrixMetaService::new();
    publish(&mut service, "replace", false);
    publish(&mut service, "delete", false);
    let expected = service.export_snapshot();
    let capture = service.capture_checkpoint_snapshot();
    publish(&mut service, "replace", false);
    publish(&mut service, "delete", true);
    publish(&mut service, "new", false);
    assert!(!capture.placements.ptr_eq(&service.placements));
    let actual = seal(capture);
    actual.validate().unwrap();
    assert_eq!(actual, expected);
    assert_ne!(actual, service.export_snapshot());
    assert_eq!(
        actual.placements[&ObjectId::new("capture", "replace")].version,
        1
    );
    assert_eq!(
        service.placements[&ObjectId::new("capture", "replace")].version,
        2
    );
}

#[test]
fn the_worker_can_materialize_a_snapshot_after_the_service_is_dropped() {
    let mut service = MatrixMetaService::new();
    for key in ["first", "second", "third"] {
        publish(&mut service, key, false);
    }
    let expected = service.export_snapshot().to_bytes().unwrap();
    let capture = service.capture_checkpoint_snapshot();
    drop(service);
    let actual = std::thread::spawn(move || seal(capture)).join().unwrap();
    actual.validate().unwrap();
    assert_eq!(actual.to_bytes().unwrap(), expected);
    let decoded = MetaSnapshot::from_bytes(&expected).unwrap();
    assert_eq!(decoded, actual);
    let _: &BTreeMap<ObjectId, ObjectPlacement> = &decoded.placements;
    let mut restored = MatrixMetaService::new();
    restored.install_placement_roster(&decoded).unwrap();
    restored.install_snapshot(decoded).unwrap();
    assert_eq!(restored.export_snapshot().to_bytes().unwrap(), expected);
    publish(&mut restored, "second", false);
    assert_eq!(restored.placements[&ObjectId::new("capture", "second")].version, 2);
    assert_eq!(actual.placements[&ObjectId::new("capture", "second")].version, 1);
}
