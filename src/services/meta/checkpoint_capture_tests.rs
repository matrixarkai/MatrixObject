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
    assert_eq!(
        restored.placements[&ObjectId::new("capture", "second")].version,
        2
    );
    assert_eq!(
        actual.placements[&ObjectId::new("capture", "second")].version,
        1
    );
}

#[test]
fn native_catalog_and_writer_leases_keep_the_checkpoint_versions() {
    use crate::chunk_catalog::ChunkOutcome;
    use crate::chunk_manifest::{
        ChunkExtent, ChunkManifest, CHUNK_FLAG_SEALED, CHUNK_KIND_PAGED, CHUNK_MANIFEST_FORMAT,
    };
    use crate::log_chunk_outcome::WriterLease;

    let mut service = MatrixMetaService::new();
    for index in 0..512 {
        let key = format!("object-{index:08}");
        let manifest = ChunkManifest {
            format_version: CHUNK_MANIFEST_FORMAT,
            bucket: "native".into(),
            key: key.clone(),
            generation: 1,
            length: 4096,
            extents: vec![ChunkExtent {
                group: "group-a".into(),
                volume_id: 1,
                chunk_id: index + 1,
                kind: CHUNK_KIND_PAGED,
                length: 4096,
                flags: CHUNK_FLAG_SEALED,
                digest: vec![1; 32],
                ..Default::default()
            }],
            kind: CHUNK_KIND_PAGED,
            page_bytes: 1 << 20,
            ..Default::default()
        };
        service
            .chunk_catalog
            .apply(&ChunkOutcome::decide(None, manifest).unwrap())
            .unwrap();
        service.writer_leases.set(
            "native",
            &key,
            &WriterLease {
                holder: "writer".into(),
                epoch: 1,
                expires_at_ms: 100000,
            },
        );
    }
    let expected = service.export_snapshot().to_bytes().unwrap();
    let capture = service.capture_checkpoint_snapshot();
    let id = ObjectId::new("native", "object-00000000");
    let before = service.chunk_catalog.get(&id).unwrap().as_ref().clone();
    let mut after = before.clone();
    after.generation += 1;
    after.extents[0].digest = vec![2; 32];
    service
        .chunk_catalog
        .apply(&ChunkOutcome::decide(Some(before), after).unwrap())
        .unwrap();
    service.writer_leases.set(
        "native",
        "object-00000000",
        &WriterLease {
            holder: "next-writer".into(),
            epoch: 2,
            expires_at_ms: 200000,
        },
    );
    service.writer_leases.remove("native", "object-00000001");
    assert_ne!(service.export_snapshot().to_bytes().unwrap(), expected);
    drop(service);
    let actual = std::thread::spawn(move || seal(capture)).join().unwrap();
    actual.validate().unwrap();
    assert_eq!(actual.to_bytes().unwrap(), expected);
    let decoded = MetaSnapshot::from_bytes(&expected).unwrap();
    assert_eq!(decoded.chunk_catalog.len(), 512);
    assert_eq!(decoded.writer_leases.len(), 512);
    assert_eq!(decoded.chunk_catalog.get(&id).unwrap().generation, 1);
    assert_eq!(
        decoded
            .writer_leases
            .get("native", "object-00000000")
            .unwrap()
            .epoch,
        1
    );
    let mut restored = MatrixMetaService::new();
    restored.install_placement_roster(&decoded).unwrap();
    restored.install_snapshot(decoded).unwrap();
    assert_eq!(restored.export_snapshot().to_bytes().unwrap(), expected);
    restored.writer_leases.remove("native", "object-00000000");
    assert_eq!(actual.writer_leases.len(), 512);
}
