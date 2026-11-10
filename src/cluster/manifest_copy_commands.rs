// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::{meta_message::MetaReply, ChunkGroupCatalog};
use crate::{
    manifest_copy::ManifestCopyRequest, object_manifest::ManifestOutcome,
    placement_outcome::PlacementOutcome, MatrixMetaService, ObjectError, ObjectId,
};

pub(super) fn prepare(
    service: &MatrixMetaService,
    groups: &ChunkGroupCatalog,
    request: &ManifestCopyRequest,
    limit: usize,
) -> Result<(PlacementOutcome, MetaReply), ObjectError> {
    request.validate()?;
    let source = service
        .object_manifests
        .get(&ObjectId::new(
            request.source_bucket.clone(),
            request.source_key.clone(),
        ))
        .filter(|manifest| !manifest.deleted)
        .ok_or_else(|| ObjectError::NotFound("copy source is missing or deleted".into()))?;
    if source.length > request.max_bytes {
        return Err(ObjectError::PreconditionFailed(
            "copy source exceeds byte limit".into(),
        ));
    }
    let mut after = source.clone();
    after.bucket = request.bucket.clone();
    after.key = request.key.clone();
    after.generation = match &request.expected {
        None => 1,
        Some(before) => before
            .generation
            .checked_add(1)
            .ok_or_else(|| ObjectError::PreconditionFailed("copy generation exhausted".into()))?,
    };
    let publication = ManifestOutcome::decide(request.expected.clone(), after.clone())?;
    groups.check_manifest(&publication)?;
    let identity = crate::manifest_copy::LiveCopySource {
        bucket: source.bucket.clone(),
        key: source.key.clone(),
        generation: source.generation,
    };
    let outcome = PlacementOutcome::publish_copied_manifest(
        service,
        publication,
        request.update_time_ms,
        identity,
    )?;
    let reply = MetaReply::Manifest(after);
    super::message::check_reply_size(reply.payload_len(), limit)?;
    Ok((outcome, reply))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::{ChunkGroupConfig, ChunkGroupIdentity};
    fn groups() -> ChunkGroupCatalog {
        ChunkGroupCatalog::new(vec![ChunkGroupConfig {
            placement_id: "group-a".into(),
            failure_domain: "rack-a".into(),
            identity: Some(ChunkGroupIdentity {
                version: 1,
                group_id: 1,
                node_id: 1,
                capacity_bytes: 1000,
                max_pages: 64,
                voters: vec![1, 2, 3],
                peer_endpoints: vec![
                    "127.0.0.1:22001".into(),
                    "127.0.0.1:22002".into(),
                    "127.0.0.1:22003".into(),
                ],
                bootstrap_voters: Vec::new(),
                bootstrap_id: 0,
            }),
            api_endpoints: vec![
                "127.0.0.1:22101".into(),
                "127.0.0.1:22102".into(),
                "127.0.0.1:22103".into(),
            ],
            member_ids: vec!["a".into(), "b".into(), "c".into()],
        }])
        .unwrap()
    }
    #[test]
    fn atomic_copy_binds_live_source_and_preserves_state_on_conflicts() {
        let (mut service, create, _) = crate::tests::manifest_publication_fixture();
        let source = create.manifest.as_ref().unwrap().after.clone().unwrap();
        create.apply(&mut service).unwrap();
        let enable = crate::staged_allocation::StagedAllocationRequest {
            version: 1,
            action: 6,
            ..Default::default()
        };
        super::super::staged_allocation_commands::prepare(&service, &groups(), &enable, 8192)
            .unwrap()
            .0
            .apply(&mut service)
            .unwrap();
        service.nodes.get_mut("group-a").unwrap().online = false;
        let request = ManifestCopyRequest {
            version: 1,
            source_bucket: source.bucket.clone(),
            source_key: source.key.clone(),
            bucket: "copy".into(),
            key: "snapshot".into(),
            expected: None,
            max_bytes: source.length,
            update_time_ms: 0,
        };
        let saved = service.export_snapshot().to_bytes().unwrap();
        assert!(prepare(&service, &groups(), &request, 1).is_err());
        assert_eq!(service.export_snapshot().to_bytes().unwrap(), saved);
        let (copy, _) = prepare(&service, &groups(), &request, 8192).unwrap();
        let mut forged = copy.clone();
        forged
            .manifest
            .as_mut()
            .unwrap()
            .after
            .as_mut()
            .unwrap()
            .extents[0]
            .data_checksum ^= 1;
        assert!(forged.apply(&mut service).is_err());
        assert_eq!(service.export_snapshot().to_bytes().unwrap(), saved);
        let mut unbound = copy.clone();
        unbound.copy_source = None;
        assert!(unbound.to_payload().is_err());
        PlacementOutcome::from_payload(&copy.to_payload().unwrap())
            .unwrap()
            .apply(&mut service)
            .unwrap();
        let copied = service
            .object_manifests
            .get(&ObjectId::new("copy", "snapshot"))
            .unwrap();
        assert_eq!(copied.extents, source.extents);
        assert!(prepare(&service, &groups(), &request, 8192).is_err());
        let mut too_small = request.clone();
        too_small.key = "too-small".into();
        too_small.max_bytes = 0;
        assert!(prepare(&service, &groups(), &too_small, 8192).is_err());
        service.nodes.get_mut("group-a").unwrap().draining = true;
        let mut draining = request.clone();
        draining.key = "draining".into();
        assert!(prepare(&service, &groups(), &draining, 8192).is_err());
        service.nodes.get_mut("group-a").unwrap().draining = false;
        let restored =
            crate::MetaSnapshot::from_bytes(&service.export_snapshot().to_bytes().unwrap())
                .unwrap();
        assert_eq!(restored.object_manifests, service.object_manifests);
    }

    #[test]
    fn atomic_copy_source_identity_does_not_duplicate_extent_payloads() {
        let mut extra = Vec::new();
        for pages in [0, 4096] {
            let (mut service, create, _) = crate::tests::manifest_publication_fixture();
            service.nodes.get_mut("group-a").unwrap().capacity_bytes = 1 << 30;
            let mut source = create.manifest.unwrap().after.unwrap();
            source.format_version = 2;
            source.length = pages;
            source.extents = (0..pages)
                .map(|index| crate::object_manifest::ObjectExtent {
                    group: "group-a".into(),
                    volume_id: 1,
                    chunk_id: 1,
                    page_index: index,
                    object_offset: index,
                    length: 1,
                    write_sequence: 1,
                    data_sha256: vec![1; 32],
                    ..Default::default()
                })
                .collect();
            PlacementOutcome::publish_manifest(
                &service,
                ManifestOutcome::decide(None, source.clone()).unwrap(),
                0,
            )
            .unwrap()
            .apply(&mut service)
            .unwrap();
            let request = ManifestCopyRequest {
                version: 1,
                source_bucket: source.bucket.clone(),
                source_key: source.key.clone(),
                bucket: "copy".into(),
                key: "snapshot".into(),
                max_bytes: u64::MAX,
                ..Default::default()
            };
            let copied = prepare(&service, &groups(), &request, 2 * 1024 * 1024)
                .unwrap()
                .0;
            let ordinary =
                PlacementOutcome::publish_manifest(&service, copied.manifest.clone().unwrap(), 0)
                    .unwrap();
            extra.push(copied.to_payload().unwrap().len() - ordinary.to_payload().unwrap().len());
        }
        assert_eq!(extra[0], extra[1]);
        assert!(extra[0] < 128);
    }

    #[test]
    fn publication_fence_and_atomic_copy_recover_from_standalone_journal() {
        let root = std::env::temp_dir().join(format!("fenced-copy-journal-{}", std::process::id()));
        std::fs::create_dir(&root).unwrap();
        struct Cleanup(std::path::PathBuf);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let _cleanup = Cleanup(root.clone());
        let mut service = MatrixMetaService::new();
        let source = crate::object_manifest::ObjectManifest {
            format_version: 2,
            bucket: "source".into(),
            key: "key".into(),
            generation: 1,
            length: 0,
            extents: Vec::new(),
            deleted: false,
        };
        PlacementOutcome::publish_manifest(
            &service,
            ManifestOutcome::decide(None, source).unwrap(),
            0,
        )
        .unwrap()
        .apply(&mut service)
        .unwrap();
        let (mut storage, _) =
            super::super::meta_snapshot_file::MetaSnapshotFile::open(&root, &mut service).unwrap();
        storage.persist(&service, &Default::default()).unwrap();
        let enable = crate::staged_allocation::StagedAllocationRequest {
            version: 1,
            action: 6,
            ..Default::default()
        };
        let (fence, _) = super::super::staged_allocation_commands::prepare(
            &service,
            &Default::default(),
            &enable,
            8192,
        )
        .unwrap();
        let id = fence.after_index;
        storage.append_outcome(&fence).unwrap();
        fence.apply(&mut service).unwrap();
        let request = ManifestCopyRequest {
            version: 1,
            source_bucket: "source".into(),
            source_key: "key".into(),
            bucket: "target".into(),
            key: "key".into(),
            ..Default::default()
        };
        let (copy, _) = prepare(&service, &Default::default(), &request, 8192).unwrap();
        storage.append_outcome(&copy).unwrap();
        copy.apply(&mut service).unwrap();
        drop(storage);
        let mut service = MatrixMetaService::new();
        let (storage, recovered) =
            super::super::meta_snapshot_file::MetaSnapshotFile::open(&root, &mut service).unwrap();
        assert!(recovered);
        assert_eq!(service.object_manifests.publication_fence(), id);
        assert!(service
            .object_manifests
            .get(&ObjectId::new("target", "key"))
            .is_some());
        drop(storage);
    }
}
