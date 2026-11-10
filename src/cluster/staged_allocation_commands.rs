// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::{meta_message::MetaReply, ChunkGroupCatalog};
use crate::{
    placement_outcome::PlacementOutcome,
    staged_allocation::{
        StagedAllocation, StagedAllocationChange, StagedAllocationReply, StagedAllocationRequest,
    },
    MatrixMetaService, ObjectError,
};

pub(super) fn prepare(
    service: &MatrixMetaService,
    groups: &ChunkGroupCatalog,
    command: &StagedAllocationRequest,
    limit: usize,
) -> Result<(PlacementOutcome, MetaReply), ObjectError> {
    command.validate()?;
    let mut outcome = PlacementOutcome::capture(service, Vec::new(), None)?;
    let mut reply = StagedAllocationReply {
        version: 1,
        ..Default::default()
    };
    let change = match command.action {
        1 => {
            let record = StagedAllocation {
                version: 1,
                allocation_id: service
                    .placement_commit_index
                    .checked_add(1)
                    .ok_or_else(|| {
                        ObjectError::PreconditionFailed(
                            "staged allocation sequence exhausted".into(),
                        )
                    })?,
                page_count: command.page_count,
                bucket: command.bucket.clone(),
                key: command.key.clone(),
                expected_generation: command.expected_generation,
                append: command.append,
            };
            reply.allocation = Some(record.clone());
            Some(StagedAllocationChange {
                before: None,
                after: Some(record),
            })
        }
        3 => {
            if command.allocation_id > service.placement_commit_index {
                return Err(ObjectError::PreconditionFailed(
                    "staged allocation is from a future position".into(),
                ));
            }
            service
                .object_manifests
                .staged_allocation(command.allocation_id)
                .cloned()
                .map(|before| StagedAllocationChange {
                    before: Some(before),
                    after: None,
                })
        }
        4 => {
            let allocation = service
                .object_manifests
                .staged_allocation(command.allocation_id)
                .cloned()
                .ok_or_else(|| {
                    ObjectError::PreconditionFailed("staged allocation is no longer active".into())
                })?;
            let manifest = command.outcome.clone().unwrap();
            allocation.check_publication(&manifest)?;
            groups.check_manifest(&manifest)?;
            outcome = PlacementOutcome::publish_staged_manifest(
                service,
                manifest,
                command.update_time_ms,
                allocation,
            )?;
            reply.manifest = outcome.manifest.as_ref().unwrap().after.clone();
            None
        }
        6 => {
            let current = service.object_manifests.publication_fence();
            let next = if current == 0 {
                service
                    .placement_commit_index
                    .checked_add(1)
                    .ok_or_else(|| {
                        ObjectError::PreconditionFailed(
                            "publication fence sequence exhausted".into(),
                        )
                    })?
            } else {
                current
            };
            if current == 0 {
                service.object_manifests.check_publication_fence(0, next)?;
                outcome.version = 5;
                outcome.after_index = next;
                outcome.publication_fence =
                    Some(crate::staged_allocation::PublicationFenceChange {
                        before: 0,
                        after: next,
                    });
            }
            reply.publication_fence = next;
            None
        }
        _ => {
            return Err(ObjectError::InvalidArgument(
                "staged decision requires mutation".into(),
            ))
        }
    };
    if let Some(change) = change {
        service.object_manifests.check_staging(&change)?;
        outcome.version = 4;
        outcome.after_index = outcome
            .before_index
            .checked_add(1)
            .ok_or_else(|| ObjectError::PreconditionFailed("metadata position exhausted".into()))?;
        outcome.staging = Some(change);
    }
    outcome.to_payload()?;
    let reply = MetaReply::StagedAllocation(reply);
    super::message::check_reply_size(reply.payload_len(), limit)?;
    Ok((outcome, reply))
}

pub(super) fn read(
    service: &MatrixMetaService,
    command: &StagedAllocationRequest,
    limit: usize,
) -> Result<MetaReply, ObjectError> {
    command.validate()?;
    let mut value = StagedAllocationReply {
        version: 1,
        ..Default::default()
    };
    match command.action {
        2 => {
            value.allocation = Some(
                service
                    .object_manifests
                    .staged_allocation(command.allocation_id)
                    .cloned()
                    .ok_or_else(|| ObjectError::NotFound("staged allocation missing".into()))?,
            )
        }
        5 => {
            (value.allocation_ids, value.has_more) = service
                .object_manifests
                .staged_ids(command.allocation_id, command.limit as usize)
        }
        7 => value.publication_fence = service.object_manifests.publication_fence(),
        _ => {
            return Err(ObjectError::InvalidArgument(
                "staged mutation requires durable dispatch".into(),
            ))
        }
    }
    let reply = MetaReply::StagedAllocation(value);
    super::message::check_reply_size(reply.payload_len(), limit)?;
    Ok(reply)
}

#[cfg(test)]
mod tests {
    #[test]
    fn publication_fence_is_durable_one_way_and_rejects_retired_namespace_publication() {
        let (mut service, create, _) = crate::tests::manifest_publication_fixture();
        create.apply(&mut service).unwrap();
        let mut reserve = request();
        reserve.key = "retired".into();
        let (allocation, _) = prepare(&service, &Default::default(), &reserve, 8192).unwrap();
        let owned = allocation.staging.as_ref().unwrap().after.clone().unwrap();
        allocation.apply(&mut service).unwrap();
        let abort = StagedAllocationRequest {
            version: 1,
            action: 3,
            allocation_id: owned.allocation_id,
            ..Default::default()
        };
        prepare(&service, &Default::default(), &abort, 8192)
            .unwrap()
            .0
            .apply(&mut service)
            .unwrap();
        let enable = StagedAllocationRequest {
            version: 1,
            action: 6,
            ..Default::default()
        };
        let before = service.export_snapshot().to_bytes().unwrap();
        assert!(prepare(&service, &Default::default(), &enable, 1).is_err());
        assert_eq!(service.export_snapshot().to_bytes().unwrap(), before);
        let (fence, _) = prepare(&service, &Default::default(), &enable, 8192).unwrap();
        let id = fence.after_index;
        PlacementOutcome::from_payload(&fence.to_payload().unwrap())
            .unwrap()
            .apply(&mut service)
            .unwrap();
        assert_eq!(service.object_manifests.publication_fence(), id);
        assert!(super::super::meta_server::decide_allocation(&service, 1, 8192).is_err());
        let duplicate = prepare(&service, &Default::default(), &enable, 8192)
            .unwrap()
            .0;
        assert!(duplicate.publication_fence.is_none());
        duplicate.apply(&mut service).unwrap();
        assert_eq!(service.placement_commit_index, id);
        let mut downgrade = fence;
        downgrade.publication_fence.as_mut().unwrap().before = id;
        downgrade.publication_fence.as_mut().unwrap().after = 0;
        assert!(downgrade.to_payload().is_err());
        let page = owned.allocation().unwrap().page(0).unwrap();
        let mut candidate = empty();
        candidate.key = "retired".into();
        candidate.length = 1;
        candidate
            .extents
            .push(crate::object_manifest::ObjectExtent {
                group: "group-a".into(),
                volume_id: page.chunk_id.volume_id(),
                chunk_id: page.chunk_id.id(),
                page_index: 0,
                length: 1,
                write_sequence: 1,
                data_sha256: vec![1; 32],
                ..Default::default()
            });
        let publication = ManifestOutcome::decide(None, candidate).unwrap();
        let saved = service.export_snapshot().to_bytes().unwrap();
        assert!(matches!(
            PlacementOutcome::publish_manifest(&service, publication, 0),
            Err(ObjectError::PreconditionFailed(_))
        ));
        assert_eq!(service.export_snapshot().to_bytes().unwrap(), saved);
        let restored = crate::MetaSnapshot::from_bytes(&saved).unwrap();
        assert_eq!(restored.object_manifests, service.object_manifests);
    }
    use super::*;
    use crate::object_manifest::{ManifestOutcome, ObjectManifest};
    fn request() -> StagedAllocationRequest {
        StagedAllocationRequest {
            version: 1,
            action: 1,
            page_count: 2,
            bucket: "bucket".into(),
            key: "key".into(),
            ..Default::default()
        }
    }
    fn empty() -> ObjectManifest {
        ObjectManifest {
            format_version: 2,
            bucket: "bucket".into(),
            key: "key".into(),
            generation: 1,
            length: 0,
            extents: Vec::new(),
            deleted: false,
        }
    }

    #[test]
    fn staged_capacity_abort_and_publication_survive_binary_recovery_without_reusing_ids() {
        let groups = ChunkGroupCatalog::default();
        let mut service = MatrixMetaService::new();
        let original = service.export_snapshot().to_bytes().unwrap();
        assert!(prepare(&service, &groups, &request(), 1).is_err());
        assert_eq!(service.export_snapshot().to_bytes().unwrap(), original);
        let mut ids = Vec::new();
        for _ in 0..crate::staged_allocation::MAX_STAGED_ALLOCATIONS {
            let (outcome, _) = prepare(&service, &groups, &request(), 8192).unwrap();
            ids.push(outcome.after_index);
            PlacementOutcome::from_payload(&outcome.to_payload().unwrap())
                .unwrap()
                .apply(&mut service)
                .unwrap();
        }
        let full = service.export_snapshot().to_bytes().unwrap();
        assert_eq!(
            crate::MetaSnapshot::from_bytes(&full)
                .unwrap()
                .object_manifests,
            service.object_manifests
        );
        assert!(prepare(&service, &groups, &request(), 8192).is_err());
        assert_eq!(service.export_snapshot().to_bytes().unwrap(), full);
        let abort = StagedAllocationRequest {
            version: 1,
            action: 3,
            allocation_id: ids[0],
            ..Default::default()
        };
        prepare(&service, &groups, &abort, 8192)
            .unwrap()
            .0
            .apply(&mut service)
            .unwrap();
        prepare(&service, &groups, &abort, 8192)
            .unwrap()
            .0
            .apply(&mut service)
            .unwrap();
        let mut publish = StagedAllocationRequest {
            version: 1,
            action: 4,
            allocation_id: ids[0],
            outcome: Some(ManifestOutcome::decide(None, empty()).unwrap()),
            ..Default::default()
        };
        assert!(prepare(&service, &groups, &publish, 8192).is_err());
        let (created, _) = prepare(&service, &groups, &request(), 8192).unwrap();
        let id = created.after_index;
        assert!(id > *ids.last().unwrap());
        created.apply(&mut service).unwrap();
        publish.allocation_id = id;
        let (committed, _) = prepare(&service, &groups, &publish, 8192).unwrap();
        let mut altered = committed.clone();
        altered
            .staging
            .as_mut()
            .unwrap()
            .before
            .as_mut()
            .unwrap()
            .key = "another".into();
        assert!(altered.to_payload().is_err());
        committed.apply(&mut service).unwrap();
        assert!(service.object_manifests.staged_allocation(id).is_none());
        assert_eq!(
            service.object_manifests.get(&empty().object_id()),
            Some(&empty())
        );
        assert!(prepare(&service, &groups, &publish, 8192).is_err());
        let restored =
            crate::MetaSnapshot::from_bytes(&service.export_snapshot().to_bytes().unwrap())
                .unwrap();
        assert_eq!(restored.object_manifests, service.object_manifests);
    }

    #[test]
    fn staging_journal_size_is_independent_of_existing_append_extents() {
        let mut small = MatrixMetaService::new();
        let mut large = MatrixMetaService::new();
        let value = empty();
        small
            .object_manifests
            .apply(&ManifestOutcome::decide(None, value.clone()).unwrap())
            .unwrap();
        let mut expanded = value;
        expanded.extents = (0..4096)
            .map(|index| crate::object_manifest::ObjectExtent {
                group: "group-a".into(),
                chunk_id: 900,
                page_index: index,
                object_offset: index,
                length: 1,
                write_sequence: 1,
                data_sha256: vec![1; 32],
                ..Default::default()
            })
            .collect();
        expanded.length = 4096;
        large
            .object_manifests
            .apply(&ManifestOutcome::decide(None, expanded).unwrap())
            .unwrap();
        let mut command = request();
        command.expected_generation = 1;
        command.append = true;
        let a = prepare(&small, &Default::default(), &command, 8192)
            .unwrap()
            .0
            .to_payload()
            .unwrap();
        let b = prepare(&large, &Default::default(), &command, 8192)
            .unwrap()
            .0
            .to_payload()
            .unwrap();
        assert_eq!(a, b);
        assert!(a.len() < 256);
    }

    #[test]
    fn staging_journal_recovers_owned_allocation_and_atomic_publication() {
        let root = std::env::temp_dir().join(format!("staging-journal-{}", std::process::id()));
        std::fs::create_dir(&root).unwrap();
        struct Cleanup(std::path::PathBuf);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let _cleanup = Cleanup(root.clone());
        let mut service = MatrixMetaService::new();
        let (mut storage, _) =
            super::super::meta_snapshot_file::MetaSnapshotFile::open(&root, &mut service).unwrap();
        storage.persist(&service, &Default::default()).unwrap();
        let (reserved, _) = prepare(&service, &Default::default(), &request(), 8192).unwrap();
        let id = reserved.after_index;
        storage.append_outcome(&reserved).unwrap();
        reserved.apply(&mut service).unwrap();
        drop(storage);
        let mut service = MatrixMetaService::new();
        let (mut storage, recovered) =
            super::super::meta_snapshot_file::MetaSnapshotFile::open(&root, &mut service).unwrap();
        assert!(recovered);
        assert!(service.object_manifests.staged_allocation(id).is_some());
        let publish = StagedAllocationRequest {
            version: 1,
            action: 4,
            allocation_id: id,
            outcome: Some(ManifestOutcome::decide(None, empty()).unwrap()),
            ..Default::default()
        };
        let (committed, _) = prepare(&service, &Default::default(), &publish, 8192).unwrap();
        storage.append_outcome(&committed).unwrap();
        committed.apply(&mut service).unwrap();
        drop(storage);
        let mut service = MatrixMetaService::new();
        let (storage, recovered) =
            super::super::meta_snapshot_file::MetaSnapshotFile::open(&root, &mut service).unwrap();
        assert!(recovered);
        assert!(service.object_manifests.staged_allocation(id).is_none());
        assert_eq!(
            service.object_manifests.get(&empty().object_id()),
            Some(&empty())
        );
        drop(storage);
    }
}
