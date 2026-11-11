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
    let timed = command.ttl_ms != 0 || matches!(command.action, 8 | 9) || (command.action == 4 && service.object_manifests
        .staged_allocation(command.allocation_id).is_some_and(|record| record.expires_at_ms != 0));
    let now = if timed { super::staged_lease_sweeper::now_ms()? } else { 0 };
    prepare_at(service, groups, command, limit, now)
}

pub(super) fn prepare_at(service: &MatrixMetaService, groups: &ChunkGroupCatalog,
    command: &StagedAllocationRequest, limit: usize, now: u64)
    -> Result<(PlacementOutcome, MetaReply), ObjectError> {
    command.validate()?;
    let mut outcome = PlacementOutcome::capture(service, Vec::new(), None)?;
    let mut reply = StagedAllocationReply {
        version: 1,
        ..Default::default()
    };
    let change = match command.action {
        1 => {
            if command.version == 3 && command.allocation_id <= service.placement_commit_index {
                let record = service.object_manifests.staged_allocation(command.allocation_id)
                    .filter(|record| crate::staged_allocation::PreparedStagedAllocation {
                        request: command.clone()
                    }.matches(record))
                    .ok_or_else(|| ObjectError::PreconditionFailed("prepared allocation position is consumed or retired".into()))?;
                if record.expires_at_ms != 0 && (now == 0 || now >= record.expires_at_ms) {
                    return Err(ObjectError::PreconditionFailed("prepared allocation lease expired".into()));
                }
                reply.allocation = Some(record.clone());
                let reply = MetaReply::StagedAllocation(reply);
                super::message::check_reply_size(reply.payload_len(), limit)?;
                return Ok((outcome, reply));
            }
            let next = service.placement_commit_index.checked_add(1)
                .ok_or_else(|| ObjectError::PreconditionFailed("staged allocation sequence exhausted".into()))?;
            if command.version == 3 && command.allocation_id != next {
                return Err(ObjectError::PreconditionFailed("prepared allocation position is not next".into()));
            }
            let record = StagedAllocation {
                version: command.version,
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
                expires_at_ms: if command.ttl_ms == 0 { 0 } else {
                    if now == 0 { return Err(ObjectError::PreconditionFailed("metadata lease clock is zero".into())); }
                    now.checked_add(command.ttl_ms).ok_or_else(|| ObjectError::PreconditionFailed("staged deadline overflow".into()))?
                },
                request_nonce: command.request_nonce.clone(),
                request_ttl_ms: if command.version == 3 { command.ttl_ms } else { 0 },
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
            if allocation.expires_at_ms != 0 && (now == 0 || now >= allocation.expires_at_ms) {
                return Err(ObjectError::PreconditionFailed("staged allocation lease expired".into()));
            }
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
        8 | 9 => {
            let before = service.object_manifests.staged_allocation(command.allocation_id).cloned();
            if command.allocation_id > service.placement_commit_index {
                return Err(ObjectError::PreconditionFailed("staged lease is from a future position".into()));
            }
            match before {
                None if command.action == 9 => None,
                None => return Err(ObjectError::NotFound("staged allocation missing".into())),
                Some(before) => {
                    if now == 0 || before.expires_at_ms == 0 || before.expires_at_ms != command.expected_expiry_ms {
                        return Err(ObjectError::PreconditionFailed("staged lease deadline changed".into()));
                    }
                    if command.action == 9 {
                        if now < before.expires_at_ms {
                            return Err(ObjectError::PreconditionFailed("staged lease has not expired".into()));
                        }
                        Some(StagedAllocationChange { before: Some(before), after: None })
                    } else {
                        if now >= before.expires_at_ms {
                            return Err(ObjectError::PreconditionFailed("staged allocation lease expired".into()));
                        }
                        let mut after = before.clone();
                        after.expires_at_ms = now.checked_add(command.ttl_ms)
                            .ok_or_else(|| ObjectError::PreconditionFailed("staged deadline overflow".into()))?;
                        if !after.renews(&before) {
                            return Err(ObjectError::PreconditionFailed("renewal must extend the deadline".into()));
                        }
                        reply.allocation = Some(after.clone());
                        Some(StagedAllocationChange { before: Some(before), after: Some(after) })
                    }
                }
            }
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
        outcome.version = if change.before.is_some() && change.after.is_some() { 7 } else { 4 };
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
        10 => {
            value.version = 2;
            value.allocation_cursor = service.placement_commit_index.checked_add(1)
                .ok_or_else(|| ObjectError::PreconditionFailed("allocation cursor exhausted".into()))?;
        }
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
    fn prepared_staging_retries_preserve_one_identity_across_renewal_expiry_and_snapshot() {
        let mut service = MatrixMetaService::new();
        let groups = ChunkGroupCatalog::default();
        let enable = StagedAllocationRequest { version: 1, action: 6, ..Default::default() };
        prepare_at(&service, &groups, &enable, 8192, 1000).unwrap().0.apply(&mut service).unwrap();
        let command = StagedAllocationRequest {
            version: 3, allocation_id: service.placement_commit_index + 1,
            ttl_ms: 1000, request_nonce: vec![7; 32], ..request()
        };
        assert!(prepare_at(&service, &groups, &command, 1, 1000).is_err());
        let (outcome, _) = prepare_at(&service, &groups, &command, 8192, 1000).unwrap();
        PlacementOutcome::from_payload(&outcome.to_payload().unwrap()).unwrap().apply(&mut service).unwrap();
        let id = command.allocation_id;
        let original = service.object_manifests.staged_allocation(id).unwrap().clone();
        // Unrelated allocation advances the metadata position.
        prepare_at(&service, &groups, &request(), 8192, 1000).unwrap().0.apply(&mut service).unwrap();
        let saved = service.export_snapshot().to_bytes().unwrap();
        let (retry, reply) = prepare_at(&service, &groups, &command, 8192, 1500).unwrap();
        assert_eq!(retry.before_index, retry.after_index);
        assert!(retry.staging.is_none());
        assert!(retry.to_payload().unwrap().len() < 256);
        assert!(matches!(reply, MetaReply::StagedAllocation(reply) if reply.allocation == Some(original.clone())));
        retry.apply(&mut service).unwrap();
        assert_eq!(service.export_snapshot().to_bytes().unwrap(), saved);
        for wrong in [
            StagedAllocationRequest { request_nonce: vec![8; 32], ..command.clone() },
            StagedAllocationRequest { page_count: 3, ..command.clone() },
            StagedAllocationRequest { ttl_ms: 2000, ..command.clone() },
            StagedAllocationRequest { key: "another".into(), ..command.clone() },
        ] {
            assert!(prepare_at(&service, &groups, &wrong, 8192, 1500).is_err());
        }
        let renew = StagedAllocationRequest { version: 2, action: 8, allocation_id: id,
            expected_expiry_ms: 2000, ttl_ms: 2000, ..Default::default() };
        prepare_at(&service, &groups, &renew, 8192, 1500).unwrap().0.apply(&mut service).unwrap();
        let renewed = service.object_manifests.staged_allocation(id).unwrap().clone();
        assert_eq!(renewed.expires_at_ms, 3500);
        let mut restored = MatrixMetaService::new();
        let snapshot = crate::MetaSnapshot::from_bytes(&service.export_snapshot().to_bytes().unwrap()).unwrap();
        restored.install_placement_roster(&snapshot).unwrap();
        restored.install_snapshot(snapshot).unwrap();
        let (_, reply) = prepare_at(&restored, &groups, &command, 8192, 2500).unwrap();
        assert!(matches!(reply, MetaReply::StagedAllocation(reply) if reply.allocation == Some(renewed)));
        assert!(prepare_at(&restored, &groups, &command, 8192, 3500).is_err());
        let expire = StagedAllocationRequest { version: 2, action: 9, allocation_id: id,
            expected_expiry_ms: 3500, ..Default::default() };
        prepare_at(&restored, &groups, &expire, 8192, 3500).unwrap().0.apply(&mut restored).unwrap();
        assert!(prepare_at(&restored, &groups, &command, 8192, 3501).is_err());
        assert!(restored.object_manifests.staged_allocation(id).is_none());
        assert!(prepare_at(&restored, &groups, &StagedAllocationRequest {
            allocation_id: restored.placement_commit_index + 2, ..command
        }, 8192, 3501).is_err());
    }

    #[test]
    fn prepared_staging_consumed_positions_never_allocate_after_publication_or_abort() {
        let mut service = MatrixMetaService::new();
        let groups = ChunkGroupCatalog::default();
        let enable = StagedAllocationRequest { version: 1, action: 6, ..Default::default() };
        prepare_at(&service, &groups, &enable, 8192, 0).unwrap().0.apply(&mut service).unwrap();
        for publish in [false, true] {
            let command = StagedAllocationRequest {
                version: 3, allocation_id: service.placement_commit_index + 1,
                request_nonce: vec![7; 32], ..request()
            };
            let competing = StagedAllocationRequest { request_nonce: vec![8; 32], ..command.clone() };
            prepare_at(&service, &groups, &command, 8192, 0).unwrap().0.apply(&mut service).unwrap();
            assert!(prepare_at(&service, &groups, &competing, 8192, 0).is_err());
            let finish = if publish {
                StagedAllocationRequest { version: 1, action: 4, allocation_id: command.allocation_id,
                    outcome: Some(crate::object_manifest::ManifestOutcome::decide(None,
                        crate::object_manifest::ObjectManifest { format_version: 2, bucket: command.bucket.clone(),
                            key: command.key.clone(), generation: 1, ..Default::default() }).unwrap()),
                    ..Default::default() }
            } else { StagedAllocationRequest { version: 1, action: 3, allocation_id: command.allocation_id, ..Default::default() } };
            prepare_at(&service, &groups, &finish, 8192, 0).unwrap().0.apply(&mut service).unwrap();
            let saved = service.export_snapshot().to_bytes().unwrap();
            assert!(prepare_at(&service, &groups, &command, 8192, 0).is_err());
            assert_eq!(service.export_snapshot().to_bytes().unwrap(), saved);
        }
        let stale = StagedAllocationRequest { version: 3, allocation_id: service.placement_commit_index + 1,
            request_nonce: vec![9; 32], key: "other".into(), ..request() };
        // Consume the position with another target, without installing an allocation.
        let next = service.placement_commit_index + 1;
        let outcome = PlacementOutcome::publish_manifest(&service, ManifestOutcome::decide(None,
            ObjectManifest { key: "cursor-consumer".into(), ..empty() }).unwrap(), 0).unwrap();
        outcome.apply(&mut service).unwrap();
        assert_eq!(service.placement_commit_index, next);
        assert!(prepare_at(&service, &groups, &stale, 8192, 0).is_err());
    }

    #[test]
    fn staged_leases_enforce_deadlines_renewal_and_capacity_without_clocked_replay() {
        let mut service = MatrixMetaService::new();
        let groups = Default::default();
        let reserve = StagedAllocationRequest { version: 2, ttl_ms: 1000, ..request() };
        assert!(prepare_at(&service, &groups, &reserve, 8192, 1000).is_err());
        let enable = StagedAllocationRequest { version: 1, action: 6, ..Default::default() };
        prepare_at(&service, &groups, &enable, 8192, 1000).unwrap().0.apply(&mut service).unwrap();
        let mut first = None;
        for _ in 0..crate::staged_allocation::MAX_STAGED_ALLOCATIONS {
            let (outcome, _) = prepare_at(&service, &groups, &reserve, 8192, 1000).unwrap();
            first.get_or_insert(outcome.after_index);
            outcome.apply(&mut service).unwrap();
        }
        assert!(prepare_at(&service, &groups, &reserve, 8192, 1000).is_err());
        let id = first.unwrap();
        let original = service.object_manifests.staged_allocation(id).unwrap().clone();
        assert_eq!(original.expires_at_ms, 2000);
        let renew = StagedAllocationRequest { version: 2, action: 8, allocation_id: id,
            expected_expiry_ms: 2000, ttl_ms: 2000, ..Default::default() };
        let (outcome, _) = prepare_at(&service, &groups, &renew, 8192, 1500).unwrap();
        assert_eq!(outcome.version, 7);
        let mut forged = outcome.clone();
        forged.staging.as_mut().unwrap().after.as_mut().unwrap().page_count += 1;
        assert!(forged.to_payload().is_err());
        PlacementOutcome::from_payload(&outcome.to_payload().unwrap()).unwrap().apply(&mut service).unwrap();
        let renewed = service.object_manifests.staged_allocation(id).unwrap().clone();
        assert_eq!(renewed.expires_at_ms, 3500);
        assert!(prepare_at(&service, &groups, &renew, 8192, 1600).is_err());
        let mut expire = StagedAllocationRequest { version: 2, action: 9, allocation_id: id,
            expected_expiry_ms: 2000, ..Default::default() };
        assert!(prepare_at(&service, &groups, &expire, 8192, 4000).is_err());
        expire.expected_expiry_ms = 3500;
        assert!(prepare_at(&service, &groups, &expire, 8192, 3499).is_err());
        let publish = StagedAllocationRequest { version: 1, action: 4, allocation_id: id,
            outcome: Some(ManifestOutcome::decide(None, empty()).unwrap()), ..Default::default() };
        // Admission at the boundary refuses publication and renewal.
        assert!(prepare_at(&service, &groups, &publish, 8192, 3500).is_err());
        let renew = StagedAllocationRequest { expected_expiry_ms: 3500, ..renew };
        assert!(prepare_at(&service, &groups, &renew, 8192, 3500).is_err());
        // Decided state restores without consulting wall time or expiring other owners.
        let snapshot = crate::MetaSnapshot::from_bytes(&service.export_snapshot().to_bytes().unwrap()).unwrap();
        assert_eq!(snapshot.object_manifests.staged_allocation(id), Some(&renewed));
        prepare_at(&service, &groups, &expire, 8192, 3500).unwrap().0.apply(&mut service).unwrap();
        assert!(service.object_manifests.staged_allocation(id).is_none());
        assert_eq!(prepare_at(&service, &groups, &expire, 8192, 3501).unwrap().0.after_index,
            service.placement_commit_index);
        assert!(prepare_at(&service, &groups, &publish, 8192, 3501).is_err());
        let (next, _) = prepare_at(&service, &groups, &reserve, 8192, 4000).unwrap();
        assert!(next.after_index > id);
        next.apply(&mut service).unwrap();
    }

    #[test]
    fn staged_lease_journal_recovers_renewal_and_decided_expiry() {
        let root = std::env::temp_dir().join(format!("staging-lease-journal-{}", std::process::id()));
        std::fs::create_dir(&root).unwrap();
        struct Cleanup(std::path::PathBuf);
        impl Drop for Cleanup { fn drop(&mut self) { let _ = std::fs::remove_dir_all(&self.0); } }
        let _cleanup = Cleanup(root.clone());
        let mut service = MatrixMetaService::new();
        let (mut storage, _) = super::super::meta_snapshot_file::MetaSnapshotFile::open(&root, &mut service).unwrap();
        storage.persist(&service, &Default::default()).unwrap();
        let enable = StagedAllocationRequest { version: 1, action: 6, ..Default::default() };
        let reserve = StagedAllocationRequest { version: 2, ttl_ms: 1000, ..request() };
        for command in [enable, reserve] {
            let (outcome, _) = prepare_at(&service, &Default::default(), &command, 8192, 1000).unwrap();
            storage.append_outcome(&outcome).unwrap();
            outcome.apply(&mut service).unwrap();
        }
        let id = service.placement_commit_index;
        let renew = StagedAllocationRequest { version: 2, action: 8, allocation_id: id,
            ttl_ms: 2000, expected_expiry_ms: 2000, ..Default::default() };
        let (outcome, _) = prepare_at(&service, &Default::default(), &renew, 8192, 1500).unwrap();
        storage.append_outcome(&outcome).unwrap();
        outcome.apply(&mut service).unwrap();
        drop(storage);
        let mut service = MatrixMetaService::new();
        let (mut storage, recovered) = super::super::meta_snapshot_file::MetaSnapshotFile::open(&root, &mut service).unwrap();
        assert!(recovered);
        assert_eq!(service.object_manifests.staged_allocation(id).unwrap().expires_at_ms, 3500);
        let expire = StagedAllocationRequest { version: 2, action: 9, allocation_id: id,
            expected_expiry_ms: 3500, ..Default::default() };
        let (outcome, _) = prepare_at(&service, &Default::default(), &expire, 8192, 3500).unwrap();
        storage.append_outcome(&outcome).unwrap();
        outcome.apply(&mut service).unwrap();
        drop(storage);
        let mut service = MatrixMetaService::new();
        let (storage, recovered) = super::super::meta_snapshot_file::MetaSnapshotFile::open(&root, &mut service).unwrap();
        assert!(recovered);
        assert!(service.object_manifests.staged_allocation(id).is_none());
        drop(storage);
    }
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
