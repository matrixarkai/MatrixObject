// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Select a bounded set of expired ownership records, without mutating state.
use crate::{staged_allocation::StagedAllocationRequest, MatrixMetaService, ObjectError};
use std::time::Duration;

pub(super) const INTERVAL: Duration = Duration::from_secs(1);
pub(super) const BATCH: usize = 4;

pub(super) fn now_ms() -> Result<u64, ObjectError> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| ObjectError::PreconditionFailed("metadata clock precedes epoch".into()))?
        .as_millis()
        .try_into()
        .map_err(|_| ObjectError::PreconditionFailed("metadata clock exceeds lease range".into()))
}

pub(super) fn commands(service: &MatrixMetaService, now: u64) -> Vec<StagedAllocationRequest> {
    service
        .object_manifests
        .expired_staged(now)
        .take(BATCH)
        .map(
            |(allocation_id, expected_expiry_ms)| StagedAllocationRequest {
                version: 2,
                action: 9,
                allocation_id,
                expected_expiry_ms,
                ..Default::default()
            },
        )
        .collect()
}

pub(super) fn all_commands(service: &MatrixMetaService, now: u64) -> Vec<super::MetaRequest> {
    let mut staged = commands(service, now).into_iter().map(super::MetaRequest::StagedAllocation);
    let mut readers = super::read_session_commands::expired(service, now).into_iter();
    (0..BATCH).filter_map(|index| if index % 2 == 0 {
        staged.next().or_else(|| readers.next())
    } else { readers.next().or_else(|| staged.next()) }).collect()
}

#[cfg(feature = "metadata-consensus")]
pub(super) fn same_cleanup(left: &super::MetaRequest, right: &super::MetaRequest) -> bool {
    match (left, right) {
        (super::MetaRequest::StagedAllocation(left), super::MetaRequest::StagedAllocation(right)) =>
            left.action == 9 && right.action == 9 && left.allocation_id == right.allocation_id,
        (super::MetaRequest::ManifestRetention(left), super::MetaRequest::ManifestRetention(right)) =>
            left.action == 7 && right.action == 7 && left.retention_id == right.retention_id,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::staged_allocation::StagedAllocation;
    #[test]
    fn read_session_and_staged_cleanup_share_a_bounded_fair_budget() {
        use crate::manifest_retention::RetainedManifest;
        use crate::object_manifest::ObjectManifest;
        let mut service = MatrixMetaService::new();
        service.object_manifests.restore_publication_fence(1);
        for id in 1..=128 {
            service.object_manifests.restore_retention(id, Some(RetainedManifest {
                retention_id: id, expires_at_ms: if id == 1 { 0 } else if id == 2 { 4000 } else { 2000 },
                manifest: Some(ObjectManifest { format_version: 2, bucket: "bucket".into(),
                    key: format!("read-{id}"), generation: 1, ..Default::default() }),
            }));
            service.object_manifests.restore_staging(id + 128, Some(StagedAllocation {
                version: if id == 1 { 1 } else { 2 }, allocation_id: id + 128, page_count: 1,
                bucket: "bucket".into(), key: format!("write-{id}"),
                expires_at_ms: if id == 1 { 0 } else if id == 2 { 4000 } else { 2000 },
                ..Default::default()
            }));
        }
        service.placement_commit_index = 256;
        let mut readers = 0;
        let mut writers = 0;
        loop {
            let selected = all_commands(&service, 2000);
            assert!(selected.len() <= BATCH);
            if selected.is_empty() { break; }
            assert!(matches!(selected[0], super::super::MetaRequest::StagedAllocation(_)));
            assert!(matches!(selected[1], super::super::MetaRequest::ManifestRetention(_)));
            for command in selected {
                match command {
                    super::super::MetaRequest::StagedAllocation(command) => {
                        super::super::staged_allocation_commands::prepare_at(&service, &Default::default(),
                            &command, 8192, 2000).unwrap().0.apply(&mut service).unwrap();
                        writers += 1;
                    }
                    super::super::MetaRequest::ManifestRetention(command) => {
                        super::super::read_session_commands::prepare_at(&service, &command, 8192, 2000)
                            .unwrap().0.apply(&mut service).unwrap();
                        readers += 1;
                    }
                    _ => panic!(),
                }
            }
        }
        assert_eq!((readers, writers), (126, 126));
        assert!(service.object_manifests.retained_manifest(1).is_some());
        assert!(service.object_manifests.retained_manifest(2).is_some());
        assert!(service.object_manifests.staged_allocation(129).is_some());
        assert!(service.object_manifests.staged_allocation(130).is_some());
        assert_eq!(all_commands(&service, 4000).len(), 2);
    }

    #[test]
    fn automatic_expiry_selection_is_bounded_preserves_live_and_permanent_owners_and_drains() {
        let mut service = MatrixMetaService::new();
        service.object_manifests.restore_publication_fence(1);
        for id in 2..=129 {
            let record = StagedAllocation {
                version: if id == 2 { 1 } else { 2 },
                allocation_id: id,
                page_count: 1,
                bucket: "bucket".into(),
                key: format!("key-{id}"),
                expires_at_ms: if id == 2 {
                    0
                } else if id == 3 {
                    4000
                } else {
                    2000
                },
                ..Default::default()
            };
            service.object_manifests.restore_staging(id, Some(record));
        }
        service.placement_commit_index = 129;
        let saved = service.export_snapshot().to_bytes().unwrap();
        assert!(commands(&service, 1999).is_empty());
        assert_eq!(
            commands(&service, 2000)
                .iter()
                .map(|c| c.allocation_id)
                .collect::<Vec<_>>(),
            [4, 5, 6, 7]
        );
        assert_eq!(service.export_snapshot().to_bytes().unwrap(), saved);
        let mut removed = 0;
        loop {
            let commands = commands(&service, 2000);
            assert!(commands.len() <= BATCH);
            if commands.is_empty() {
                break;
            }
            for command in commands {
                let (outcome, _) = super::super::staged_allocation_commands::prepare_at(
                    &service,
                    &Default::default(),
                    &command,
                    8192,
                    2000,
                )
                .unwrap();
                outcome.apply(&mut service).unwrap();
                removed += 1;
            }
        }
        assert_eq!(removed, 126);
        assert!(service.object_manifests.staged_allocation(2).is_some());
        assert!(service.object_manifests.staged_allocation(3).is_some());
        assert_eq!(commands(&service, 4000).len(), 1);
    }
}
