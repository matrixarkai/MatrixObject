// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Durable reader protection and compact renewal decisions.
use super::{meta_message::MetaReply, MetaRequest};
use crate::{
    manifest_retention::{
        ManifestRetentionChange, ManifestRetentionReply, ManifestRetentionRequest,
        RetainedManifest, RetentionDeadlineChange,
    },
    placement_outcome::PlacementOutcome,
    MatrixMetaService, ObjectError,
};

fn refused(message: &str) -> ObjectError {
    ObjectError::PreconditionFailed(message.into())
}

pub(super) fn remaining(record: &RetainedManifest) -> Result<u64, ObjectError> {
    if record.expires_at_ms == 0 {
        return Ok(0);
    }
    let now = super::staged_lease_sweeper::now_ms()?;
    if now == 0 || now >= record.expires_at_ms {
        return Err(refused("read session expired"));
    }
    Ok((record.expires_at_ms - now).min(3_600_000))
}

pub(super) fn prepare_at(
    service: &MatrixMetaService,
    command: &ManifestRetentionRequest,
    limit: usize,
    now: u64,
) -> Result<(PlacementOutcome, MetaReply), ObjectError> {
    command.validate()?;
    if command.version != 2 || now == 0 {
        return Err(refused("read session requires a valid metadata clock"));
    }
    let mut outcome = PlacementOutcome::capture(service, Vec::new(), None)?;
    let mut reply = ManifestRetentionReply {
        version: 1,
        ..Default::default()
    };
    match command.action {
        5 => {
            let manifest = service
                .object_manifests
                .get(&super::meta_message::object_id(
                    &command.bucket,
                    &command.key,
                ))
                .filter(|manifest| {
                    !manifest.deleted
                        && (command.generation == 0 || command.generation == manifest.generation)
                })
                .cloned()
                .ok_or_else(|| refused("current generation cannot open a read session"))?;
            let record = RetainedManifest {
                retention_id: service
                    .placement_commit_index
                    .checked_add(1)
                    .ok_or_else(|| refused("read session identity exhausted"))?,
                manifest: Some(manifest),
                expires_at_ms: now
                    .checked_add(command.ttl_ms)
                    .ok_or_else(|| refused("read session deadline overflow"))?,
            };
            let change = ManifestRetentionChange {
                before: None,
                after: Some(record.clone()),
            };
            service.object_manifests.check_retention(&change)?;
            reply.retained = Some(record);
            reply.version = 3;
            reply.lease_ms = command.ttl_ms;
            outcome.version = 3;
            outcome.retention = Some(change);
        }
        6 | 7 => {
            if command.retention_id > service.placement_commit_index {
                return Err(refused("read session is from a future position"));
            }
            match service
                .object_manifests
                .retained_manifest(command.retention_id)
            {
                None if command.action == 7 => {}
                None => return Err(ObjectError::NotFound("read session missing".into())),
                Some(record) => {
                    if record.expires_at_ms == 0
                        || record.expires_at_ms != command.expected_expiry_ms
                    {
                        return Err(refused("read session deadline changed"));
                    }
                    if command.action == 7 {
                        if now < record.expires_at_ms {
                            return Err(refused("read session has not expired"));
                        }
                        outcome.version = 3;
                        outcome.retention = Some(ManifestRetentionChange {
                            before: Some(record.clone()),
                            after: None,
                        });
                    } else {
                        if now >= record.expires_at_ms {
                            return Err(refused("read session expired"));
                        }
                        let after = now
                            .checked_add(command.ttl_ms)
                            .ok_or_else(|| refused("read session deadline overflow"))?;
                        let change = RetentionDeadlineChange {
                            retention_id: command.retention_id,
                            before: record.expires_at_ms,
                            after,
                        };
                        service.object_manifests.check_retention_deadline(&change)?;
                        outcome.version = 8;
                        outcome.retention_deadline = Some(change);
                        reply.version = 2;
                        reply.retention_id = command.retention_id;
                        reply.expires_at_ms = after;
                    }
                }
            }
        }
        _ => return Err(refused("read session requires a lease mutation")),
    }
    if outcome.retention.is_some() || outcome.retention_deadline.is_some() {
        outcome.after_index = outcome
            .before_index
            .checked_add(1)
            .ok_or_else(|| refused("metadata position exhausted"))?;
    }
    outcome.to_payload()?;
    let reply = MetaReply::ManifestRetention(reply);
    super::message::check_reply_size(reply.payload_len(), limit)?;
    Ok((outcome, reply))
}

pub(super) fn expired(service: &MatrixMetaService, now: u64) -> Vec<MetaRequest> {
    service
        .object_manifests
        .expired_retention(now)
        .take(super::staged_lease_sweeper::BATCH)
        .map(|(retention_id, expected_expiry_ms)| {
            MetaRequest::ManifestRetention(ManifestRetentionRequest {
                version: 2,
                action: 7,
                retention_id,
                expected_expiry_ms,
                ..Default::default()
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object_manifest::{ObjectExtent, ObjectManifest};
    fn fixture(count: u64) -> MatrixMetaService {
        let mut service = MatrixMetaService::new();
        let manifest = ObjectManifest {
            format_version: 2,
            bucket: "bucket".into(),
            key: "object".into(),
            generation: 1,
            length: count,
            extents: (0..count)
                .map(|index| ObjectExtent {
                    group: "archive".into(),
                    chunk_id: 1,
                    page_index: index,
                    object_offset: index,
                    length: 1,
                    write_sequence: 1,
                    data_sha256: vec![7; 32],
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        manifest.validate().unwrap();
        service
            .object_manifests
            .apply(&crate::object_manifest::ManifestOutcome::decide(None, manifest).unwrap())
            .unwrap();
        service
    }
    fn open() -> ManifestRetentionRequest {
        ManifestRetentionRequest {
            version: 2,
            action: 5,
            bucket: "bucket".into(),
            key: "object".into(),
            ttl_ms: 1000,
            ..Default::default()
        }
    }
    #[test]
    fn read_session_deadlines_preserve_old_generations_and_reject_stale_expiry() {
        let mut service = fixture(1);
        assert!(prepare_at(&service, &open(), 1, 1000).is_err());
        assert!(prepare_at(&service, &open(), 8192, 0).is_err());
        let (outcome, reply) = prepare_at(&service, &open(), 8192, 1000).unwrap();
        PlacementOutcome::from_payload(&outcome.to_payload().unwrap())
            .unwrap()
            .apply(&mut service)
            .unwrap();
        let MetaReply::ManifestRetention(reply) = reply else {
            panic!()
        };
        let original = reply.retained.unwrap();
        assert_eq!(original.expires_at_ms, 2000);
        let manifest = original.manifest.as_ref().unwrap();
        service
            .object_manifests
            .apply(
                &crate::object_manifest::ManifestOutcome::decide(
                    Some(manifest.clone()),
                    ObjectManifest {
                        generation: 2,
                        deleted: true,
                        length: 0,
                        extents: Vec::new(),
                        ..manifest.clone()
                    },
                )
                .unwrap(),
            )
            .unwrap();
        let renew = ManifestRetentionRequest {
            version: 2,
            action: 6,
            retention_id: original.retention_id,
            ttl_ms: 2000,
            expected_expiry_ms: 2000,
            ..Default::default()
        };
        let (renewal, _) = prepare_at(&service, &renew, 8192, 1500).unwrap();
        renewal.apply(&mut service).unwrap();
        assert_eq!(
            service
                .object_manifests
                .retained_manifest(original.retention_id)
                .unwrap()
                .manifest,
            original.manifest
        );
        assert!(prepare_at(&service, &renew, 8192, 1600).is_err());
        let expiry = ManifestRetentionRequest {
            version: 2,
            action: 7,
            retention_id: original.retention_id,
            expected_expiry_ms: 2000,
            ..Default::default()
        };
        assert!(prepare_at(&service, &expiry, 8192, 4000).is_err());
        let expiry = ManifestRetentionRequest {
            expected_expiry_ms: 3500,
            ..expiry
        };
        assert!(prepare_at(&service, &expiry, 8192, 3499).is_err());
        assert!(prepare_at(
            &service,
            &ManifestRetentionRequest {
                expected_expiry_ms: 3500,
                ..renew
            },
            8192,
            3500
        )
        .is_err());
        let (removal, _) = prepare_at(&service, &expiry, 8192, 3500).unwrap();
        removal.apply(&mut service).unwrap();
        assert!(service
            .object_manifests
            .retained_manifest(original.retention_id)
            .is_none());
        assert_eq!(
            prepare_at(&service, &expiry, 8192, 3501)
                .unwrap()
                .0
                .after_index,
            service.placement_commit_index
        );
        assert!(prepare_at(&service, &open(), 8192, 4000).is_err());
    }
    #[test]
    fn read_session_renewal_stays_small_with_large_manifests_and_replays_without_a_clock() {
        for count in [1, 4096] {
            let mut service = fixture(count);
            let (outcome, reply) = prepare_at(&service, &open(), 1024 * 1024, 1000).unwrap();
            outcome.apply(&mut service).unwrap();
            let MetaReply::ManifestRetention(reply) = reply else {
                panic!()
            };
            let original = reply.retained.unwrap();
            let renew = ManifestRetentionRequest {
                version: 2,
                action: 6,
                retention_id: original.retention_id,
                expected_expiry_ms: 2000,
                ttl_ms: 2000,
                ..Default::default()
            };
            let (outcome, reply) = prepare_at(&service, &renew, 64, 1500).unwrap();
            assert_eq!(outcome.version, 8);
            assert!(outcome.retention.is_none());
            assert!(outcome.to_payload().unwrap().len() < 128);
            assert!(reply.payload_len() < 32);
            for cut in 0..outcome.to_payload().unwrap().len() {
                assert!(
                    PlacementOutcome::from_payload(&outcome.to_payload().unwrap()[..cut]).is_err()
                );
            }
            PlacementOutcome::from_payload(&outcome.to_payload().unwrap())
                .unwrap()
                .apply(&mut service)
                .unwrap();
            let snapshot =
                crate::MetaSnapshot::from_bytes(&service.export_snapshot().to_bytes().unwrap())
                    .unwrap();
            let mut recovered = MatrixMetaService::new();
            recovered.install_placement_roster(&snapshot).unwrap();
            recovered.install_snapshot(snapshot).unwrap();
            assert_eq!(
                recovered
                    .object_manifests
                    .retained_manifest(original.retention_id)
                    .unwrap()
                    .expires_at_ms,
                3500
            );
            assert_eq!(
                recovered.object_manifests.to_bytes().unwrap(),
                service.object_manifests.to_bytes().unwrap()
            );
            assert!(outcome.apply(&mut recovered).is_err());
        }
    }
    #[test]
    fn read_session_renewal_works_at_capacity_and_rejects_permanent_conversion() {
        let mut service = fixture(1);
        for _ in 0..crate::manifest_retention::MAX_RETAINED_MANIFESTS {
            prepare_at(&service, &open(), 8192, 1000)
                .unwrap()
                .0
                .apply(&mut service)
                .unwrap();
        }
        let saved = service.export_snapshot().to_bytes().unwrap();
        assert!(prepare_at(&service, &open(), 8192, 1000).is_err());
        assert_eq!(service.export_snapshot().to_bytes().unwrap(), saved);
        let id = service.object_manifests.retention_ids(0, 1).0[0];
        let renew = ManifestRetentionRequest {
            version: 2,
            action: 6,
            retention_id: id,
            expected_expiry_ms: 2000,
            ttl_ms: 2000,
            ..Default::default()
        };
        prepare_at(&service, &renew, 64, 1500)
            .unwrap()
            .0
            .apply(&mut service)
            .unwrap();
        let mut record = service
            .object_manifests
            .retained_manifest(id)
            .unwrap()
            .clone();
        record.expires_at_ms = 0;
        service.object_manifests.restore_retention(id, Some(record));
        assert!(prepare_at(
            &service,
            &ManifestRetentionRequest {
                expected_expiry_ms: 3500,
                ..renew
            },
            64,
            1600
        )
        .is_err());
    }
}
