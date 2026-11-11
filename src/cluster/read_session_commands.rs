// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Durable reader protection with compact creation, renewal and removal.
use super::{meta_message::MetaReply, MetaRequest};
use crate::{
    manifest_retention::{
        ManifestRetentionReply, ManifestRetentionRequest, RetainedManifest, RetentionDeadlineChange,
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
                .ok_or_else(|| refused("current generation cannot open a read session"))?;
            let open = crate::read_session_outcome::ReadSessionOpen {
                retention_id: service
                    .placement_commit_index
                    .checked_add(1)
                    .ok_or_else(|| refused("read session identity exhausted"))?,
                bucket: manifest.bucket.clone(),
                key: manifest.key.clone(),
                generation: manifest.generation,
                expires_at_ms: now
                    .checked_add(command.ttl_ms)
                    .ok_or_else(|| refused("read session deadline overflow"))?,
                manifest_sha256: crate::read_session_outcome::manifest_sha256(manifest)?,
            };
            service.object_manifests.check_read_session_open(&open)?;
            reply.retained = Some(RetainedManifest {
                retention_id: open.retention_id,
                manifest: Some(manifest.clone()),
                expires_at_ms: open.expires_at_ms,
            });
            reply.version = 3;
            reply.lease_ms = command.ttl_ms;
            outcome.version = 9;
            outcome.read_session_open = Some(open);
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
                        outcome.version = 10;
                        outcome.read_session_removal =
                            Some(crate::read_session_outcome::ReadSessionRemoval {
                                retention_id: command.retention_id,
                                expected_expiry_ms: record.expires_at_ms,
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
    if outcome.read_session_open.is_some()
        || outcome.read_session_removal.is_some()
        || outcome.retention_deadline.is_some()
    {
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
    #[test]
    fn compact_read_session_decisions_reject_changed_bindings_and_preserve_captured_snapshots() {
        let mut service = fixture(1);
        let (open, _) = prepare_at(&service, &open(), 8192, 1000).unwrap();
        assert_eq!(open.version, 9);
        let before = service.export_snapshot().to_bytes().unwrap();
        let mut wrong = open.clone();
        wrong.read_session_open.as_mut().unwrap().manifest_sha256[0] ^= 1;
        assert!(wrong.apply(&mut service).is_err());
        assert_eq!(service.export_snapshot().to_bytes().unwrap(), before);
        let mut wrong = open.clone();
        wrong.read_session_open.as_mut().unwrap().generation += 1;
        assert!(wrong.apply(&mut service).is_err());
        assert_eq!(service.export_snapshot().to_bytes().unwrap(), before);
        let mut mixed = open.clone();
        mixed.read_session_removal = Some(crate::read_session_outcome::ReadSessionRemoval {
            retention_id: 1,
            expected_expiry_ms: 2000,
        });
        assert!(mixed.to_payload().is_err());
        PlacementOutcome::from_payload(&open.to_payload().unwrap())
            .unwrap()
            .apply(&mut service)
            .unwrap();
        let id = service.placement_commit_index;
        let captured = service.export_snapshot();
        let captured_bytes = captured.to_bytes().unwrap();
        let release = ManifestRetentionRequest {
            version: 1,
            action: 2,
            retention_id: id,
            ..Default::default()
        };
        let (mut stale_release, _) =
            super::super::meta_server::decide_retention(&service, &release, 8192).unwrap();
        assert_eq!(stale_release.version, 10);
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
        assert_eq!(captured.to_bytes().unwrap(), captured_bytes);
        assert_eq!(
            captured
                .object_manifests
                .retained_manifest(id)
                .unwrap()
                .expires_at_ms,
            2000
        );
        stale_release.before_index = service.placement_commit_index;
        stale_release.after_index = service.placement_commit_index + 1;
        let saved = service.export_snapshot().to_bytes().unwrap();
        assert!(stale_release.apply(&mut service).is_err());
        assert_eq!(service.export_snapshot().to_bytes().unwrap(), saved);
        assert_eq!(
            service
                .object_manifests
                .retained_manifest(id)
                .unwrap()
                .expires_at_ms,
            3500
        );
    }

    #[test]
    fn compact_read_session_journal_size_is_independent_of_extent_count_and_recovers_mixed_history()
    {
        let mut compact_sizes = Vec::new();
        for count in [1, 4096] {
            let root = std::env::temp_dir().join(format!(
                "compact-read-session-{}-{count}",
                std::process::id()
            ));
            std::fs::create_dir(&root).unwrap();
            struct Cleanup(std::path::PathBuf);
            impl Drop for Cleanup {
                fn drop(&mut self) {
                    let _ = std::fs::remove_dir_all(&self.0);
                }
            }
            let _cleanup = Cleanup(root.clone());
            let mut service = fixture(count);
            let (mut storage, _) =
                super::super::meta_snapshot_file::MetaSnapshotFile::open(&root, &mut service)
                    .unwrap();
            storage.persist(&service, &Default::default()).unwrap();
            let path = root.join("metadata.outcomes");
            let size = || std::fs::metadata(&path).unwrap().len();
            let legacy_record = RetainedManifest {
                retention_id: service.placement_commit_index + 1,
                manifest: service
                    .object_manifests
                    .get(&crate::ObjectId::new("bucket", "object"))
                    .cloned(),
                expires_at_ms: 2000,
            };
            let mut legacy = PlacementOutcome::capture(&service, Vec::new(), None).unwrap();
            legacy.version = 3;
            legacy.after_index += 1;
            legacy.retention = Some(crate::manifest_retention::ManifestRetentionChange {
                before: None,
                after: Some(legacy_record.clone()),
            });
            let start = size();
            storage.append_outcome(&legacy).unwrap();
            legacy.apply(&mut service).unwrap();
            let mut legacy_remove = PlacementOutcome::capture(&service, Vec::new(), None).unwrap();
            legacy_remove.version = 3;
            legacy_remove.after_index += 1;
            legacy_remove.retention = Some(crate::manifest_retention::ManifestRetentionChange {
                before: Some(legacy_record),
                after: None,
            });
            storage.append_outcome(&legacy_remove).unwrap();
            legacy_remove.apply(&mut service).unwrap();
            let legacy_bytes = size() - start;
            let (create, _) = prepare_at(&service, &open(), 1024 * 1024, 1000).unwrap();
            assert_eq!(create.version, 9);
            assert!(create.retention.is_none());
            let before = size();
            storage.append_outcome(&create).unwrap();
            create.apply(&mut service).unwrap();
            let creation_bytes = size() - before;
            let id = service.placement_commit_index;
            let renew = ManifestRetentionRequest {
                version: 2,
                action: 6,
                retention_id: id,
                expected_expiry_ms: 2000,
                ttl_ms: 2000,
                ..Default::default()
            };
            let (renewal, _) = prepare_at(&service, &renew, 64, 1500).unwrap();
            storage.append_outcome(&renewal).unwrap();
            renewal.apply(&mut service).unwrap();
            let expected = service.object_manifests.clone();
            drop(storage);
            let mut recovered = MatrixMetaService::new();
            let (mut storage, restored) =
                super::super::meta_snapshot_file::MetaSnapshotFile::open(&root, &mut recovered)
                    .unwrap();
            assert!(restored);
            assert_eq!(recovered.object_manifests, expected);
            let release = ManifestRetentionRequest {
                version: 1,
                action: 2,
                retention_id: id,
                ..Default::default()
            };
            let (remove, _) =
                super::super::meta_server::decide_retention(&recovered, &release, 8192).unwrap();
            assert_eq!(remove.version, 10);
            assert!(remove.retention.is_none());
            let before = size();
            storage.append_outcome(&remove).unwrap();
            remove.apply(&mut recovered).unwrap();
            let removal_bytes = size() - before;
            let (create, _) = prepare_at(&recovered, &open(), 1024 * 1024, 1000).unwrap();
            storage.append_outcome(&create).unwrap();
            create.apply(&mut recovered).unwrap();
            let id = recovered.placement_commit_index;
            let expire = ManifestRetentionRequest {
                version: 2,
                action: 7,
                retention_id: id,
                expected_expiry_ms: 2000,
                ..Default::default()
            };
            let (expiry, _) = prepare_at(&recovered, &expire, 64, 2000).unwrap();
            assert_eq!(expiry.version, 10);
            storage.append_outcome(&expiry).unwrap();
            expiry.apply(&mut recovered).unwrap();
            drop(storage);
            let mut final_state = MatrixMetaService::new();
            let (_storage, restored) =
                super::super::meta_snapshot_file::MetaSnapshotFile::open(&root, &mut final_state)
                    .unwrap();
            assert!(restored);
            assert_eq!(final_state.object_manifests, recovered.object_manifests);
            assert!(final_state
                .object_manifests
                .retention_ids(0, 32)
                .0
                .is_empty());
            let compact_bytes = creation_bytes + removal_bytes;
            assert_eq!(compact_bytes, 150);
            assert_eq!(legacy_bytes, if count == 1 { 252 } else { 466588 });
            compact_sizes.push(compact_bytes);
            println!("extents={count} legacy_cycle_wal_bytes={legacy_bytes} compact_cycle_wal_bytes={compact_bytes}");
        }
        assert_eq!(compact_sizes[0], compact_sizes[1]);
    }
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
