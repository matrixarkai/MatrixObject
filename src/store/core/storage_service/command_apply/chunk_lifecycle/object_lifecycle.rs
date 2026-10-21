// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    fn apply_object_lifecycle_command(&mut self, command: ObjectCommand) -> Result<ObjectMetadata, ObjectError> {
        match command {
            ObjectCommand::DeleteObject {
                bucket,
                key,
                update_time_ms,
                if_match_etag,
            } => self.apply_delete(bucket, key, update_time_ms, if_match_etag),
            ObjectCommand::DeleteObjectWithRootRelease {
                bucket,
                key,
                update_time_ms,
                if_match_etag,
            } => self.apply_delete_with_root_release(bucket, key, update_time_ms, if_match_etag),
            ObjectCommand::BatchDeleteObjects {
                requests,
                max_items,
                release_root_placements,
                update_time_ms,
            } => {
                let report = self.batch_delete_objects(
                    requests,
                    max_items,
                    release_root_placements,
                    update_time_ms,
                )?;
                Ok(metadata_control_metadata(
                    "_batchdelete",
                    "objects",
                    report.deleted_objects,
                    report.failed_objects,
                    "objectstore/batch-delete",
                    report.deleted_bytes,
                ))
            }
            ObjectCommand::DeleteObjectVersion {
                bucket,
                key,
                version,
                update_time_ms,
            } => self.apply_delete_object_version(bucket, key, version, update_time_ms),
            ObjectCommand::DeleteObjectsByPrefix {
                bucket,
                prefix,
                marker,
                max_objects,
                release_root_placements,
                update_time_ms,
                expected_delete_batch_proof_checksum,
            } => {
                let report = self.delete_objects_by_prefix_fenced_with_delete_proof(
                    &bucket,
                    &prefix,
                    marker.as_deref(),
                    max_objects,
                    release_root_placements,
                    update_time_ms,
                    None,
                    expected_delete_batch_proof_checksum.as_deref(),
                )?;
                Ok(metadata_control_metadata(
                    "_prefixdelete",
                    &prefix,
                    report.batch.deleted_objects,
                    report.batch.failed_objects,
                    if report.has_more {
                        "objectstore/prefix-delete-more"
                    } else {
                        "objectstore/prefix-delete"
                    },
                    report
                        .next_marker
                        .as_deref()
                        .map(stable_string_u64)
                        .unwrap_or(0),
                ))
            }
            ObjectCommand::RefreshObjectRootPlacement {
                bucket,
                key,
                replica_count,
                update_time_ms,
            } => {
                self.apply_refresh_object_root_placement(bucket, key, replica_count, update_time_ms)
            }
            ObjectCommand::RefreshAllObjectRootPlacements {
                marker,
                max_objects,
                replica_count,
                update_time_ms,
            } => {
                let report = self.apply_refresh_object_root_placements_page(
                    marker,
                    max_objects,
                    replica_count,
                    update_time_ms,
                )?;
                Ok(metadata_control_metadata(
                    "_rootplacement",
                    if report.has_more {
                        "refresh-all-more"
                    } else {
                        "refresh-all"
                    },
                    report.refreshed_objects,
                    report.scanned_objects,
                    "root/refresh-all-object-placements",
                    update_time_ms,
                ))
            }
            ObjectCommand::CommitRootPlacementMigration {
                plan,
                expected_plan_checksum,
                min_free_bytes,
            } => {
                let placement = self.commit_object_placement_migration_with_min_free(
                    plan,
                    &expected_plan_checksum,
                    min_free_bytes,
                )?;
                Ok(ObjectMetadata {
                    object_id: placement.object_id,
                    version: placement.version,
                    length: placement.length,
                    etag_digest_bits: 0,
                    has_etag: false,
                    content_type: "root/placement-migration".to_string().into(),
                    user_metadata: BTreeMap::new().into(),
                    storage_class: StorageClass::Standard,
                    lock: ObjectLock::default(),
                    tags: BTreeMap::new().into(),
                    update_time_ms: placement.update_time_ms,
                    extents: Vec::new().into(),
                    deleted: false,
                })
            }
            ObjectCommand::SetBucketVersioning { bucket, enabled } => {
                self.apply_set_bucket_versioning(bucket, enabled, now_millis())
            }
            _ => unreachable!("command classified outside ObjectLifecycle"),
        }
    }
}
