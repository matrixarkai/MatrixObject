impl MatrixObjectStore {
    fn apply_object_write_command(&mut self, command: ObjectCommand) -> Result<ObjectMetadata, ObjectError> {
        match command {
            ObjectCommand::PutObject {
                bucket,
                key,
                data,
                content_type,
                update_time_ms,
                options,
            } => self.apply_put_storage_batched(
                bucket,
                key,
                data,
                content_type,
                update_time_ms,
                options,
                "put_object",
            ),
            ObjectCommand::AppendObject {
                bucket,
                key,
                data,
                content_type,
                update_time_ms,
                options,
            } => self.apply_append(bucket, key, data, content_type, update_time_ms, options),
            ObjectCommand::AppendObjectWithRootPlacement {
                bucket,
                key,
                data,
                content_type,
                update_time_ms,
                options,
                replica_count,
            } => self.apply_append_with_root_placement(
                bucket,
                key,
                data,
                content_type,
                update_time_ms,
                options,
                replica_count,
            ),
            ObjectCommand::BatchPutObjects {
                requests,
                max_items,
                max_payload_bytes,
                update_time_ms,
            } => {
                let report = self.batch_put_objects_at(
                    requests,
                    max_items,
                    max_payload_bytes,
                    update_time_ms,
                    None,
                )?;
                Ok(metadata_control_metadata(
                    "_batchput",
                    "objects",
                    report.put_objects,
                    report.failed_objects,
                    "objectstore/batch-put",
                    report.payload_bytes,
                ))
            }
            ObjectCommand::BatchPutObjectsWithRootPlacement {
                requests,
                max_items,
                max_payload_bytes,
                update_time_ms,
                replica_count,
            } => {
                let report = self.batch_put_objects_with_root_placement_at(
                    requests,
                    max_items,
                    max_payload_bytes,
                    update_time_ms,
                    replica_count,
                    None,
                )?;
                Ok(metadata_control_metadata(
                    "_batchput",
                    "objects-root-placement",
                    report.put_objects,
                    report.failed_objects,
                    "objectstore/batch-put-root-placement",
                    report.payload_bytes,
                ))
            }
            ObjectCommand::CopyObject {
                source_bucket,
                source_key,
                source_version,
                destination_bucket,
                destination_key,
                content_type,
                update_time_ms,
                options,
            } => self.apply_copy(
                source_bucket,
                source_key,
                source_version,
                destination_bucket,
                destination_key,
                content_type,
                update_time_ms,
                options,
            ),
            ObjectCommand::CopyObjectWithRootPlacement {
                source_bucket,
                source_key,
                source_version,
                destination_bucket,
                destination_key,
                content_type,
                update_time_ms,
                options,
                replica_count,
            } => self.apply_copy_with_root_placement(
                source_bucket,
                source_key,
                source_version,
                destination_bucket,
                destination_key,
                content_type,
                update_time_ms,
                options,
                replica_count,
            ),
            ObjectCommand::BatchCopyObjects {
                requests,
                max_items,
                max_copied_bytes,
                update_time_ms,
            } => {
                let report =
                    self.batch_copy_objects(requests, max_items, max_copied_bytes, update_time_ms)?;
                Ok(metadata_control_metadata(
                    "_batchcopy",
                    "objects",
                    report.copied_objects,
                    report.failed_objects,
                    "objectstore/batch-copy",
                    report.copied_bytes,
                ))
            }
            ObjectCommand::BatchCopyObjectsWithRootPlacement {
                requests,
                max_items,
                max_copied_bytes,
                update_time_ms,
                replica_count,
            } => {
                let report = self.batch_copy_objects_with_root_placement(
                    requests,
                    max_items,
                    max_copied_bytes,
                    replica_count,
                    update_time_ms,
                )?;
                Ok(metadata_control_metadata(
                    "_batchcopy",
                    "objects-root-placement",
                    report.copied_objects,
                    report.failed_objects,
                    "objectstore/batch-copy-root-placement",
                    report.copied_bytes,
                ))
            }
            ObjectCommand::SetObjectTags {
                bucket,
                key,
                tags,
                update_time_ms,
            } => self.apply_set_object_tags(bucket, key, tags, update_time_ms),
            ObjectCommand::SetObjectLegalHold {
                bucket,
                key,
                enabled,
                update_time_ms,
            } => self.apply_set_object_legal_hold(bucket, key, enabled, update_time_ms),
            ObjectCommand::SetObjectRetention {
                bucket,
                key,
                retention_until_ms,
                update_time_ms,
            } => self.apply_set_object_retention(bucket, key, retention_until_ms, update_time_ms),
            ObjectCommand::TransitionObject {
                bucket,
                key,
                storage_class,
                update_time_ms,
            } => self.apply_transition_object(bucket, key, storage_class, update_time_ms, None),
            ObjectCommand::TransitionObjectWithRootPlacement {
                bucket,
                key,
                storage_class,
                update_time_ms,
                replica_count,
            } => self.apply_transition_object_with_root_placement(
                bucket,
                key,
                storage_class,
                update_time_ms,
                replica_count,
                None,
            ),
            ObjectCommand::RestoreObject {
                bucket,
                key,
                restore_until_ms,
                update_time_ms,
            } => self.apply_restore_object(bucket, key, restore_until_ms, update_time_ms),
            ObjectCommand::RestoreObjectVersion {
                bucket,
                key,
                version,
                update_time_ms,
            } => self.apply_restore_object_version(bucket, key, version, update_time_ms),
            ObjectCommand::RestoreObjectVersionWithRootPlacement {
                bucket,
                key,
                version,
                update_time_ms,
                replica_count,
            } => self.apply_restore_object_version_with_root_placement(
                bucket,
                key,
                version,
                update_time_ms,
                replica_count,
            ),
            ObjectCommand::PutObjectWithRootPlacement {
                bucket,
                key,
                data,
                content_type,
                update_time_ms,
                options,
                replica_count,
            } => self.apply_put_with_root_placement(
                bucket,
                key,
                data,
                content_type,
                update_time_ms,
                options,
                replica_count,
            ),
            _ => unreachable!("command classified outside ObjectWrite"),
        }
    }
}
