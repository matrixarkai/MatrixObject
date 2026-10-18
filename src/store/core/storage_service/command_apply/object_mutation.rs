impl MatrixObjectStore {
    fn apply_object_mutation_command(
        &mut self,
        command: ObjectCommand,
        raft_entry_index: Option<u64>,
    ) -> Result<ObjectMetadata, ObjectError> {
        match command {
            ObjectCommand::ApplyPreparedMetaRouteCacheConfig { prepared } => {
                let config = self.apply_prepared_metadata_route_cache_config(prepared)?;
                Ok(metadata_control_metadata(
                    "_metaroutecache",
                    "prepared-config",
                    config.entries as u64,
                    config.ttl_ms,
                    "metaserver/prepared-route-cache-config",
                    self.metadata_route_cache_status().entries,
                ))
            }
            ObjectCommand::ApplyPreparedMetaRetentionConfig { prepared } => {
                let report = self.apply_prepared_metadata_retention_config(prepared)?;
                Ok(metadata_control_metadata(
                    "_metaretention",
                    "prepared-limits",
                    report.change_log_retention_limit,
                    report.mutation_receipt_retention_limit,
                    "metaserver/prepared-retention-config",
                    report
                        .trimmed_change_records
                        .saturating_add(report.trimmed_batch_receipts)
                        .saturating_add(report.trimmed_lease_receipts)
                        .saturating_add(report.trimmed_coordinator_config_receipts)
                        .saturating_add(report.trimmed_coordinator_session_receipts)
                        .saturating_add(report.trimmed_shard_receipts)
                        .saturating_add(report.trimmed_transaction_receipts),
                ))
            }
            ObjectCommand::CompactMetaTombstones {
                prefix,
                after_key,
                max_records,
                max_compact_bytes,
            } => {
                let report = self.compact_metadata_tombstones(
                    &prefix,
                    after_key.as_deref(),
                    max_records,
                    max_compact_bytes,
                )?;
                Ok(metadata_control_metadata(
                    "_metatombstone",
                    &prefix,
                    report.compacted_records,
                    report.compacted_bytes,
                    if report.truncated || report.budget_limited {
                        "metaserver/tombstone-compact-more"
                    } else {
                        "metaserver/tombstone-compact"
                    },
                    report.last_tombstone_compaction_commit_index,
                ))
            }
            ObjectCommand::DeleteMetaByPrefix {
                prefix,
                max_records,
                lease_owner,
                lease_epoch,
                now_ms,
            } => {
                let deleted = self.delete_metadata_by_prefix(
                    &prefix,
                    max_records,
                    lease_owner.as_deref(),
                    lease_epoch,
                    now_ms,
                )?;
                Ok(metadata_control_metadata(
                    "_metaprefix",
                    &prefix,
                    0,
                    deleted.len() as u64,
                    "metaserver/prefix-delete",
                    now_ms,
                ))
            }
            ObjectCommand::CommitObjectChangeCursor {
                consumer_id,
                sequence,
                update_time_ms,
            } => {
                let cursor =
                    self.commit_object_change_cursor(&consumer_id, sequence, update_time_ms)?;
                Ok(metadata_control_metadata(
                    "_objectchange",
                    &cursor.consumer_id,
                    cursor.sequence,
                    0,
                    "objectstore/change-cursor",
                    cursor.update_time_ms,
                ))
            }
            ObjectCommand::CompactObjectChangefeed { max_records } => {
                let report = self.compact_object_changefeed(max_records)?;
                Ok(metadata_control_metadata(
                    "_objectchange",
                    "compact",
                    report.safe_floor_sequence,
                    report.compacted_records,
                    "objectstore/changefeed-compact",
                    report.retained_records,
                ))
            }
            ObjectCommand::ConfigureBucketRetention {
                bucket,
                retain_for_ms,
                update_time_ms,
            } => self.apply_configure_bucket_retention(bucket, retain_for_ms, update_time_ms),
            ObjectCommand::ConfigureBucketRetentionIdempotent {
                bucket,
                retain_for_ms,
                update_time_ms,
                idempotency_key,
            } => self
                .apply_configure_bucket_retention_idempotent(
                    bucket,
                    retain_for_ms,
                    update_time_ms,
                    idempotency_key,
                    raft_entry_index.unwrap_or(0),
                )
                .map(|receipt| receipt.metadata),
            ObjectCommand::ConfigureBucketQuota {
                bucket,
                quota,
                update_time_ms,
            } => self.apply_configure_bucket_quota(bucket, quota, update_time_ms),
            ObjectCommand::ConfigureBucketQuotaIdempotent {
                bucket,
                quota,
                update_time_ms,
                idempotency_key,
            } => self
                .apply_configure_bucket_quota_idempotent(
                    bucket,
                    quota,
                    update_time_ms,
                    idempotency_key,
                    raft_entry_index.unwrap_or(0),
                )
                .map(|receipt| receipt.metadata),
            ObjectCommand::ConfigureBucketLifecycle {
                bucket,
                policy,
                update_time_ms,
            } => self.apply_configure_bucket_lifecycle(bucket, policy, update_time_ms),
            ObjectCommand::ConfigureBucketLifecycleIdempotent {
                bucket,
                policy,
                update_time_ms,
                idempotency_key,
            } => self
                .apply_configure_bucket_lifecycle_idempotent(
                    bucket,
                    policy,
                    update_time_ms,
                    idempotency_key,
                    raft_entry_index.unwrap_or(0),
                )
                .map(|receipt| receipt.metadata),
            ObjectCommand::ConfigureBucketPolicy {
                bucket,
                policy,
                update_time_ms,
            } => self.apply_configure_bucket_policy(bucket, policy, update_time_ms),
            ObjectCommand::ConfigureBucketPolicyIdempotent {
                bucket,
                policy,
                update_time_ms,
                idempotency_key,
            } => self
                .apply_configure_bucket_policy_idempotent(
                    bucket,
                    policy,
                    update_time_ms,
                    idempotency_key,
                    raft_entry_index.unwrap_or(0),
                )
                .map(|receipt| receipt.metadata),
            ObjectCommand::ConfigureBucketTagging {
                bucket,
                tag_set,
                update_time_ms,
            } => self.apply_configure_bucket_tagging(bucket, tag_set, update_time_ms),
            ObjectCommand::ConfigureBucketTaggingIdempotent {
                bucket,
                tag_set,
                update_time_ms,
                idempotency_key,
            } => self
                .apply_configure_bucket_tagging_idempotent(
                    bucket,
                    tag_set,
                    update_time_ms,
                    idempotency_key,
                    raft_entry_index.unwrap_or(0),
                )
                .map(|receipt| receipt.metadata),
            ObjectCommand::ConfigureBucketCors {
                bucket,
                policy,
                update_time_ms,
            } => self.apply_configure_bucket_cors(bucket, policy, update_time_ms),
            ObjectCommand::ConfigureBucketCorsIdempotent {
                bucket,
                policy,
                update_time_ms,
                idempotency_key,
            } => self
                .apply_configure_bucket_cors_idempotent(
                    bucket,
                    policy,
                    update_time_ms,
                    idempotency_key,
                    raft_entry_index.unwrap_or(0),
                )
                .map(|receipt| receipt.report.metadata),
            ObjectCommand::ConfigureBucketVersioningIdempotent {
                bucket,
                enabled,
                update_time_ms,
                idempotency_key,
            } => self
                .apply_set_bucket_versioning_idempotent(
                    bucket,
                    enabled,
                    update_time_ms,
                    idempotency_key,
                    raft_entry_index.unwrap_or(0),
                )
                .map(|receipt| receipt.metadata),
            ObjectCommand::ApplyBucketLifecyclePolicies {
                max_actions,
                now_ms,
            } => {
                let result = self.apply_bucket_lifecycle_policies_at(max_actions, now_ms)?;
                Ok(ObjectMetadata {
                    object_id: ObjectId::new("_lifecycle", "apply"),
                    version: 0,
                    length: result.total_actions(),
                    etag: String::new().into(),
                    content_type: "lifecycle/apply".to_string().into(),
                    user_metadata: BTreeMap::new().into(),
                    storage_class: k_standard_storage_class().to_string().into(),
                    lock: ObjectLock::default(),
                    tags: BTreeMap::new().into(),
                    update_time_ms: now_ms,
                    extents: Vec::new().into(),
                    deleted: false,
                })
            }
            ObjectCommand::ApplyBucketLifecycleTransitions {
                max_actions,
                now_ms,
            } => {
                let result = self.apply_bucket_lifecycle_transitions_at(max_actions, now_ms)?;
                Ok(ObjectMetadata {
                    object_id: ObjectId::new("_lifecycle", "transition"),
                    version: 0,
                    length: result.transitioned_current_objects,
                    etag: String::new().into(),
                    content_type: "lifecycle/transition".to_string().into(),
                    user_metadata: BTreeMap::new().into(),
                    storage_class: k_standard_storage_class().to_string().into(),
                    lock: ObjectLock::default(),
                    tags: BTreeMap::new().into(),
                    update_time_ms: now_ms,
                    extents: Vec::new().into(),
                    deleted: false,
                })
            }
            ObjectCommand::ApplyLegacyBucketLifecycle {
                bucket,
                noncurrent_versions_to_keep,
                now_ms,
            } => {
                let expired =
                    self.apply_lifecycle_at(&bucket, noncurrent_versions_to_keep, now_ms)?;
                Ok(ObjectMetadata {
                    object_id: ObjectId::new("_lifecycle", &bucket),
                    version: 0,
                    length: expired,
                    etag: String::new().into(),
                    content_type: "lifecycle/legacy".to_string().into(),
                    user_metadata: BTreeMap::new().into(),
                    storage_class: k_standard_storage_class().to_string().into(),
                    lock: ObjectLock::default(),
                    tags: BTreeMap::new().into(),
                    update_time_ms: now_ms,
                    extents: Vec::new().into(),
                    deleted: false,
                })
            }
            _ => unreachable!("command classified outside ObjectMutation"),
        }
    }
}
