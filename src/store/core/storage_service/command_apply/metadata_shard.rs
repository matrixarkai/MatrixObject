// SPDX-License-Identifier: Apache-2.0
impl MatrixObjectStore {
    fn apply_metadata_shard_command(
        &mut self,
        command: ObjectCommand,
        raft_entry_index: Option<u64>,
    ) -> Result<ObjectMetadata, ObjectError> {
        match command {
            ObjectCommand::AbortMultipartUploadIdempotent {
                upload_id,
                update_time_ms,
                idempotency_key,
            } => {
                let upload = self.apply_abort_multipart_upload_idempotent(
                    upload_id,
                    update_time_ms,
                    idempotency_key,
                    raft_entry_index.unwrap_or(0),
                )?;
                Ok(ObjectMetadata {
                    object_id: ObjectId::new("_multipart", upload.upload_id),
                    version: 0,
                    length: upload.total_bytes,
                    etag_digest_bits: 0,
                    has_etag: false,
                    content_type: "multipart/abort-idempotent".to_string().into(),
                    user_metadata: BTreeMap::new().into(),
                    storage_class: StorageClass::Standard,
                    lock: ObjectLock::default(),
                    tags: BTreeMap::new().into(),
                    update_time_ms,
                    extents: Vec::new().into(),
                    deleted: true,
                })
            }
            ObjectCommand::TrimMultipartReceipts {
                retain_complete_receipts,
                retain_abort_receipts,
                update_time_ms,
            } => self.trim_multipart_receipts(
                retain_complete_receipts,
                retain_abort_receipts,
                update_time_ms,
            ),
            ObjectCommand::ConfigureMetaShard { shard } => {
                self.configure_metadata_shard(shard.clone())?;
                Ok(metadata_control_metadata(
                    "_metashard",
                    &shard.shard_id,
                    shard.epoch,
                    0,
                    if shard.draining {
                        "metaserver/shard-draining"
                    } else {
                        "metaserver/shard-serving"
                    },
                    shard.epoch,
                ))
            }
            ObjectCommand::ConfigureMetaShardIdempotent {
                shard,
                idempotency_key,
            } => {
                let receipt =
                    self.configure_metadata_shard_idempotent(shard.clone(), &idempotency_key)?;
                Ok(metadata_control_metadata(
                    "_metashard",
                    &shard.shard_id,
                    shard.epoch,
                    receipt.commit_index,
                    if receipt.deduped {
                        "metaserver/shard-configure-idempotent-deduped"
                    } else if shard.draining {
                        "metaserver/shard-configure-idempotent-draining"
                    } else {
                        "metaserver/shard-configure-idempotent"
                    },
                    receipt.update_time_ms,
                ))
            }
            ObjectCommand::SplitMetaShard {
                source_shard_id,
                split_key,
                left_shard_id,
                right_shard_id,
                left_owner_node_id,
                right_owner_node_id,
                epoch,
                left_draining,
                right_draining,
                now_ms,
            } => {
                let split = self.split_metadata_shard(
                    &source_shard_id,
                    &split_key,
                    &left_shard_id,
                    &right_shard_id,
                    &left_owner_node_id,
                    &right_owner_node_id,
                    epoch,
                    left_draining,
                    right_draining,
                    now_ms,
                )?;
                Ok(metadata_control_metadata(
                    "_metashard",
                    &format!("{}+{}", split.left.shard_id, split.right.shard_id),
                    epoch,
                    self.meta_service.shard_count(),
                    "metaserver/shard-split",
                    now_ms,
                ))
            }
            ObjectCommand::SplitMetaShardIdempotent {
                source_shard_id,
                split_key,
                left_shard_id,
                right_shard_id,
                left_owner_node_id,
                right_owner_node_id,
                epoch,
                left_draining,
                right_draining,
                now_ms,
                idempotency_key,
            } => {
                let receipt = self.split_metadata_shard_idempotent(
                    &source_shard_id,
                    &split_key,
                    &left_shard_id,
                    &right_shard_id,
                    &left_owner_node_id,
                    &right_owner_node_id,
                    epoch,
                    left_draining,
                    right_draining,
                    now_ms,
                    &idempotency_key,
                )?;
                Ok(metadata_control_metadata(
                    "_metashard",
                    &receipt
                        .shards
                        .iter()
                        .map(|shard| shard.shard_id.as_str())
                        .collect::<Vec<_>>()
                        .join("+"),
                    epoch,
                    self.meta_service.shard_count(),
                    if receipt.deduped {
                        "metaserver/shard-split-idempotent-deduped"
                    } else {
                        "metaserver/shard-split-idempotent"
                    },
                    now_ms,
                ))
            }
            ObjectCommand::MergeMetaShards {
                left_shard_id,
                right_shard_id,
                merged_shard_id,
                owner_node_id,
                epoch,
                draining,
                now_ms,
            } => {
                let merged = self.merge_metadata_shards(
                    &left_shard_id,
                    &right_shard_id,
                    &merged_shard_id,
                    &owner_node_id,
                    epoch,
                    draining,
                    now_ms,
                )?;
                Ok(metadata_control_metadata(
                    "_metashard",
                    &merged.shard_id,
                    merged.epoch,
                    self.meta_service.shard_count(),
                    "metaserver/shard-merge",
                    now_ms,
                ))
            }
            ObjectCommand::MergeMetaShardsIdempotent {
                left_shard_id,
                right_shard_id,
                merged_shard_id,
                owner_node_id,
                epoch,
                draining,
                now_ms,
                idempotency_key,
            } => {
                let receipt = self.merge_metadata_shards_idempotent(
                    &left_shard_id,
                    &right_shard_id,
                    &merged_shard_id,
                    &owner_node_id,
                    epoch,
                    draining,
                    now_ms,
                    &idempotency_key,
                )?;
                let shard_id = receipt
                    .shards
                    .first()
                    .map(|shard| shard.shard_id.clone())
                    .unwrap_or_else(|| merged_shard_id.clone());
                Ok(metadata_control_metadata(
                    "_metashard",
                    &shard_id,
                    epoch,
                    self.meta_service.shard_count(),
                    if receipt.deduped {
                        "metaserver/shard-merge-idempotent-deduped"
                    } else {
                        "metaserver/shard-merge-idempotent"
                    },
                    now_ms,
                ))
            }
            ObjectCommand::MigrateMetaShard {
                shard_id,
                new_owner_node_id,
                expected_epoch,
                new_epoch,
                draining,
                now_ms,
            } => {
                let shard = self.migrate_metadata_shard(
                    &shard_id,
                    &new_owner_node_id,
                    expected_epoch,
                    new_epoch,
                    draining,
                    now_ms,
                )?;
                Ok(metadata_control_metadata(
                    "_metashard",
                    &shard.shard_id,
                    shard.epoch,
                    self.meta_service.shard_count(),
                    if shard.draining {
                        "metaserver/shard-migrate-draining"
                    } else {
                        "metaserver/shard-migrate"
                    },
                    now_ms,
                ))
            }
            ObjectCommand::MigrateMetaShardIdempotent {
                shard_id,
                new_owner_node_id,
                expected_epoch,
                new_epoch,
                draining,
                now_ms,
                idempotency_key,
            } => {
                let receipt = self.migrate_metadata_shard_idempotent(
                    &shard_id,
                    &new_owner_node_id,
                    expected_epoch,
                    new_epoch,
                    draining,
                    now_ms,
                    &idempotency_key,
                )?;
                Ok(metadata_control_metadata(
                    "_metashard",
                    &shard_id,
                    new_epoch,
                    self.meta_service.shard_count(),
                    if receipt.deduped {
                        "metaserver/shard-migrate-idempotent-deduped"
                    } else if draining {
                        "metaserver/shard-migrate-idempotent-draining"
                    } else {
                        "metaserver/shard-migrate-idempotent"
                    },
                    now_ms,
                ))
            }
            ObjectCommand::FailoverMetaShards {
                failed_owner_node_id,
                replacement_owner_node_id,
                max_shards,
                now_ms,
                expected_plan_checksum,
                expected_shard_topology_epoch,
                expected_shard_topology_checksum,
                expected_coordinator_session_checksum,
            } => {
                let report = self.execute_metadata_shard_failover_control_fenced(
                    &failed_owner_node_id,
                    &replacement_owner_node_id,
                    max_shards,
                    now_ms,
                    &expected_plan_checksum,
                    expected_shard_topology_epoch,
                    non_empty_str(&expected_shard_topology_checksum),
                    non_empty_str(&expected_coordinator_session_checksum),
                )?;
                Ok(metadata_control_metadata(
                    "_metashard",
                    "failover",
                    report.migrated_shards,
                    report.planned_shards,
                    "metaserver/shard-failover",
                    now_ms,
                ))
            }
            ObjectCommand::ExpiredMetaShardOwnerFailover {
                expired_owner_node_id,
                replacement_owner_node_id,
                max_shards,
                now_ms,
                expected_plan_checksum,
                expected_shard_topology_epoch,
                expected_shard_topology_checksum,
                expected_coordinator_session_checksum,
            } => {
                let report = self.execute_expired_metadata_shard_owner_failover_control_fenced(
                    &expired_owner_node_id,
                    &replacement_owner_node_id,
                    max_shards,
                    now_ms,
                    &expected_plan_checksum,
                    expected_shard_topology_epoch,
                    non_empty_str(&expected_shard_topology_checksum),
                    non_empty_str(&expected_coordinator_session_checksum),
                )?;
                Ok(metadata_control_metadata(
                    "_metashard",
                    "expired-owner-failover",
                    report.migrated_shards,
                    report.planned_shards,
                    "metaserver/shard-expired-owner-failover",
                    now_ms,
                ))
            }
            ObjectCommand::RebalanceMetaShards {
                source_owner_node_id,
                target_owner_node_id,
                max_shards,
                now_ms,
                expected_plan_checksum,
                expected_shard_topology_epoch,
                expected_shard_topology_checksum,
                expected_coordinator_session_checksum,
            } => {
                let report = self.execute_metadata_shard_rebalance_control_fenced(
                    &source_owner_node_id,
                    &target_owner_node_id,
                    max_shards,
                    now_ms,
                    &expected_plan_checksum,
                    expected_shard_topology_epoch,
                    non_empty_str(&expected_shard_topology_checksum),
                    non_empty_str(&expected_coordinator_session_checksum),
                )?;
                Ok(metadata_control_metadata(
                    "_metashard",
                    "rebalance",
                    report.migrated_shards,
                    report.planned_shards,
                    "metaserver/shard-rebalance",
                    now_ms,
                ))
            }
            ObjectCommand::SplitOversizedMetaShards {
                max_records_per_shard,
                max_payload_bytes_per_shard,
                max_splits,
                now_ms,
                expected_plan_checksum,
                expected_shard_topology_epoch,
                expected_shard_topology_checksum,
            } => {
                let report = self.split_oversized_metadata_shards_fenced(
                    max_records_per_shard,
                    max_payload_bytes_per_shard,
                    max_splits,
                    now_ms,
                    &expected_plan_checksum,
                    expected_shard_topology_epoch,
                    non_empty_str(&expected_shard_topology_checksum),
                )?;
                Ok(metadata_control_metadata(
                    "_metashard",
                    "autosplit",
                    report.split_shards,
                    report.planned_shards,
                    "metaserver/shard-autosplit",
                    now_ms,
                ))
            }
            ObjectCommand::ApplyPreparedMetaShardMigrationPlan { prepared } => {
                let operation = prepared.operation.clone();
                let now_ms = prepared.now_ms;
                let report = self.apply_prepared_metadata_shard_migration_plan(prepared)?;
                Ok(metadata_control_metadata(
                    "_metashard",
                    &operation,
                    report.migrated_shards,
                    report.planned_shards,
                    match operation.as_str() {
                        "failover" => "metaserver/prepared-shard-failover",
                        "rebalance" => "metaserver/prepared-shard-rebalance",
                        _ => "metaserver/prepared-shard-migration",
                    },
                    now_ms,
                ))
            }
            _ => unreachable!("command classified outside MetadataShard"),
        }
    }
}
