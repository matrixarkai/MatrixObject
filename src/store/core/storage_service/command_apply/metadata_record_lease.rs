// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    fn apply_metadata_record_lease_command(
        &mut self,
        command: ObjectCommand,
        _raft_entry_index: Option<u64>,
    ) -> Result<ObjectMetadata, ObjectError> {
        match command {
            ObjectCommand::ApplyPreparedMetaShardSplit { prepared } => {
                let now_ms = prepared.now_ms;
                let split = self.apply_prepared_metadata_shard_split(prepared)?;
                Ok(metadata_control_metadata(
                    "_metashard",
                    &format!("{}+{}", split.left.shard_id, split.right.shard_id),
                    split.left.epoch.max(split.right.epoch),
                    self.meta_service.shard_count(),
                    "metaserver/prepared-shard-split",
                    now_ms,
                ))
            }
            ObjectCommand::ApplyPreparedMetaShardMerge { prepared } => {
                let now_ms = prepared.now_ms;
                let merged = self.apply_prepared_metadata_shard_merge(prepared)?;
                Ok(metadata_control_metadata(
                    "_metashard",
                    &merged.shard_id,
                    merged.epoch,
                    self.meta_service.shard_count(),
                    "metaserver/prepared-shard-merge",
                    now_ms,
                ))
            }
            ObjectCommand::ApplyPreparedMetaShardMigration { prepared } => {
                let now_ms = prepared.now_ms;
                let shard = self.apply_prepared_metadata_shard_migration(prepared)?;
                Ok(metadata_control_metadata(
                    "_metashard",
                    &shard.shard_id,
                    shard.epoch,
                    self.meta_service.shard_count(),
                    if shard.draining {
                        "metaserver/prepared-shard-migration-draining"
                    } else {
                        "metaserver/prepared-shard-migration"
                    },
                    now_ms,
                ))
            }
            ObjectCommand::ApplyPreparedMetaShardRecordHandoff { prepared } => {
                let shard_id = prepared.shard.shard_id.clone();
                let record_count = prepared.records.len() as u64;
                let receipt = self.apply_prepared_metadata_shard_record_handoff(prepared)?;
                Ok(metadata_control_metadata(
                    "_metashard",
                    &shard_id,
                    record_count,
                    receipt.first_commit_index,
                    if receipt.deduped {
                        "metaserver/prepared-shard-record-handoff-deduped"
                    } else {
                        "metaserver/prepared-shard-record-handoff"
                    },
                    receipt.now_ms,
                ))
            }
            ObjectCommand::CheckMetaReadSet { conditions, now_ms } => {
                let report = self.check_metadata_read_set(conditions)?;
                Ok(metadata_control_metadata(
                    "_metareadset",
                    "check",
                    report.checked_records,
                    report.matched_records,
                    "metaserver/readset-check",
                    now_ms.max(report.max_version),
                ))
            }
            ObjectCommand::CommitMetaBatchIdempotent {
                mutations,
                lease_owner,
                lease_epoch,
                now_ms,
                idempotency_key,
            } => {
                let (records, receipt) = self.commit_metadata_batch_idempotent(
                    mutations,
                    lease_owner.as_deref(),
                    lease_epoch,
                    now_ms,
                    &idempotency_key,
                )?;
                Ok(metadata_control_metadata(
                    "_metawrite",
                    "generic",
                    records.len() as u64,
                    receipt.deduped as u64,
                    "metaserver/idempotent-batch",
                    receipt.last_commit_index,
                ))
            }
            ObjectCommand::ApplyPreparedMetaBatch { prepared } => {
                let (records, receipt) = self.meta_service.apply_prepared_batch(prepared)?;
                Ok(metadata_control_metadata(
                    "_metawrite",
                    "prepared-generic",
                    records.len() as u64,
                    receipt.deduped as u64,
                    "metaserver/prepared-batch",
                    receipt.last_commit_index,
                ))
            }
            ObjectCommand::ApplyPreparedMetaTransaction { prepared } => {
                let receipt = self.meta_service.apply_prepared_transaction(prepared)?;
                Ok(metadata_control_metadata(
                    "_metatxn",
                    &receipt.transaction.transaction_id,
                    receipt.commit_index,
                    receipt.deduped as u64,
                    if receipt.deduped {
                        "metaserver/prepared-transaction-deduped"
                    } else {
                        "metaserver/prepared-transaction"
                    },
                    receipt.update_time_ms,
                ))
            }
            ObjectCommand::ApplyPreparedMetaTransactionGc { prepared } => {
                let report = self.apply_prepared_metadata_transaction_gc(prepared)?;
                Ok(metadata_control_metadata(
                    "_metatxn",
                    "prepared-gc",
                    report.compacted_transactions,
                    report.removed_transaction_ids.len() as u64,
                    "metaserver/prepared-transaction-gc",
                    self.last_metadata_commit_index(),
                ))
            }
            ObjectCommand::ApplyPreparedMetaChangeCheckpoint { prepared } => {
                let checkpoint = self
                    .meta_service
                    .apply_prepared_change_checkpoint(prepared)?;
                Ok(metadata_control_metadata(
                    "_metachangecheckpoint",
                    &change_checkpoint_key(&checkpoint.consumer_id, &checkpoint.prefix),
                    checkpoint.after_commit_index,
                    checkpoint.after_change_sequence,
                    "metaserver/prepared-change-checkpoint",
                    checkpoint.update_time_ms,
                ))
            }
            ObjectCommand::ApplyPreparedMetaChangefeedCompaction { prepared } => {
                let report = self.apply_prepared_metadata_changefeed_compaction(prepared)?;
                Ok(metadata_control_metadata(
                    "_metachange",
                    "prepared-changefeed-compaction",
                    report.compacted_changes,
                    report.remaining_changes,
                    "metaserver/prepared-changefeed-compact",
                    report.safe_sequence,
                ))
            }
            ObjectCommand::ApplyPreparedMetaTombstoneCompaction { prepared } => {
                let report = self.apply_prepared_metadata_tombstone_compaction(prepared)?;
                Ok(metadata_control_metadata(
                    "_metatombstone",
                    &report
                        .next_key_marker
                        .clone()
                        .unwrap_or_else(|| "prepared-tombstone-compaction".to_string()),
                    report.compacted_records,
                    report.compacted_bytes,
                    if report.truncated || report.budget_limited {
                        "metaserver/prepared-tombstone-compact-more"
                    } else {
                        "metaserver/prepared-tombstone-compact"
                    },
                    report.last_tombstone_compaction_commit_index,
                ))
            }
            ObjectCommand::ApplyPreparedMetaPrefixDelete { prepared } => {
                let prefix = prepared.prefix.clone();
                let now_ms = prepared.now_ms;
                let (deleted, receipt) = self.apply_prepared_metadata_prefix_delete(prepared)?;
                Ok(metadata_control_metadata(
                    "_metaprefix",
                    &prefix,
                    receipt.deduped as u64,
                    deleted.len() as u64,
                    if receipt.deduped {
                        "metaserver/prepared-prefix-delete-deduped"
                    } else {
                        "metaserver/prepared-prefix-delete"
                    },
                    now_ms,
                ))
            }
            ObjectCommand::CommitMetaOwnerFencedBatch {
                mutations,
                expected_owner,
                expected_epoch,
                lease_owner,
                lease_epoch,
                now_ms,
            } => {
                let records = self.commit_owner_fenced_metadata_batch(
                    mutations,
                    &expected_owner,
                    expected_epoch,
                    lease_owner.as_deref(),
                    lease_epoch,
                    now_ms,
                )?;
                Ok(metadata_control_metadata(
                    "_metawrite",
                    &expected_owner,
                    records.len() as u64,
                    expected_epoch,
                    "metaserver/owner-fenced-batch",
                    now_ms,
                ))
            }
            ObjectCommand::CommitMetaOwnerFencedBatchIdempotent {
                mutations,
                expected_owner,
                expected_epoch,
                lease_owner,
                lease_epoch,
                now_ms,
                idempotency_key,
            } => {
                let (records, receipt) = self.commit_owner_fenced_metadata_batch_idempotent(
                    mutations,
                    &expected_owner,
                    expected_epoch,
                    lease_owner.as_deref(),
                    lease_epoch,
                    now_ms,
                    &idempotency_key,
                )?;
                Ok(metadata_control_metadata(
                    "_metawrite",
                    &expected_owner,
                    records.len() as u64,
                    receipt.deduped as u64,
                    "metaserver/idempotent-owner-fenced-batch",
                    receipt.last_commit_index,
                ))
            }
            ObjectCommand::AcquireMetaLease {
                namespace,
                owner,
                ttl_ms,
                now_ms,
            } => {
                let lease = self.acquire_metadata_lease(&namespace, &owner, ttl_ms, now_ms)?;
                Ok(metadata_lease_metadata(&lease, "metaserver/lease-acquire"))
            }
            ObjectCommand::AcquireMetaLeaseIdempotent {
                namespace,
                owner,
                ttl_ms,
                now_ms,
                idempotency_key,
            } => {
                let receipt = self.acquire_metadata_lease_idempotent(
                    &namespace,
                    &owner,
                    ttl_ms,
                    now_ms,
                    &idempotency_key,
                )?;
                Ok(metadata_lease_metadata(
                    &receipt.lease,
                    "metaserver/lease-acquire-idempotent",
                ))
            }
            ObjectCommand::RenewMetaLease {
                namespace,
                owner,
                epoch,
                ttl_ms,
                now_ms,
            } => {
                let lease = self.renew_metadata_lease(&namespace, &owner, epoch, ttl_ms, now_ms)?;
                Ok(metadata_lease_metadata(&lease, "metaserver/lease-renew"))
            }
            ObjectCommand::RenewMetaLeaseIdempotent {
                namespace,
                owner,
                epoch,
                ttl_ms,
                now_ms,
                idempotency_key,
            } => {
                let receipt = self.renew_metadata_lease_idempotent(
                    &namespace,
                    &owner,
                    epoch,
                    ttl_ms,
                    now_ms,
                    &idempotency_key,
                )?;
                Ok(metadata_lease_metadata(
                    &receipt.lease,
                    "metaserver/lease-renew-idempotent",
                ))
            }
            ObjectCommand::ReleaseMetaLease {
                namespace,
                owner,
                epoch,
            } => {
                let lease = self.release_metadata_lease(&namespace, &owner, epoch)?;
                Ok(metadata_lease_metadata(&lease, "metaserver/lease-release"))
            }
            ObjectCommand::ReleaseMetaLeaseIdempotent {
                namespace,
                owner,
                epoch,
                idempotency_key,
            } => {
                let receipt = self.release_metadata_lease_idempotent(
                    &namespace,
                    &owner,
                    epoch,
                    &idempotency_key,
                )?;
                Ok(metadata_lease_metadata(
                    &receipt.lease,
                    "metaserver/lease-release-idempotent",
                ))
            }
            ObjectCommand::ApplyPreparedMetaLease { prepared } => {
                let lease = self.meta_service.apply_prepared_lease(prepared)?;
                Ok(metadata_lease_metadata(&lease, "metaserver/prepared-lease"))
            }
            ObjectCommand::ExpireMetaLeases {
                prefix,
                now_ms,
                limit,
            } => {
                let report = self.expire_metadata_leases(&prefix, now_ms, limit)?;
                Ok(metadata_control_metadata(
                    "_metalease",
                    &prefix,
                    report.expired_leases,
                    report.scanned_leases,
                    "metaserver/lease-expire",
                    now_ms,
                ))
            }
            ObjectCommand::ApplyPreparedMetaLeaseExpiry { prepared } => {
                let now_ms = prepared.now_ms;
                let report = self.meta_service.apply_prepared_lease_expiry(prepared)?;
                Ok(metadata_control_metadata(
                    "_metalease",
                    &report.prefix,
                    report.expired_leases,
                    report.scanned_leases,
                    "metaserver/prepared-lease-expire",
                    now_ms,
                ))
            }
            _ => unreachable!("command classified outside MetadataRecordLease"),
        }
    }
}
