// SPDX-License-Identifier: Apache-2.0
impl MatrixObjectStore {
    fn apply_bucket_policy_command(
        &mut self,
        command: ObjectCommand,
        _raft_entry_index: Option<u64>,
    ) -> Result<ObjectMetadata, ObjectError> {
        match command {
            ObjectCommand::CommitMetaTransaction {
                transaction_id,
                owner,
                now_ms,
            } => {
                let records = self.commit_metadata_transaction(&transaction_id, &owner, now_ms)?;
                Ok(metadata_control_metadata(
                    "_metatxn",
                    &transaction_id,
                    0,
                    records.len() as u64,
                    "metaserver/transaction-commit",
                    now_ms,
                ))
            }
            ObjectCommand::CommitMetaTransactionIdempotent {
                transaction_id,
                owner,
                now_ms,
                idempotency_key,
            } => {
                let receipt = self.commit_metadata_transaction_idempotent(
                    &transaction_id,
                    &owner,
                    now_ms,
                    &idempotency_key,
                )?;
                Ok(metadata_control_metadata(
                    "_metatxn",
                    &receipt.transaction.transaction_id,
                    receipt.commit_index,
                    receipt.deduped as u64,
                    if receipt.deduped {
                        "metaserver/transaction-commit-idempotent-deduped"
                    } else {
                        "metaserver/transaction-commit-idempotent"
                    },
                    receipt.update_time_ms,
                ))
            }
            ObjectCommand::AbortMetaTransaction {
                transaction_id,
                owner,
                now_ms,
            } => {
                let transaction =
                    self.abort_metadata_transaction(&transaction_id, &owner, now_ms)?;
                Ok(metadata_control_metadata(
                    "_metatxn",
                    &transaction.transaction_id,
                    0,
                    transaction.mutations.len() as u64,
                    "metaserver/transaction-abort",
                    transaction.update_time_ms,
                ))
            }
            ObjectCommand::AbortMetaTransactionIdempotent {
                transaction_id,
                owner,
                now_ms,
                idempotency_key,
            } => {
                let receipt = self.abort_metadata_transaction_idempotent(
                    &transaction_id,
                    &owner,
                    now_ms,
                    &idempotency_key,
                )?;
                Ok(metadata_control_metadata(
                    "_metatxn",
                    &receipt.transaction.transaction_id,
                    receipt.commit_index,
                    receipt.deduped as u64,
                    if receipt.deduped {
                        "metaserver/transaction-abort-idempotent-deduped"
                    } else {
                        "metaserver/transaction-abort-idempotent"
                    },
                    receipt.update_time_ms,
                ))
            }
            ObjectCommand::ExpireMetaTransactions { now_ms } => {
                let expired = self.expire_metadata_transactions(now_ms)?;
                Ok(metadata_control_metadata(
                    "_metatxn",
                    "expired",
                    0,
                    expired.len() as u64,
                    "metaserver/transaction-expire",
                    now_ms,
                ))
            }
            ObjectCommand::GcMetaTransactions {
                now_ms,
                max_transactions,
                min_age_ms,
            } => {
                let report = self.gc_metadata_transactions(now_ms, max_transactions, min_age_ms);
                Ok(metadata_control_metadata(
                    "_metatxn",
                    "gc",
                    report.compacted_transactions,
                    report.removed_transaction_ids.len() as u64,
                    "metaserver/transaction-gc",
                    now_ms,
                ))
            }
            ObjectCommand::CommitMetaChangeCheckpoint { checkpoint } => {
                let checkpoint = self.commit_metadata_change_checkpoint(checkpoint)?;
                Ok(metadata_control_metadata(
                    "_metachange",
                    &change_checkpoint_key(&checkpoint.consumer_id, &checkpoint.prefix),
                    checkpoint.after_commit_index,
                    checkpoint.after_change_sequence,
                    "metaserver/change-checkpoint",
                    checkpoint.update_time_ms,
                ))
            }
            ObjectCommand::CompactMetaChangefeed {
                prefix,
                max_changes,
            } => {
                let report = self.compact_metadata_changefeed(&prefix, max_changes)?;
                Ok(metadata_control_metadata(
                    "_metachange",
                    &prefix,
                    report.compacted_changes,
                    report.remaining_changes,
                    "metaserver/changefeed-compact",
                    report.safe_sequence,
                ))
            }
            ObjectCommand::CompactMetaSnapshot {
                snapshot_commit_index,
                max_change_records,
                max_batch_receipts,
            } => {
                let report = self.compact_metadata_snapshot_retention(
                    snapshot_commit_index,
                    max_change_records,
                    max_batch_receipts,
                )?;
                Ok(metadata_control_metadata(
                    "_metasnapshot",
                    "raft-floor",
                    report.raft_snapshot_floor_commit_index,
                    report
                        .compacted_change_records
                        .saturating_add(report.compacted_batch_receipts)
                        .saturating_add(report.compacted_lease_receipts)
                        .saturating_add(report.compacted_coordinator_config_receipts)
                        .saturating_add(report.compacted_coordinator_session_receipts),
                    "metaserver/snapshot-compact",
                    report
                        .raft_snapshot_compacted_change_count
                        .saturating_add(report.raft_snapshot_compacted_batch_receipt_count),
                ))
            }
            ObjectCommand::ApplyPreparedMetaSnapshotCompaction { prepared } => {
                let report = self.apply_prepared_metadata_snapshot_compaction(prepared)?;
                Ok(metadata_control_metadata(
                    "_metasnapshot",
                    "prepared-raft-floor",
                    report.raft_snapshot_floor_commit_index,
                    report
                        .compacted_change_records
                        .saturating_add(report.compacted_batch_receipts)
                        .saturating_add(report.compacted_lease_receipts)
                        .saturating_add(report.compacted_coordinator_config_receipts)
                        .saturating_add(report.compacted_coordinator_session_receipts),
                    "metaserver/prepared-snapshot-compact",
                    report
                        .raft_snapshot_compacted_change_count
                        .saturating_add(report.raft_snapshot_compacted_batch_receipt_count),
                ))
            }
            ObjectCommand::ConfigureMetaRetention {
                change_log_retention_limit,
                mutation_receipt_retention_limit,
            } => {
                let report = self.configure_metadata_retention(
                    change_log_retention_limit,
                    mutation_receipt_retention_limit,
                )?;
                Ok(metadata_control_metadata(
                    "_metaretention",
                    "limits",
                    report.change_log_retention_limit,
                    report.mutation_receipt_retention_limit,
                    "metaserver/retention-config",
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
            ObjectCommand::ConfigureMetaAdmissionPolicy { policy } => {
                let policy = self.configure_metadata_admission_policy(policy)?;
                Ok(metadata_control_metadata(
                    "_metaadmission",
                    "policy",
                    policy.max_batch_mutations as u64,
                    policy.max_batch_value_bytes,
                    "metaserver/admission-policy",
                    policy.max_open_transactions as u64,
                ))
            }
            ObjectCommand::ApplyPreparedMetaAdmissionPolicy { prepared } => {
                let policy = self.apply_prepared_metadata_admission_policy(prepared)?;
                Ok(metadata_control_metadata(
                    "_metaadmission",
                    "prepared-policy",
                    policy.max_batch_mutations as u64,
                    policy.max_batch_value_bytes,
                    "metaserver/prepared-admission-policy",
                    policy.max_open_transactions as u64,
                ))
            }
            ObjectCommand::ConfigureMetaQos { config } => {
                let config = self.configure_metadata_qos(config)?;
                Ok(metadata_control_metadata(
                    "_metaqos",
                    "policy",
                    config.window_ms,
                    config.max_write_ops_per_window,
                    "metaserver/qos-config",
                    config.max_write_bytes_per_window,
                ))
            }
            ObjectCommand::ApplyPreparedMetaQos { prepared } => {
                let config = self.apply_prepared_metadata_qos_config(prepared)?;
                Ok(metadata_control_metadata(
                    "_metaqos",
                    "prepared-policy",
                    config.window_ms,
                    config.max_write_ops_per_window,
                    "metaserver/prepared-qos-config",
                    config.max_write_bytes_per_window,
                ))
            }
            ObjectCommand::ConfigureMetaRouteCache { config } => {
                let config = self.configure_metadata_route_cache(config)?;
                Ok(metadata_control_metadata(
                    "_metaroutecache",
                    "config",
                    config.entries as u64,
                    config.ttl_ms,
                    "metaserver/route-cache-config",
                    self.metadata_route_cache_status().entries,
                ))
            }
            _ => unreachable!("command classified outside BucketPolicy"),
        }
    }
}
