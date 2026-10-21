// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixMetaService {
    pub fn metadata_shards(&self) -> Vec<MetadataShard> {
        self.shards.values().cloned().collect()
    }

    pub fn last_applied_metadata_mutation_commit_index(&self) -> u64 {
        let mut last_commit = self.next_commit_index.saturating_sub(1);
        last_commit = last_commit.max(self.changes.last().map_or(0, |change| change.commit_index));
        for receipt in self.batch_request_receipts.values() {
            last_commit = last_commit.max(receipt.last_commit_index);
        }
        for receipt in self.lease_receipts.values() {
            last_commit = last_commit.max(receipt.commit_index);
        }
        for receipt in self.coordinator_config_receipts.values() {
            last_commit = last_commit.max(receipt.commit_index);
        }
        for receipt in self.coordinator_session_receipts.values() {
            last_commit = last_commit.max(receipt.commit_index);
        }
        for receipt in self.shard_receipts.values() {
            last_commit = last_commit.max(receipt.commit_index);
        }
        for receipt in self.transaction_receipts.values() {
            last_commit = last_commit.max(receipt.commit_index);
        }
        for checkpoint in self.change_checkpoints.values() {
            last_commit = last_commit.max(checkpoint.after_commit_index);
        }
        last_commit = last_commit.max(self.last_tombstone_compaction_commit_index);
        last_commit.max(self.raft_snapshot_floor_commit_index)
    }

    pub fn last_applied_metadata_mutation_time_ms(&self) -> u64 {
        let mut last_update = self
            .changes
            .last()
            .map_or(0, |change| change.update_time_ms);
        for receipt in self.batch_request_receipts.values() {
            last_update = last_update.max(receipt.now_ms);
        }
        for receipt in self.lease_receipts.values() {
            last_update = last_update.max(receipt.update_time_ms);
        }
        for receipt in self.coordinator_config_receipts.values() {
            last_update = last_update.max(receipt.update_time_ms);
        }
        for receipt in self.coordinator_session_receipts.values() {
            last_update = last_update.max(receipt.update_time_ms);
        }
        for receipt in self.shard_receipts.values() {
            last_update = last_update.max(receipt.update_time_ms);
        }
        for receipt in self.transaction_receipts.values() {
            last_update = last_update.max(receipt.update_time_ms);
        }
        for checkpoint in self.change_checkpoints.values() {
            last_update = last_update.max(checkpoint.update_time_ms);
        }
        for transaction in self.transactions.values() {
            last_update = last_update.max(transaction.update_time_ms);
        }
        last_update
    }

    pub fn raft_readiness(&self, min_read_index: u64, now_ms: u64) -> MetadataRaftReadiness {
        let applied_commit_index = self.next_commit_index.saturating_sub(1);
        let last_applied_metadata_mutation_commit_index =
            self.last_applied_metadata_mutation_commit_index();
        let last_applied_metadata_mutation_time_ms = self.last_applied_metadata_mutation_time_ms();
        let last_applied_metadata_mutation_age_ms = if last_applied_metadata_mutation_time_ms == 0
            || now_ms < last_applied_metadata_mutation_time_ms
        {
            0
        } else {
            now_ms - last_applied_metadata_mutation_time_ms
        };
        let max_retained_metadata_commit_index = self
            .changes
            .last()
            .map(|change| change.commit_index)
            .unwrap_or(0)
            .max(self.raft_snapshot_floor_commit_index);
        let first_retained_change_sequence =
            self.changes.first().map_or(0, |change| change.sequence);
        let last_retained_change_sequence = self.changes.last().map_or(0, |change| change.sequence);
        let first_retained_metadata_commit_index =
            self.changes.first().map_or(0, |change| change.commit_index);
        let last_retained_metadata_commit_index =
            self.changes.last().map_or(0, |change| change.commit_index);
        let retained_mutation_receipts = self.retained_mutation_receipt_count();
        let last_change_sequence = self.changes.last().map_or(0, |change| change.sequence);
        let max_change_checkpoint_lag = self
            .change_checkpoints
            .values()
            .map(|checkpoint| last_change_sequence.saturating_sub(checkpoint.after_change_sequence))
            .max()
            .unwrap_or(0);
        let mut coordinator_config_material = Vec::new();
        append_metadata_coordinator_config(
            &mut coordinator_config_material,
            &self.coordinator_config,
        );
        let coordinator_config_checksum = etag(&coordinator_config_material);
        let external_coordinator_required =
            !is_embedded_coordinator_backend(&self.coordinator_config.backend);
        let external_coordinator_endpoints_ready =
            !external_coordinator_required || !self.coordinator_config.endpoints.is_empty();
        let open_transaction_admission_pressure =
            self.transactions.len() >= self.admission_policy.max_open_transactions;
        let namespace_lease_admission_pressure =
            self.leases.len() >= self.admission_policy.max_namespace_leases;
        let coordinator_session_admission_pressure =
            self.coordinator_sessions.len() >= self.admission_policy.max_coordinator_sessions;
        let shard_admission_pressure = self.shards.len() >= self.admission_policy.max_shards;
        let mut readiness = MetadataRaftReadiness {
            raft_term: 0,
            applied_commit_index,
            last_applied_metadata_mutation_commit_index,
            last_applied_metadata_mutation_time_ms,
            last_applied_metadata_mutation_age_ms,
            metadata_state_fingerprint: self.metadata_state_fingerprint(),
            max_retained_metadata_commit_index,
            min_read_index,
            total_shards: self.shards.len() as u64,
            active_owner_sessions: self
                .coordinator_sessions
                .values()
                .filter(|session| {
                    session.expire_time_ms > now_ms
                        && self.coordinator_session_endpoint_is_fenced(&session.endpoint)
                })
                .count() as u64,
            expired_owner_sessions: self
                .coordinator_sessions
                .values()
                .filter(|session| session.expire_time_ms <= now_ms)
                .count() as u64,
            active_namespace_leases: self
                .leases
                .values()
                .filter(|lease| lease.expire_time_ms > now_ms)
                .count() as u64,
            expired_namespace_leases: self
                .leases
                .values()
                .filter(|lease| lease.expire_time_ms <= now_ms)
                .count() as u64,
            open_transactions: self
                .transactions
                .values()
                .filter(|transaction| transaction.state == "pending")
                .count() as u64,
            terminal_transactions: self
                .transactions
                .values()
                .filter(|transaction| transaction.state != "pending")
                .count() as u64,
            metadata_records: self.records.len() as u64,
            deleted_metadata_records: self
                .records
                .values()
                .filter(|record| record.deleted)
                .count() as u64,
            retained_change_records: self.changes.len() as u64,
            first_retained_change_sequence,
            last_retained_change_sequence,
            first_retained_metadata_commit_index,
            last_retained_metadata_commit_index,
            change_checkpoint_count: self.change_checkpoints.len() as u64,
            raft_snapshot_floor_commit_index: self.raft_snapshot_floor_commit_index,
            shard_topology_receipt_count: self.shard_receipts.len() as u64,
            transaction_receipt_count: self.transaction_receipts.len() as u64,
            raft_leadership_fence_supported: true,
            raft_apply_envelope_supported: true,
            compacted_tombstone_count: self.compacted_tombstone_count,
            last_tombstone_compaction_commit_index: self.last_tombstone_compaction_commit_index,
            raft_snapshot_compacted_change_count: self.raft_snapshot_compacted_change_count,
            raft_snapshot_compacted_batch_receipt_count: self
                .raft_snapshot_compacted_batch_receipt_count,
            change_log_retention_limit: self.change_log_retention_limit as u64,
            mutation_receipt_retention_limit: self.mutation_receipt_retention_limit as u64,
            retained_mutation_receipts,
            change_log_pressure: self.change_log_retention_limit > 0
                && self.changes.len() >= self.change_log_retention_limit,
            mutation_receipt_pressure: self.mutation_receipt_retention_limit > 0
                && retained_mutation_receipts >= self.mutation_receipt_retention_limit as u64,
            max_change_checkpoint_lag,
            max_batch_mutations: self.admission_policy.max_batch_mutations as u64,
            max_batch_value_bytes: self.admission_policy.max_batch_value_bytes,
            max_open_transactions: self.admission_policy.max_open_transactions as u64,
            max_namespace_leases: self.admission_policy.max_namespace_leases as u64,
            max_coordinator_sessions: self.admission_policy.max_coordinator_sessions as u64,
            max_shards: self.admission_policy.max_shards as u64,
            remaining_open_transactions: self
                .admission_policy
                .max_open_transactions
                .saturating_sub(self.transactions.len())
                as u64,
            remaining_namespace_leases: self
                .admission_policy
                .max_namespace_leases
                .saturating_sub(self.leases.len()) as u64,
            remaining_coordinator_sessions: self
                .admission_policy
                .max_coordinator_sessions
                .saturating_sub(self.coordinator_sessions.len())
                as u64,
            remaining_shards: self
                .admission_policy
                .max_shards
                .saturating_sub(self.shards.len()) as u64,
            open_transaction_admission_pressure,
            namespace_lease_admission_pressure,
            coordinator_session_admission_pressure,
            shard_admission_pressure,
            qos_window_ms: self.qos_config.window_ms,
            qos_max_write_ops_per_window: self.qos_config.max_write_ops_per_window,
            qos_max_write_bytes_per_window: self.qos_config.max_write_bytes_per_window,
            qos_window_start_ms: self.qos_state.window_start_ms,
            qos_write_ops_in_window: self.qos_state.write_ops_in_window,
            qos_write_bytes_in_window: self.qos_state.write_bytes_in_window,
            qos_throttled_write_ops: self.qos_state.throttled_write_ops,
            qos_throttled_write_bytes: self.qos_state.throttled_write_bytes,
            qos_pressure: self.metadata_qos_enabled()
                && ((self.qos_config.max_write_ops_per_window > 0
                    && self.qos_state.write_ops_in_window
                        >= self.qos_config.max_write_ops_per_window)
                    || (self.qos_config.max_write_bytes_per_window > 0
                        && self.qos_state.write_bytes_in_window
                            >= self.qos_config.max_write_bytes_per_window)),
            coordinator_config_checksum,
            coordinator_backend: self.coordinator_config.backend.clone(),
            coordinator_normalized_backend: self.coordinator_config.normalized_backend(),
            coordinator_policy_format: coordinator_policy_format().to_string(),
            coordinator_policy_checksum: metadata_coordinator_policy_checksum(
                &self.coordinator_config,
            ),
            coordinator_preferred_backend: preferred_production_coordinator_backend().to_string(),
            coordinator_preferred_backend_active: coordinator_preferred_backend_active(
                &self.coordinator_config.backend,
            ),
            coordinator_external_compatibility_mode: external_coordinator_compatibility_mode(
                &self.coordinator_config.backend,
            ),
            coordinator_production_ready: self.coordinator_config.production_ready(),
            coordinator_external_dependency: self.coordinator_config.external_dependency(),
            coordinator_config_revision: self.coordinator_config.revision,
            coordinator_config_endpoint_count: self.coordinator_config.endpoints.len() as u64,
            coordinator_config_receipt_count: self.coordinator_config_receipts.len() as u64,
            external_coordinator_required,
            external_coordinator_endpoints_ready,
            external_coordinator_owner_sessions_ready: !external_coordinator_required,
            external_coordinator_ready: !external_coordinator_required
                && external_coordinator_endpoints_ready,
            admission_pressure: open_transaction_admission_pressure
                || namespace_lease_admission_pressure
                || coordinator_session_admission_pressure
                || shard_admission_pressure,
            ..MetadataRaftReadiness::default()
        };
        readiness.raft_log_commit_index = readiness.applied_commit_index;
        for shard in self.shards.values() {
            if shard.draining {
                readiness.draining_shards = readiness.draining_shards.saturating_add(1);
                continue;
            }
            if (is_embedded_coordinator_backend(&self.coordinator_config.backend)
                && self.coordinator_sessions.is_empty())
                || self
                    .active_fenced_owner_session(&shard.owner_node_id, now_ms)
                    .is_some()
            {
                readiness.writable_shards = readiness.writable_shards.saturating_add(1);
            } else {
                readiness.stale_owner_shards = readiness.stale_owner_shards.saturating_add(1);
            }
        }
        readiness.finalize();
        readiness
    }
}
