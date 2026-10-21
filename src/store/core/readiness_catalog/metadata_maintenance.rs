// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn run_metadata_maintenance(
        &mut self,
        now_ms: u64,
        lease_prefix: &str,
        lease_limit: usize,
        session_limit: usize,
        transaction_limit: usize,
    ) -> Result<MetadataMaintenanceReport, ObjectError> {
        let mut report = self.meta_service.run_maintenance(
            now_ms,
            lease_prefix,
            lease_limit,
            session_limit,
            transaction_limit,
        )?;
        report.route_cache_audit =
            self.audit_metadata_route_cache_fenced_sampled(now_ms, true, None, None, None, 0)?;
        report.continuation = metadata_maintenance_continuation(
            &report.lease_report,
            &report.expired_sessions,
            &report.expired_transactions,
            &report.tombstone_report,
            lease_limit,
            session_limit,
            transaction_limit,
            false,
            "",
            "",
        );
        Ok(report)
    }

    pub fn plan_metadata_maintenance(
        &self,
        now_ms: u64,
        lease_prefix: &str,
        lease_limit: usize,
        session_limit: usize,
        transaction_limit: usize,
    ) -> Result<MetadataMaintenancePlan, ObjectError> {
        self.meta_service.plan_maintenance(
            now_ms,
            lease_prefix,
            lease_limit,
            session_limit,
            transaction_limit,
            self.last_metadata_commit_index(),
        )
    }

    pub fn plan_metadata_maintenance_with_payload_budget(
        &self,
        now_ms: u64,
        lease_prefix: &str,
        lease_limit: usize,
        session_limit: usize,
        transaction_limit: usize,
        max_payload_bytes: u64,
    ) -> Result<MetadataMaintenancePlan, ObjectError> {
        self.meta_service.plan_maintenance_with_payload_budget(
            now_ms,
            lease_prefix,
            lease_limit,
            session_limit,
            transaction_limit,
            self.last_metadata_commit_index(),
            max_payload_bytes,
        )
    }

    pub fn plan_metadata_integrity(
        &self,
        now_ms: u64,
        tombstone_prefix: &str,
        max_sample_count: usize,
        min_read_index: u64,
    ) -> Result<MetadataIntegrityPlan, ObjectError> {
        validate_meta_key(tombstone_prefix)?;
        let sample_limit = max_sample_count.clamp(1, 64);
        let readiness = self.metadata_raft_readiness(min_read_index, now_ms);
        if !readiness.readable {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata integrity plan read index is not satisfied: reason={} applied={} required={}",
                readiness.reason, readiness.applied_commit_index, readiness.min_read_index
            )));
        }

        let snapshot = self.meta_service.export_snapshot();
        let mut active_owners = BTreeSet::new();
        let mut active_session_count = 0u64;
        let mut expired_sessions = Vec::new();
        for session in snapshot.coordinator_sessions.values() {
            if session.expire_time_ms > now_ms {
                active_session_count = active_session_count.saturating_add(1);
                active_owners.insert(session.owner.clone());
            } else if expired_sessions.len() < sample_limit {
                expired_sessions.push(session.clone());
            }
        }
        let expired_session_count = snapshot
            .coordinator_sessions
            .values()
            .filter(|session| session.expire_time_ms <= now_ms)
            .count() as u64;

        let mut writable_shard_count = 0u64;
        let mut draining_shard_count = 0u64;
        let mut stale_owner_shards = Vec::new();
        let mut stale_owner_shard_count = 0u64;
        for shard in snapshot.shards.values() {
            if shard.draining {
                draining_shard_count = draining_shard_count.saturating_add(1);
            } else if active_owners.contains(&shard.owner_node_id) {
                writable_shard_count = writable_shard_count.saturating_add(1);
            } else {
                stale_owner_shard_count = stale_owner_shard_count.saturating_add(1);
                if stale_owner_shards.len() < sample_limit {
                    stale_owner_shards.push(shard.clone());
                }
            }
        }

        let mut active_lease_count = 0u64;
        let mut expired_lease_count = 0u64;
        let mut orphaned_lease_owner_count = 0u64;
        let mut expired_leases = Vec::new();
        for lease in snapshot.leases.values() {
            if lease.expire_time_ms <= now_ms {
                expired_lease_count = expired_lease_count.saturating_add(1);
                if expired_leases.len() < sample_limit {
                    expired_leases.push(lease.clone());
                }
            } else {
                active_lease_count = active_lease_count.saturating_add(1);
                if !active_owners.contains(&lease.owner) {
                    orphaned_lease_owner_count = orphaned_lease_owner_count.saturating_add(1);
                }
            }
        }

        let mut open_transaction_count = 0u64;
        let mut expired_transaction_count = 0u64;
        let mut expired_transactions = Vec::new();
        for transaction in snapshot.transactions.values() {
            if transaction.state == "pending" {
                open_transaction_count = open_transaction_count.saturating_add(1);
                if transaction.expire_time_ms <= now_ms {
                    expired_transaction_count = expired_transaction_count.saturating_add(1);
                    if expired_transactions.len() < sample_limit {
                        expired_transactions.push(transaction.clone());
                    }
                }
            }
        }

        let live_record_count = snapshot
            .records
            .values()
            .filter(|record| !record.deleted)
            .count() as u64;
        let deleted_record_count = snapshot
            .records
            .values()
            .filter(|record| record.deleted)
            .count() as u64;
        let tombstone_report =
            self.plan_metadata_tombstone_compaction(tombstone_prefix, None, sample_limit, 0)?;
        let compactable_tombstone_count = tombstone_report.compactable_records;
        let compactable_tombstones = snapshot
            .records
            .values()
            .filter(|record| record.deleted && record.key.starts_with(tombstone_prefix))
            .take(sample_limit)
            .cloned()
            .collect::<Vec<_>>();

        let mut safe_change_sequence = snapshot.next_change_sequence.saturating_sub(1);
        let mut checkpoint_seen = false;
        for checkpoint in snapshot.change_checkpoints.values() {
            if !checkpoint_seen {
                safe_change_sequence = checkpoint.after_change_sequence;
                checkpoint_seen = true;
            } else {
                safe_change_sequence = safe_change_sequence.min(checkpoint.after_change_sequence);
            }
        }
        if !checkpoint_seen {
            safe_change_sequence = 0;
        }
        let compactable_change_count = snapshot
            .changes
            .iter()
            .filter(|change| change.sequence <= safe_change_sequence)
            .count() as u64;

        let route_cache = self.metadata_route_cache_status_at(now_ms);
        let control_proof = self.metadata_control_proof_at(now_ms);
        let needs_owner_failover = stale_owner_shard_count > 0;
        let needs_session_expiry = expired_session_count > 0;
        let needs_lease_expiry = expired_lease_count > 0 || orphaned_lease_owner_count > 0;
        let needs_transaction_gc = expired_transaction_count > 0;
        let needs_tombstone_compaction = compactable_tombstone_count > 0;
        let needs_changefeed_compaction = compactable_change_count > 0;
        let needs_route_cache_eviction = route_cache.stale_entries > 0;
        let needs_snapshot_compaction =
            readiness.change_log_pressure || readiness.mutation_receipt_pressure;
        let needs_read_repair =
            readiness.metadata_apply_commit_lag > 0 || !readiness.metadata_commit_bounds_valid;
        let needs_action = needs_owner_failover
            || needs_session_expiry
            || needs_lease_expiry
            || needs_transaction_gc
            || needs_tombstone_compaction
            || needs_changefeed_compaction
            || needs_route_cache_eviction
            || needs_snapshot_compaction
            || needs_read_repair;

        let mut plan = MetadataIntegrityPlan {
            plan_format: "MOSRSMETA-INTEGRITY1".to_string(),
            now_ms,
            max_sample_count: sample_limit,
            tombstone_prefix: tombstone_prefix.to_string(),
            min_read_index,
            readiness,
            control_proof,
            live_record_count,
            deleted_record_count,
            compactable_tombstone_count,
            compactable_tombstones,
            active_session_count,
            expired_session_count,
            expired_sessions,
            active_lease_count,
            expired_lease_count,
            orphaned_lease_owner_count,
            expired_leases,
            open_transaction_count,
            expired_transaction_count,
            expired_transactions,
            writable_shard_count,
            draining_shard_count,
            stale_owner_shard_count,
            stale_owner_shards,
            change_log_count: snapshot.changes.len() as u64,
            change_checkpoint_count: snapshot.change_checkpoints.len() as u64,
            safe_change_sequence,
            compactable_change_count,
            route_cache_entry_count: route_cache.entries,
            route_cache_stale_entries: route_cache.stale_entries,
            route_cache_expired_entries: route_cache.expired_entries,
            needs_owner_failover,
            needs_session_expiry,
            needs_lease_expiry,
            needs_transaction_gc,
            needs_tombstone_compaction,
            needs_changefeed_compaction,
            needs_route_cache_eviction,
            needs_snapshot_compaction,
            needs_read_repair,
            needs_action,
            plan_checksum: String::new(),
            plan_bytes: 0,
        };
        let (checksum, bytes) = metadata_integrity_plan_checksum(&plan);
        plan.plan_checksum = checksum;
        plan.plan_bytes = bytes;
        Ok(plan)
    }

    pub fn prepare_metadata_maintenance_raft_entry(
        &self,
        now_ms: u64,
        lease_prefix: &str,
        lease_limit: usize,
        session_limit: usize,
        transaction_limit: usize,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataMaintenance, ObjectError> {
        let mut prepared = self.meta_service.prepare_maintenance_raft_entry(
            now_ms,
            lease_prefix,
            lease_limit,
            session_limit,
            transaction_limit,
            expected_previous_commit_index,
        )?;
        prepared.route_cache_status = self.metadata_route_cache_status_at(now_ms);
        prepared.raft_entry_checksum =
            prepared_metadata_maintenance_checksum(&PreparedMetadataMaintenance {
                raft_entry_checksum: String::new(),
                ..prepared.clone()
            });
        Ok(prepared)
    }

    pub fn prepare_metadata_maintenance_raft_entry_with_payload_budget(
        &self,
        now_ms: u64,
        lease_prefix: &str,
        lease_limit: usize,
        session_limit: usize,
        transaction_limit: usize,
        expected_previous_commit_index: u64,
        max_payload_bytes: u64,
    ) -> Result<PreparedMetadataMaintenance, ObjectError> {
        let mut prepared = self
            .meta_service
            .prepare_maintenance_raft_entry_with_payload_budget(
                now_ms,
                lease_prefix,
                lease_limit,
                session_limit,
                transaction_limit,
                expected_previous_commit_index,
                max_payload_bytes,
            )?;
        prepared.route_cache_status = self.metadata_route_cache_status_at(now_ms);
        prepared.raft_entry_checksum =
            prepared_metadata_maintenance_checksum(&PreparedMetadataMaintenance {
                raft_entry_checksum: String::new(),
                ..prepared.clone()
            });
        Ok(prepared)
    }

    pub fn apply_prepared_metadata_maintenance(
        &mut self,
        prepared: PreparedMetadataMaintenance,
    ) -> Result<MetadataMaintenanceReport, ObjectError> {
        let mut report = self
            .meta_service
            .apply_prepared_maintenance(prepared.clone())?;
        report.route_cache_audit = self.audit_metadata_route_cache_fenced_sampled(
            prepared.now_ms,
            true,
            None,
            None,
            None,
            0,
        )?;
        Ok(report)
    }
}
