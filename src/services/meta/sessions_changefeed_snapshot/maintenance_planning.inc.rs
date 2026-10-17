impl MatrixMetaService {
    pub fn expire_transactions(&mut self, now_ms: u64) -> Vec<MetadataTransaction> {
        self.expire_transactions_bounded(now_ms, usize::MAX)
    }

    pub fn expire_transactions_bounded(
        &mut self,
        now_ms: u64,
        limit: usize,
    ) -> Vec<MetadataTransaction> {
        if limit == 0 {
            return Vec::new();
        }
        let mut expired = Vec::new();
        for transaction in self.transactions.values_mut() {
            if transaction.state == "pending" && transaction.expire_time_ms <= now_ms {
                transaction.state = "expired".to_string();
                transaction.update_time_ms = now_ms;
                expired.push(transaction.clone());
                if expired.len() >= limit {
                    break;
                }
            }
        }
        if !expired.is_empty() {
            self.next_commit_index = self.next_commit_index.saturating_add(1);
        }
        expired
    }

    fn plan_expired_transactions_bounded(
        &self,
        now_ms: u64,
        limit: usize,
    ) -> Vec<MetadataTransaction> {
        if limit == 0 {
            return Vec::new();
        }
        let mut expired = Vec::new();
        for transaction in self.transactions.values() {
            if transaction.state == "pending" && transaction.expire_time_ms <= now_ms {
                let mut image = transaction.clone();
                image.state = "expired".to_string();
                image.update_time_ms = now_ms;
                expired.push(image);
                if expired.len() >= limit {
                    break;
                }
            }
        }
        expired
    }

    pub fn run_maintenance(
        &mut self,
        now_ms: u64,
        lease_prefix: &str,
        lease_limit: usize,
        session_limit: usize,
        transaction_limit: usize,
    ) -> Result<MetadataMaintenanceReport, ObjectError> {
        let lease_report = if lease_limit == 0 {
            MetadataLeaseExpiryReport {
                prefix: lease_prefix.to_string(),
                limit: 0,
                ..MetadataLeaseExpiryReport::default()
            }
        } else {
            self.expire_leases(lease_prefix, now_ms, lease_limit)?
        };
        let expired_sessions = if session_limit == 0 {
            Vec::new()
        } else {
            self.expire_coordinator_sessions(now_ms, session_limit)
        };
        let expired_transactions = if transaction_limit == 0 {
            Vec::new()
        } else {
            self.expire_transactions_bounded(now_ms, transaction_limit)
        };
        Ok(MetadataMaintenanceReport {
            now_ms,
            lease_report,
            expired_sessions,
            expired_transactions,
            tombstone_report: MetadataTombstoneCompactionReport::default(),
            compacted_tombstones: Vec::new(),
            route_cache_audit: MetadataRouteCacheAudit::default(),
            continuation: MetadataMaintenanceContinuation::default(),
        })
    }

    pub fn prepare_maintenance_raft_entry(
        &self,
        now_ms: u64,
        lease_prefix: &str,
        lease_limit: usize,
        session_limit: usize,
        transaction_limit: usize,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataMaintenance, ObjectError> {
        self.prepare_maintenance_raft_entry_with_payload_budget(
            now_ms,
            lease_prefix,
            lease_limit,
            session_limit,
            transaction_limit,
            expected_previous_commit_index,
            0,
        )
    }

    pub fn prepare_maintenance_raft_entry_with_payload_budget(
        &self,
        now_ms: u64,
        lease_prefix: &str,
        lease_limit: usize,
        session_limit: usize,
        transaction_limit: usize,
        expected_previous_commit_index: u64,
        max_payload_bytes: u64,
    ) -> Result<PreparedMetadataMaintenance, ObjectError> {
        if expected_previous_commit_index != self.next_commit_index.saturating_sub(1) {
            return Err(ObjectError::PreconditionFailed(
                "metadata maintenance prepare previous commit fence mismatch".to_string(),
            ));
        }
        let lease_report = if lease_limit == 0 {
            MetadataLeaseExpiryReport {
                prefix: lease_prefix.to_string(),
                limit: 0,
                ..MetadataLeaseExpiryReport::default()
            }
        } else {
            self.plan_expired_leases(lease_prefix, now_ms, lease_limit)?
        };
        let expired_sessions = if session_limit == 0 {
            Vec::new()
        } else {
            self.plan_expired_coordinator_sessions(now_ms, session_limit)
        };
        let expired_transactions =
            self.plan_expired_transactions_bounded(now_ms, transaction_limit);
        let (mut tombstone_report, tombstone_keys) = if lease_limit == 0 {
            (MetadataTombstoneCompactionReport::default(), Vec::new())
        } else {
            self.collect_tombstone_compaction(lease_prefix, None, lease_limit, 0, true)?
        };
        let compacted_tombstones: Vec<MetadataRecord> = tombstone_keys
            .iter()
            .filter_map(|key| self.records.get(key).cloned())
            .collect();
        if compacted_tombstones.len() != tombstone_keys.len() {
            return Err(ObjectError::Corruption(
                "metadata maintenance tombstone plan selected a missing record".to_string(),
            ));
        }
        if !compacted_tombstones.is_empty() {
            tombstone_report.compacted_tombstone_count = self
                .compacted_tombstone_count
                .saturating_add(tombstone_report.compacted_records);
            tombstone_report.last_tombstone_compaction_commit_index =
                expected_previous_commit_index.saturating_add(1);
        }
        let request_checksum = metadata_maintenance_request_checksum(
            now_ms,
            lease_prefix,
            lease_limit,
            session_limit,
            transaction_limit,
        );
        let (metadata_control_proof_checksum, metadata_control_proof_bytes) =
            self.metadata_control_proof_digest_at(now_ms);
        let mut prepared = PreparedMetadataMaintenance {
            now_ms,
            lease_prefix: lease_prefix.to_string(),
            lease_limit,
            session_limit,
            transaction_limit,
            max_payload_bytes,
            maintenance_payload_bytes: 0,
            payload_budget_limited: false,
            payload_budget_phase: String::new(),
            payload_budget_marker: String::new(),
            lease_report,
            expired_sessions,
            expired_transactions,
            tombstone_report,
            compacted_tombstones,
            route_cache_status: MetadataRouteCacheStatus::default(),
            metadata_read_index: 0,
            metadata_raft_term: 0,
            metadata_control_proof_format: "MOSRS-META-SERVICE-CONTROL3".to_string(),
            metadata_control_proof_checksum,
            metadata_control_proof_bytes,
            request_checksum,
            expected_previous_commit_index,
            prepared_commit_index: expected_previous_commit_index.saturating_add(1),
            raft_entry_checksum: String::new(),
        };
        apply_metadata_maintenance_payload_budget(&mut prepared)?;
        prepared.raft_entry_checksum = prepared_metadata_maintenance_checksum(&prepared);
        Ok(prepared)
    }

    pub fn plan_maintenance(
        &self,
        now_ms: u64,
        lease_prefix: &str,
        lease_limit: usize,
        session_limit: usize,
        transaction_limit: usize,
        expected_previous_commit_index: u64,
    ) -> Result<MetadataMaintenancePlan, ObjectError> {
        let prepared = self.prepare_maintenance_raft_entry(
            now_ms,
            lease_prefix,
            lease_limit,
            session_limit,
            transaction_limit,
            expected_previous_commit_index,
        )?;
        let readiness = self.raft_readiness(expected_previous_commit_index, now_ms);
        Ok(metadata_maintenance_plan_from_prepared(prepared, readiness))
    }

    pub fn plan_maintenance_with_payload_budget(
        &self,
        now_ms: u64,
        lease_prefix: &str,
        lease_limit: usize,
        session_limit: usize,
        transaction_limit: usize,
        expected_previous_commit_index: u64,
        max_payload_bytes: u64,
    ) -> Result<MetadataMaintenancePlan, ObjectError> {
        let prepared = self.prepare_maintenance_raft_entry_with_payload_budget(
            now_ms,
            lease_prefix,
            lease_limit,
            session_limit,
            transaction_limit,
            expected_previous_commit_index,
            max_payload_bytes,
        )?;
        let readiness = self.raft_readiness(expected_previous_commit_index, now_ms);
        Ok(metadata_maintenance_plan_from_prepared(prepared, readiness))
    }

    pub fn plan_maintenance_fenced(
        &self,
        now_ms: u64,
        lease_prefix: &str,
        lease_limit: usize,
        session_limit: usize,
        transaction_limit: usize,
        expected_previous_commit_index: u64,
        expected_plan_checksum: &str,
    ) -> Result<MetadataMaintenancePlan, ObjectError> {
        let prepared = self.prepare_maintenance_raft_entry(
            now_ms,
            lease_prefix,
            lease_limit,
            session_limit,
            transaction_limit,
            expected_previous_commit_index,
        )?;
        let readiness = self.raft_readiness(expected_previous_commit_index, now_ms);
        let plan = metadata_maintenance_plan_from_prepared_fenced(
            prepared,
            readiness,
            expected_plan_checksum,
        );
        if !expected_plan_checksum.is_empty() && plan.actual_plan_checksum != expected_plan_checksum
        {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata maintenance plan checksum mismatch: expected {} actual {}",
                expected_plan_checksum, plan.actual_plan_checksum
            )));
        }
        Ok(plan)
    }
}
