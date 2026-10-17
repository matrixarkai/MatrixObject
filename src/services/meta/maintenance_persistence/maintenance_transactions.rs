impl MatrixMetaService {
    pub fn apply_prepared_maintenance(
        &mut self,
        prepared: PreparedMetadataMaintenance,
    ) -> Result<MetadataMaintenanceReport, ObjectError> {
        let expected_checksum =
            prepared_metadata_maintenance_checksum(&PreparedMetadataMaintenance {
                raft_entry_checksum: String::new(),
                ..prepared.clone()
            });
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(
                "metadata prepared maintenance raft entry checksum mismatch".to_string(),
            ));
        }
        if prepared.request_checksum
            != metadata_maintenance_request_checksum(
                prepared.now_ms,
                &prepared.lease_prefix,
                prepared.lease_limit,
                prepared.session_limit,
                prepared.transaction_limit,
            )
        {
            return Err(ObjectError::Corruption(
                "metadata prepared maintenance request checksum mismatch".to_string(),
            ));
        }
        if prepared.prepared_commit_index
            != prepared.expected_previous_commit_index.saturating_add(1)
        {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared maintenance commit index must be adjacent".to_string(),
            ));
        }
        if self.prepared_maintenance_already_applied(&prepared) {
            let continuation = metadata_maintenance_continuation(
                &prepared.lease_report,
                &prepared.expired_sessions,
                &prepared.expired_transactions,
                &prepared.tombstone_report,
                prepared.lease_limit,
                prepared.session_limit,
                prepared.transaction_limit,
                prepared.payload_budget_limited,
                &prepared.payload_budget_phase,
                &prepared.payload_budget_marker,
            );
            return Ok(MetadataMaintenanceReport {
                now_ms: prepared.now_ms,
                lease_report: prepared.lease_report,
                expired_sessions: prepared.expired_sessions,
                expired_transactions: prepared.expired_transactions,
                tombstone_report: prepared.tombstone_report,
                compacted_tombstones: prepared.compacted_tombstones,
                route_cache_audit: MetadataRouteCacheAudit::default(),
                continuation,
            });
        }
        if prepared.expected_previous_commit_index != self.next_commit_index.saturating_sub(1) {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared maintenance previous commit fence mismatch".to_string(),
            ));
        }
        self.check_expected_metadata_control_proof(
            &prepared.metadata_control_proof_checksum,
            prepared.now_ms,
        )?;
        let current = self.prepare_maintenance_raft_entry(
            prepared.now_ms,
            &prepared.lease_prefix,
            prepared.lease_limit,
            prepared.session_limit,
            prepared.transaction_limit,
            prepared.expected_previous_commit_index,
        )?;
        if current.lease_report != prepared.lease_report
            || current.expired_sessions != prepared.expired_sessions
            || current.expired_transactions != prepared.expired_transactions
            || current.tombstone_report != prepared.tombstone_report
            || current.compacted_tombstones != prepared.compacted_tombstones
        {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared maintenance image changed before apply".to_string(),
            ));
        }
        for lease in &prepared.lease_report.expired {
            match self.leases.get(&lease.namespace) {
                Some(current) if current == lease => {}
                _ => {
                    return Err(ObjectError::PreconditionFailed(
                        "metadata prepared maintenance lease image mismatch".to_string(),
                    ));
                }
            }
        }
        for session in &prepared.expired_sessions {
            match self.coordinator_sessions.get(&session.session_id) {
                Some(current) if current == session => {}
                _ => {
                    return Err(ObjectError::PreconditionFailed(
                        "metadata prepared maintenance coordinator session image mismatch"
                            .to_string(),
                    ));
                }
            }
        }
        for transaction in &prepared.expired_transactions {
            match self.transactions.get(&transaction.transaction_id) {
                Some(current)
                    if current.state == "pending"
                        && current.expire_time_ms <= prepared.now_ms
                        && current.owner == transaction.owner
                        && current.mutations == transaction.mutations => {}
                _ => {
                    return Err(ObjectError::PreconditionFailed(
                        "metadata prepared maintenance transaction image mismatch".to_string(),
                    ));
                }
            }
        }
        for record in &prepared.compacted_tombstones {
            match self.records.get(&record.key) {
                Some(current) if current == record => {}
                _ => {
                    return Err(ObjectError::PreconditionFailed(
                        "metadata prepared maintenance tombstone image mismatch".to_string(),
                    ));
                }
            }
        }
        for lease in &prepared.lease_report.expired {
            self.leases.remove(&lease.namespace);
        }
        for session in &prepared.expired_sessions {
            self.coordinator_sessions.remove(&session.session_id);
        }
        for transaction in &prepared.expired_transactions {
            if let Some(current) = self.transactions.get_mut(&transaction.transaction_id) {
                current.state = "expired".to_string();
                current.update_time_ms = prepared.now_ms;
            }
        }
        for record in &prepared.compacted_tombstones {
            self.records.remove(&record.key);
        }
        if prepared.tombstone_report.compacted_records > 0 {
            self.compacted_tombstone_count = prepared.tombstone_report.compacted_tombstone_count;
            self.last_tombstone_compaction_commit_index = prepared
                .tombstone_report
                .last_tombstone_compaction_commit_index;
        }
        self.next_commit_index = prepared.prepared_commit_index.saturating_add(1);
        let continuation = metadata_maintenance_continuation(
            &prepared.lease_report,
            &prepared.expired_sessions,
            &prepared.expired_transactions,
            &prepared.tombstone_report,
            prepared.lease_limit,
            prepared.session_limit,
            prepared.transaction_limit,
            prepared.payload_budget_limited,
            &prepared.payload_budget_phase,
            &prepared.payload_budget_marker,
        );
        Ok(MetadataMaintenanceReport {
            now_ms: prepared.now_ms,
            lease_report: prepared.lease_report,
            expired_sessions: prepared.expired_sessions,
            expired_transactions: prepared.expired_transactions,
            tombstone_report: prepared.tombstone_report,
            compacted_tombstones: prepared.compacted_tombstones,
            route_cache_audit: MetadataRouteCacheAudit::default(),
            continuation,
        })
    }

    pub fn gc_transactions(
        &mut self,
        now_ms: u64,
        max_transactions: usize,
        min_age_ms: u64,
    ) -> MetadataTransactionGcReport {
        let limit = if max_transactions == 0 {
            usize::MAX
        } else {
            max_transactions
        };
        let mut report = MetadataTransactionGcReport::default();
        let mut remove_ids = Vec::new();
        for (transaction_id, transaction) in &self.transactions {
            report.scanned_transactions = report.scanned_transactions.saturating_add(1);
            if remove_ids.len() >= limit {
                report.skipped_transactions = report.skipped_transactions.saturating_add(1);
                continue;
            }
            let terminal = matches!(
                transaction.state.as_str(),
                "committed" | "aborted" | "expired"
            );
            let old_enough = transaction.update_time_ms.saturating_add(min_age_ms) <= now_ms;
            if terminal && old_enough {
                remove_ids.push(transaction_id.clone());
            }
        }
        for transaction_id in remove_ids {
            if self.transactions.remove(&transaction_id).is_some() {
                report.compacted_transactions = report.compacted_transactions.saturating_add(1);
                report.removed_transaction_ids.push(transaction_id);
            }
        }
        if report.compacted_transactions > 0 {
            self.next_commit_index = self.next_commit_index.saturating_add(1);
        }
        report
    }

    pub fn prepare_transaction_gc_raft_entry(
        &self,
        now_ms: u64,
        max_transactions: usize,
        min_age_ms: u64,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataTransactionGc, ObjectError> {
        if expected_previous_commit_index != self.next_commit_index.saturating_sub(1) {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata transaction gc expected previous commit {} does not match current {}",
                expected_previous_commit_index,
                self.next_commit_index.saturating_sub(1)
            )));
        }
        let limit = if max_transactions == 0 {
            usize::MAX
        } else {
            max_transactions
        };
        let mut report = MetadataTransactionGcReport::default();
        let mut removed_transactions = Vec::new();
        for transaction in self.transactions.values() {
            report.scanned_transactions = report.scanned_transactions.saturating_add(1);
            if removed_transactions.len() >= limit {
                report.skipped_transactions = report.skipped_transactions.saturating_add(1);
                continue;
            }
            let terminal = matches!(
                transaction.state.as_str(),
                "committed" | "aborted" | "expired"
            );
            let old_enough = transaction.update_time_ms.saturating_add(min_age_ms) <= now_ms;
            if terminal && old_enough {
                report.compacted_transactions = report.compacted_transactions.saturating_add(1);
                report
                    .removed_transaction_ids
                    .push(transaction.transaction_id.clone());
                removed_transactions.push(transaction.clone());
            }
        }
        let prepared_commit_index = if removed_transactions.is_empty() {
            expected_previous_commit_index
        } else {
            expected_previous_commit_index.saturating_add(1)
        };
        let request_checksum =
            metadata_transaction_gc_request_checksum(now_ms, max_transactions, min_age_ms);
        let (metadata_control_proof_checksum, metadata_control_proof_bytes) =
            self.metadata_control_proof_digest_at(now_ms);
        let mut prepared = PreparedMetadataTransactionGc {
            now_ms,
            max_transactions,
            min_age_ms,
            report,
            removed_transactions,
            request_checksum,
            metadata_control_proof_format: "MOSRS-META-SERVICE-CONTROL3".to_string(),
            metadata_control_proof_checksum,
            metadata_control_proof_bytes,
            metadata_control_proof_now_ms: now_ms,
            expected_previous_commit_index,
            prepared_commit_index,
            raft_entry_checksum: String::new(),
        };
        prepared.raft_entry_checksum = prepared_metadata_transaction_gc_checksum(&prepared);
        Ok(prepared)
    }

    pub fn apply_prepared_transaction_gc(
        &mut self,
        prepared: PreparedMetadataTransactionGc,
    ) -> Result<MetadataTransactionGcReport, ObjectError> {
        let expected_checksum =
            prepared_metadata_transaction_gc_checksum(&PreparedMetadataTransactionGc {
                raft_entry_checksum: String::new(),
                ..prepared.clone()
            });
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(
                "metadata prepared transaction gc raft entry checksum mismatch".to_string(),
            ));
        }
        if prepared.request_checksum
            != metadata_transaction_gc_request_checksum(
                prepared.now_ms,
                prepared.max_transactions,
                prepared.min_age_ms,
            )
        {
            return Err(ObjectError::Corruption(
                "metadata prepared transaction gc request checksum mismatch".to_string(),
            ));
        }
        if !prepared.removed_transactions.is_empty()
            && prepared.prepared_commit_index <= self.next_commit_index.saturating_sub(1)
            && prepared
                .removed_transactions
                .iter()
                .all(|transaction| !self.transactions.contains_key(&transaction.transaction_id))
        {
            return Ok(prepared.report);
        }
        self.check_expected_metadata_control_proof(
            &prepared.metadata_control_proof_checksum,
            prepared.metadata_control_proof_now_ms,
        )?;
        if prepared.expected_previous_commit_index != self.next_commit_index.saturating_sub(1) {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared transaction gc previous commit fence mismatch".to_string(),
            ));
        }
        let expected_commit_index = if prepared.removed_transactions.is_empty() {
            prepared.expected_previous_commit_index
        } else {
            prepared.expected_previous_commit_index.saturating_add(1)
        };
        if prepared.prepared_commit_index != expected_commit_index {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared transaction gc commit index mismatch".to_string(),
            ));
        }
        let current = self.prepare_transaction_gc_raft_entry(
            prepared.now_ms,
            prepared.max_transactions,
            prepared.min_age_ms,
            prepared.expected_previous_commit_index,
        )?;
        if current.report != prepared.report
            || current.removed_transactions != prepared.removed_transactions
        {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared transaction gc no longer matches current state".to_string(),
            ));
        }
        if prepared.removed_transactions.is_empty() {
            return Ok(prepared.report);
        }
        for transaction in &prepared.removed_transactions {
            match self.transactions.get(&transaction.transaction_id) {
                Some(current) if current == transaction => {}
                _ => {
                    return Err(ObjectError::PreconditionFailed(
                        "metadata prepared transaction gc transaction image changed".to_string(),
                    ));
                }
            }
        }
        for transaction in &prepared.removed_transactions {
            self.transactions.remove(&transaction.transaction_id);
        }
        self.next_commit_index = self.next_commit_index.saturating_add(1);
        Ok(prepared.report)
    }
}
