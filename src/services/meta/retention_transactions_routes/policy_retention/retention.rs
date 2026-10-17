impl MatrixMetaService {
    pub fn configure_retention(
        &mut self,
        change_log_retention_limit: usize,
        mutation_receipt_retention_limit: usize,
    ) -> Result<MetadataRetentionConfigReport, ObjectError> {
        validate_meta_retention_limit("change_log_retention_limit", change_log_retention_limit)?;
        validate_meta_retention_limit(
            "mutation_receipt_retention_limit",
            mutation_receipt_retention_limit,
        )?;
        self.change_log_retention_limit = change_log_retention_limit;
        self.mutation_receipt_retention_limit = mutation_receipt_retention_limit;
        let mut report = self.trim_meta_retention();
        report.change_log_retention_limit = change_log_retention_limit as u64;
        report.mutation_receipt_retention_limit = mutation_receipt_retention_limit as u64;
        self.next_commit_index = self.next_commit_index.saturating_add(1);
        Ok(report)
    }
    fn trim_meta_retention(&mut self) -> MetadataRetentionConfigReport {
        let trimmed_change_records =
            trim_vec_front_to_retention(&mut self.changes, self.change_log_retention_limit);
        let trimmed_batch_receipts = trim_btree_map_to_retention(
            &mut self.batch_request_receipts,
            self.mutation_receipt_retention_limit,
        );
        let trimmed_lease_receipts = trim_btree_map_to_retention(
            &mut self.lease_receipts,
            self.mutation_receipt_retention_limit,
        );
        let trimmed_coordinator_config_receipts = trim_btree_map_to_retention(
            &mut self.coordinator_config_receipts,
            self.mutation_receipt_retention_limit,
        );
        let trimmed_coordinator_session_receipts = trim_btree_map_to_retention(
            &mut self.coordinator_session_receipts,
            self.mutation_receipt_retention_limit,
        );
        let trimmed_shard_receipts = trim_btree_map_to_retention(
            &mut self.shard_receipts,
            self.mutation_receipt_retention_limit,
        );
        let trimmed_transaction_receipts = trim_btree_map_to_retention(
            &mut self.transaction_receipts,
            self.mutation_receipt_retention_limit,
        );
        MetadataRetentionConfigReport {
            change_log_retention_limit: self.change_log_retention_limit as u64,
            mutation_receipt_retention_limit: self.mutation_receipt_retention_limit as u64,
            trimmed_change_records: trimmed_change_records as u64,
            trimmed_batch_receipts: trimmed_batch_receipts as u64,
            trimmed_lease_receipts: trimmed_lease_receipts as u64,
            trimmed_coordinator_config_receipts: trimmed_coordinator_config_receipts as u64,
            trimmed_coordinator_session_receipts: trimmed_coordinator_session_receipts as u64,
            trimmed_shard_receipts: trimmed_shard_receipts as u64,
            trimmed_transaction_receipts: trimmed_transaction_receipts as u64,
            retained_change_records: self.changes.len() as u64,
            retained_mutation_receipts: self.retained_mutation_receipt_count(),
        }
    }

    fn retained_mutation_receipt_count(&self) -> u64 {
        self.batch_request_receipts
            .len()
            .saturating_add(self.lease_receipts.len())
            .saturating_add(self.coordinator_config_receipts.len())
            .saturating_add(self.coordinator_session_receipts.len())
            .saturating_add(self.shard_receipts.len())
            .saturating_add(self.transaction_receipts.len()) as u64
    }

    pub fn prepare_retention_config_raft_entry(
        &self,
        change_log_retention_limit: usize,
        mutation_receipt_retention_limit: usize,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataRetentionConfig, ObjectError> {
        if expected_previous_commit_index != self.next_commit_index.saturating_sub(1) {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata retention expected previous commit {} does not match current {}",
                expected_previous_commit_index,
                self.next_commit_index.saturating_sub(1)
            )));
        }
        let before_state_fingerprint = self.metadata_state_fingerprint();
        let mut projected = self.clone();
        let report = projected
            .configure_retention(change_log_retention_limit, mutation_receipt_retention_limit)?;
        let request_checksum = metadata_retention_config_request_checksum(
            change_log_retention_limit,
            mutation_receipt_retention_limit,
        );
        let mut prepared = PreparedMetadataRetentionConfig {
            change_log_retention_limit,
            mutation_receipt_retention_limit,
            report,
            before_state_fingerprint,
            metadata_control_proof_format: String::new(),
            metadata_control_proof_checksum: String::new(),
            metadata_control_proof_bytes: 0,
            metadata_control_proof_now_ms: 0,
            request_checksum,
            expected_previous_commit_index,
            prepared_commit_index: expected_previous_commit_index.saturating_add(1),
            raft_entry_checksum: String::new(),
        };
        prepared.raft_entry_checksum = prepared_metadata_retention_config_checksum(&prepared);
        Ok(prepared)
    }

    pub fn apply_prepared_retention_config(
        &mut self,
        prepared: PreparedMetadataRetentionConfig,
    ) -> Result<MetadataRetentionConfigReport, ObjectError> {
        let expected_checksum =
            prepared_metadata_retention_config_checksum(&PreparedMetadataRetentionConfig {
                raft_entry_checksum: String::new(),
                ..prepared.clone()
            });
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(
                "prepared metadata retention checksum mismatch".to_string(),
            ));
        }
        if prepared.request_checksum
            != metadata_retention_config_request_checksum(
                prepared.change_log_retention_limit,
                prepared.mutation_receipt_retention_limit,
            )
        {
            return Err(ObjectError::Corruption(
                "prepared metadata retention request checksum mismatch".to_string(),
            ));
        }
        if prepared.prepared_commit_index
            != prepared.expected_previous_commit_index.saturating_add(1)
        {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata retention commit index is not adjacent".to_string(),
            ));
        }
        if self.metadata_log_has_applied(prepared.prepared_commit_index)
            && self.change_log_retention_limit == prepared.change_log_retention_limit
            && self.mutation_receipt_retention_limit == prepared.mutation_receipt_retention_limit
        {
            return Ok(prepared.report);
        }
        if prepared.expected_previous_commit_index != self.next_commit_index.saturating_sub(1) {
            return Err(ObjectError::PreconditionFailed(format!(
                "prepared metadata retention expected previous commit {} does not match current {}",
                prepared.expected_previous_commit_index,
                self.next_commit_index.saturating_sub(1)
            )));
        }
        if prepared.before_state_fingerprint != self.metadata_state_fingerprint() {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata retention source state changed".to_string(),
            ));
        }
        let report = self.configure_retention(
            prepared.change_log_retention_limit,
            prepared.mutation_receipt_retention_limit,
        )?;
        if report != prepared.report {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata retention no longer matches current state".to_string(),
            ));
        }
        Ok(report)
    }
}
