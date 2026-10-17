impl MatrixObjectStore {
    pub fn metadata_changes_after(&self, sequence: u64, limit: usize) -> Vec<MetadataChange> {
        self.meta_service.list_changes_after(sequence, limit)
    }

    pub fn page_metadata_changes(
        &self,
        prefix: &str,
        after_sequence: u64,
        max_changes: usize,
        max_payload_bytes: u64,
    ) -> Result<MetadataChangePage, ObjectError> {
        self.meta_service
            .page_changes(prefix, after_sequence, max_changes, max_payload_bytes)
    }

    pub fn watch_metadata_changes(
        &self,
        prefix: &str,
        after_sequence: u64,
        max_changes: usize,
        max_payload_bytes: u64,
    ) -> Result<MetadataWatchPage, ObjectError> {
        self.meta_service
            .watch_changes(prefix, after_sequence, max_changes, max_payload_bytes)
    }

    pub fn compact_metadata_changefeed(
        &mut self,
        prefix: &str,
        max_changes: usize,
    ) -> Result<MetadataChangefeedCompactionReport, ObjectError> {
        self.meta_service.compact_changefeed(prefix, max_changes)
    }

    pub fn compact_metadata_snapshot_retention(
        &mut self,
        snapshot_commit_index: u64,
        max_change_records: usize,
        max_batch_receipts: usize,
    ) -> Result<MetadataSnapshotCompactionReport, ObjectError> {
        self.meta_service.compact_raft_snapshot_retention(
            snapshot_commit_index,
            max_change_records,
            max_batch_receipts,
        )
    }

    pub fn prepare_metadata_snapshot_compaction_raft_entry(
        &self,
        snapshot_commit_index: u64,
        max_change_records: usize,
        max_batch_receipts: usize,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataSnapshotCompaction, ObjectError> {
        self.meta_service.prepare_snapshot_compaction_raft_entry(
            snapshot_commit_index,
            max_change_records,
            max_batch_receipts,
            expected_previous_commit_index,
        )
    }

    pub fn apply_prepared_metadata_snapshot_compaction(
        &mut self,
        prepared: PreparedMetadataSnapshotCompaction,
    ) -> Result<MetadataSnapshotCompactionReport, ObjectError> {
        self.meta_service
            .apply_prepared_snapshot_compaction(prepared)
    }

    pub fn configure_metadata_retention(
        &mut self,
        change_log_retention_limit: usize,
        mutation_receipt_retention_limit: usize,
    ) -> Result<MetadataRetentionConfigReport, ObjectError> {
        self.meta_service
            .configure_retention(change_log_retention_limit, mutation_receipt_retention_limit)
    }

    pub fn metadata_admission_policy(&self) -> MetadataAdmissionPolicy {
        self.meta_service.admission_policy()
    }

    pub fn metadata_qos_config(&self) -> MetadataQosConfig {
        self.meta_service.metadata_qos_config()
    }

    pub fn metadata_qos_state(&self) -> MetadataQosState {
        self.meta_service.metadata_qos_state()
    }

    pub fn configure_metadata_admission_policy(
        &mut self,
        policy: MetadataAdmissionPolicy,
    ) -> Result<MetadataAdmissionPolicy, ObjectError> {
        self.meta_service.configure_admission_policy(policy)
    }

    pub fn prepare_metadata_admission_policy_raft_entry(
        &self,
        policy: MetadataAdmissionPolicy,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataAdmissionPolicy, ObjectError> {
        self.meta_service
            .prepare_admission_policy_raft_entry(policy, expected_previous_commit_index)
    }

    pub fn prepare_metadata_admission_policy_raft_entry_fenced(
        &self,
        policy: MetadataAdmissionPolicy,
        expected_previous_commit_index: u64,
        expected_metadata_control_proof_checksum: &str,
        now_ms: u64,
    ) -> Result<PreparedMetadataAdmissionPolicy, ObjectError> {
        let proof = self.check_expected_metadata_control_proof(
            expected_metadata_control_proof_checksum,
            now_ms,
        )?;
        let mut prepared = self
            .prepare_metadata_admission_policy_raft_entry(policy, expected_previous_commit_index)?;
        prepared.metadata_control_proof_format = proof.state_format;
        prepared.metadata_control_proof_checksum =
            expected_metadata_control_proof_checksum.to_string();
        prepared.metadata_control_proof_bytes = proof.state_bytes;
        prepared.metadata_control_proof_now_ms = now_ms;
        prepared.raft_entry_checksum = prepared_metadata_admission_policy_checksum(&prepared);
        Ok(prepared)
    }

    pub fn apply_prepared_metadata_admission_policy(
        &mut self,
        prepared: PreparedMetadataAdmissionPolicy,
    ) -> Result<MetadataAdmissionPolicy, ObjectError> {
        self.meta_service.apply_prepared_admission_policy(prepared)
    }

    pub fn configure_metadata_qos(
        &mut self,
        config: MetadataQosConfig,
    ) -> Result<MetadataQosConfig, ObjectError> {
        self.meta_service.configure_qos(config)
    }

    pub fn prepare_metadata_qos_config_raft_entry(
        &self,
        config: MetadataQosConfig,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataQosConfig, ObjectError> {
        self.meta_service
            .prepare_qos_config_raft_entry(config, expected_previous_commit_index)
    }

    pub fn prepare_metadata_qos_config_raft_entry_fenced(
        &self,
        config: MetadataQosConfig,
        expected_previous_commit_index: u64,
        expected_metadata_control_proof_checksum: &str,
        now_ms: u64,
    ) -> Result<PreparedMetadataQosConfig, ObjectError> {
        let proof = self.check_expected_metadata_control_proof(
            expected_metadata_control_proof_checksum,
            now_ms,
        )?;
        let mut prepared =
            self.prepare_metadata_qos_config_raft_entry(config, expected_previous_commit_index)?;
        prepared.metadata_control_proof_format = proof.state_format;
        prepared.metadata_control_proof_checksum =
            expected_metadata_control_proof_checksum.to_string();
        prepared.metadata_control_proof_bytes = proof.state_bytes;
        prepared.metadata_control_proof_now_ms = now_ms;
        prepared.raft_entry_checksum = prepared_metadata_qos_config_checksum(&prepared);
        Ok(prepared)
    }

    pub fn apply_prepared_metadata_qos_config(
        &mut self,
        prepared: PreparedMetadataQosConfig,
    ) -> Result<MetadataQosConfig, ObjectError> {
        self.meta_service.apply_prepared_qos_config(prepared)
    }

    pub fn prepare_metadata_retention_config_raft_entry(
        &self,
        change_log_retention_limit: usize,
        mutation_receipt_retention_limit: usize,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataRetentionConfig, ObjectError> {
        self.meta_service.prepare_retention_config_raft_entry(
            change_log_retention_limit,
            mutation_receipt_retention_limit,
            expected_previous_commit_index,
        )
    }

    pub fn prepare_metadata_retention_config_raft_entry_fenced(
        &self,
        change_log_retention_limit: usize,
        mutation_receipt_retention_limit: usize,
        expected_previous_commit_index: u64,
        expected_metadata_control_proof_checksum: &str,
        now_ms: u64,
    ) -> Result<PreparedMetadataRetentionConfig, ObjectError> {
        let proof = self.check_expected_metadata_control_proof(
            expected_metadata_control_proof_checksum,
            now_ms,
        )?;
        let mut prepared = self.prepare_metadata_retention_config_raft_entry(
            change_log_retention_limit,
            mutation_receipt_retention_limit,
            expected_previous_commit_index,
        )?;
        prepared.metadata_control_proof_format = proof.state_format;
        prepared.metadata_control_proof_checksum =
            expected_metadata_control_proof_checksum.to_string();
        prepared.metadata_control_proof_bytes = proof.state_bytes;
        prepared.metadata_control_proof_now_ms = now_ms;
        prepared.raft_entry_checksum = prepared_metadata_retention_config_checksum(&prepared);
        Ok(prepared)
    }

    pub fn apply_prepared_metadata_retention_config(
        &mut self,
        prepared: PreparedMetadataRetentionConfig,
    ) -> Result<MetadataRetentionConfigReport, ObjectError> {
        self.meta_service.apply_prepared_retention_config(prepared)
    }
}
