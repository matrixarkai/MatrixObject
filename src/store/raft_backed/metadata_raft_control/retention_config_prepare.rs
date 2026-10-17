impl<R: RustRaftLog> RaftBackedObjectStore<R> {
pub fn prepare_configure_meta_retention_raft_entry(
        &self,
        change_log_retention_limit: usize,
        mutation_receipt_retention_limit: usize,
    ) -> Result<PreparedMetadataRetentionConfig, ObjectError> {
        self.store.prepare_metadata_retention_config_raft_entry(
            change_log_retention_limit,
            mutation_receipt_retention_limit,
            self.store.last_metadata_commit_index(),
        )
    }

    pub fn prepare_configure_meta_retention_raft_entry_linearizable(
        &self,
        change_log_retention_limit: usize,
        mutation_receipt_retention_limit: usize,
        now_ms: u64,
    ) -> Result<PreparedMetadataRetentionConfig, ObjectError> {
        self.ensure_metadata_read_index("prepare metadata retention", now_ms)?;
        let proof = self.store.metadata_control_proof_at(now_ms);
        self.prepare_configure_meta_retention_raft_entry_fenced(
            change_log_retention_limit,
            mutation_receipt_retention_limit,
            &proof.state_checksum,
            now_ms,
        )
    }

    pub fn prepare_configure_meta_retention_raft_entry_fenced(
        &self,
        change_log_retention_limit: usize,
        mutation_receipt_retention_limit: usize,
        expected_metadata_control_proof_checksum: &str,
        now_ms: u64,
    ) -> Result<PreparedMetadataRetentionConfig, ObjectError> {
        self.store
            .prepare_metadata_retention_config_raft_entry_fenced(
                change_log_retention_limit,
                mutation_receipt_retention_limit,
                self.store.last_metadata_commit_index(),
                expected_metadata_control_proof_checksum,
                now_ms,
            )
    }

    pub fn prepare_configure_meta_retention_raft_entry_linearizable_fenced(
        &self,
        change_log_retention_limit: usize,
        mutation_receipt_retention_limit: usize,
        expected_metadata_control_proof_checksum: &str,
        now_ms: u64,
    ) -> Result<PreparedMetadataRetentionConfig, ObjectError> {
        self.ensure_metadata_read_index("prepare fenced metadata retention", now_ms)?;
        self.prepare_configure_meta_retention_raft_entry_fenced(
            change_log_retention_limit,
            mutation_receipt_retention_limit,
            expected_metadata_control_proof_checksum,
            now_ms,
        )
    }

    pub fn prepare_configure_meta_retention_raft_entry_with_read_proof_linearizable_fenced(
        &self,
        change_log_retention_limit: usize,
        mutation_receipt_retention_limit: usize,
        expected_metadata_control_proof_checksum: &str,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<PreparedMetadataRetentionConfig>, ObjectError> {
        self.prepare_configure_meta_retention_raft_entry_with_read_proof_linearizable_fenced_committed_at(
            change_log_retention_limit,
            mutation_receipt_retention_limit,
            expected_metadata_control_proof_checksum,
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prepare_configure_meta_retention_raft_entry_with_read_proof_linearizable_fenced_committed_at(
        &self,
        change_log_retention_limit: usize,
        mutation_receipt_retention_limit: usize,
        expected_metadata_control_proof_checksum: &str,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<PreparedMetadataRetentionConfig>, ObjectError> {
        let readiness = self.ensure_metadata_read_index_committed_at(
            "prepare proven metadata retention",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        let expected_metadata_control_proof_checksum =
            if expected_metadata_control_proof_checksum.is_empty() {
                readiness.metadata_control_proof_checksum.as_str()
            } else {
                expected_metadata_control_proof_checksum
            };
        let prepared = self.prepare_configure_meta_retention_raft_entry_fenced(
            change_log_retention_limit,
            mutation_receipt_retention_limit,
            expected_metadata_control_proof_checksum,
            now_ms,
        )?;
        Ok(proven_metadata_read(prepared, &readiness))
    }
}
