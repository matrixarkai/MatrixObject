// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {

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
