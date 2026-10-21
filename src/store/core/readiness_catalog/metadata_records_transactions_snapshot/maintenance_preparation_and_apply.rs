// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn prepare_metadata_transaction_gc_raft_entry(
        &self,
        now_ms: u64,
        max_transactions: usize,
        min_age_ms: u64,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataTransactionGc, ObjectError> {
        self.meta_service.prepare_transaction_gc_raft_entry(
            now_ms,
            max_transactions,
            min_age_ms,
            expected_previous_commit_index,
        )
    }

    pub fn apply_prepared_metadata_transaction_gc(
        &mut self,
        prepared: PreparedMetadataTransactionGc,
    ) -> Result<MetadataTransactionGcReport, ObjectError> {
        self.meta_service.apply_prepared_transaction_gc(prepared)
    }

    pub fn prepare_metadata_change_checkpoint_raft_entry(
        &self,
        checkpoint: MetadataChangeCheckpoint,
        idempotency_key: &str,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataChangeCheckpoint, ObjectError> {
        self.meta_service.prepare_change_checkpoint_raft_entry(
            checkpoint,
            idempotency_key,
            expected_previous_commit_index,
        )
    }

    pub fn apply_prepared_metadata_change_checkpoint(
        &mut self,
        prepared: PreparedMetadataChangeCheckpoint,
    ) -> Result<MetadataChangeCheckpoint, ObjectError> {
        self.meta_service.apply_prepared_change_checkpoint(prepared)
    }

    pub fn prepare_metadata_changefeed_compaction_raft_entry(
        &self,
        prefix: &str,
        max_changes: usize,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataChangefeedCompaction, ObjectError> {
        self.meta_service.prepare_changefeed_compaction_raft_entry(
            prefix,
            max_changes,
            expected_previous_commit_index,
        )
    }

    pub fn apply_prepared_metadata_changefeed_compaction(
        &mut self,
        prepared: PreparedMetadataChangefeedCompaction,
    ) -> Result<MetadataChangefeedCompactionReport, ObjectError> {
        self.meta_service
            .apply_prepared_changefeed_compaction(prepared)
    }

    pub fn prepare_metadata_tombstone_compaction_raft_entry(
        &self,
        prefix: &str,
        after_key: Option<&str>,
        max_records: usize,
        max_compact_bytes: u64,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataTombstoneCompaction, ObjectError> {
        self.meta_service.prepare_tombstone_compaction_raft_entry(
            prefix,
            after_key,
            max_records,
            max_compact_bytes,
            expected_previous_commit_index,
        )
    }

    pub fn apply_prepared_metadata_tombstone_compaction(
        &mut self,
        prepared: PreparedMetadataTombstoneCompaction,
    ) -> Result<MetadataTombstoneCompactionReport, ObjectError> {
        self.meta_service
            .apply_prepared_tombstone_compaction(prepared)
    }
}
