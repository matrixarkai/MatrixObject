impl MatrixObjectStore {
    pub fn plan_metadata_tombstone_compaction(
        &self,
        prefix: &str,
        after_key: Option<&str>,
        max_records: usize,
        max_compact_bytes: u64,
    ) -> Result<MetadataTombstoneCompactionReport, ObjectError> {
        self.meta_service.plan_tombstone_compaction(
            prefix,
            after_key,
            max_records,
            max_compact_bytes,
        )
    }

    pub fn compact_metadata_tombstones(
        &mut self,
        prefix: &str,
        after_key: Option<&str>,
        max_records: usize,
        max_compact_bytes: u64,
    ) -> Result<MetadataTombstoneCompactionReport, ObjectError> {
        self.meta_service
            .compact_tombstones(prefix, after_key, max_records, max_compact_bytes)
    }

    pub fn delete_metadata_by_prefix(
        &mut self,
        prefix: &str,
        max_records: usize,
        lease_owner: Option<&str>,
        lease_epoch: Option<u64>,
        now_ms: u64,
    ) -> Result<Vec<MetadataRecord>, ObjectError> {
        self.meta_service
            .delete_by_prefix(prefix, max_records, lease_owner, lease_epoch, now_ms)
    }

    pub fn prepare_metadata_prefix_delete_raft_entry(
        &self,
        prefix: &str,
        max_records: usize,
        lease_owner: Option<&str>,
        lease_epoch: Option<u64>,
        now_ms: u64,
        idempotency_key: &str,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataPrefixDelete, ObjectError> {
        self.meta_service.prepare_prefix_delete_raft_entry(
            prefix,
            max_records,
            lease_owner,
            lease_epoch,
            now_ms,
            idempotency_key,
            expected_previous_commit_index,
        )
    }

    pub fn prepare_metadata_prefix_delete_raft_entry_bounded(
        &self,
        prefix: &str,
        max_records: usize,
        max_payload_bytes: u64,
        lease_owner: Option<&str>,
        lease_epoch: Option<u64>,
        now_ms: u64,
        idempotency_key: &str,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataPrefixDelete, ObjectError> {
        self.meta_service.prepare_prefix_delete_raft_entry_bounded(
            prefix,
            max_records,
            max_payload_bytes,
            lease_owner,
            lease_epoch,
            now_ms,
            idempotency_key,
            expected_previous_commit_index,
        )
    }

    pub fn apply_prepared_metadata_prefix_delete(
        &mut self,
        prepared: PreparedMetadataPrefixDelete,
    ) -> Result<(Vec<MetadataRecord>, MetadataBatchReceipt), ObjectError> {
        self.meta_service.apply_prepared_prefix_delete(prepared)
    }

    pub fn commit_metadata_change_checkpoint(
        &mut self,
        checkpoint: MetadataChangeCheckpoint,
    ) -> Result<MetadataChangeCheckpoint, ObjectError> {
        self.meta_service.commit_change_checkpoint(checkpoint)
    }

    pub fn metadata_change_checkpoint(
        &self,
        consumer_id: &str,
        prefix: &str,
    ) -> Result<MetadataChangeCheckpoint, ObjectError> {
        self.meta_service.get_change_checkpoint(consumer_id, prefix)
    }

    pub fn metadata_change_checkpoints(&self) -> Vec<MetadataChangeCheckpoint> {
        self.meta_service.list_change_checkpoints()
    }

    pub fn page_metadata_change_checkpoints(
        &self,
        prefix: &str,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
    ) -> Result<MetadataChangeCheckpointPage, ObjectError> {
        self.meta_service
            .page_change_checkpoints(prefix, marker, limit, max_payload_bytes)
    }

    pub fn plan_metadata_change_checkpoint_lag(
        &self,
        prefix: &str,
        marker: Option<&str>,
        max_checkpoints: usize,
        target_change_sequence: Option<u64>,
        target_commit_index: Option<u64>,
        min_change_lag: u64,
        min_commit_lag: u64,
        include_not_behind: bool,
        only_compacted: bool,
        max_payload_bytes: u64,
    ) -> Result<MetadataChangeCheckpointLagPlan, ObjectError> {
        self.meta_service.plan_change_checkpoint_lag(
            prefix,
            marker,
            max_checkpoints,
            target_change_sequence,
            target_commit_index,
            min_change_lag,
            min_commit_lag,
            include_not_behind,
            only_compacted,
            max_payload_bytes,
        )
    }
}
