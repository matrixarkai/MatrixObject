impl MatrixObjectStore {

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
}
