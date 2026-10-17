impl MatrixObjectStore {
    pub fn commit_owner_fenced_metadata_batch(
        &mut self,
        mutations: Vec<MetadataMutation>,
        expected_owner: &str,
        expected_epoch: u64,
        lease_owner: Option<&str>,
        lease_epoch: Option<u64>,
        now_ms: u64,
    ) -> Result<Vec<MetadataRecord>, ObjectError> {
        self.meta_service.commit_owner_fenced_batch(
            mutations,
            expected_owner,
            expected_epoch,
            lease_owner,
            lease_epoch,
            now_ms,
        )
    }

    pub fn commit_owner_fenced_metadata_batch_idempotent(
        &mut self,
        mutations: Vec<MetadataMutation>,
        expected_owner: &str,
        expected_epoch: u64,
        lease_owner: Option<&str>,
        lease_epoch: Option<u64>,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<(Vec<MetadataRecord>, MetadataBatchReceipt), ObjectError> {
        self.meta_service.commit_owner_fenced_batch_idempotent(
            mutations,
            expected_owner,
            expected_epoch,
            lease_owner,
            lease_epoch,
            now_ms,
            idempotency_key,
        )
    }

    pub fn list_metadata_batch_receipts(
        &self,
        marker: Option<&str>,
        limit: usize,
        owner_fenced: Option<bool>,
        expected_owner: Option<&str>,
        expected_epoch: Option<u64>,
        lease_owner: Option<&str>,
        record_key_prefix: Option<&str>,
        max_payload_bytes: u64,
    ) -> Result<MetadataBatchReceiptPage, ObjectError> {
        self.meta_service.list_batch_receipts(
            marker,
            limit,
            owner_fenced,
            expected_owner,
            expected_epoch,
            lease_owner,
            record_key_prefix,
            max_payload_bytes,
        )
    }

    pub fn list_metadata_mutation_receipts(
        &self,
        marker: Option<&str>,
        limit: usize,
        method: Option<&str>,
        request_checksum: Option<&str>,
        owner_fenced: Option<bool>,
        record_key_prefix: Option<&str>,
        max_payload_bytes: u64,
    ) -> Result<MetadataMutationReceiptPage, ObjectError> {
        self.meta_service.list_mutation_receipts(
            marker,
            limit,
            method,
            request_checksum,
            owner_fenced,
            record_key_prefix,
            max_payload_bytes,
        )
    }

    pub fn begin_metadata_transaction(
        &mut self,
        transaction_id: &str,
        owner: &str,
        ttl_ms: u64,
        mutations: Vec<MetadataMutation>,
        now_ms: u64,
    ) -> Result<MetadataTransaction, ObjectError> {
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let transaction = self.meta_service.begin_transaction(
            transaction_id,
            owner,
            ttl_ms,
            mutations.clone(),
            now_ms,
        )?;
        self.invalidate_metadata_route_cache();
        if self.options.persistent_snapshot_flush_on_commit {
            if let Err(err) = self.flush_persistent_snapshot() {
                if let Some(rollback) = rollback {
                    self.restore_object_mutation_rollback(rollback);
                }
                return Err(err);
            }
        }
        if let Err(err) = self.append_metadata_transaction_begin_journal_record(
            transaction_id,
            owner,
            ttl_ms,
            &mutations,
            now_ms,
        ) {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(transaction)
    }

    pub fn metadata_transaction(
        &self,
        transaction_id: &str,
    ) -> Result<MetadataTransaction, ObjectError> {
        self.meta_service.get_transaction(transaction_id)
    }

    pub fn list_metadata_lease_receipts(
        &self,
        marker: Option<&str>,
        limit: usize,
        lease_namespace: Option<&str>,
        owner: Option<&str>,
        released: Option<bool>,
    ) -> Result<MetadataLeaseReceiptPage, ObjectError> {
        self.meta_service
            .list_lease_receipts(marker, limit, lease_namespace, owner, released)
    }

    pub fn list_metadata_shard_receipts(
        &self,
        marker: Option<&str>,
        limit: usize,
        operation: Option<&str>,
        shard_id: Option<&str>,
    ) -> Result<MetadataShardReceiptPage, ObjectError> {
        self.meta_service
            .list_shard_receipts(marker, limit, operation, shard_id)
    }

    pub fn list_metadata_transactions(
        &self,
        prefix: &str,
        marker: Option<&str>,
        limit: usize,
        include_terminal: bool,
        owner: Option<&str>,
        state: Option<&str>,
        only_expired: bool,
        now_ms: u64,
    ) -> Result<MetadataTransactionPage, ObjectError> {
        self.meta_service.list_transactions(
            prefix,
            marker,
            limit,
            include_terminal,
            owner,
            state,
            only_expired,
            now_ms,
        )
    }

    pub fn list_metadata_transaction_summaries(
        &self,
        prefix: &str,
        marker: Option<&str>,
        limit: usize,
        include_terminal: bool,
        owner: Option<&str>,
        state: Option<&str>,
        only_expired: bool,
        now_ms: u64,
        max_payload_bytes: u64,
    ) -> Result<MetadataTransactionSummaryPage, ObjectError> {
        self.meta_service.list_transaction_summaries(
            prefix,
            marker,
            limit,
            include_terminal,
            owner,
            state,
            only_expired,
            now_ms,
            max_payload_bytes,
        )
    }
}
