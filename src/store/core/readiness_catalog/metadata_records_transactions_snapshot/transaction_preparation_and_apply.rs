impl MatrixObjectStore {
    pub fn prepare_metadata_transaction_raft_entry(
        &self,
        operation: &str,
        transaction_id: &str,
        owner: &str,
        ttl_ms: u64,
        mutations: Vec<MetadataMutation>,
        now_ms: u64,
        idempotency_key: &str,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataTransaction, ObjectError> {
        self.meta_service.prepare_transaction_raft_entry(
            operation,
            transaction_id,
            owner,
            ttl_ms,
            mutations,
            now_ms,
            idempotency_key,
            expected_previous_commit_index,
        )
    }

    pub fn prepare_metadata_transaction_raft_entry_bounded(
        &self,
        operation: &str,
        transaction_id: &str,
        owner: &str,
        ttl_ms: u64,
        mutations: Vec<MetadataMutation>,
        max_mutations: usize,
        max_payload_bytes: u64,
        now_ms: u64,
        idempotency_key: &str,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataTransaction, ObjectError> {
        if max_mutations == 0 {
            return Err(ObjectError::InvalidArgument(
                "metadata prepared transaction max_mutations must be positive".to_string(),
            ));
        }
        if max_payload_bytes == 0 {
            return Err(ObjectError::InvalidArgument(
                "metadata prepared transaction max_payload_bytes must be positive".to_string(),
            ));
        }
        let budgeted_mutations = if operation == "commit" {
            self.meta_service
                .transactions
                .get(transaction_id)
                .map(|transaction| transaction.mutations.as_slice())
                .unwrap_or(mutations.as_slice())
        } else {
            mutations.as_slice()
        };
        if budgeted_mutations.len() > max_mutations {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata prepared transaction mutation count {} exceeds max_mutations {}",
                budgeted_mutations.len(),
                max_mutations
            )));
        }
        let payload_bytes = MatrixMetaService::metadata_write_payload_bytes(budgeted_mutations)?;
        if payload_bytes > max_payload_bytes {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata prepared transaction payload bytes {} exceeds max_payload_bytes {}",
                payload_bytes, max_payload_bytes
            )));
        }
        self.prepare_metadata_transaction_raft_entry(
            operation,
            transaction_id,
            owner,
            ttl_ms,
            mutations,
            now_ms,
            idempotency_key,
            expected_previous_commit_index,
        )
    }

    pub fn apply_prepared_metadata_transaction(
        &mut self,
        prepared: PreparedMetadataTransaction,
    ) -> Result<MetadataTransactionReceipt, ObjectError> {
        self.meta_service.apply_prepared_transaction(prepared)
    }
}
