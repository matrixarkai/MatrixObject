impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_begin_meta_transaction_prepared(
        &mut self,
        transaction_id: &str,
        owner: &str,
        ttl_ms: u64,
        mutations: Vec<MetadataMutation>,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_metadata_transaction_raft_entry(
            "begin",
            transaction_id,
            owner,
            ttl_ms,
            mutations,
            now_ms,
            idempotency_key,
        )?;
        self.propose_prepared_metadata_transaction(prepared)
    }

    pub fn propose_metadata_transaction_prepared_bounded(
        &mut self,
        operation: &str,
        transaction_id: &str,
        owner: &str,
        ttl_ms: u64,
        mutations: Vec<MetadataMutation>,
        max_mutations: usize,
        max_payload_bytes: u64,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_metadata_transaction_raft_entry_bounded(
            operation,
            transaction_id,
            owner,
            ttl_ms,
            mutations,
            max_mutations,
            max_payload_bytes,
            now_ms,
            idempotency_key,
        )?;
        self.propose_prepared_metadata_transaction(prepared)
    }

    pub fn propose_begin_meta_transaction_prepared_bounded(
        &mut self,
        transaction_id: &str,
        owner: &str,
        ttl_ms: u64,
        mutations: Vec<MetadataMutation>,
        max_mutations: usize,
        max_payload_bytes: u64,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_metadata_transaction_prepared_bounded(
            "begin",
            transaction_id,
            owner,
            ttl_ms,
            mutations,
            max_mutations,
            max_payload_bytes,
            now_ms,
            idempotency_key,
        )
    }

    pub fn propose_renew_meta_transaction_prepared(
        &mut self,
        transaction_id: &str,
        owner: &str,
        ttl_ms: u64,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_metadata_transaction_raft_entry(
            "renew",
            transaction_id,
            owner,
            ttl_ms,
            Vec::new(),
            now_ms,
            idempotency_key,
        )?;
        self.propose_prepared_metadata_transaction(prepared)
    }
}
