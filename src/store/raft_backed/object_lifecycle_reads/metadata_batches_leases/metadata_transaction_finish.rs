// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_commit_meta_transaction_prepared(
        &mut self,
        transaction_id: &str,
        owner: &str,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_metadata_transaction_raft_entry(
            "commit",
            transaction_id,
            owner,
            0,
            Vec::new(),
            now_ms,
            idempotency_key,
        )?;
        self.propose_prepared_metadata_transaction(prepared)
    }

    pub fn propose_commit_meta_transaction_prepared_bounded(
        &mut self,
        transaction_id: &str,
        owner: &str,
        max_mutations: usize,
        max_payload_bytes: u64,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_metadata_transaction_prepared_bounded(
            "commit",
            transaction_id,
            owner,
            0,
            Vec::new(),
            max_mutations,
            max_payload_bytes,
            now_ms,
            idempotency_key,
        )
    }

    pub fn propose_abort_meta_transaction_prepared(
        &mut self,
        transaction_id: &str,
        owner: &str,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_metadata_transaction_raft_entry(
            "abort",
            transaction_id,
            owner,
            0,
            Vec::new(),
            now_ms,
            idempotency_key,
        )?;
        self.propose_prepared_metadata_transaction(prepared)
    }
}
