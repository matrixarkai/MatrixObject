// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn prepare_metadata_transaction_raft_entry(
        &self,
        operation: &str,
        transaction_id: &str,
        owner: &str,
        ttl_ms: u64,
        mutations: Vec<MetadataMutation>,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<PreparedMetadataTransaction, ObjectError> {
        self.store.prepare_metadata_transaction_raft_entry(
            operation,
            transaction_id,
            owner,
            ttl_ms,
            mutations,
            now_ms,
            idempotency_key,
            self.store.last_applied_index(),
        )
    }

    pub fn prepare_metadata_transaction_raft_entry_with_read_proof_linearizable_fenced(
        &self,
        operation: &str,
        transaction_id: &str,
        owner: &str,
        ttl_ms: u64,
        mutations: Vec<MetadataMutation>,
        now_ms: u64,
        idempotency_key: &str,
        expected_metadata_control_proof_checksum: &str,
    ) -> Result<ProvenMetadataRead<PreparedMetadataTransaction>, ObjectError> {
        self.prepare_metadata_transaction_raft_entry_with_read_proof_linearizable_fenced_committed_at(
            operation,
            transaction_id,
            owner,
            ttl_ms,
            mutations,
            now_ms,
            idempotency_key,
            expected_metadata_control_proof_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prepare_metadata_transaction_raft_entry_with_read_proof_linearizable_fenced_committed_at(
        &self,
        operation: &str,
        transaction_id: &str,
        owner: &str,
        ttl_ms: u64,
        mutations: Vec<MetadataMutation>,
        now_ms: u64,
        idempotency_key: &str,
        expected_metadata_control_proof_checksum: &str,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<PreparedMetadataTransaction>, ObjectError> {
        let readiness = self.ensure_metadata_read_index_committed_at(
            "prepare proven metadata transaction",
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
        let prepared = self.prepare_metadata_transaction_raft_entry(
            operation,
            transaction_id,
            owner,
            ttl_ms,
            mutations,
            now_ms,
            idempotency_key,
        )?;
        if prepared.metadata_control_proof_checksum != expected_metadata_control_proof_checksum {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata transaction control proof checksum mismatch".to_string(),
            ));
        }
        Ok(proven_metadata_read(prepared, &readiness))
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
    ) -> Result<PreparedMetadataTransaction, ObjectError> {
        self.store.prepare_metadata_transaction_raft_entry_bounded(
            operation,
            transaction_id,
            owner,
            ttl_ms,
            mutations,
            max_mutations,
            max_payload_bytes,
            now_ms,
            idempotency_key,
            self.store.last_applied_index(),
        )
    }

    pub fn prepare_metadata_transaction_raft_entry_bounded_with_read_proof_linearizable_fenced(
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
        expected_metadata_control_proof_checksum: &str,
    ) -> Result<ProvenMetadataRead<PreparedMetadataTransaction>, ObjectError> {
        self.prepare_metadata_transaction_raft_entry_bounded_with_read_proof_linearizable_fenced_committed_at(
            operation,
            transaction_id,
            owner,
            ttl_ms,
            mutations,
            max_mutations,
            max_payload_bytes,
            now_ms,
            idempotency_key,
            expected_metadata_control_proof_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prepare_metadata_transaction_raft_entry_bounded_with_read_proof_linearizable_fenced_committed_at(
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
        expected_metadata_control_proof_checksum: &str,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<PreparedMetadataTransaction>, ObjectError> {
        let readiness = self.ensure_metadata_read_index_committed_at(
            "prepare proven bounded metadata transaction",
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
        if prepared.metadata_control_proof_checksum != expected_metadata_control_proof_checksum {
            return Err(ObjectError::PreconditionFailed(
                "prepared bounded metadata transaction control proof checksum mismatch".to_string(),
            ));
        }
        Ok(proven_metadata_read(prepared, &readiness))
    }
}
