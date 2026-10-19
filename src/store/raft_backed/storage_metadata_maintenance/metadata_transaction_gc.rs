// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn prepare_gc_metadata_transactions_raft_entry(
        &self,
        now_ms: u64,
        max_transactions: usize,
        min_age_ms: u64,
    ) -> Result<PreparedMetadataTransactionGc, ObjectError> {
        self.store.prepare_metadata_transaction_gc_raft_entry(
            now_ms,
            max_transactions,
            min_age_ms,
            self.store.last_metadata_commit_index(),
        )
    }

    pub fn prepare_gc_metadata_transactions_raft_entry_with_read_proof_linearizable_fenced(
        &self,
        now_ms: u64,
        max_transactions: usize,
        min_age_ms: u64,
        expected_metadata_control_proof_checksum: &str,
    ) -> Result<ProvenMetadataRead<PreparedMetadataTransactionGc>, ObjectError> {
        self.prepare_gc_metadata_transactions_raft_entry_with_read_proof_linearizable_fenced_committed_at(
            now_ms,
            max_transactions,
            min_age_ms,
            expected_metadata_control_proof_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prepare_gc_metadata_transactions_raft_entry_with_read_proof_linearizable_fenced_committed_at(
        &self,
        now_ms: u64,
        max_transactions: usize,
        min_age_ms: u64,
        expected_metadata_control_proof_checksum: &str,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<PreparedMetadataTransactionGc>, ObjectError> {
        let readiness = self.ensure_metadata_read_index_committed_at(
            "prepare proven metadata transaction gc",
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
        let prepared =
            self.prepare_gc_metadata_transactions_raft_entry(now_ms, max_transactions, min_age_ms)?;
        if prepared.metadata_control_proof_checksum != expected_metadata_control_proof_checksum {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata transaction gc control proof checksum mismatch".to_string(),
            ));
        }
        Ok(proven_metadata_read(prepared, &readiness))
    }

    pub fn propose_prepared_metadata_transaction_gc(
        &mut self,
        prepared: PreparedMetadataTransactionGc,
    ) -> Result<ObjectMetadata, ObjectError> {
        if self.raft.commit_index() != self.store.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata transaction gc requires raft/store commit alignment".to_string(),
            ));
        }
        self.store
            .clone()
            .apply_command(ObjectCommand::ApplyPreparedMetaTransactionGc {
                prepared: prepared.clone(),
            })?;
        let entry = self
            .raft
            .append(ObjectCommand::ApplyPreparedMetaTransactionGc { prepared })?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata transaction gc raft index is not contiguous".to_string(),
            ));
        }
        self.store.apply_raft_entry(&entry)
    }

    pub fn propose_proven_metadata_transaction_gc_fenced(
        &mut self,
        proven_prepared: ProvenMetadataRead<PreparedMetadataTransactionGc>,
        expected_metadata_control_proof_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_metadata_control_proof_checksum =
            expected_metadata_control_proof_checksum.as_ref();
        if expected_metadata_control_proof_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven metadata transaction gc requires expected metadata control proof checksum"
                    .to_string(),
            ));
        }
        if proven_prepared.value.metadata_control_proof_checksum
            != expected_metadata_control_proof_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven metadata transaction gc control proof checksum mismatch".to_string(),
            ));
        }
        let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
        if expected_raft_entry_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven metadata transaction gc requires expected raft entry checksum".to_string(),
            ));
        }
        proven_prepared
            .value
            .verify_expected_raft_entry_checksum(expected_raft_entry_checksum)?;
        if self.store.last_metadata_commit_index() < proven_prepared.proof.metadata_commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven metadata transaction gc proof is not applied: applied={} required={}",
                self.store.last_metadata_commit_index(),
                proven_prepared.proof.metadata_commit_index
            )));
        }
        self.propose_prepared_metadata_transaction_gc(proven_prepared.value)
    }

    pub fn propose_gc_meta_transactions_prepared(
        &mut self,
        now_ms: u64,
        max_transactions: usize,
        min_age_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared =
            self.prepare_gc_metadata_transactions_raft_entry(now_ms, max_transactions, min_age_ms)?;
        self.propose_prepared_metadata_transaction_gc(prepared)
    }

}
