impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_prepared_chunk_batch_discard(
        &mut self,
        prepared: PreparedChunkBatchDiscard,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_prepared_chunk_batch_discard_apply(prepared)
            .map(|(metadata, _)| metadata)
    }

    fn propose_prepared_chunk_batch_discard_apply(
        &mut self,
        prepared: PreparedChunkBatchDiscard,
    ) -> Result<(ObjectMetadata, ChunkBatchDiscardReceipt), ObjectError> {
        if self.raft.commit_index() != self.store.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "prepared chunk batch discard requires raft/store commit alignment".to_string(),
            ));
        }
        let receipt = self
            .store
            .clone()
            .apply_prepared_chunk_batch_discard(prepared.clone())?;
        let entry = self
            .raft
            .append(ObjectCommand::ApplyPreparedChunkBatchDiscard { prepared })?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::PreconditionFailed(
                "prepared chunk batch discard raft index is not contiguous".to_string(),
            ));
        }
        let metadata = self.store.apply_raft_entry(&entry)?;
        Ok((metadata, receipt))
    }

    pub fn propose_prepared_chunk_batch_discard_with_status(
        &mut self,
        prepared: PreparedChunkBatchDiscard,
    ) -> Result<PreparedChunkBatchDiscardApplyReport, ObjectError> {
        let mutation_id = prepared.receipt.idempotency_key.clone();
        let mutation_request_checksum = prepared.receipt.request_checksum.clone();
        let (metadata, receipt) = self.propose_prepared_chunk_batch_discard_apply(prepared)?;
        let mutation_commit_index = self.raft.commit_index();
        Ok(PreparedChunkBatchDiscardApplyReport {
            mutation_id,
            mutation_request_checksum,
            mutation_commit_index,
            next_log_index: mutation_commit_index.saturating_add(1),
            committed: true,
            prepared: false,
            external_raft_required: false,
            deduped: receipt.deduped,
            receipt,
            metadata,
        })
    }

    pub fn propose_prepared_chunk_batch_discard_fenced(
        &mut self,
        prepared: PreparedChunkBatchDiscard,
        expected_control_checksum: Option<&str>,
        expected_raft_entry_checksum: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        if expected_raft_entry_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "prepared chunk batch discard requires expected raft entry checksum".to_string(),
            ));
        }
        prepared.verify_expected_raft_entry_checksum(expected_raft_entry_checksum)?;
        let expected_control_checksum = expected_control_checksum.ok_or_else(|| {
            ObjectError::PreconditionFailed(
                "prepared chunk batch discard requires expected control checksum".to_string(),
            )
        })?;
        if expected_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "prepared chunk batch discard requires expected control checksum".to_string(),
            ));
        }
        if prepared.expected_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "prepared chunk batch discard requires prepared control checksum".to_string(),
            ));
        }
        if prepared.expected_control_checksum != expected_control_checksum {
            return Err(ObjectError::PreconditionFailed(
                "prepared chunk batch discard control checksum does not match caller fence"
                    .to_string(),
            ));
        }
        let replica = self
            .store
            .page_service
            .replicas
            .get(prepared.replica_index)
            .ok_or_else(|| {
                ObjectError::InvalidArgument("replica index out of range".to_string())
            })?;
        replica.check_expected_control_checksum(Some(expected_control_checksum))?;
        self.propose_prepared_chunk_batch_discard(prepared)
    }

    pub fn propose_prepared_chunk_batch_discard_linearizable_fenced(
        &mut self,
        prepared: PreparedChunkBatchDiscard,
        expected_control_checksum: Option<&str>,
        expected_raft_entry_checksum: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        if expected_raft_entry_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "prepared chunk batch discard requires expected raft entry checksum".to_string(),
            ));
        }
        prepared.verify_expected_raft_entry_checksum(expected_raft_entry_checksum)?;
        let requested_read_index = self.raft.commit_index();
        if self.store.last_applied_index() < requested_read_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "propose prepared chunk batch discard requires applied commit index {} but store is at {}",
                requested_read_index,
                self.store.last_applied_index()
            )));
        }
        let expected_control_checksum = expected_control_checksum.ok_or_else(|| {
            ObjectError::PreconditionFailed(
                "prepared chunk batch discard requires expected control checksum".to_string(),
            )
        })?;
        if expected_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "prepared chunk batch discard requires expected control checksum".to_string(),
            ));
        }
        if prepared.expected_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "prepared chunk batch discard requires prepared control checksum".to_string(),
            ));
        }
        if prepared.expected_control_checksum != expected_control_checksum {
            return Err(ObjectError::PreconditionFailed(
                "prepared chunk batch discard control checksum does not match caller fence"
                    .to_string(),
            ));
        }
        let replica = self
            .store
            .page_service
            .replicas
            .get(prepared.replica_index)
            .ok_or_else(|| {
                ObjectError::InvalidArgument("replica index out of range".to_string())
            })?;
        let proof = replica.control_proof();
        if proof.state_checksum != expected_control_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "propose prepared chunk batch discard control proof checksum mismatch: expected {} got {}",
                expected_control_checksum, proof.state_checksum
            )));
        }
        self.propose_prepared_chunk_batch_discard(prepared)
    }

    pub fn propose_proven_chunk_batch_discard_fenced(
        &mut self,
        proven_prepared: ProvenStorageRead<PreparedChunkBatchDiscard>,
        expected_control_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_control_checksum = expected_control_checksum.as_ref();
        if expected_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven chunk batch discard requires expected control checksum".to_string(),
            ));
        }
        if proven_prepared.proof.control_checksum != expected_control_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven chunk batch discard control checksum mismatch: expected {} actual {}",
                expected_control_checksum, proven_prepared.proof.control_checksum
            )));
        }
        if self.store.last_applied_index() < proven_prepared.proof.storage_applied_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven chunk batch discard storage proof is not applied: applied={} required={}",
                self.store.last_applied_index(),
                proven_prepared.proof.storage_applied_index
            )));
        }
        self.propose_prepared_chunk_batch_discard_linearizable_fenced(
            proven_prepared.value,
            Some(expected_control_checksum),
            expected_raft_entry_checksum.as_ref(),
        )
    }

    pub fn propose_chunk_batch_discard_prepared(
        &mut self,
        replica_index: usize,
        chunk_id: ChunkId,
        ranges: Vec<DataRange>,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_chunk_batch_discard_raft_entry(
            replica_index,
            chunk_id,
            ranges,
            idempotency_key,
        )?;
        self.propose_prepared_chunk_batch_discard(prepared)
    }

    pub fn propose_chunk_batch_discard_prepared_bounded(
        &mut self,
        replica_index: usize,
        chunk_id: ChunkId,
        ranges: Vec<DataRange>,
        max_discard_bytes: u64,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_chunk_batch_discard_raft_entry_bounded(
            replica_index,
            chunk_id,
            ranges,
            max_discard_bytes,
            idempotency_key,
        )?;
        self.propose_prepared_chunk_batch_discard(prepared)
    }

    pub fn propose_chunk_batch_discard_prepared_bounded_fenced(
        &mut self,
        replica_index: usize,
        chunk_id: ChunkId,
        ranges: Vec<DataRange>,
        max_discard_bytes: u64,
        idempotency_key: &str,
        expected_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self
            .prepare_chunk_batch_discard_raft_entry_bounded_with_read_proof_linearizable_fenced(
                replica_index,
                chunk_id,
                ranges,
                max_discard_bytes,
                idempotency_key,
                expected_control_checksum,
            )?
            .value;
        let expected_raft_entry_checksum = prepared.raft_entry_checksum.clone();
        self.propose_prepared_chunk_batch_discard_linearizable_fenced(
            prepared,
            expected_control_checksum,
            &expected_raft_entry_checksum,
        )
    }
}
