impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_prepared_page_batch_write(
        &mut self,
        prepared: PreparedPageBatchWrite,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_prepared_page_batch_write_apply(prepared)
            .map(|(metadata, _)| metadata)
    }

    fn propose_prepared_page_batch_write_apply(
        &mut self,
        prepared: PreparedPageBatchWrite,
    ) -> Result<(ObjectMetadata, ChunkBatchWriteReceipt), ObjectError> {
        if self.raft.commit_index() != self.store.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "prepared page batch write requires raft/store commit alignment".to_string(),
            ));
        }
        let receipt = self
            .store
            .clone()
            .apply_prepared_page_batch_write(prepared.clone())?;
        let entry = self
            .raft
            .append(ObjectCommand::ApplyPreparedPageBatchWrite { prepared })?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::PreconditionFailed(
                "prepared page batch write raft index is not contiguous".to_string(),
            ));
        }
        let metadata = self.store.apply_raft_entry(&entry)?;
        Ok((metadata, receipt))
    }

    pub fn propose_prepared_page_batch_write_with_status(
        &mut self,
        prepared: PreparedPageBatchWrite,
    ) -> Result<PreparedPageBatchWriteApplyReport, ObjectError> {
        let mutation_id = prepared.receipt.idempotency_key.clone();
        let mutation_request_checksum = prepared.receipt.request_checksum.clone();
        let (metadata, receipt) = self.propose_prepared_page_batch_write_apply(prepared)?;
        let mutation_commit_index = self.raft.commit_index();
        Ok(PreparedPageBatchWriteApplyReport {
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

    pub fn propose_prepared_page_batch_write_linearizable_fenced(
        &mut self,
        prepared: PreparedPageBatchWrite,
        expected_route_control_checksum: Option<&str>,
        expected_raft_entry_checksum: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        if expected_raft_entry_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "prepared page batch write requires expected raft entry checksum".to_string(),
            ));
        }
        prepared.verify_expected_raft_entry_checksum(expected_raft_entry_checksum)?;
        let expected_route_control_checksum = Self::require_expected_route_control_checksum(
            "prepared block batch write",
            expected_route_control_checksum,
        )?;
        self.ensure_storage_control_read_index(
            "propose prepared block batch write",
            Some(expected_route_control_checksum),
        )?;
        if prepared.expected_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "prepared page batch write requires prepared route checksum".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum != expected_route_control_checksum {
            return Err(ObjectError::PreconditionFailed(
                "prepared page batch write route checksum does not match caller fence".to_string(),
            ));
        }
        self.propose_prepared_page_batch_write(prepared)
    }

    pub fn propose_proven_page_batch_write_fenced(
        &mut self,
        proven_prepared: ProvenStorageRead<PreparedPageBatchWrite>,
        expected_route_control_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_route_control_checksum = expected_route_control_checksum.as_ref();
        if expected_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven page batch write requires expected route control checksum".to_string(),
            ));
        }
        if proven_prepared.proof.control_checksum != expected_route_control_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven page batch write route checksum mismatch: expected {} actual {}",
                expected_route_control_checksum, proven_prepared.proof.control_checksum
            )));
        }
        if self.store.last_applied_index() < proven_prepared.proof.storage_applied_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven page batch write storage proof is not applied: applied={} required={}",
                self.store.last_applied_index(),
                proven_prepared.proof.storage_applied_index
            )));
        }
        self.propose_prepared_page_batch_write_linearizable_fenced(
            proven_prepared.value,
            Some(expected_route_control_checksum),
            expected_raft_entry_checksum.as_ref(),
        )
    }

    pub fn propose_page_batch_write_prepared(
        &mut self,
        writes: Vec<ChunkRangeWrite>,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_page_batch_write_raft_entry(writes, idempotency_key)?;
        self.propose_prepared_page_batch_write(prepared)
    }

    pub fn propose_page_batch_write_prepared_bounded(
        &mut self,
        writes: Vec<ChunkRangeWrite>,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_page_batch_write_raft_entry_bounded(
            writes,
            max_write_bytes,
            max_chunk_write_count,
            idempotency_key,
        )?;
        self.propose_prepared_page_batch_write(prepared)
    }

    pub fn propose_page_batch_write_prepared_bounded_fenced(
        &mut self,
        writes: Vec<ChunkRangeWrite>,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
        idempotency_key: &str,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_storage_control_read_index(
            "prepare block batch write",
            expected_route_control_checksum,
        )?;
        let prepared = self
            .store
            .prepare_page_batch_write_raft_entry_bounded_fenced(
                writes,
                max_write_bytes,
                max_chunk_write_count,
                idempotency_key,
                self.store.last_applied_index(),
                expected_route_control_checksum,
            )?;
        let expected_raft_entry_checksum = prepared.raft_entry_checksum.clone();
        self.propose_prepared_page_batch_write_linearizable_fenced(
            prepared,
            expected_route_control_checksum,
            &expected_raft_entry_checksum,
        )
    }
}
