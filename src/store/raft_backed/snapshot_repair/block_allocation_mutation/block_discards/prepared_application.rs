impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_prepared_page_batch_discard(
        &mut self,
        prepared: PreparedPageBatchDiscard,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_prepared_page_batch_discard_apply(prepared)
            .map(|(metadata, _)| metadata)
    }

    fn propose_prepared_page_batch_discard_apply(
        &mut self,
        prepared: PreparedPageBatchDiscard,
    ) -> Result<(ObjectMetadata, PageBatchDiscardReceipt), ObjectError> {
        if self.raft.commit_index() != self.store.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "prepared block batch discard requires raft/store commit alignment".to_string(),
            ));
        }
        let receipt = self
            .store
            .clone()
            .apply_prepared_page_batch_discard(prepared.clone())?;
        let entry = self
            .raft
            .append(ObjectCommand::ApplyPreparedPageBatchDiscard { prepared })?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::PreconditionFailed(
                "prepared block batch discard raft index is not contiguous".to_string(),
            ));
        }
        let metadata = self.store.apply_raft_entry(&entry)?;
        Ok((metadata, receipt))
    }

    pub fn propose_prepared_page_batch_discard_with_status(
        &mut self,
        prepared: PreparedPageBatchDiscard,
    ) -> Result<PreparedPageBatchDiscardApplyReport, ObjectError> {
        let mutation_id = prepared.receipt.idempotency_key.clone();
        let mutation_request_checksum = prepared.receipt.request_checksum.clone();
        let (metadata, receipt) = self.propose_prepared_page_batch_discard_apply(prepared)?;
        let mutation_commit_index = self.raft.commit_index();
        Ok(PreparedPageBatchDiscardApplyReport {
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

    pub fn propose_prepared_page_batch_discard_linearizable_fenced(
        &mut self,
        prepared: PreparedPageBatchDiscard,
        expected_route_control_checksum: Option<&str>,
        expected_raft_entry_checksum: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        if expected_raft_entry_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "prepared block batch discard requires expected raft entry checksum".to_string(),
            ));
        }
        prepared.verify_expected_raft_entry_checksum(expected_raft_entry_checksum)?;
        let expected_route_control_checksum = Self::require_expected_route_control_checksum(
            "prepared block batch discard",
            expected_route_control_checksum,
        )?;
        self.ensure_storage_control_read_index(
            "propose prepared block batch discard",
            Some(expected_route_control_checksum),
        )?;
        if prepared.expected_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "prepared block batch discard requires prepared route checksum".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum != expected_route_control_checksum {
            return Err(ObjectError::PreconditionFailed(
                "prepared block batch discard route checksum does not match caller fence"
                    .to_string(),
            ));
        }
        self.propose_prepared_page_batch_discard(prepared)
    }

    pub fn propose_proven_page_batch_discard_fenced(
        &mut self,
        proven_prepared: ProvenStorageRead<PreparedPageBatchDiscard>,
        expected_route_control_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_route_control_checksum = expected_route_control_checksum.as_ref();
        if expected_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven block batch discard requires expected route control checksum".to_string(),
            ));
        }
        if proven_prepared.proof.control_checksum != expected_route_control_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven block batch discard route checksum mismatch: expected {} actual {}",
                expected_route_control_checksum, proven_prepared.proof.control_checksum
            )));
        }
        if self.store.last_applied_index() < proven_prepared.proof.storage_applied_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven block batch discard storage proof is not applied: applied={} required={}",
                self.store.last_applied_index(),
                proven_prepared.proof.storage_applied_index
            )));
        }
        self.propose_prepared_page_batch_discard_linearizable_fenced(
            proven_prepared.value,
            Some(expected_route_control_checksum),
            expected_raft_entry_checksum.as_ref(),
        )
    }

}
