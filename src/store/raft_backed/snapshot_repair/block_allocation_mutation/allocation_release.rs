impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn prepare_page_allocation_release_raft_entry(
        &self,
        allocation_id: &str,
        idempotency_key: &str,
    ) -> Result<PreparedPageAllocationRelease, ObjectError> {
        self.store.prepare_page_allocation_release_raft_entry(
            allocation_id,
            idempotency_key,
            self.store.last_applied_index(),
        )
    }

    pub fn prepare_page_allocation_release_envelope_linearizable(
        &self,
        allocation_id: &str,
        idempotency_key: &str,
    ) -> Result<PreparedPageAllocationReleaseEnvelope, ObjectError> {
        if self.raft.commit_index() != self.store.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "block allocation release prepare read index is not satisfied".to_string(),
            ));
        }
        let prepared =
            self.prepare_page_allocation_release_raft_entry(allocation_id, idempotency_key)?;
        Ok(make_prepared_page_allocation_release_envelope(&prepared))
    }

    pub fn prepare_page_allocation_release_envelope_with_read_proof_linearizable(
        &self,
        allocation_id: &str,
        idempotency_key: &str,
    ) -> Result<ProvenPreparedPageAllocationReleaseEnvelope, ObjectError> {
        let requested_read_index = self.raft.commit_index();
        let envelope = self.prepare_page_allocation_release_envelope_linearizable(
            allocation_id,
            idempotency_key,
        )?;
        let proof = self.page_storage_read_proof(requested_read_index);
        Ok(ProvenPreparedPageAllocationReleaseEnvelope { envelope, proof })
    }

    pub fn propose_prepared_page_allocation_release(
        &mut self,
        prepared: PreparedPageAllocationRelease,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_prepared_page_allocation_release_apply(prepared)
            .map(|(metadata, _)| metadata)
    }

    fn propose_prepared_page_allocation_release_apply(
        &mut self,
        prepared: PreparedPageAllocationRelease,
    ) -> Result<(ObjectMetadata, PageAllocationReceipt), ObjectError> {
        if self.raft.commit_index() != self.store.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "prepared block allocation release requires raft/store commit alignment"
                    .to_string(),
            ));
        }
        let receipt = self
            .store
            .clone()
            .apply_prepared_page_allocation_release(prepared.clone())?;
        let entry = self
            .raft
            .append(ObjectCommand::ApplyPreparedPageAllocationRelease { prepared })?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::PreconditionFailed(
                "prepared block allocation release raft index is not contiguous".to_string(),
            ));
        }
        let metadata = self.store.apply_raft_entry(&entry)?;
        Ok((metadata, receipt))
    }

    pub fn propose_prepared_page_allocation_release_with_status(
        &mut self,
        prepared: PreparedPageAllocationRelease,
    ) -> Result<PreparedPageAllocationReleaseApplyReport, ObjectError> {
        let mutation_id = prepared.idempotency_key.clone();
        let mutation_request_checksum = prepared.request_checksum.clone();
        let (metadata, receipt) = self.propose_prepared_page_allocation_release_apply(prepared)?;
        let mutation_commit_index = self.raft.commit_index();
        Ok(PreparedPageAllocationReleaseApplyReport {
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

    pub fn propose_prepared_page_allocation_release_fenced(
        &mut self,
        prepared: PreparedPageAllocationRelease,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
        if expected_raft_entry_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "block allocation release requires expected raft entry checksum".to_string(),
            ));
        }
        prepared.verify_expected_raft_entry_checksum(expected_raft_entry_checksum)?;
        self.propose_prepared_page_allocation_release(prepared)
    }

    pub fn propose_proven_page_allocation_release_envelope_fenced(
        &mut self,
        proven_envelope: ProvenPreparedPageAllocationReleaseEnvelope,
        expected_route_control_checksum: impl AsRef<str>,
        expected_envelope_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_route_control_checksum = expected_route_control_checksum.as_ref();
        if expected_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven block allocation release requires expected route control checksum"
                    .to_string(),
            ));
        }
        if proven_envelope.proof.control_checksum != expected_route_control_checksum
            || proven_envelope.envelope.expected_route_control_checksum
                != expected_route_control_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven block allocation release route checksum mismatch".to_string(),
            ));
        }
        if self.store.last_applied_index() < proven_envelope.proof.storage_applied_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven block allocation release proof is not applied: applied={} required={}",
                self.store.last_applied_index(),
                proven_envelope.proof.storage_applied_index
            )));
        }
        let expected_envelope_checksum = expected_envelope_checksum.as_ref();
        if expected_envelope_checksum.is_empty()
            || proven_envelope.envelope.envelope_checksum != expected_envelope_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven block allocation release envelope checksum mismatch".to_string(),
            ));
        }
        let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
        if expected_raft_entry_checksum.is_empty()
            || proven_envelope.envelope.raft_entry_checksum != expected_raft_entry_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven block allocation release raft entry checksum mismatch".to_string(),
            ));
        }
        let prepared = self.prepare_page_allocation_release_raft_entry(
            &proven_envelope.envelope.allocation_id,
            &proven_envelope.envelope.idempotency_key,
        )?;
        let actual_envelope = make_prepared_page_allocation_release_envelope(&prepared);
        if actual_envelope.envelope_checksum != proven_envelope.envelope.envelope_checksum {
            return Err(ObjectError::PreconditionFailed(
                "proven block allocation release prepared envelope changed".to_string(),
            ));
        }
        self.propose_prepared_page_allocation_release_fenced(
            prepared,
            expected_raft_entry_checksum,
        )
    }
}
