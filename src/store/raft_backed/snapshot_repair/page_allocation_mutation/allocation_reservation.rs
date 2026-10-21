// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn prepare_page_allocation_reservation_raft_entry(
        &self,
        length: u64,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<PreparedPageAllocationReservation, ObjectError> {
        self.store.prepare_page_allocation_reservation_raft_entry(
            length,
            now_ms,
            idempotency_key,
            self.store.last_applied_index(),
        )
    }

    pub fn prepare_page_allocation_reservation_envelope_linearizable(
        &self,
        length: u64,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<PreparedPageAllocationReservationEnvelope, ObjectError> {
        if self.raft.commit_index() != self.store.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "page allocation reservation prepare read index is not satisfied".to_string(),
            ));
        }
        let prepared =
            self.prepare_page_allocation_reservation_raft_entry(length, now_ms, idempotency_key)?;
        Ok(make_prepared_page_allocation_reservation_envelope(
            &prepared,
        ))
    }

    pub fn prepare_page_allocation_reservation_envelope_with_read_proof_linearizable(
        &self,
        length: u64,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<ProvenPreparedPageAllocationReservationEnvelope, ObjectError> {
        let requested_read_index = self.raft.commit_index();
        let envelope = self.prepare_page_allocation_reservation_envelope_linearizable(
            length,
            now_ms,
            idempotency_key,
        )?;
        let proof = self.page_storage_read_proof(requested_read_index);
        Ok(ProvenPreparedPageAllocationReservationEnvelope { envelope, proof })
    }

    pub fn propose_prepared_page_allocation_reservation(
        &mut self,
        prepared: PreparedPageAllocationReservation,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_prepared_page_allocation_reservation_apply(prepared)
            .map(|(metadata, _)| metadata)
    }

    fn propose_prepared_page_allocation_reservation_apply(
        &mut self,
        prepared: PreparedPageAllocationReservation,
    ) -> Result<(ObjectMetadata, ChunkAllocationReceipt), ObjectError> {
        if self.raft.commit_index() != self.store.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "prepared page allocation reservation requires raft/store commit alignment"
                    .to_string(),
            ));
        }
        let receipt = self
            .store
            .clone()
            .apply_prepared_page_allocation_reservation(prepared.clone())?;
        let entry = self
            .raft
            .append(ObjectCommand::ApplyPreparedPageAllocationReservation { prepared })?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::PreconditionFailed(
                "prepared page allocation reservation raft index is not contiguous".to_string(),
            ));
        }
        let metadata = self.store.apply_raft_entry(&entry)?;
        Ok((metadata, receipt))
    }

    pub fn propose_prepared_page_allocation_reservation_with_status(
        &mut self,
        prepared: PreparedPageAllocationReservation,
    ) -> Result<PreparedPageAllocationReservationApplyReport, ObjectError> {
        let mutation_id = prepared.idempotency_key.clone();
        let mutation_request_checksum = prepared.request_checksum.clone();
        let (metadata, receipt) =
            self.propose_prepared_page_allocation_reservation_apply(prepared)?;
        let mutation_commit_index = self.raft.commit_index();
        Ok(PreparedPageAllocationReservationApplyReport {
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

    pub fn propose_prepared_page_allocation_reservation_fenced(
        &mut self,
        prepared: PreparedPageAllocationReservation,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
        if expected_raft_entry_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "page allocation reservation requires expected raft entry checksum".to_string(),
            ));
        }
        prepared.verify_expected_raft_entry_checksum(expected_raft_entry_checksum)?;
        self.propose_prepared_page_allocation_reservation(prepared)
    }

    pub fn propose_proven_page_allocation_reservation_envelope_fenced(
        &mut self,
        proven_envelope: ProvenPreparedPageAllocationReservationEnvelope,
        expected_route_control_checksum: impl AsRef<str>,
        expected_envelope_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_route_control_checksum = expected_route_control_checksum.as_ref();
        if expected_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven page allocation reservation requires expected route control checksum"
                    .to_string(),
            ));
        }
        if proven_envelope.proof.control_checksum != expected_route_control_checksum
            || proven_envelope.envelope.expected_route_control_checksum
                != expected_route_control_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven page allocation reservation route checksum mismatch".to_string(),
            ));
        }
        if self.store.last_applied_index() < proven_envelope.proof.storage_applied_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven page allocation reservation proof is not applied: applied={} required={}",
                self.store.last_applied_index(),
                proven_envelope.proof.storage_applied_index
            )));
        }
        let expected_envelope_checksum = expected_envelope_checksum.as_ref();
        if expected_envelope_checksum.is_empty()
            || proven_envelope.envelope.envelope_checksum != expected_envelope_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven page allocation reservation envelope checksum mismatch".to_string(),
            ));
        }
        let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
        if expected_raft_entry_checksum.is_empty()
            || proven_envelope.envelope.raft_entry_checksum != expected_raft_entry_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven page allocation reservation raft entry checksum mismatch".to_string(),
            ));
        }
        let prepared = self.prepare_page_allocation_reservation_raft_entry(
            proven_envelope.envelope.length,
            proven_envelope.envelope.now_ms,
            &proven_envelope.envelope.idempotency_key,
        )?;
        let actual_envelope = make_prepared_page_allocation_reservation_envelope(&prepared);
        if actual_envelope.envelope_checksum != proven_envelope.envelope.envelope_checksum {
            return Err(ObjectError::PreconditionFailed(
                "proven page allocation reservation prepared envelope changed".to_string(),
            ));
        }
        self.propose_prepared_page_allocation_reservation_fenced(
            prepared,
            expected_raft_entry_checksum,
        )
    }

    pub fn prepare_page_batch_allocation_reservation_raft_entry(
        &self,
        lengths: Vec<u64>,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<PreparedPageBatchAllocationReservation, ObjectError> {
        self.store
            .prepare_page_batch_allocation_reservation_raft_entry(
                lengths,
                now_ms,
                idempotency_key,
                self.store.last_applied_index(),
            )
    }

    pub fn prepare_page_batch_allocation_reservation_envelope_linearizable(
        &self,
        lengths: Vec<u64>,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<PreparedPageBatchAllocationReservationEnvelope, ObjectError> {
        if self.raft.commit_index() != self.store.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "page batch allocation reservation prepare read index is not satisfied"
                    .to_string(),
            ));
        }
        let prepared = self.prepare_page_batch_allocation_reservation_raft_entry(
            lengths,
            now_ms,
            idempotency_key,
        )?;
        Ok(make_prepared_page_batch_allocation_reservation_envelope(
            &prepared,
        ))
    }

    pub fn prepare_page_batch_allocation_reservation_envelope_with_read_proof_linearizable(
        &self,
        lengths: Vec<u64>,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<ProvenPreparedPageBatchAllocationReservationEnvelope, ObjectError> {
        let requested_read_index = self.raft.commit_index();
        let envelope = self.prepare_page_batch_allocation_reservation_envelope_linearizable(
            lengths,
            now_ms,
            idempotency_key,
        )?;
        let proof = self.page_storage_read_proof(requested_read_index);
        Ok(ProvenPreparedPageBatchAllocationReservationEnvelope { envelope, proof })
    }

    pub fn propose_prepared_page_batch_allocation_reservation(
        &mut self,
        prepared: PreparedPageBatchAllocationReservation,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_prepared_page_batch_allocation_reservation_apply(prepared)
            .map(|(metadata, _)| metadata)
    }

    fn propose_prepared_page_batch_allocation_reservation_apply(
        &mut self,
        prepared: PreparedPageBatchAllocationReservation,
    ) -> Result<(ObjectMetadata, ChunkBatchAllocationReceipt), ObjectError> {
        if self.raft.commit_index() != self.store.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "prepared page batch allocation reservation requires raft/store commit alignment"
                    .to_string(),
            ));
        }
        let receipt = self
            .store
            .clone()
            .apply_prepared_page_batch_allocation_reservation(prepared.clone())?;
        let entry = self
            .raft
            .append(ObjectCommand::ApplyPreparedPageBatchAllocationReservation { prepared })?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::PreconditionFailed(
                "prepared page batch allocation reservation raft index is not contiguous"
                    .to_string(),
            ));
        }
        let metadata = self.store.apply_raft_entry(&entry)?;
        Ok((metadata, receipt))
    }

    pub fn propose_prepared_page_batch_allocation_reservation_with_status(
        &mut self,
        prepared: PreparedPageBatchAllocationReservation,
    ) -> Result<PreparedPageBatchAllocationReservationApplyReport, ObjectError> {
        let mutation_id = prepared.idempotency_key.clone();
        let mutation_request_checksum = prepared.request_checksum.clone();
        let (metadata, receipt) =
            self.propose_prepared_page_batch_allocation_reservation_apply(prepared)?;
        let mutation_commit_index = self.raft.commit_index();
        Ok(PreparedPageBatchAllocationReservationApplyReport {
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

    pub fn propose_prepared_page_batch_allocation_reservation_fenced(
        &mut self,
        prepared: PreparedPageBatchAllocationReservation,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
        if expected_raft_entry_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "page batch allocation reservation requires expected raft entry checksum"
                    .to_string(),
            ));
        }
        prepared.verify_expected_raft_entry_checksum(expected_raft_entry_checksum)?;
        self.propose_prepared_page_batch_allocation_reservation(prepared)
    }

    pub fn propose_proven_page_batch_allocation_reservation_envelope_fenced(
        &mut self,
        proven_envelope: ProvenPreparedPageBatchAllocationReservationEnvelope,
        expected_route_control_checksum: impl AsRef<str>,
        expected_envelope_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_route_control_checksum = expected_route_control_checksum.as_ref();
        if expected_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven page batch allocation reservation requires expected route control checksum"
                    .to_string(),
            ));
        }
        if proven_envelope.proof.control_checksum != expected_route_control_checksum
            || proven_envelope.envelope.expected_route_control_checksum
                != expected_route_control_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven page batch allocation reservation route checksum mismatch".to_string(),
            ));
        }
        if self.store.last_applied_index() < proven_envelope.proof.storage_applied_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven page batch allocation reservation proof is not applied: applied={} required={}",
                self.store.last_applied_index(),
                proven_envelope.proof.storage_applied_index
            )));
        }
        let expected_envelope_checksum = expected_envelope_checksum.as_ref();
        if expected_envelope_checksum.is_empty()
            || proven_envelope.envelope.envelope_checksum != expected_envelope_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven page batch allocation reservation envelope checksum mismatch".to_string(),
            ));
        }
        let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
        if expected_raft_entry_checksum.is_empty()
            || proven_envelope.envelope.raft_entry_checksum != expected_raft_entry_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven page batch allocation reservation raft entry checksum mismatch"
                    .to_string(),
            ));
        }
        let prepared = self.prepare_page_batch_allocation_reservation_raft_entry(
            proven_envelope.envelope.lengths.clone(),
            proven_envelope.envelope.now_ms,
            &proven_envelope.envelope.idempotency_key,
        )?;
        let actual_envelope = make_prepared_page_batch_allocation_reservation_envelope(&prepared);
        if actual_envelope.envelope_checksum != proven_envelope.envelope.envelope_checksum {
            return Err(ObjectError::PreconditionFailed(
                "proven page batch allocation reservation prepared envelope changed".to_string(),
            ));
        }
        self.propose_prepared_page_batch_allocation_reservation_fenced(
            prepared,
            expected_raft_entry_checksum,
        )
    }
}
