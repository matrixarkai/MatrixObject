// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_prepared_repair_audited_page_replicas(
        &mut self,
        prepared: PreparedPageAuditRepair,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_prepared_repair_audited_page_replicas_apply(prepared)
            .map(|(metadata, _, _)| metadata)
    }

    fn propose_prepared_repair_audited_page_replicas_apply(
        &mut self,
        prepared: PreparedPageAuditRepair,
    ) -> Result<
        (
            ObjectMetadata,
            ChunkReplicaAuditRepairReport,
            ChunkAuditRepairReceipt,
        ),
        ObjectError,
    > {
        if self.raft.commit_index() != self.store.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "prepared page audit repair requires raft/store commit alignment".to_string(),
            ));
        }
        let (report, receipt) = self
            .store
            .clone()
            .apply_prepared_page_audit_repair(prepared.clone())?;
        let entry = self
            .raft
            .append(ObjectCommand::ApplyPreparedPageAuditRepair { prepared })?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::PreconditionFailed(
                "prepared page audit repair raft index is not contiguous".to_string(),
            ));
        }
        let metadata = self.store.apply_raft_entry(&entry)?;
        Ok((metadata, report, receipt))
    }

    pub fn propose_prepared_repair_audited_page_replicas_with_status(
        &mut self,
        prepared: PreparedPageAuditRepair,
    ) -> Result<PreparedPageAuditRepairApplyReport, ObjectError> {
        let mutation_id = prepared.raft_entry_checksum.clone();
        let mutation_request_checksum = prepared.raft_entry_checksum.clone();
        let (metadata, report, receipt) =
            self.propose_prepared_repair_audited_page_replicas_apply(prepared)?;
        let mutation_commit_index = self.raft.commit_index();
        Ok(PreparedPageAuditRepairApplyReport {
            mutation_id,
            mutation_request_checksum,
            mutation_commit_index,
            next_log_index: mutation_commit_index.saturating_add(1),
            committed: true,
            prepared: false,
            external_raft_required: false,
            deduped: receipt.deduped,
            receipt,
            report,
            metadata,
        })
    }

    pub fn propose_repair_audited_page_replicas_prepared(
        &mut self,
        after_chunk_id: Option<PageId>,
        max_chunks: usize,
        max_payload_bytes: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_repair_audited_page_replicas_raft_entry(
            after_chunk_id,
            max_chunks,
            max_payload_bytes,
        )?;
        self.propose_prepared_repair_audited_page_replicas(prepared)
    }

    pub fn propose_repair_audited_page_replicas_prepared_bounded(
        &mut self,
        after_chunk_id: Option<PageId>,
        max_chunks: usize,
        max_payload_bytes: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_repair_audited_page_replicas_raft_entry_bounded(
            after_chunk_id,
            max_chunks,
            max_payload_bytes,
        )?;
        self.propose_prepared_repair_audited_page_replicas(prepared)
    }

    pub fn propose_repair_audited_page_replicas_linearizable_fenced(
        &mut self,
        after_chunk_id: Option<PageId>,
        max_chunks: usize,
        max_payload_bytes: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_repair_audited_page_replicas_raft_entry_linearizable_fenced(
            after_chunk_id,
            max_chunks,
            max_payload_bytes,
            expected_page_route_control_checksum,
        )?;
        let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
        if expected_raft_entry_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "page audit repair requires expected raft entry checksum".to_string(),
            ));
        }
        prepared.verify_expected_raft_entry_checksum(expected_raft_entry_checksum)?;
        self.propose_prepared_repair_audited_page_replicas(prepared)
    }

    pub fn propose_repair_audited_page_replicas_linearizable_fenced_with_status(
        &mut self,
        after_chunk_id: Option<PageId>,
        max_chunks: usize,
        max_payload_bytes: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<PreparedPageAuditRepairApplyReport, ObjectError> {
        let prepared = self.prepare_repair_audited_page_replicas_raft_entry_linearizable_fenced(
            after_chunk_id,
            max_chunks,
            max_payload_bytes,
            expected_page_route_control_checksum,
        )?;
        let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
        if expected_raft_entry_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "page audit repair requires expected raft entry checksum".to_string(),
            ));
        }
        prepared.verify_expected_raft_entry_checksum(expected_raft_entry_checksum)?;
        self.propose_prepared_repair_audited_page_replicas_with_status(prepared)
    }

    pub fn propose_proven_repair_audited_page_replicas_fenced(
        &mut self,
        proven_prepared: ProvenStorageRead<PreparedPageAuditRepair>,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        if expected_page_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven page audit repair requires expected route control checksum".to_string(),
            ));
        }
        if proven_prepared.proof.control_checksum != expected_page_route_control_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven page audit repair route checksum mismatch: expected {} actual {}",
                expected_page_route_control_checksum, proven_prepared.proof.control_checksum
            )));
        }
        if self.store.last_applied_index() < proven_prepared.proof.storage_applied_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven page audit repair storage proof is not applied: applied={} required={}",
                self.store.last_applied_index(),
                proven_prepared.proof.storage_applied_index
            )));
        }
        let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
        if expected_raft_entry_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven page audit repair requires expected raft entry checksum".to_string(),
            ));
        }
        proven_prepared
            .value
            .verify_expected_raft_entry_checksum(expected_raft_entry_checksum)?;
        if proven_prepared.value.expected_route_control_checksum
            != expected_page_route_control_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven page audit repair prepared route checksum does not match caller fence"
                    .to_string(),
            ));
        }
        self.propose_prepared_repair_audited_page_replicas(proven_prepared.value)
    }

    pub fn propose_proven_repair_audited_page_replicas_envelope_fenced(
        &mut self,
        proven_envelope: ProvenPreparedPageAuditRepairEnvelope,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_envelope_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        if expected_page_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven page audit repair envelope requires expected route control checksum"
                    .to_string(),
            ));
        }
        if proven_envelope.proof.control_checksum != expected_page_route_control_checksum
            || proven_envelope.envelope.expected_route_control_checksum
                != expected_page_route_control_checksum
            || proven_envelope.envelope.route_control_proof_checksum
                != expected_page_route_control_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven page audit repair envelope route checksum mismatch".to_string(),
            ));
        }
        if self.store.last_applied_index() < proven_envelope.proof.storage_applied_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven page audit repair envelope storage proof is not applied: applied={} required={}",
                self.store.last_applied_index(),
                proven_envelope.proof.storage_applied_index
            )));
        }
        let expected_envelope_checksum = expected_envelope_checksum.as_ref();
        if expected_envelope_checksum.is_empty()
            || proven_envelope.envelope.envelope_checksum != expected_envelope_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven page audit repair envelope checksum mismatch".to_string(),
            ));
        }
        let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
        if expected_raft_entry_checksum.is_empty()
            || proven_envelope.envelope.raft_entry_checksum != expected_raft_entry_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven page audit repair envelope raft entry checksum mismatch".to_string(),
            ));
        }
        let prepared = self.prepare_repair_audited_page_replicas_raft_entry_linearizable_fenced(
            proven_envelope.envelope.after_chunk_id.clone(),
            proven_envelope.envelope.max_chunks as usize,
            proven_envelope.envelope.max_payload_bytes,
            expected_page_route_control_checksum,
        )?;
        let actual_envelope = make_prepared_page_audit_repair_envelope(&prepared);
        if actual_envelope != proven_envelope.envelope {
            return Err(ObjectError::PreconditionFailed(
                "proven page audit repair envelope changed before proposal".to_string(),
            ));
        }
        self.propose_prepared_repair_audited_page_replicas(prepared)
    }
}
