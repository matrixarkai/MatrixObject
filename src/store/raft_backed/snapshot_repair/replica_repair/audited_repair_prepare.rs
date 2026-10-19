impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn prepare_repair_audited_page_replicas_raft_entry(
        &self,
        after_chunk_id: Option<PageId>,
        max_chunks: usize,
        max_payload_bytes: u64,
    ) -> Result<PreparedPageAuditRepair, ObjectError> {
        self.store.prepare_page_audit_repair_raft_entry(
            after_chunk_id,
            max_chunks,
            max_payload_bytes,
            self.store.last_applied_index(),
        )
    }

    pub fn prepare_repair_audited_page_replicas_raft_entry_bounded(
        &self,
        after_chunk_id: Option<PageId>,
        max_chunks: usize,
        max_payload_bytes: u64,
    ) -> Result<PreparedPageAuditRepair, ObjectError> {
        self.store.prepare_page_audit_repair_raft_entry_bounded(
            after_chunk_id,
            max_chunks,
            max_payload_bytes,
            self.store.last_applied_index(),
        )
    }

    pub fn prepare_repair_audited_page_replicas_raft_entry_linearizable(
        &self,
        after_chunk_id: Option<PageId>,
        max_chunks: usize,
        max_payload_bytes: u64,
    ) -> Result<PreparedPageAuditRepair, ObjectError> {
        self.prepare_repair_audited_page_replicas_raft_entry_linearizable_fenced(
            after_chunk_id,
            max_chunks,
            max_payload_bytes,
            "",
        )
    }

    pub fn prepare_repair_audited_page_replicas_raft_entry_linearizable_fenced(
        &self,
        after_chunk_id: Option<PageId>,
        max_chunks: usize,
        max_payload_bytes: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<PreparedPageAuditRepair, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready(
            "block audit repair",
            expected_page_route_control_checksum,
        )?;
        let mut prepared = self.prepare_repair_audited_page_replicas_raft_entry_bounded(
            after_chunk_id,
            max_chunks,
            max_payload_bytes,
        )?;
        prepared.report.expected_route_control_checksum =
            expected_page_route_control_checksum.to_string();
        prepared.expected_route_control_checksum =
            expected_page_route_control_checksum.to_string();
        prepared.raft_entry_checksum = prepared_page_audit_repair_checksum(&prepared);
        Ok(prepared)
    }

    pub fn prepare_repair_audited_page_replicas_raft_entry_with_read_proof_linearizable_fenced(
        &self,
        after_chunk_id: Option<PageId>,
        max_chunks: usize,
        max_payload_bytes: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenStorageRead<PreparedPageAuditRepair>, ObjectError> {
        self.prepare_repair_audited_page_replicas_raft_entry_with_read_proof_linearizable_fenced_committed_at(
            after_chunk_id,
            max_chunks,
            max_payload_bytes,
            expected_page_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prepare_repair_audited_page_replicas_raft_entry_with_read_proof_linearizable_fenced_committed_at(
        &self,
        after_chunk_id: Option<PageId>,
        max_chunks: usize,
        max_payload_bytes: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenStorageRead<PreparedPageAuditRepair>, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        let requested_read_index = self
            .raft
            .commit_index()
            .max(min_read_index)
            .max(min_commit_index);
        self.ensure_page_route_control_ready_at(
            "block audit repair",
            expected_page_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        let prepared = self.prepare_repair_audited_page_replicas_raft_entry_linearizable_fenced(
            after_chunk_id,
            max_chunks,
            max_payload_bytes,
            expected_page_route_control_checksum,
        )?;
        let proof = self.page_storage_read_proof_committed(requested_read_index, min_commit_index);
        Ok(ProvenStorageRead {
            value: prepared,
            proof,
        })
    }

    pub fn prepare_repair_audited_page_replicas_envelope_linearizable_fenced(
        &self,
        after_chunk_id: Option<PageId>,
        max_chunks: usize,
        max_payload_bytes: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<PreparedPageAuditRepairEnvelope, ObjectError> {
        let prepared = self.prepare_repair_audited_page_replicas_raft_entry_linearizable_fenced(
            after_chunk_id,
            max_chunks,
            max_payload_bytes,
            expected_page_route_control_checksum,
        )?;
        Ok(make_prepared_page_audit_repair_envelope(&prepared))
    }

    pub fn prepare_repair_audited_page_replicas_envelope_with_read_proof_linearizable_fenced(
        &self,
        after_chunk_id: Option<PageId>,
        max_chunks: usize,
        max_payload_bytes: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenPreparedPageAuditRepairEnvelope, ObjectError> {
        self.prepare_repair_audited_page_replicas_envelope_with_read_proof_linearizable_fenced_committed_at(
            after_chunk_id,
            max_chunks,
            max_payload_bytes,
            expected_page_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prepare_repair_audited_page_replicas_envelope_with_read_proof_linearizable_fenced_committed_at(
        &self,
        after_chunk_id: Option<PageId>,
        max_chunks: usize,
        max_payload_bytes: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenPreparedPageAuditRepairEnvelope, ObjectError> {
        let proven_prepared = self
            .prepare_repair_audited_page_replicas_raft_entry_with_read_proof_linearizable_fenced_committed_at(
                after_chunk_id,
                max_chunks,
                max_payload_bytes,
                expected_page_route_control_checksum,
                min_read_index,
                min_commit_index,
            )?;
        Ok(ProvenPreparedPageAuditRepairEnvelope {
            envelope: make_prepared_page_audit_repair_envelope(&proven_prepared.value),
            proof: proven_prepared.proof,
        })
    }
}
