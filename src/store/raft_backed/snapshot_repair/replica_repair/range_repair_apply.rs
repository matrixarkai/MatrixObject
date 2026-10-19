impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_prepared_repair_page_range(
        &mut self,
        prepared: PreparedPageRangeRepair,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_prepared_repair_page_range_apply(prepared)
            .map(|(metadata, _, _)| metadata)
    }

    fn propose_prepared_repair_page_range_apply(
        &mut self,
        prepared: PreparedPageRangeRepair,
    ) -> Result<(ObjectMetadata, BoundedRepairReport, ChunkRangeRepairReceipt), ObjectError> {
        if self.raft.commit_index() != self.store.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "prepared page range repair requires raft/store commit alignment".to_string(),
            ));
        }
        let (report, receipt) = self
            .store
            .clone()
            .apply_prepared_page_range_repair(prepared.clone())?;
        let entry = self
            .raft
            .append(ObjectCommand::ApplyPreparedPageRangeRepair { prepared })?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::PreconditionFailed(
                "prepared page range repair raft index is not contiguous".to_string(),
            ));
        }
        let metadata = self.store.apply_raft_entry(&entry)?;
        Ok((metadata, report, receipt))
    }

    pub fn propose_prepared_repair_page_range_with_status(
        &mut self,
        prepared: PreparedPageRangeRepair,
    ) -> Result<PreparedPageRangeRepairApplyReport, ObjectError> {
        let mutation_id = prepared.raft_entry_checksum.clone();
        let mutation_request_checksum = prepared.raft_entry_checksum.clone();
        let (metadata, report, receipt) =
            self.propose_prepared_repair_page_range_apply(prepared)?;
        let mutation_commit_index = self.raft.commit_index();
        Ok(PreparedPageRangeRepairApplyReport {
            mutation_id,
            mutation_request_checksum,
            mutation_commit_index,
            next_log_index: mutation_commit_index.saturating_add(1),
            committed: true,
            prepared: false,
            external_raft_required: false,
            deduped: receipt.deduped,
            report,
            receipt,
            metadata,
        })
    }

    pub fn propose_repair_page_range_prepared_linearizable_fenced(
        &mut self,
        chunk_id: ChunkId,
        offset: u64,
        length: u64,
        max_repair_chunks: usize,
        start_chunk_index: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_repair_page_range_raft_entry_linearizable_fenced(
            chunk_id,
            offset,
            length,
            max_repair_chunks,
            start_chunk_index,
            expected_page_route_control_checksum,
        )?;
        let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
        if expected_raft_entry_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "page range repair requires expected raft entry checksum".to_string(),
            ));
        }
        prepared.verify_expected_raft_entry_checksum(expected_raft_entry_checksum)?;
        self.propose_prepared_repair_page_range(prepared)
    }

    pub fn propose_proven_repair_page_range_fenced(
        &mut self,
        proven_prepared: ProvenStorageRead<PreparedPageRangeRepair>,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        if expected_page_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven page range repair requires expected route control checksum".to_string(),
            ));
        }
        if proven_prepared.proof.control_checksum != expected_page_route_control_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven page range repair route checksum mismatch: expected {} actual {}",
                expected_page_route_control_checksum, proven_prepared.proof.control_checksum
            )));
        }
        if self.store.last_applied_index() < proven_prepared.proof.storage_applied_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven page range repair storage proof is not applied: applied={} required={}",
                self.store.last_applied_index(),
                proven_prepared.proof.storage_applied_index
            )));
        }
        let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
        if expected_raft_entry_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven page range repair requires expected raft entry checksum".to_string(),
            ));
        }
        proven_prepared
            .value
            .verify_expected_raft_entry_checksum(expected_raft_entry_checksum)?;
        if proven_prepared.value.expected_route_control_checksum
            != expected_page_route_control_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven page range repair prepared route checksum does not match caller fence"
                    .to_string(),
            ));
        }
        self.propose_prepared_repair_page_range(proven_prepared.value)
    }

    pub fn propose_proven_repair_page_range_envelope_fenced(
        &mut self,
        proven_envelope: ProvenPreparedPageRepairEnvelope,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_envelope_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        if expected_page_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven page range repair envelope requires expected route control checksum"
                    .to_string(),
            ));
        }
        if proven_envelope.envelope.operation != "range" {
            return Err(ObjectError::InvalidArgument(
                "proven page range repair envelope requires range operation".to_string(),
            ));
        }
        if proven_envelope.proof.control_checksum != expected_page_route_control_checksum
            || proven_envelope.envelope.expected_route_control_checksum
                != expected_page_route_control_checksum
            || proven_envelope.envelope.route_control_proof_checksum
                != expected_page_route_control_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven page range repair envelope route checksum mismatch".to_string(),
            ));
        }
        if self.store.last_applied_index() < proven_envelope.proof.storage_applied_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven page range repair envelope storage proof is not applied: applied={} required={}",
                self.store.last_applied_index(),
                proven_envelope.proof.storage_applied_index
            )));
        }
        let expected_envelope_checksum = expected_envelope_checksum.as_ref();
        if expected_envelope_checksum.is_empty()
            || proven_envelope.envelope.envelope_checksum != expected_envelope_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven page range repair envelope checksum mismatch".to_string(),
            ));
        }
        let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
        if expected_raft_entry_checksum.is_empty()
            || proven_envelope.envelope.raft_entry_checksum != expected_raft_entry_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven page range repair envelope raft entry checksum mismatch".to_string(),
            ));
        }
        let chunk_id = proven_envelope.envelope.chunk_id.clone().ok_or_else(|| {
            ObjectError::InvalidArgument(
                "proven page range repair envelope missing chunk id".to_string(),
            )
        })?;
        let prepared = self.prepare_repair_page_range_raft_entry_linearizable_fenced(
            chunk_id,
            proven_envelope.envelope.offset,
            proven_envelope.envelope.length,
            proven_envelope.envelope.max_repair_chunks as usize,
            proven_envelope.envelope.start_chunk_index,
            expected_page_route_control_checksum,
        )?;
        let actual_envelope = prepared_page_range_repair_envelope(&prepared);
        if actual_envelope != proven_envelope.envelope {
            return Err(ObjectError::PreconditionFailed(
                "proven page range repair envelope changed before proposal".to_string(),
            ));
        }
        self.propose_prepared_repair_page_range(prepared)
    }
}
