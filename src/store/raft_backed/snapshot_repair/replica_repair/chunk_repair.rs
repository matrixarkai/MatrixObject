impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn prepare_repair_page_chunk_raft_entry(
        &self,
        chunk_id: PageId,
    ) -> Result<PreparedPageChunkRepair, ObjectError> {
        self.store
            .prepare_page_chunk_repair_raft_entry(chunk_id, self.store.last_applied_index())
    }

    pub fn prepare_repair_page_chunk_raft_entry_linearizable_fenced(
        &self,
        chunk_id: PageId,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<PreparedPageChunkRepair, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready(
            "block chunk repair prepare",
            expected_page_route_control_checksum,
        )?;
        let mut prepared = self.prepare_repair_page_chunk_raft_entry(chunk_id)?;
        prepared.report.expected_route_control_checksum =
            expected_page_route_control_checksum.to_string();
        prepared.expected_route_control_checksum =
            expected_page_route_control_checksum.to_string();
        prepared.raft_entry_checksum = prepared_page_chunk_repair_checksum(&prepared);
        Ok(prepared)
    }

    pub fn prepare_repair_page_chunk_raft_entry_with_read_proof_linearizable_fenced(
        &self,
        chunk_id: PageId,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenStorageRead<PreparedPageChunkRepair>, ObjectError> {
        self.prepare_repair_page_chunk_raft_entry_with_read_proof_linearizable_fenced_committed_at(
            chunk_id,
            expected_page_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prepare_repair_page_chunk_raft_entry_with_read_proof_linearizable_fenced_committed_at(
        &self,
        chunk_id: PageId,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenStorageRead<PreparedPageChunkRepair>, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        let requested_read_index = self
            .raft
            .commit_index()
            .max(min_read_index)
            .max(min_commit_index);
        self.ensure_storage_control_read_index_committed_at(
            "prepare block chunk repair",
            Some(expected_page_route_control_checksum),
            min_read_index,
            min_commit_index,
        )?;
        let prepared = self.prepare_repair_page_chunk_raft_entry_linearizable_fenced(
            chunk_id,
            expected_page_route_control_checksum,
        )?;
        let proof = self.page_storage_read_proof_committed(requested_read_index, min_commit_index);
        Ok(ProvenStorageRead {
            value: prepared,
            proof,
        })
    }

    pub fn prepare_repair_page_chunk_envelope_linearizable_fenced(
        &self,
        chunk_id: PageId,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<PreparedPageRepairEnvelope, ObjectError> {
        let prepared = self.prepare_repair_page_chunk_raft_entry_linearizable_fenced(
            chunk_id,
            expected_page_route_control_checksum,
        )?;
        Ok(prepared_page_chunk_repair_envelope(&prepared))
    }

    pub fn prepare_repair_page_chunk_envelope_with_read_proof_linearizable_fenced(
        &self,
        chunk_id: PageId,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenPreparedPageRepairEnvelope, ObjectError> {
        self.prepare_repair_page_chunk_envelope_with_read_proof_linearizable_fenced_committed_at(
            chunk_id,
            expected_page_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prepare_repair_page_chunk_envelope_with_read_proof_linearizable_fenced_committed_at(
        &self,
        chunk_id: PageId,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenPreparedPageRepairEnvelope, ObjectError> {
        let prepared = self
            .prepare_repair_page_chunk_raft_entry_with_read_proof_linearizable_fenced_committed_at(
                chunk_id,
                expected_page_route_control_checksum,
                min_read_index,
                min_commit_index,
            )?;
        Ok(ProvenPreparedPageRepairEnvelope {
            envelope: prepared_page_chunk_repair_envelope(&prepared.value),
            proof: prepared.proof,
        })
    }

    pub fn propose_prepared_repair_page_chunk(
        &mut self,
        prepared: PreparedPageChunkRepair,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_prepared_repair_page_chunk_apply(prepared)
            .map(|(metadata, _, _)| metadata)
    }

    fn propose_prepared_repair_page_chunk_apply(
        &mut self,
        prepared: PreparedPageChunkRepair,
    ) -> Result<(ObjectMetadata, ReplicaRepairReport, ReplicaRepairReceipt), ObjectError> {
        if self.raft.commit_index() != self.store.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "prepared block chunk repair requires raft/store commit alignment".to_string(),
            ));
        }
        let (report, receipt) = self
            .store
            .clone()
            .apply_prepared_page_chunk_repair(prepared.clone())?;
        let entry = self
            .raft
            .append(ObjectCommand::ApplyPreparedPageChunkRepair { prepared })?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::PreconditionFailed(
                "prepared block chunk repair raft index is not contiguous".to_string(),
            ));
        }
        let metadata = self.store.apply_raft_entry(&entry)?;
        Ok((metadata, report, receipt))
    }

    pub fn propose_prepared_repair_page_chunk_with_status(
        &mut self,
        prepared: PreparedPageChunkRepair,
    ) -> Result<PreparedPageChunkRepairApplyReport, ObjectError> {
        let mutation_id = prepared.raft_entry_checksum.clone();
        let mutation_request_checksum = prepared.raft_entry_checksum.clone();
        let (metadata, report, receipt) =
            self.propose_prepared_repair_page_chunk_apply(prepared)?;
        let mutation_commit_index = self.raft.commit_index();
        Ok(PreparedPageChunkRepairApplyReport {
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

    pub fn propose_repair_page_chunk_prepared_linearizable_fenced(
        &mut self,
        chunk_id: PageId,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_repair_page_chunk_raft_entry_linearizable_fenced(
            chunk_id,
            expected_page_route_control_checksum,
        )?;
        let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
        if expected_raft_entry_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "block chunk repair requires expected raft entry checksum".to_string(),
            ));
        }
        prepared.verify_expected_raft_entry_checksum(expected_raft_entry_checksum)?;
        self.propose_prepared_repair_page_chunk(prepared)
    }

    pub fn propose_proven_repair_page_chunk_fenced(
        &mut self,
        proven_prepared: ProvenStorageRead<PreparedPageChunkRepair>,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        if expected_page_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven block chunk repair requires expected route control checksum".to_string(),
            ));
        }
        if proven_prepared.proof.control_checksum != expected_page_route_control_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven block chunk repair route checksum mismatch: expected {} actual {}",
                expected_page_route_control_checksum, proven_prepared.proof.control_checksum
            )));
        }
        if self.store.last_applied_index() < proven_prepared.proof.storage_applied_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven block chunk repair storage proof is not applied: applied={} required={}",
                self.store.last_applied_index(),
                proven_prepared.proof.storage_applied_index
            )));
        }
        let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
        if expected_raft_entry_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven block chunk repair requires expected raft entry checksum".to_string(),
            ));
        }
        proven_prepared
            .value
            .verify_expected_raft_entry_checksum(expected_raft_entry_checksum)?;
        if proven_prepared.value.expected_route_control_checksum
            != expected_page_route_control_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven block chunk repair prepared route checksum does not match caller fence"
                    .to_string(),
            ));
        }
        self.propose_prepared_repair_page_chunk(proven_prepared.value)
    }

    pub fn propose_proven_repair_page_chunk_envelope_fenced(
        &mut self,
        proven_envelope: ProvenPreparedPageRepairEnvelope,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_envelope_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        if expected_page_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven block chunk repair envelope requires expected route control checksum"
                    .to_string(),
            ));
        }
        if proven_envelope.envelope.operation != "chunk" {
            return Err(ObjectError::InvalidArgument(
                "proven block chunk repair envelope requires chunk operation".to_string(),
            ));
        }
        if proven_envelope.proof.control_checksum != expected_page_route_control_checksum
            || proven_envelope.envelope.expected_route_control_checksum
                != expected_page_route_control_checksum
            || proven_envelope.envelope.route_control_proof_checksum
                != expected_page_route_control_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven block chunk repair envelope route checksum mismatch".to_string(),
            ));
        }
        if self.store.last_applied_index() < proven_envelope.proof.storage_applied_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven block chunk repair envelope storage proof is not applied: applied={} required={}",
                self.store.last_applied_index(),
                proven_envelope.proof.storage_applied_index
            )));
        }
        let expected_envelope_checksum = expected_envelope_checksum.as_ref();
        if expected_envelope_checksum.is_empty()
            || proven_envelope.envelope.envelope_checksum != expected_envelope_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven block chunk repair envelope checksum mismatch".to_string(),
            ));
        }
        let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
        if expected_raft_entry_checksum.is_empty()
            || proven_envelope.envelope.raft_entry_checksum != expected_raft_entry_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven block chunk repair envelope raft entry checksum mismatch".to_string(),
            ));
        }
        let chunk_id = proven_envelope.envelope.chunk_id.clone().ok_or_else(|| {
            ObjectError::InvalidArgument(
                "proven block chunk repair envelope missing chunk id".to_string(),
            )
        })?;
        let prepared = self.prepare_repair_page_chunk_raft_entry_linearizable_fenced(
            chunk_id,
            expected_page_route_control_checksum,
        )?;
        let actual_envelope = prepared_page_chunk_repair_envelope(&prepared);
        if actual_envelope != proven_envelope.envelope {
            return Err(ObjectError::PreconditionFailed(
                "proven block chunk repair envelope changed before proposal".to_string(),
            ));
        }
        self.propose_prepared_repair_page_chunk(prepared)
    }

}
