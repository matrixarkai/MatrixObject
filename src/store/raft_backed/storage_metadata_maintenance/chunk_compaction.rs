impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_proven_chunk_compaction_fenced(
        &mut self,
        proven_prepared: ProvenStorageRead<PreparedChunkCompaction>,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        if expected_page_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven chunk compaction requires expected route control checksum".to_string(),
            ));
        }
        if proven_prepared.proof.control_checksum != expected_page_route_control_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven chunk compaction route checksum mismatch: expected {} actual {}",
                expected_page_route_control_checksum, proven_prepared.proof.control_checksum
            )));
        }
        if self.store.last_applied_index() < proven_prepared.proof.storage_applied_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven chunk compaction storage proof is not applied: applied={} required={}",
                self.store.last_applied_index(),
                proven_prepared.proof.storage_applied_index
            )));
        }
        let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
        if expected_raft_entry_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven chunk compaction requires expected raft entry checksum".to_string(),
            ));
        }
        proven_prepared
            .value
            .verify_expected_raft_entry_checksum(expected_raft_entry_checksum)?;
        if proven_prepared.value.expected_route_control_checksum
            != expected_page_route_control_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven chunk compaction prepared route checksum does not match caller fence"
                    .to_string(),
            ));
        }
        self.propose_prepared_chunk_compaction_linearizable_fenced(
            proven_prepared.value,
            expected_page_route_control_checksum,
        )
    }

    pub fn propose_proven_chunk_compaction_envelope_fenced(
        &mut self,
        proven_envelope: ProvenStorageRead<PreparedChunkCompactionEnvelope>,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_envelope_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        if expected_page_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven chunk compaction envelope requires expected route control checksum"
                    .to_string(),
            ));
        }
        if proven_envelope.proof.control_checksum != expected_page_route_control_checksum
            || proven_envelope.value.expected_route_control_checksum
                != expected_page_route_control_checksum
            || proven_envelope.value.route_control_proof_checksum
                != expected_page_route_control_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven chunk compaction envelope route checksum mismatch".to_string(),
            ));
        }
        if self.store.last_applied_index() < proven_envelope.proof.storage_applied_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven chunk compaction envelope storage proof is not applied: applied={} required={}",
                self.store.last_applied_index(),
                proven_envelope.proof.storage_applied_index
            )));
        }
        let expected_envelope_checksum = expected_envelope_checksum.as_ref();
        if expected_envelope_checksum.is_empty()
            || proven_envelope.value.envelope_checksum != expected_envelope_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven chunk compaction envelope checksum mismatch".to_string(),
            ));
        }
        let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
        if expected_raft_entry_checksum.is_empty()
            || proven_envelope.value.raft_entry_checksum != expected_raft_entry_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven chunk compaction envelope raft entry checksum mismatch".to_string(),
            ));
        }
        let prepared = if proven_envelope
            .value
            .expected_object_gc_report_proof_checksum
            .is_empty()
        {
            self.prepare_compact_chunk_raft_entry_linearizable_fenced(
                &proven_envelope.value.chunk_id,
                proven_envelope.value.min_stale_bytes,
                proven_envelope.value.update_time_ms,
                expected_page_route_control_checksum,
            )?
        } else {
            self.prepare_compact_chunk_raft_entry_linearizable_gc_proof_fenced(
                &proven_envelope.value.chunk_id,
                proven_envelope.value.min_stale_bytes,
                proven_envelope.value.update_time_ms,
                expected_page_route_control_checksum,
                &proven_envelope
                    .value
                    .expected_object_gc_report_proof_checksum,
            )?
        };
        let actual_envelope = make_prepared_chunk_compaction_envelope(&prepared);
        if actual_envelope != proven_envelope.value {
            return Err(ObjectError::PreconditionFailed(
                "proven chunk compaction envelope changed before proposal".to_string(),
            ));
        }
        if proven_envelope
            .value
            .expected_object_gc_report_proof_checksum
            .is_empty()
        {
            self.propose_prepared_chunk_compaction_linearizable_fenced(
                prepared,
                expected_page_route_control_checksum,
            )
        } else {
            self.propose_prepared_chunk_compaction_linearizable_gc_proof_fenced(
                prepared,
                expected_page_route_control_checksum,
                &proven_envelope
                    .value
                    .expected_object_gc_report_proof_checksum,
            )
        }
    }

    pub fn propose_compact_chunk_prepared(
        &mut self,
        chunk_id: &ChunkId,
        min_stale_bytes: u64,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared =
            self.prepare_compact_chunk_raft_entry(chunk_id, min_stale_bytes, update_time_ms)?;
        self.propose_prepared_chunk_compaction(prepared)
    }

    pub fn propose_compact_chunk_prepared_linearizable_fenced(
        &mut self,
        chunk_id: &ChunkId,
        min_stale_bytes: u64,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        let prepared = self.prepare_compact_chunk_raft_entry_linearizable_fenced(
            chunk_id,
            min_stale_bytes,
            update_time_ms,
            expected_page_route_control_checksum,
        )?;
        self.propose_prepared_chunk_compaction_linearizable_fenced(
            prepared,
            expected_page_route_control_checksum,
        )
    }

    pub fn propose_compact_chunk_prepared_linearizable_gc_proof_fenced(
        &mut self,
        chunk_id: &ChunkId,
        min_stale_bytes: u64,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_object_gc_report_proof_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        let expected_object_gc_report_proof_checksum =
            expected_object_gc_report_proof_checksum.as_ref();
        let prepared = self.prepare_compact_chunk_raft_entry_linearizable_gc_proof_fenced(
            chunk_id,
            min_stale_bytes,
            update_time_ms,
            expected_page_route_control_checksum,
            expected_object_gc_report_proof_checksum,
        )?;
        self.propose_prepared_chunk_compaction_linearizable_gc_proof_fenced(
            prepared,
            expected_page_route_control_checksum,
            expected_object_gc_report_proof_checksum,
        )
    }

    pub fn propose_compact_all(
        &mut self,
        min_stale_bytes: u64,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::CompactAll {
            min_stale_bytes,
            update_time_ms,
            expected_page_route_control_checksum: String::new(),
            expected_object_gc_report_proof_checksum: String::new(),
            expected_full_compaction_plan_checksum: String::new(),
        })
    }

    pub fn plan_full_compaction_with_read_proof_linearizable_fenced(
        &self,
        min_stale_bytes: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_object_gc_report_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenStorageRead<FullCompactionPlan>, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        let requested_read_index = self.raft.commit_index();
        self.ensure_page_route_control_ready(
            "full compaction plan",
            expected_page_route_control_checksum,
        )?;
        let plan = self.store.plan_full_compaction_fenced(
            min_stale_bytes,
            Some(expected_page_route_control_checksum),
            expected_object_gc_report_proof_checksum,
        )?;
        let proof = self.page_storage_read_proof(requested_read_index);
        Ok(ProvenStorageRead { value: plan, proof })
    }

    pub fn propose_compact_all_linearizable_gc_proof_fenced(
        &mut self,
        min_stale_bytes: u64,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_object_gc_report_proof_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        let expected_object_gc_report_proof_checksum =
            expected_object_gc_report_proof_checksum.as_ref();
        self.ensure_page_route_control_ready(
            "full compaction",
            expected_page_route_control_checksum,
        )?;
        if expected_object_gc_report_proof_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "full compaction requires expected object GC report proof checksum".to_string(),
            ));
        }
        self.store.gc_report_with_discard_policy_proof_fenced(
            min_stale_bytes,
            0,
            Some(expected_page_route_control_checksum),
            expected_object_gc_report_proof_checksum,
        )?;
        self.propose(ObjectCommand::CompactAll {
            min_stale_bytes,
            update_time_ms,
            expected_page_route_control_checksum: expected_page_route_control_checksum
                .to_string(),
            expected_object_gc_report_proof_checksum: expected_object_gc_report_proof_checksum
                .to_string(),
            expected_full_compaction_plan_checksum: String::new(),
        })
    }

    pub fn propose_compact_all_linearizable_plan_fenced(
        &mut self,
        min_stale_bytes: u64,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_object_gc_report_proof_checksum: impl AsRef<str>,
        expected_full_compaction_plan_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        let expected_object_gc_report_proof_checksum =
            expected_object_gc_report_proof_checksum.as_ref();
        let expected_full_compaction_plan_checksum =
            expected_full_compaction_plan_checksum.as_ref();
        let plan = self.store.plan_full_compaction_fenced(
            min_stale_bytes,
            Some(expected_page_route_control_checksum),
            expected_object_gc_report_proof_checksum,
        )?;
        plan.verify_expected_proof_checksum(expected_full_compaction_plan_checksum)?;
        self.propose(ObjectCommand::CompactAll {
            min_stale_bytes,
            update_time_ms,
            expected_page_route_control_checksum: expected_page_route_control_checksum
                .to_string(),
            expected_object_gc_report_proof_checksum: expected_object_gc_report_proof_checksum
                .to_string(),
            expected_full_compaction_plan_checksum: expected_full_compaction_plan_checksum
                .to_string(),
        })
    }

}
