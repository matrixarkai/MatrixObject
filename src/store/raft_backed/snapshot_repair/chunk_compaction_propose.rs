// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_prepared_chunk_compaction(
        &mut self,
        prepared: PreparedChunkCompaction,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_prepared_chunk_compaction_apply(prepared)
    }

    fn propose_prepared_chunk_compaction_apply(
        &mut self,
        prepared: PreparedChunkCompaction,
    ) -> Result<ObjectMetadata, ObjectError> {
        if self.raft.commit_index() != self.store.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "prepared chunk compaction requires raft/store commit alignment".to_string(),
            ));
        }
        self.store
            .clone()
            .apply_command(ObjectCommand::ApplyPreparedChunkCompaction {
                prepared: prepared.clone(),
            })?;
        let entry = self
            .raft
            .append(ObjectCommand::ApplyPreparedChunkCompaction { prepared })?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::PreconditionFailed(
                "prepared chunk compaction raft index is not contiguous".to_string(),
            ));
        }
        self.store.apply_raft_entry(&entry)
    }

    fn prepared_chunk_compaction_apply_report(
        &self,
        prepared: &PreparedChunkCompaction,
        metadata: ObjectMetadata,
    ) -> PreparedChunkCompactionApplyReport {
        let source_stale_bytes = prepared
            .source_stale_ranges
            .iter()
            .fold(0u64, |sum, range| sum.saturating_add(range.length));
        let source_discarded_bytes = prepared
            .source_discarded_ranges
            .iter()
            .fold(0u64, |sum, range| sum.saturating_add(range.length));
        let mutation_commit_index = self.raft.commit_index();
        PreparedChunkCompactionApplyReport {
            mutation_id: prepared.raft_entry_checksum.clone(),
            mutation_request_checksum: prepared.request_checksum.clone(),
            mutation_commit_index,
            next_log_index: mutation_commit_index.saturating_add(1),
            committed: true,
            prepared: false,
            external_raft_required: false,
            deduped: false,
            chunk_id: prepared.chunk_id.clone(),
            min_stale_bytes: prepared.min_stale_bytes,
            live_object_count: prepared.live_objects.len() as u64,
            source_stale_range_count: prepared.source_stale_ranges.len() as u64,
            source_stale_bytes,
            source_discarded_range_count: prepared.source_discarded_ranges.len() as u64,
            source_discarded_bytes,
            expected_object_gc_report_proof_checksum: prepared
                .expected_object_gc_report_proof_checksum
                .clone(),
            metadata,
        }
    }

    pub fn propose_prepared_chunk_compaction_with_status(
        &mut self,
        prepared: PreparedChunkCompaction,
    ) -> Result<PreparedChunkCompactionApplyReport, ObjectError> {
        let report_source = prepared.clone();
        let metadata = self.propose_prepared_chunk_compaction_apply(prepared)?;
        Ok(self.prepared_chunk_compaction_apply_report(&report_source, metadata))
    }

    pub fn propose_prepared_chunk_compaction_linearizable_fenced(
        &mut self,
        prepared: PreparedChunkCompaction,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready(
            "prepared segment compaction",
            expected_page_route_control_checksum,
        )?;
        if expected_page_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "prepared chunk compaction requires expected route control checksum".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "prepared chunk compaction requires prepared route control checksum".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum != expected_page_route_control_checksum {
            return Err(ObjectError::PreconditionFailed(
                "prepared chunk compaction route checksum does not match caller fence"
                    .to_string(),
            ));
        }
        self.propose_prepared_chunk_compaction(prepared)
    }

    pub fn propose_prepared_chunk_compaction_linearizable_fenced_with_status(
        &mut self,
        prepared: PreparedChunkCompaction,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<PreparedChunkCompactionApplyReport, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready(
            "prepared segment compaction",
            expected_page_route_control_checksum,
        )?;
        if expected_page_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "prepared chunk compaction requires expected route control checksum".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "prepared chunk compaction requires prepared route control checksum".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum != expected_page_route_control_checksum {
            return Err(ObjectError::PreconditionFailed(
                "prepared chunk compaction route checksum does not match caller fence"
                    .to_string(),
            ));
        }
        self.propose_prepared_chunk_compaction_with_status(prepared)
    }

    pub fn propose_prepared_chunk_compaction_linearizable_gc_proof_fenced(
        &mut self,
        prepared: PreparedChunkCompaction,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_object_gc_report_proof_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        let expected_object_gc_report_proof_checksum =
            expected_object_gc_report_proof_checksum.as_ref();
        if expected_object_gc_report_proof_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "prepared chunk compaction requires expected object GC report proof checksum"
                    .to_string(),
            ));
        }
        if prepared.expected_object_gc_report_proof_checksum
            != expected_object_gc_report_proof_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "prepared chunk compaction GC proof checksum does not match caller fence"
                    .to_string(),
            ));
        }
        self.store.gc_report_with_discard_policy_proof_fenced(
            prepared.min_stale_bytes,
            0,
            Some(expected_page_route_control_checksum),
            expected_object_gc_report_proof_checksum,
        )?;
        self.propose_prepared_chunk_compaction_linearizable_fenced(
            prepared,
            expected_page_route_control_checksum,
        )
    }

    pub fn propose_prepared_chunk_compaction_linearizable_gc_proof_fenced_with_status(
        &mut self,
        prepared: PreparedChunkCompaction,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_object_gc_report_proof_checksum: impl AsRef<str>,
    ) -> Result<PreparedChunkCompactionApplyReport, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        let expected_object_gc_report_proof_checksum =
            expected_object_gc_report_proof_checksum.as_ref();
        if expected_object_gc_report_proof_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "prepared chunk compaction requires expected object GC report proof checksum"
                    .to_string(),
            ));
        }
        if prepared.expected_object_gc_report_proof_checksum
            != expected_object_gc_report_proof_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "prepared chunk compaction GC proof checksum does not match caller fence"
                    .to_string(),
            ));
        }
        self.store.gc_report_with_discard_policy_proof_fenced(
            prepared.min_stale_bytes,
            0,
            Some(expected_page_route_control_checksum),
            expected_object_gc_report_proof_checksum,
        )?;
        self.propose_prepared_chunk_compaction_linearizable_fenced_with_status(
            prepared,
            expected_page_route_control_checksum,
        )
    }
}
