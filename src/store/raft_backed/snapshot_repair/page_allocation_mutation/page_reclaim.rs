// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn prepare_page_reclaim_raft_entry_bounded_linearizable_fenced(
        &self,
        max_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
        min_discard_ratio_ppm: u64,
        max_discard_ranges: usize,
        max_trim_bytes: u64,
        now_ms: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedPageReplicaReclaim, ObjectError> {
        self.ensure_storage_control_read_index(
            "prepare block aggregate reclaim",
            expected_route_control_checksum,
        )?;
        self.store.prepare_page_reclaim_raft_entry_fenced(
            max_chunks,
            max_rewrite_bytes,
            min_discarded_bytes,
            min_discard_ratio_ppm,
            max_discard_ranges,
            max_trim_bytes,
            now_ms,
            self.store.last_applied_index(),
            expected_route_control_checksum,
        )
    }

    pub fn propose_prepared_page_reclaim(
        &mut self,
        prepared: PreparedPageReplicaReclaim,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_prepared_page_reclaim_apply(prepared)
            .map(|(metadata, _)| metadata)
    }

    fn propose_prepared_page_reclaim_apply(
        &mut self,
        prepared: PreparedPageReplicaReclaim,
    ) -> Result<(ObjectMetadata, ReplicaPartialReclaimReport), ObjectError> {
        if self.raft.commit_index() != self.store.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "prepared page reclaim requires raft/store commit alignment".to_string(),
            ));
        }
        let report = self
            .store
            .clone()
            .apply_prepared_page_reclaim(prepared.clone())?;
        let entry = self
            .raft
            .append(ObjectCommand::ApplyPreparedPageReclaim { prepared })?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::PreconditionFailed(
                "prepared page reclaim raft index is not contiguous".to_string(),
            ));
        }
        let metadata = self.store.apply_raft_entry(&entry)?;
        Ok((metadata, report))
    }

    pub fn propose_prepared_page_reclaim_with_status(
        &mut self,
        prepared: PreparedPageReplicaReclaim,
    ) -> Result<PreparedPageReclaimApplyReport, ObjectError> {
        let mutation_id = prepared.raft_entry_checksum.clone();
        let mutation_request_checksum = prepared.raft_entry_checksum.clone();
        let (metadata, report) = self.propose_prepared_page_reclaim_apply(prepared)?;
        let mutation_commit_index = self.raft.commit_index();
        Ok(PreparedPageReclaimApplyReport {
            mutation_id,
            mutation_request_checksum,
            mutation_commit_index,
            next_log_index: mutation_commit_index.saturating_add(1),
            committed: true,
            prepared: false,
            external_raft_required: false,
            deduped: false,
            report,
            metadata,
        })
    }

    pub fn propose_prepared_page_reclaim_linearizable_fenced(
        &mut self,
        prepared: PreparedPageReplicaReclaim,
        expected_route_control_checksum: Option<&str>,
        expected_raft_entry_checksum: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_prepared_page_reclaim_linearizable_fenced_apply(
            prepared,
            expected_route_control_checksum,
            expected_raft_entry_checksum,
        )
        .map(|(metadata, _)| metadata)
    }

    fn propose_prepared_page_reclaim_linearizable_fenced_apply(
        &mut self,
        prepared: PreparedPageReplicaReclaim,
        expected_route_control_checksum: Option<&str>,
        expected_raft_entry_checksum: &str,
    ) -> Result<(ObjectMetadata, ReplicaPartialReclaimReport), ObjectError> {
        if expected_raft_entry_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "prepared page reclaim requires expected raft entry checksum".to_string(),
            ));
        }
        prepared.verify_expected_raft_entry_checksum(expected_raft_entry_checksum)?;
        let expected_route_control_checksum = Self::require_expected_route_control_checksum(
            "prepared block reclaim",
            expected_route_control_checksum,
        )?;
        self.ensure_storage_control_read_index(
            "propose prepared block aggregate reclaim",
            Some(expected_route_control_checksum),
        )?;
        if prepared.expected_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "prepared page reclaim requires prepared route checksum".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum != expected_route_control_checksum {
            return Err(ObjectError::PreconditionFailed(
                "prepared page reclaim route checksum does not match caller fence".to_string(),
            ));
        }
        self.propose_prepared_page_reclaim_apply(prepared)
    }

    pub fn propose_prepared_page_reclaim_linearizable_fenced_with_status(
        &mut self,
        prepared: PreparedPageReplicaReclaim,
        expected_route_control_checksum: Option<&str>,
        expected_raft_entry_checksum: &str,
    ) -> Result<PreparedPageReclaimApplyReport, ObjectError> {
        let mutation_id = prepared.raft_entry_checksum.clone();
        let mutation_request_checksum = prepared.raft_entry_checksum.clone();
        let (metadata, report) = self.propose_prepared_page_reclaim_linearizable_fenced_apply(
            prepared,
            expected_route_control_checksum,
            expected_raft_entry_checksum,
        )?;
        let mutation_commit_index = self.raft.commit_index();
        Ok(PreparedPageReclaimApplyReport {
            mutation_id,
            mutation_request_checksum,
            mutation_commit_index,
            next_log_index: mutation_commit_index.saturating_add(1),
            committed: true,
            prepared: false,
            external_raft_required: false,
            deduped: false,
            report,
            metadata,
        })
    }

    pub fn propose_page_reclaim_prepared_bounded_fenced(
        &mut self,
        max_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
        min_discard_ratio_ppm: u64,
        max_discard_ranges: usize,
        max_trim_bytes: u64,
        now_ms: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_page_reclaim_raft_entry_bounded_linearizable_fenced(
            max_chunks,
            max_rewrite_bytes,
            min_discarded_bytes,
            min_discard_ratio_ppm,
            max_discard_ranges,
            max_trim_bytes,
            now_ms,
            expected_route_control_checksum,
        )?;
        let expected_raft_entry_checksum = prepared.raft_entry_checksum.clone();
        self.propose_prepared_page_reclaim_linearizable_fenced(
            prepared,
            expected_route_control_checksum,
            &expected_raft_entry_checksum,
        )
    }

    pub fn propose_page_reclaim_prepared_bounded_fenced_with_status(
        &mut self,
        max_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
        min_discard_ratio_ppm: u64,
        max_discard_ranges: usize,
        max_trim_bytes: u64,
        now_ms: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedPageReclaimApplyReport, ObjectError> {
        let prepared = self.prepare_page_reclaim_raft_entry_bounded_linearizable_fenced(
            max_chunks,
            max_rewrite_bytes,
            min_discarded_bytes,
            min_discard_ratio_ppm,
            max_discard_ranges,
            max_trim_bytes,
            now_ms,
            expected_route_control_checksum,
        )?;
        let expected_raft_entry_checksum = prepared.raft_entry_checksum.clone();
        self.propose_prepared_page_reclaim_linearizable_fenced_with_status(
            prepared,
            expected_route_control_checksum,
            &expected_raft_entry_checksum,
        )
    }

    pub fn propose_proven_page_reclaim_fenced(
        &mut self,
        proven_prepared: ProvenStorageRead<PreparedPageReplicaReclaim>,
        expected_route_control_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_route_control_checksum = expected_route_control_checksum.as_ref();
        if expected_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven page reclaim requires expected route control checksum".to_string(),
            ));
        }
        if proven_prepared.proof.control_checksum != expected_route_control_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven page reclaim route checksum mismatch: expected {} actual {}",
                expected_route_control_checksum, proven_prepared.proof.control_checksum
            )));
        }
        if self.store.last_applied_index() < proven_prepared.proof.storage_applied_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven page reclaim storage proof is not applied: applied={} required={}",
                self.store.last_applied_index(),
                proven_prepared.proof.storage_applied_index
            )));
        }
        self.propose_prepared_page_reclaim_linearizable_fenced(
            proven_prepared.value,
            Some(expected_route_control_checksum),
            expected_raft_entry_checksum.as_ref(),
        )
    }
}
