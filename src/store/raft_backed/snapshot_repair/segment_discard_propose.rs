impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_prepared_slab_discard(
        &mut self,
        prepared: PreparedSlabDiscard,
    ) -> Result<ObjectMetadata, ObjectError> {
        if self.raft.commit_index() != self.store.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "prepared segment discard requires raft/store commit alignment".to_string(),
            ));
        }
        self.store
            .clone()
            .apply_command(ObjectCommand::ApplyPreparedSlabDiscard {
                prepared: prepared.clone(),
            })?;
        let entry = self
            .raft
            .append(ObjectCommand::ApplyPreparedSlabDiscard { prepared })?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::PreconditionFailed(
                "prepared segment discard raft index is not contiguous".to_string(),
            ));
        }
        self.store.apply_raft_entry(&entry)
    }

    pub fn propose_prepared_slab_discard_linearizable_fenced(
        &mut self,
        prepared: PreparedSlabDiscard,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready(
            "prepared segment discard",
            expected_page_route_control_checksum,
        )?;
        if expected_page_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "prepared segment discard requires expected route control checksum".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "prepared segment discard requires prepared route control checksum".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum != expected_page_route_control_checksum {
            return Err(ObjectError::PreconditionFailed(
                "prepared segment discard route checksum does not match caller fence".to_string(),
            ));
        }
        self.propose_prepared_slab_discard(prepared)
    }

    pub fn propose_proven_slab_discard_fenced(
        &mut self,
        proven_prepared: ProvenStorageRead<PreparedSlabDiscard>,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        if expected_page_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven segment discard requires expected route control checksum".to_string(),
            ));
        }
        if proven_prepared.proof.control_checksum != expected_page_route_control_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven segment discard route checksum mismatch: expected {} actual {}",
                expected_page_route_control_checksum, proven_prepared.proof.control_checksum
            )));
        }
        if self.store.last_applied_index() < proven_prepared.proof.storage_applied_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven segment discard storage proof is not applied: applied={} required={}",
                self.store.last_applied_index(),
                proven_prepared.proof.storage_applied_index
            )));
        }
        let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
        if expected_raft_entry_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven segment discard requires expected raft entry checksum".to_string(),
            ));
        }
        proven_prepared
            .value
            .verify_expected_raft_entry_checksum(expected_raft_entry_checksum)?;
        if proven_prepared.value.expected_route_control_checksum
            != expected_page_route_control_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven segment discard prepared route checksum does not match caller fence"
                    .to_string(),
            ));
        }
        self.propose_prepared_slab_discard_linearizable_fenced(
            proven_prepared.value,
            expected_page_route_control_checksum,
        )
    }

    pub fn propose_proven_slab_discard_envelope_fenced(
        &mut self,
        proven_envelope: ProvenStorageRead<PreparedSlabDiscardEnvelope>,
        ranges: Vec<DataRange>,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_envelope_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        if expected_page_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven segment discard envelope requires expected route control checksum"
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
                "proven segment discard envelope route checksum mismatch".to_string(),
            ));
        }
        if self.store.last_applied_index() < proven_envelope.proof.storage_applied_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven segment discard envelope storage proof is not applied: applied={} required={}",
                self.store.last_applied_index(),
                proven_envelope.proof.storage_applied_index
            )));
        }
        let expected_envelope_checksum = expected_envelope_checksum.as_ref();
        if expected_envelope_checksum.is_empty()
            || proven_envelope.value.envelope_checksum != expected_envelope_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven segment discard envelope checksum mismatch".to_string(),
            ));
        }
        let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
        if expected_raft_entry_checksum.is_empty()
            || proven_envelope.value.raft_entry_checksum != expected_raft_entry_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven segment discard envelope raft entry checksum mismatch".to_string(),
            ));
        }
        let prepared = self.prepare_slab_discard_raft_entry_linearizable_fenced(
            &proven_envelope.value.slab_id,
            ranges,
            expected_page_route_control_checksum,
        )?;
        let actual_envelope = make_prepared_slab_discard_envelope(&prepared);
        if actual_envelope != proven_envelope.value {
            return Err(ObjectError::PreconditionFailed(
                "proven segment discard envelope changed before proposal".to_string(),
            ));
        }
        self.propose_prepared_slab_discard_linearizable_fenced(
            prepared,
            expected_page_route_control_checksum,
        )
    }

    pub fn propose_slab_discard_prepared_linearizable_fenced(
        &mut self,
        slab_id: &SlabId,
        ranges: Vec<DataRange>,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        let prepared = self.prepare_slab_discard_raft_entry_linearizable_fenced(
            slab_id,
            ranges,
            expected_page_route_control_checksum,
        )?;
        self.propose_prepared_slab_discard_linearizable_fenced(
            prepared,
            expected_page_route_control_checksum,
        )
    }

    pub fn propose_slab_discards_prepared_linearizable_fenced(
        &mut self,
        min_stale_bytes: u64,
        max_ranges_per_slab: usize,
        max_slabs: usize,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<SlabDiscardRunReport, ObjectError> {
        let mut route_checksum = expected_page_route_control_checksum.as_ref().to_string();
        if route_checksum.is_empty() {
            route_checksum = self
                .store
                .page_service
                .route_control_proof()
                .state_checksum;
        }
        self.ensure_page_route_control_ready("segment discard run", &route_checksum)?;
        if route_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "segment discard run requires block route control checksum".to_string(),
            ));
        }
        let plans = self
            .store
            .plan_discard(min_stale_bytes, max_ranges_per_slab);
        let mut report = SlabDiscardRunReport {
            planned_slabs: plans.len() as u64,
            planned_discard_bytes: plans
                .iter()
                .map(|plan| plan.discard_bytes)
                .fold(0u64, saturating_add),
            route_control_proof: self.store.page_service.route_control_proof(),
            expected_route_control_checksum: route_checksum.clone(),
            ..SlabDiscardRunReport::default()
        };
        for plan in plans {
            if plan.compact_instead {
                report.skipped_compaction_slabs =
                    report.skipped_compaction_slabs.saturating_add(1);
                report.compaction_slab_ids.push(plan.slab_id);
                continue;
            }
            if max_slabs > 0 && report.attempted_slabs >= max_slabs as u64 {
                report.skipped_limit_slabs = report.skipped_limit_slabs.saturating_add(1);
                continue;
            }
            report.attempted_slabs = report.attempted_slabs.saturating_add(1);
            let prepared = match self.prepare_slab_discard_raft_entry_linearizable_fenced(
                &plan.slab_id,
                plan.ranges,
                &route_checksum,
            ) {
                Ok(prepared) => prepared,
                Err(err) => {
                    report.failed_slabs = report.failed_slabs.saturating_add(1);
                    if report.first_error.is_empty() {
                        report.first_error = err.to_string();
                    }
                    continue;
                }
            };
            let slab_id = prepared.slab_id.clone();
            let discard_bytes = prepared.discard_bytes;
            match self
                .propose_prepared_slab_discard_linearizable_fenced(prepared, &route_checksum)
            {
                Ok(_) => {
                    report.discarded_slabs = report.discarded_slabs.saturating_add(1);
                    report.discard_bytes = report.discard_bytes.saturating_add(discard_bytes);
                    report.discarded_slab_ids.push(slab_id);
                }
                Err(err) => {
                    report.failed_slabs = report.failed_slabs.saturating_add(1);
                    if report.first_error.is_empty() {
                        report.first_error = err.to_string();
                    }
                }
            }
        }
        Ok(report)
    }

    pub fn propose_slab_discards_prepared_linearizable_gc_proof_fenced(
        &mut self,
        min_stale_bytes: u64,
        max_ranges_per_slab: usize,
        max_slabs: usize,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_object_gc_report_proof_checksum: impl AsRef<str>,
    ) -> Result<SlabDiscardRunReport, ObjectError> {
        let mut route_checksum = expected_page_route_control_checksum.as_ref().to_string();
        if route_checksum.is_empty() {
            route_checksum = self
                .store
                .page_service
                .route_control_proof()
                .state_checksum;
        }
        self.store.gc_report_with_discard_policy_proof_fenced(
            min_stale_bytes,
            0,
            Some(&route_checksum),
            expected_object_gc_report_proof_checksum,
        )?;
        self.propose_slab_discards_prepared_linearizable_fenced(
            min_stale_bytes,
            max_ranges_per_slab,
            max_slabs,
            route_checksum,
        )
    }

}
