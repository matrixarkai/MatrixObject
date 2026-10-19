// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_proven_storage_maintenance_envelope_fenced(
        &mut self,
        proven_envelope: ProvenPreparedStorageMaintenanceEnvelope,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_envelope_checksum: impl AsRef<str>,
        expected_plan_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        Ok(self
            .propose_proven_storage_maintenance_envelope_fenced_with_report(
                proven_envelope,
                expected_page_route_control_checksum,
                expected_envelope_checksum,
                expected_plan_checksum,
            )?
            .metadata)
    }

    pub fn propose_proven_storage_maintenance_envelope_fenced_with_report(
        &mut self,
        proven_envelope: ProvenPreparedStorageMaintenanceEnvelope,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_envelope_checksum: impl AsRef<str>,
        expected_plan_checksum: impl AsRef<str>,
    ) -> Result<StorageMaintenanceApplyReport, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        if expected_page_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven storage maintenance requires expected page route control checksum"
                    .to_string(),
            ));
        }
        if proven_envelope.proof.control_checksum != expected_page_route_control_checksum
            || proven_envelope.envelope.route_control_proof_checksum
                != expected_page_route_control_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven storage maintenance route control checksum mismatch".to_string(),
            ));
        }
        if self.store.last_applied_index() < proven_envelope.proof.storage_applied_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven storage maintenance proof is not applied: applied={} required={}",
                self.store.last_applied_index(),
                proven_envelope.proof.storage_applied_index
            )));
        }
        let expected_envelope_checksum = expected_envelope_checksum.as_ref();
        if expected_envelope_checksum.is_empty()
            || proven_envelope.envelope.envelope_checksum != expected_envelope_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven storage maintenance envelope checksum mismatch".to_string(),
            ));
        }
        let expected_plan_checksum = expected_plan_checksum.as_ref();
        if expected_plan_checksum.is_empty()
            || proven_envelope.envelope.plan_checksum != expected_plan_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven storage maintenance plan checksum mismatch".to_string(),
            ));
        }
        let actual_envelope = self
            .prepare_storage_maintenance_envelope_linearizable_with_control_fence(
                proven_envelope.envelope.max_actions as usize,
                proven_envelope.envelope.min_capacity_free_bytes,
                proven_envelope.envelope.min_reclaimable_bytes,
                proven_envelope.envelope.max_reclaim_chunks as usize,
                proven_envelope.envelope.max_rewrite_bytes,
                proven_envelope.envelope.min_discarded_bytes,
                proven_envelope.envelope.min_discard_ratio_ppm,
                proven_envelope.envelope.max_discard_ranges as usize,
                proven_envelope.envelope.max_trim_bytes,
                proven_envelope.envelope.now_ms,
                expected_page_route_control_checksum,
            )?;
        if actual_envelope.envelope_checksum != proven_envelope.envelope.envelope_checksum {
            return Err(ObjectError::PreconditionFailed(
                "proven storage maintenance envelope changed before proposal".to_string(),
            ));
        }
        let before_commit_index = self.raft.commit_index();
        let mutation_id = format!(
            "storage-maintenance:{}:{}",
            proven_envelope.envelope.plan_checksum, proven_envelope.envelope.envelope_checksum
        );
        let control_fields = proven_envelope.to_control_fields("");
        let request_material_capacity = encoded_string_len(
            "MOSRS-STORAGE-MAINTENANCE-APPLY-REQUEST1",
        ) + control_fields
            .iter()
            .map(|(name, value)| encoded_string_len(name) + encoded_string_len(value))
            .sum::<usize>();
        let mut request_material = Vec::with_capacity(request_material_capacity);
        append_string(
            &mut request_material,
            "MOSRS-STORAGE-MAINTENANCE-APPLY-REQUEST1",
        );
        for (name, value) in control_fields {
            append_string(&mut request_material, &name);
            append_string(&mut request_material, &value);
        }
        let mutation_request_checksum = etag(&request_material);
        let metadata = self.propose_run_storage_maintenance_linearizable_with_control_fence(
            proven_envelope.envelope.max_actions as usize,
            proven_envelope.envelope.min_capacity_free_bytes,
            proven_envelope.envelope.min_reclaimable_bytes,
            proven_envelope.envelope.max_reclaim_chunks as usize,
            proven_envelope.envelope.max_rewrite_bytes,
            proven_envelope.envelope.min_discarded_bytes,
            proven_envelope.envelope.min_discard_ratio_ppm,
            proven_envelope.envelope.max_discard_ranges as usize,
            proven_envelope.envelope.max_trim_bytes,
            proven_envelope.envelope.now_ms,
            expected_page_route_control_checksum,
            expected_plan_checksum,
        )?;
        let mutation_commit_index = self.raft.commit_index();
        Ok(StorageMaintenanceApplyReport {
            mutation_id,
            mutation_request_checksum,
            mutation_commit_index,
            next_log_index: mutation_commit_index.saturating_add(1),
            committed: mutation_commit_index > before_commit_index,
            prepared: false,
            external_raft_required: false,
            deduped: mutation_commit_index == before_commit_index,
            metadata,
            proven_envelope,
        })
    }

    pub fn propose_run_storage_maintenance_linearizable(
        &mut self,
        max_actions: usize,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        max_reclaim_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
        min_discard_ratio_ppm: u64,
        max_discard_ranges: usize,
        max_trim_bytes: u64,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_run_storage_maintenance_linearizable_fenced(
            max_actions,
            min_capacity_free_bytes,
            min_reclaimable_bytes,
            max_reclaim_chunks,
            max_rewrite_bytes,
            min_discarded_bytes,
            min_discard_ratio_ppm,
            max_discard_ranges,
            max_trim_bytes,
            now_ms,
            "",
        )
    }

    pub fn propose_run_storage_maintenance_linearizable_with_report(
        &mut self,
        max_actions: usize,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        max_reclaim_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
        min_discard_ratio_ppm: u64,
        max_discard_ranges: usize,
        max_trim_bytes: u64,
        now_ms: u64,
    ) -> Result<StorageMaintenanceApplyReport, ObjectError> {
        self.propose_run_storage_maintenance_linearizable_fenced_with_report(
            max_actions,
            min_capacity_free_bytes,
            min_reclaimable_bytes,
            max_reclaim_chunks,
            max_rewrite_bytes,
            min_discarded_bytes,
            min_discard_ratio_ppm,
            max_discard_ranges,
            max_trim_bytes,
            now_ms,
            "",
        )
    }

    pub fn propose_run_storage_maintenance_linearizable_with_control_fence(
        &mut self,
        max_actions: usize,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        max_reclaim_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
        min_discard_ratio_ppm: u64,
        max_discard_ranges: usize,
        max_trim_bytes: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_plan_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let plan = self.plan_storage_maintenance_linearizable_with_control_fence(
            max_actions,
            min_capacity_free_bytes,
            min_reclaimable_bytes,
            max_reclaim_chunks,
            max_rewrite_bytes,
            min_discarded_bytes,
            min_discard_ratio_ppm,
            max_discard_ranges,
            max_trim_bytes,
            now_ms,
            expected_page_route_control_checksum,
        )?;
        let expected_plan_checksum = expected_plan_checksum.as_ref();
        if !expected_plan_checksum.is_empty() && expected_plan_checksum != plan.plan_checksum {
            return Err(ObjectError::InvalidArgument(format!(
                "storage maintenance plan checksum mismatch: expected {} actual {}",
                expected_plan_checksum, plan.plan_checksum
            )));
        }
        self.propose_run_storage_maintenance_control_fenced(
            max_actions,
            min_capacity_free_bytes,
            min_reclaimable_bytes,
            max_reclaim_chunks,
            max_rewrite_bytes,
            min_discarded_bytes,
            min_discard_ratio_ppm,
            max_discard_ranges,
            max_trim_bytes,
            now_ms,
            plan.plan_checksum,
            plan.readiness.route_control_proof.state_checksum,
        )
    }

    pub fn propose_run_storage_maintenance_linearizable_with_control_fence_and_report(
        &mut self,
        max_actions: usize,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        max_reclaim_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
        min_discard_ratio_ppm: u64,
        max_discard_ranges: usize,
        max_trim_bytes: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_plan_checksum: impl AsRef<str>,
    ) -> Result<StorageMaintenanceApplyReport, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        let proven = self
            .prepare_storage_maintenance_envelope_with_read_proof_linearizable_with_control_fence(
                max_actions,
                min_capacity_free_bytes,
                min_reclaimable_bytes,
                max_reclaim_chunks,
                max_rewrite_bytes,
                min_discarded_bytes,
                min_discard_ratio_ppm,
                max_discard_ranges,
                max_trim_bytes,
                now_ms,
                expected_page_route_control_checksum,
            )?;
        let expected_route = if expected_page_route_control_checksum.is_empty() {
            proven.envelope.route_control_proof_checksum.clone()
        } else {
            expected_page_route_control_checksum.to_string()
        };
        let expected_envelope = proven.envelope.envelope_checksum.clone();
        let expected_plan = if expected_plan_checksum.as_ref().is_empty() {
            proven.envelope.plan_checksum.clone()
        } else {
            expected_plan_checksum.as_ref().to_string()
        };
        self.propose_proven_storage_maintenance_envelope_fenced_with_report(
            proven,
            expected_route,
            expected_envelope,
            expected_plan,
        )
    }

    pub fn propose_run_storage_maintenance_linearizable_fenced(
        &mut self,
        max_actions: usize,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        max_reclaim_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
        min_discard_ratio_ppm: u64,
        max_discard_ranges: usize,
        max_trim_bytes: u64,
        now_ms: u64,
        expected_plan_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_run_storage_maintenance_linearizable_with_control_fence(
            max_actions,
            min_capacity_free_bytes,
            min_reclaimable_bytes,
            max_reclaim_chunks,
            max_rewrite_bytes,
            min_discarded_bytes,
            min_discard_ratio_ppm,
            max_discard_ranges,
            max_trim_bytes,
            now_ms,
            "",
            expected_plan_checksum,
        )
    }

    pub fn propose_run_storage_maintenance_linearizable_fenced_with_report(
        &mut self,
        max_actions: usize,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        max_reclaim_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
        min_discard_ratio_ppm: u64,
        max_discard_ranges: usize,
        max_trim_bytes: u64,
        now_ms: u64,
        expected_plan_checksum: impl AsRef<str>,
    ) -> Result<StorageMaintenanceApplyReport, ObjectError> {
        self.propose_run_storage_maintenance_linearizable_with_control_fence_and_report(
            max_actions,
            min_capacity_free_bytes,
            min_reclaimable_bytes,
            max_reclaim_chunks,
            max_rewrite_bytes,
            min_discarded_bytes,
            min_discard_ratio_ppm,
            max_discard_ranges,
            max_trim_bytes,
            now_ms,
            "",
            expected_plan_checksum,
        )
    }

}
