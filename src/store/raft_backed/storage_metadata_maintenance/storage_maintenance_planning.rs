impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_run_storage_maintenance(
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
        self.propose(ObjectCommand::RunStorageMaintenance {
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
            expected_plan_checksum: String::new(),
            expected_page_route_control_checksum: String::new(),
        })
    }

    pub fn propose_run_storage_maintenance_fenced(
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
        expected_plan_checksum: impl Into<String>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::RunStorageMaintenance {
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
            expected_plan_checksum: expected_plan_checksum.into(),
            expected_page_route_control_checksum: String::new(),
        })
    }

    pub fn propose_run_storage_maintenance_control_fenced(
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
        expected_plan_checksum: impl Into<String>,
        expected_page_route_control_checksum: impl Into<String>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.into();
        self.ensure_page_route_control_ready(
            "storage maintenance",
            &expected_page_route_control_checksum,
        )?;
        self.propose(ObjectCommand::RunStorageMaintenance {
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
            expected_plan_checksum: expected_plan_checksum.into(),
            expected_page_route_control_checksum,
        })
    }

    pub fn plan_storage_maintenance_linearizable(
        &self,
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
    ) -> Result<StorageMaintenancePlan, ObjectError> {
        self.plan_storage_maintenance_linearizable_with_control_fence(
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

    pub fn plan_storage_maintenance_linearizable_with_control_fence(
        &self,
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
    ) -> Result<StorageMaintenancePlan, ObjectError> {
        let commit_index = self.raft.commit_index();
        if self.store.last_applied_index() < commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "storage maintenance read index is not satisfied: applied={} required={}",
                self.store.last_applied_index(),
                commit_index
            )));
        }
        let plan = self.store.plan_storage_maintenance(
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
        )?;
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        if !expected_page_route_control_checksum.is_empty()
            && expected_page_route_control_checksum
                != plan.readiness.route_control_proof.state_checksum
        {
            return Err(ObjectError::PreconditionFailed(format!(
                "storage maintenance block route control checksum mismatch: expected {} actual {}",
                expected_page_route_control_checksum,
                plan.readiness.route_control_proof.state_checksum
            )));
        }
        Ok(plan)
    }

    pub fn plan_storage_maintenance_with_read_proof_linearizable_with_control_fence(
        &self,
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
    ) -> Result<ProvenStorageRead<StorageMaintenancePlan>, ObjectError> {
        let requested_read_index = self.raft.commit_index();
        let value = self.plan_storage_maintenance_linearizable_with_control_fence(
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
        let proof = self.page_storage_read_proof(requested_read_index);
        Ok(ProvenStorageRead { value, proof })
    }

    pub fn prepare_storage_maintenance_envelope_linearizable(
        &self,
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
    ) -> Result<PreparedStorageMaintenanceEnvelope, ObjectError> {
        self.prepare_storage_maintenance_envelope_linearizable_with_control_fence(
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

    pub fn prepare_storage_maintenance_envelope_linearizable_with_control_fence(
        &self,
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
    ) -> Result<PreparedStorageMaintenanceEnvelope, ObjectError> {
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
        Ok(make_prepared_storage_maintenance_envelope(
            &plan,
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
        ))
    }

    pub fn prepare_storage_maintenance_envelope_with_read_proof_linearizable_with_control_fence(
        &self,
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
    ) -> Result<ProvenPreparedStorageMaintenanceEnvelope, ObjectError> {
        let requested_read_index = self.raft.commit_index();
        let envelope = self.prepare_storage_maintenance_envelope_linearizable_with_control_fence(
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
        let proof = self.page_storage_read_proof(requested_read_index);
        Ok(ProvenPreparedStorageMaintenanceEnvelope { envelope, proof })
    }

}
