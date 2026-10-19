impl MatrixObjectStore {
    pub fn plan_storage_maintenance(
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
        let mut plan = self.chunk_service.plan_storage_maintenance(
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
        let chunk_discards = self.plan_discard(min_discarded_bytes, max_discard_ranges);
        for discard in &chunk_discards {
            plan.actions.push(StorageMaintenanceAction {
                action: if discard.compact_instead {
                    "compact_object_segment".to_string()
                } else {
                    "discard_object_segment".to_string()
                },
                reason: if discard.compact_instead {
                    "fragmented_object_stale_ranges".to_string()
                } else {
                    "object_stale_ranges".to_string()
                },
                priority: if discard.compact_instead { 44 } else { 42 },
                replica_index: None,
                page_id: None,
                estimated_bytes: discard.discard_bytes,
            });
        }
        plan.actions.sort_by(|left, right| {
            right
                .priority
                .cmp(&left.priority)
                .then_with(|| right.estimated_bytes.cmp(&left.estimated_bytes))
                .then_with(|| left.action.cmp(&right.action))
                .then_with(|| left.reason.cmp(&right.reason))
                .then_with(|| left.replica_index.cmp(&right.replica_index))
                .then_with(|| left.page_id.cmp(&right.page_id))
        });
        let truncated = max_actions > 0 && plan.actions.len() > max_actions;
        if truncated {
            plan.actions.truncate(max_actions);
        }
        plan.chunk_discards = chunk_discards;
        plan.reclaim_pressure = plan.reclaim_pressure || !plan.chunk_discards.is_empty();
        plan.truncated = plan.truncated || truncated;
        plan.plan_checksum = storage_maintenance_plan_checksum(&plan);
        Ok(plan)
    }

    pub fn run_storage_maintenance(
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
    ) -> Result<StorageMaintenanceRunReport, ObjectError> {
        self.run_storage_maintenance_fenced(
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
}
