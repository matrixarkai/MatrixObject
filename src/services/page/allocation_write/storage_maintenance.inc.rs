impl MatrixChunkService {
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
        let readiness =
            self.storage_readiness(min_capacity_free_bytes, min_reclaimable_bytes, now_ms);
        let reclaim = self.plan_chunk_replica_reclaim(
            None,
            max_reclaim_chunks,
            max_rewrite_bytes,
            min_discarded_bytes,
            min_discard_ratio_ppm,
            max_discard_ranges,
            max_trim_bytes,
        )?;
        let action_capacity = readiness
            .replica_readiness
            .len()
            .saturating_mul(2)
            .saturating_add(reclaim.candidates.len())
            .saturating_add(4);
        let mut actions = Vec::with_capacity(action_capacity);

        if !readiness.readable || !readiness.writable {
            actions.push(StorageMaintenanceAction {
                action: "restore_storage_quorum".to_string(),
                reason: readiness.reason.clone(),
                priority: 100,
                replica_index: None,
                page_id: None,
                estimated_bytes: 0,
            });
        }
        for (index, replica) in readiness.replica_readiness.iter().enumerate() {
            if self.quarantined_replicas.contains(&index) {
                actions.push(StorageMaintenanceAction {
                    action: "repair_or_restore_quarantined_replica".to_string(),
                    reason: "replica_quarantine".to_string(),
                    priority: 90,
                    replica_index: Some(index),
                    page_id: None,
                    estimated_bytes: replica.physical_bytes,
                });
            } else if self.disabled_chunk_endpoints.contains(&index) {
                actions.push(StorageMaintenanceAction {
                    action: "refresh_or_enable_chunk_endpoint".to_string(),
                    reason: "chunk_endpoint_disabled".to_string(),
                    priority: 85,
                    replica_index: Some(index),
                    page_id: None,
                    estimated_bytes: replica.physical_bytes,
                });
            } else if replica.frozen {
                actions.push(StorageMaintenanceAction {
                    action: "thaw_or_finish_replica_maintenance".to_string(),
                    reason: "replica_frozen".to_string(),
                    priority: 80,
                    replica_index: Some(index),
                    page_id: None,
                    estimated_bytes: replica.physical_bytes,
                });
            } else if replica.draining {
                actions.push(StorageMaintenanceAction {
                    action: "migrate_draining_replica_traffic".to_string(),
                    reason: "replica_draining".to_string(),
                    priority: 70,
                    replica_index: Some(index),
                    page_id: None,
                    estimated_bytes: replica.physical_bytes,
                });
            } else if replica.capacity_pressure {
                actions.push(StorageMaintenanceAction {
                    action: "relieve_capacity_pressure".to_string(),
                    reason: "capacity_pressure".to_string(),
                    priority: 60,
                    replica_index: Some(index),
                    page_id: None,
                    estimated_bytes: min_capacity_free_bytes
                        .saturating_sub(replica.capacity_free_bytes.min(min_capacity_free_bytes)),
                });
            }
            if replica.trim_flow_limited {
                actions.push(StorageMaintenanceAction {
                    action: "increase_or_wait_trim_flow_budget".to_string(),
                    reason: "trim_flow_limited".to_string(),
                    priority: 50,
                    replica_index: Some(index),
                    page_id: None,
                    estimated_bytes: replica.reclaimable_bytes,
                });
            }
        }

        if let Some(candidate) = &reclaim.first_trim_budget_skipped_candidate {
            actions.push(StorageMaintenanceAction {
                action: "increase_trim_budget_or_rewrite_candidate".to_string(),
                reason: "trim_budget_limited".to_string(),
                priority: 55,
                replica_index: Some(candidate.replica_index),
                page_id: Some(candidate.candidate.page_id.clone()),
                estimated_bytes: reclaim
                    .skipped_by_trim_budget_bytes
                    .max(candidate.candidate.trim_bytes),
            });
        }
        if let Some(candidate) = &reclaim.first_budget_skipped_candidate {
            actions.push(StorageMaintenanceAction {
                action: "increase_reclaim_rewrite_budget".to_string(),
                reason: "rewrite_budget_limited".to_string(),
                priority: 45,
                replica_index: Some(candidate.replica_index),
                page_id: Some(candidate.candidate.page_id.clone()),
                estimated_bytes: reclaim
                    .skipped_by_budget_bytes
                    .max(candidate.candidate.rewrite_bytes),
            });
        }
        if !reclaim.continuation_safe && (reclaim.truncated || reclaim.budget_limited) {
            actions.push(StorageMaintenanceAction {
                action: "replan_reclaim_with_continuation_budget".to_string(),
                reason: if reclaim.budget_limited {
                    "reclaim_continuation_budget_limited".to_string()
                } else {
                    "reclaim_continuation_marker_missing".to_string()
                },
                priority: 58,
                replica_index: reclaim
                    .first_budget_skipped_candidate
                    .as_ref()
                    .map(|candidate| candidate.replica_index),
                page_id: reclaim
                    .first_budget_skipped_candidate
                    .as_ref()
                    .map(|candidate| candidate.candidate.page_id.clone()),
                estimated_bytes: reclaim
                    .skipped_by_budget_bytes
                    .max(reclaim.remaining_discard_bytes),
            });
        }

        for candidate in &reclaim.candidates {
            actions.push(StorageMaintenanceAction {
                action: "reclaim_discarded_chunk".to_string(),
                reason: candidate.candidate.reason.clone(),
                priority: 40u64.saturating_add(candidate.candidate.reclaim_score.min(19)),
                replica_index: Some(candidate.replica_index),
                page_id: Some(candidate.candidate.page_id.clone()),
                estimated_bytes: candidate.candidate.discarded_bytes,
            });
        }
        actions.sort_by(|left, right| {
            right
                .priority
                .cmp(&left.priority)
                .then_with(|| right.estimated_bytes.cmp(&left.estimated_bytes))
                .then_with(|| left.action.cmp(&right.action))
                .then_with(|| left.reason.cmp(&right.reason))
                .then_with(|| left.replica_index.cmp(&right.replica_index))
                .then_with(|| left.page_id.cmp(&right.page_id))
        });
        let truncated = max_actions > 0 && actions.len() > max_actions;
        if truncated {
            actions.truncate(max_actions);
        }
        let capacity_pressure = readiness.capacity_pressure_replicas > 0;
        let reclaim_pressure = readiness.reclaimable_replicas > 0 || reclaim.scheduled_chunks > 0;
        let trim_flow_pressure = readiness.trim_flow_limited_replicas > 0;
        let quorum_pressure = !readiness.readable || !readiness.writable;
        let reclaim_continuation_safe = reclaim.continuation_safe;
        let reclaim_next_marker_advanced = reclaim.next_marker_advanced;
        let mut plan = StorageMaintenancePlan {
            readiness,
            reclaim,
            chunk_discards: Vec::new(),
            actions,
            plan_checksum: String::new(),
            reclaim_continuation_safe,
            reclaim_next_marker_advanced,
            capacity_pressure,
            reclaim_pressure,
            trim_flow_pressure,
            quorum_pressure,
            max_actions,
            truncated,
        };
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

    pub fn run_storage_maintenance_fenced(
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
        expected_plan_checksum: &str,
    ) -> Result<StorageMaintenanceRunReport, ObjectError> {
        self.run_storage_maintenance_control_fenced(
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
            expected_plan_checksum,
            "",
        )
    }

    pub fn run_storage_maintenance_control_fenced(
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
        expected_plan_checksum: &str,
        expected_route_control_checksum: &str,
    ) -> Result<StorageMaintenanceRunReport, ObjectError> {
        self.check_expected_route_control_checksum(
            (!expected_route_control_checksum.is_empty())
                .then_some(expected_route_control_checksum),
        )?;
        let plan = self.plan_storage_maintenance(
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
        if !expected_plan_checksum.is_empty() && plan.plan_checksum != expected_plan_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "page storage maintenance plan checksum mismatch: expected {} actual {}",
                expected_plan_checksum, plan.plan_checksum
            )));
        }
        if !expected_route_control_checksum.is_empty()
            && expected_route_control_checksum != plan.readiness.route_control_proof.state_checksum
        {
            return Err(ObjectError::PreconditionFailed(format!(
                "page storage maintenance route checksum mismatch: expected {} actual {}",
                expected_route_control_checksum, plan.readiness.route_control_proof.state_checksum
            )));
        }
        let route_control_proof = plan.readiness.route_control_proof.clone();
        let repair = if max_actions > 0 {
            self.repair_audited_chunk_replicas(None, max_actions, 0)?
        } else {
            ChunkReplicaAuditRepairReport::default()
        };
        let reclaim_actions = plan
            .actions
            .iter()
            .filter(|action| action.action == "reclaim_discarded_chunk")
            .count() as u64;
        let reclaim = if reclaim_actions > 0 {
            self.reclaim_chunk_replicas_with_limits_at(
                max_reclaim_chunks,
                max_rewrite_bytes,
                min_discarded_bytes,
                min_discard_ratio_ppm,
                max_discard_ranges,
                max_trim_bytes,
                now_ms,
            )
        } else {
            ReplicaPartialReclaimReport::default()
        };
        let executed_actions = if reclaim.reclaimed_chunks > 0 {
            reclaim_actions
        } else {
            0
        };
        let repair_actions = repair.repaired_chunks;
        let skipped_actions = plan.actions.len().saturating_sub(executed_actions as usize) as u64;
        let post_readiness =
            self.storage_readiness(min_capacity_free_bytes, min_reclaimable_bytes, now_ms);
        let route_control_fenced = !expected_route_control_checksum.is_empty();
        let route_control_fence_matched = !route_control_fenced
            || expected_route_control_checksum == route_control_proof.state_checksum;
        Ok(StorageMaintenanceRunReport {
            reclaim_continuation_safe: plan.reclaim_continuation_safe,
            reclaim_next_marker_advanced: plan.reclaim_next_marker_advanced,
            reclaim_continuation_blocked: !plan.reclaim_continuation_safe
                && (plan.reclaim.truncated || plan.reclaim.budget_limited),
            plan,
            repair,
            reclaim,
            chunk_discard: ChunkDiscardRunReport::default(),
            post_readiness,
            route_control_proof,
            expected_route_control_checksum: expected_route_control_checksum.to_string(),
            route_control_fenced,
            route_control_fence_matched,
            executed_actions: executed_actions.saturating_add(repair_actions),
            skipped_actions,
            reclaimed: executed_actions > 0,
        })
    }
}
