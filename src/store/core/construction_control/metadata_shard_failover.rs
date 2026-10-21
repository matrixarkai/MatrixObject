// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn plan_metadata_shard_failover(
        &self,
        failed_owner_node_id: &str,
        replacement_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
    ) -> Result<MetadataShardMigrationReport, ObjectError> {
        self.plan_metadata_shard_failover_fenced(
            failed_owner_node_id,
            replacement_owner_node_id,
            max_shards,
            now_ms,
            None,
            None,
            None,
        )
    }

    pub fn plan_metadata_shard_failover_fenced(
        &self,
        failed_owner_node_id: &str,
        replacement_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
        expected_shard_topology_epoch: Option<u64>,
        expected_shard_topology_checksum: Option<&str>,
        expected_coordinator_session_checksum: Option<&str>,
    ) -> Result<MetadataShardMigrationReport, ObjectError> {
        self.meta_service.plan_shard_failover_fenced(
            failed_owner_node_id,
            replacement_owner_node_id,
            max_shards,
            now_ms,
            expected_shard_topology_epoch,
            expected_shard_topology_checksum,
            expected_coordinator_session_checksum,
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
                "storage maintenance plan checksum mismatch: expected {} actual {}",
                expected_plan_checksum, plan.plan_checksum
            )));
        }
        if !expected_route_control_checksum.is_empty()
            && expected_route_control_checksum != plan.readiness.route_control_proof.state_checksum
        {
            return Err(ObjectError::PreconditionFailed(format!(
                "storage maintenance page route control checksum mismatch: expected {} actual {}",
                expected_route_control_checksum, plan.readiness.route_control_proof.state_checksum
            )));
        }
        let mut report = self.chunk_service.run_storage_maintenance_control_fenced(
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
            expected_route_control_checksum,
        )?;
        let selected_chunk_discards = plan
            .actions
            .iter()
            .filter(|action| action.action == "discard_object_segment")
            .count();
        let chunk_discard = self.apply_planned_chunk_discards(
            &plan,
            selected_chunk_discards,
            expected_route_control_checksum,
        );
        report.chunk_discard = chunk_discard;
        report.executed_actions = report
            .executed_actions
            .saturating_add(report.chunk_discard.discarded_chunks);
        report.skipped_actions =
            plan.actions
                .len()
                .saturating_sub(report.executed_actions as usize) as u64;
        report.reclaimed = report.reclaimed || report.chunk_discard.discarded_chunks > 0;
        report.plan = plan;
        report.post_readiness = self.chunk_service.storage_readiness(
            min_capacity_free_bytes,
            min_reclaimable_bytes,
            now_ms,
        );
        Ok(report)
    }

    fn apply_planned_chunk_discards(
        &mut self,
        plan: &StorageMaintenancePlan,
        max_slabs: usize,
        expected_route_control_checksum: &str,
    ) -> ChunkDiscardRunReport {
        let mut report = ChunkDiscardRunReport {
            planned_chunks: plan.chunk_discards.len() as u64,
            planned_discard_bytes: plan
                .chunk_discards
                .iter()
                .map(|discard| discard.discard_bytes)
                .fold(0u64, saturating_add),
            route_control_proof: plan.readiness.route_control_proof.clone(),
            expected_route_control_checksum: expected_route_control_checksum.to_string(),
            ..ChunkDiscardRunReport::default()
        };
        for discard in &plan.chunk_discards {
            if discard.compact_instead {
                report.skipped_compaction_chunks =
                    report.skipped_compaction_chunks.saturating_add(1);
                report
                    .compaction_chunk_ids
                    .push(discard.chunk_id.clone());
                continue;
            }
            if max_slabs == 0 || report.attempted_chunks >= max_slabs as u64 {
                report.skipped_limit_chunks = report.skipped_limit_chunks.saturating_add(1);
                continue;
            }
            report.attempted_chunks = report.attempted_chunks.saturating_add(1);
            match self.apply_discard_borrowed(&discard.chunk_id, &discard.ranges) {
                Ok(()) => {
                    report.discarded_chunks = report.discarded_chunks.saturating_add(1);
                    report.discard_bytes =
                        report.discard_bytes.saturating_add(discard.discard_bytes);
                    report
                        .discarded_chunk_ids
                        .push(discard.chunk_id.clone());
                }
                Err(err) => {
                    report.failed_chunks = report.failed_chunks.saturating_add(1);
                    if report.first_error.is_empty() {
                        report.first_error = err.to_string();
                    }
                }
            }
        }
        report
    }

    pub fn execute_metadata_shard_failover(
        &mut self,
        failed_owner_node_id: &str,
        replacement_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
    ) -> Result<MetadataShardMigrationReport, ObjectError> {
        self.execute_metadata_shard_failover_fenced(
            failed_owner_node_id,
            replacement_owner_node_id,
            max_shards,
            now_ms,
            "",
        )
    }

    pub fn execute_metadata_shard_failover_fenced(
        &mut self,
        failed_owner_node_id: &str,
        replacement_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
        expected_plan_checksum: &str,
    ) -> Result<MetadataShardMigrationReport, ObjectError> {
        self.execute_metadata_shard_failover_control_fenced(
            failed_owner_node_id,
            replacement_owner_node_id,
            max_shards,
            now_ms,
            expected_plan_checksum,
            None,
            None,
            None,
        )
    }

    pub fn execute_metadata_shard_failover_control_fenced(
        &mut self,
        failed_owner_node_id: &str,
        replacement_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
        expected_plan_checksum: &str,
        expected_shard_topology_epoch: Option<u64>,
        expected_shard_topology_checksum: Option<&str>,
        expected_coordinator_session_checksum: Option<&str>,
    ) -> Result<MetadataShardMigrationReport, ObjectError> {
        let report = self.meta_service.execute_shard_failover_control_fenced(
            failed_owner_node_id,
            replacement_owner_node_id,
            max_shards,
            now_ms,
            expected_plan_checksum,
            expected_shard_topology_epoch,
            expected_shard_topology_checksum,
            expected_coordinator_session_checksum,
        )?;
        if report.migrated_shards > 0 {
            self.invalidate_metadata_route_cache();
        }
        Ok(report)
    }

    pub fn plan_expired_metadata_shard_owner_failover(
        &self,
        expired_owner_node_id: &str,
        replacement_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
    ) -> Result<MetadataShardMigrationReport, ObjectError> {
        self.plan_expired_metadata_shard_owner_failover_fenced(
            expired_owner_node_id,
            replacement_owner_node_id,
            max_shards,
            now_ms,
            None,
            None,
            None,
        )
    }

    pub fn plan_expired_metadata_shard_owner_failover_fenced(
        &self,
        expired_owner_node_id: &str,
        replacement_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
        expected_shard_topology_epoch: Option<u64>,
        expected_shard_topology_checksum: Option<&str>,
        expected_coordinator_session_checksum: Option<&str>,
    ) -> Result<MetadataShardMigrationReport, ObjectError> {
        self.meta_service.plan_expired_shard_owner_failover_fenced(
            expired_owner_node_id,
            replacement_owner_node_id,
            max_shards,
            now_ms,
            expected_shard_topology_epoch,
            expected_shard_topology_checksum,
            expected_coordinator_session_checksum,
        )
    }

    pub fn execute_expired_metadata_shard_owner_failover(
        &mut self,
        expired_owner_node_id: &str,
        replacement_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
    ) -> Result<MetadataShardMigrationReport, ObjectError> {
        self.execute_expired_metadata_shard_owner_failover_fenced(
            expired_owner_node_id,
            replacement_owner_node_id,
            max_shards,
            now_ms,
            "",
        )
    }

    pub fn execute_expired_metadata_shard_owner_failover_fenced(
        &mut self,
        expired_owner_node_id: &str,
        replacement_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
        expected_plan_checksum: &str,
    ) -> Result<MetadataShardMigrationReport, ObjectError> {
        self.execute_expired_metadata_shard_owner_failover_control_fenced(
            expired_owner_node_id,
            replacement_owner_node_id,
            max_shards,
            now_ms,
            expected_plan_checksum,
            None,
            None,
            None,
        )
    }

    pub fn execute_expired_metadata_shard_owner_failover_control_fenced(
        &mut self,
        expired_owner_node_id: &str,
        replacement_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
        expected_plan_checksum: &str,
        expected_shard_topology_epoch: Option<u64>,
        expected_shard_topology_checksum: Option<&str>,
        expected_coordinator_session_checksum: Option<&str>,
    ) -> Result<MetadataShardMigrationReport, ObjectError> {
        let report = self
            .meta_service
            .execute_expired_shard_owner_failover_control_fenced(
                expired_owner_node_id,
                replacement_owner_node_id,
                max_shards,
                now_ms,
                expected_plan_checksum,
                expected_shard_topology_epoch,
                expected_shard_topology_checksum,
                expected_coordinator_session_checksum,
            )?;
        if report.migrated_shards > 0 {
            self.invalidate_metadata_route_cache();
        }
        Ok(report)
    }

}
