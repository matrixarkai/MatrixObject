// SPDX-License-Identifier: Apache-2.0
impl MatrixMetaService {
    pub fn plan_shard_failover(
        &self,
        failed_owner_node_id: &str,
        replacement_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
    ) -> Result<MetadataShardMigrationReport, ObjectError> {
        self.plan_shard_failover_fenced(
            failed_owner_node_id,
            replacement_owner_node_id,
            max_shards,
            now_ms,
            None,
            None,
            None,
        )
    }

    pub fn plan_shard_failover_fenced(
        &self,
        failed_owner_node_id: &str,
        replacement_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
        expected_shard_topology_epoch: Option<u64>,
        expected_shard_topology_checksum: Option<&str>,
        expected_coordinator_session_checksum: Option<&str>,
    ) -> Result<MetadataShardMigrationReport, ObjectError> {
        self.plan_shard_owner_migration(
            failed_owner_node_id,
            replacement_owner_node_id,
            max_shards,
            false,
            "owner_failover",
            now_ms,
            expected_shard_topology_epoch,
            expected_shard_topology_checksum,
            expected_coordinator_session_checksum,
        )
    }

    pub fn execute_shard_failover(
        &mut self,
        failed_owner_node_id: &str,
        replacement_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
    ) -> Result<MetadataShardMigrationReport, ObjectError> {
        self.execute_shard_failover_fenced(
            failed_owner_node_id,
            replacement_owner_node_id,
            max_shards,
            now_ms,
            "",
        )
    }

    pub fn execute_shard_failover_fenced(
        &mut self,
        failed_owner_node_id: &str,
        replacement_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
        expected_plan_checksum: &str,
    ) -> Result<MetadataShardMigrationReport, ObjectError> {
        self.execute_shard_failover_control_fenced(
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

    pub fn execute_shard_failover_control_fenced(
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
        let mut report = self.plan_shard_failover_fenced(
            failed_owner_node_id,
            replacement_owner_node_id,
            max_shards,
            now_ms,
            expected_shard_topology_epoch,
            expected_shard_topology_checksum,
            expected_coordinator_session_checksum,
        )?;
        if !expected_plan_checksum.is_empty() && report.plan_checksum != expected_plan_checksum {
            return Err(ObjectError::PreconditionFailed(
                "metadata shard failover plan checksum fence mismatch".to_string(),
            ));
        }
        report.expected_plan_checksum = expected_plan_checksum.to_string();
        self.execute_planned_shard_migrations(&mut report, now_ms);
        Ok(report)
    }

}
