impl MatrixObjectStore {
    pub fn plan_metadata_shard_rebalance(
        &self,
        source_owner_node_id: &str,
        target_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
    ) -> Result<MetadataShardMigrationReport, ObjectError> {
        self.plan_metadata_shard_rebalance_fenced(
            source_owner_node_id,
            target_owner_node_id,
            max_shards,
            now_ms,
            None,
            None,
            None,
        )
    }

    pub fn plan_metadata_shard_rebalance_fenced(
        &self,
        source_owner_node_id: &str,
        target_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
        expected_shard_topology_epoch: Option<u64>,
        expected_shard_topology_checksum: Option<&str>,
        expected_coordinator_session_checksum: Option<&str>,
    ) -> Result<MetadataShardMigrationReport, ObjectError> {
        self.meta_service.plan_shard_rebalance_fenced(
            source_owner_node_id,
            target_owner_node_id,
            max_shards,
            now_ms,
            expected_shard_topology_epoch,
            expected_shard_topology_checksum,
            expected_coordinator_session_checksum,
        )
    }

    pub fn execute_metadata_shard_rebalance(
        &mut self,
        source_owner_node_id: &str,
        target_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
    ) -> Result<MetadataShardMigrationReport, ObjectError> {
        self.execute_metadata_shard_rebalance_fenced(
            source_owner_node_id,
            target_owner_node_id,
            max_shards,
            now_ms,
            "",
        )
    }

    pub fn execute_metadata_shard_rebalance_fenced(
        &mut self,
        source_owner_node_id: &str,
        target_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
        expected_plan_checksum: &str,
    ) -> Result<MetadataShardMigrationReport, ObjectError> {
        self.execute_metadata_shard_rebalance_control_fenced(
            source_owner_node_id,
            target_owner_node_id,
            max_shards,
            now_ms,
            expected_plan_checksum,
            None,
            None,
            None,
        )
    }

    pub fn execute_metadata_shard_rebalance_control_fenced(
        &mut self,
        source_owner_node_id: &str,
        target_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
        expected_plan_checksum: &str,
        expected_shard_topology_epoch: Option<u64>,
        expected_shard_topology_checksum: Option<&str>,
        expected_coordinator_session_checksum: Option<&str>,
    ) -> Result<MetadataShardMigrationReport, ObjectError> {
        let report = self.meta_service.execute_shard_rebalance_control_fenced(
            source_owner_node_id,
            target_owner_node_id,
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
