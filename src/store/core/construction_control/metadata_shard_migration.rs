impl MatrixObjectStore {
    pub fn prepare_metadata_shard_migration_plan_raft_entry(
        &self,
        operation: &str,
        source_owner_node_id: &str,
        target_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
    ) -> Result<PreparedMetadataShardMigrationPlan, ObjectError> {
        self.prepare_metadata_shard_migration_plan_raft_entry_fenced(
            operation,
            source_owner_node_id,
            target_owner_node_id,
            max_shards,
            now_ms,
            "",
        )
    }

    pub fn prepare_metadata_shard_migration_plan_raft_entry_fenced(
        &self,
        operation: &str,
        source_owner_node_id: &str,
        target_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
        expected_plan_checksum: &str,
    ) -> Result<PreparedMetadataShardMigrationPlan, ObjectError> {
        self.meta_service
            .prepare_shard_migration_plan_raft_entry_fenced(
                operation,
                source_owner_node_id,
                target_owner_node_id,
                max_shards,
                now_ms,
                expected_plan_checksum,
            )
    }

    pub fn apply_prepared_metadata_shard_migration_plan(
        &mut self,
        prepared: PreparedMetadataShardMigrationPlan,
    ) -> Result<MetadataShardMigrationReport, ObjectError> {
        let report = self
            .meta_service
            .apply_prepared_shard_migration_plan(prepared)?;
        if report.migrated_shards > 0 {
            self.invalidate_metadata_route_cache();
        }
        Ok(report)
    }

    pub fn prepare_metadata_shard_split_raft_entry(
        &self,
        source_shard_id: &str,
        split_key: &str,
        left_shard_id: &str,
        right_shard_id: &str,
        left_owner_node_id: &str,
        right_owner_node_id: &str,
        epoch: u64,
        left_draining: bool,
        right_draining: bool,
        now_ms: u64,
    ) -> Result<PreparedMetadataShardSplit, ObjectError> {
        self.meta_service.prepare_shard_split_raft_entry(
            source_shard_id,
            split_key,
            left_shard_id,
            right_shard_id,
            left_owner_node_id,
            right_owner_node_id,
            epoch,
            left_draining,
            right_draining,
            now_ms,
        )
    }

    pub fn apply_prepared_metadata_shard_split(
        &mut self,
        prepared: PreparedMetadataShardSplit,
    ) -> Result<MetadataShardSplit, ObjectError> {
        let split = self.meta_service.apply_prepared_shard_split(prepared)?;
        self.invalidate_metadata_route_cache();
        Ok(split)
    }

    pub fn prepare_metadata_shard_merge_raft_entry(
        &self,
        left_shard_id: &str,
        right_shard_id: &str,
        merged_shard_id: &str,
        owner_node_id: &str,
        epoch: u64,
        draining: bool,
        now_ms: u64,
    ) -> Result<PreparedMetadataShardMerge, ObjectError> {
        self.meta_service.prepare_shard_merge_raft_entry(
            left_shard_id,
            right_shard_id,
            merged_shard_id,
            owner_node_id,
            epoch,
            draining,
            now_ms,
        )
    }

    pub fn apply_prepared_metadata_shard_merge(
        &mut self,
        prepared: PreparedMetadataShardMerge,
    ) -> Result<MetadataShard, ObjectError> {
        let merged = self.meta_service.apply_prepared_shard_merge(prepared)?;
        self.invalidate_metadata_route_cache();
        Ok(merged)
    }

    pub fn prepare_metadata_shard_migration_raft_entry(
        &self,
        shard_id: &str,
        new_owner_node_id: &str,
        expected_owner_node_id: Option<&str>,
        expected_epoch: u64,
        new_epoch: u64,
        draining: bool,
        now_ms: u64,
    ) -> Result<PreparedMetadataShardMigration, ObjectError> {
        self.meta_service.prepare_shard_migration_raft_entry(
            shard_id,
            new_owner_node_id,
            expected_owner_node_id,
            expected_epoch,
            new_epoch,
            draining,
            now_ms,
        )
    }

    pub fn apply_prepared_metadata_shard_migration(
        &mut self,
        prepared: PreparedMetadataShardMigration,
    ) -> Result<MetadataShard, ObjectError> {
        let shard = self.meta_service.apply_prepared_shard_migration(prepared)?;
        self.invalidate_metadata_route_cache();
        Ok(shard)
    }

    pub fn prepare_metadata_shard_record_handoff_raft_entry(
        &self,
        shard_id: &str,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        include_deleted: bool,
        source_node_id: &str,
        expected_shard_owner: Option<&str>,
        expected_shard_epoch: Option<u64>,
        handoff_token: Option<&str>,
        apply_mutation_id: &str,
    ) -> Result<PreparedMetadataShardRecordHandoff, ObjectError> {
        self.meta_service.prepare_shard_record_handoff_raft_entry(
            shard_id,
            marker,
            limit,
            max_payload_bytes,
            include_deleted,
            source_node_id,
            expected_shard_owner,
            expected_shard_epoch,
            handoff_token,
            apply_mutation_id,
            self.last_metadata_commit_index(),
        )
    }

    pub fn apply_prepared_metadata_shard_record_handoff(
        &mut self,
        prepared: PreparedMetadataShardRecordHandoff,
    ) -> Result<MetadataBatchReceipt, ObjectError> {
        self.meta_service
            .apply_prepared_shard_record_handoff(prepared)
    }

}
