impl MatrixObjectStore {
    pub fn plan_object_placement_migration(
        &self,
        bucket: &str,
        key: &str,
        source_node_id: &str,
        target_node_id: &str,
    ) -> Result<PlacementMigrationPlan, ObjectError> {
        validate_bucket(bucket)?;
        validate_key(key)?;
        self.meta_service.plan_placement_migration(
            &ObjectId::new(bucket, key),
            source_node_id,
            target_node_id,
        )
    }

    pub fn commit_object_placement_migration(
        &mut self,
        plan: PlacementMigrationPlan,
        expected_plan_checksum: &str,
    ) -> Result<ObjectPlacement, ObjectError> {
        self.meta_service
            .commit_placement_migration(plan, expected_plan_checksum)
    }

    pub fn commit_object_placement_migration_with_min_free(
        &mut self,
        plan: PlacementMigrationPlan,
        expected_plan_checksum: &str,
        min_free_bytes: u64,
    ) -> Result<ObjectPlacement, ObjectError> {
        self.meta_service.commit_placement_migration_with_min_free(
            plan,
            expected_plan_checksum,
            min_free_bytes,
        )
    }

    pub fn prepare_root_placement_migration_raft_entry(
        &self,
        plan: PlacementMigrationPlan,
        expected_plan_checksum: &str,
    ) -> Result<PreparedRootPlacementMigration, ObjectError> {
        self.meta_service
            .prepare_placement_migration_raft_entry(plan, expected_plan_checksum)
    }

    pub fn prepare_root_placement_migration_raft_entry_with_min_free(
        &self,
        plan: PlacementMigrationPlan,
        expected_plan_checksum: &str,
        min_free_bytes: u64,
    ) -> Result<PreparedRootPlacementMigration, ObjectError> {
        self.meta_service
            .prepare_placement_migration_raft_entry_with_min_free(
                plan,
                expected_plan_checksum,
                min_free_bytes,
            )
    }

    pub fn apply_prepared_root_placement_migration(
        &mut self,
        prepared: PreparedRootPlacementMigration,
    ) -> Result<ObjectPlacement, ObjectError> {
        self.meta_service
            .apply_prepared_placement_migration(prepared)
    }

    pub fn plan_draining_root_placement_repairs(
        &self,
        max_objects: usize,
    ) -> RootPlacementRepairReport {
        self.meta_service
            .plan_draining_placement_repairs(max_objects)
    }

    pub fn repair_draining_root_placements(
        &mut self,
        max_objects: usize,
    ) -> RootPlacementRepairReport {
        self.meta_service.repair_draining_placements(max_objects)
    }

    pub fn plan_unhealthy_root_placement_repairs(
        &self,
        marker: Option<&ObjectId>,
        max_objects: usize,
        expected_replica_count: usize,
        min_free_bytes: u64,
        max_payload_bytes: u64,
    ) -> Result<RootPlacementRepairReport, ObjectError> {
        self.meta_service.plan_unhealthy_placement_repairs(
            marker,
            max_objects,
            expected_replica_count,
            min_free_bytes,
            max_payload_bytes,
        )
    }

    pub fn repair_unhealthy_root_placements(
        &mut self,
        marker: Option<&ObjectId>,
        max_objects: usize,
        expected_replica_count: usize,
        min_free_bytes: u64,
        max_payload_bytes: u64,
    ) -> Result<RootPlacementRepairReport, ObjectError> {
        self.meta_service.repair_unhealthy_placements(
            marker,
            max_objects,
            expected_replica_count,
            min_free_bytes,
            max_payload_bytes,
        )
    }

    pub fn plan_root_capacity_rebalance(
        &self,
        max_objects: usize,
        min_free_bytes: u64,
    ) -> RootCapacityRebalanceReport {
        self.meta_service
            .plan_capacity_rebalance(max_objects, min_free_bytes)
    }

    pub fn rebalance_root_capacity(
        &mut self,
        max_objects: usize,
        min_free_bytes: u64,
    ) -> RootCapacityRebalanceReport {
        self.meta_service
            .rebalance_capacity(max_objects, min_free_bytes)
    }

    pub fn prepare_root_capacity_rebalance_raft_entry(
        &self,
        max_objects: usize,
        min_free_bytes: u64,
    ) -> PreparedRootCapacityRebalance {
        self.meta_service
            .prepare_capacity_rebalance_raft_entry(max_objects, min_free_bytes)
    }

    pub fn prepare_draining_root_placement_repair_raft_entry(
        &self,
        max_objects: usize,
    ) -> PreparedRootPlacementRepair {
        self.meta_service
            .prepare_draining_placement_repair_raft_entry(max_objects)
    }

    pub fn apply_prepared_draining_root_placement_repair(
        &mut self,
        prepared: PreparedRootPlacementRepair,
    ) -> Result<RootPlacementRepairReport, ObjectError> {
        self.meta_service
            .apply_prepared_draining_placement_repair(prepared)
    }

    pub fn prepare_unhealthy_root_placement_repair_raft_entry(
        &self,
        marker: Option<&ObjectId>,
        max_objects: usize,
        expected_replica_count: usize,
        min_free_bytes: u64,
        max_payload_bytes: u64,
    ) -> Result<PreparedRootUnhealthyPlacementRepair, ObjectError> {
        self.meta_service
            .prepare_unhealthy_placement_repair_raft_entry(
                marker,
                max_objects,
                expected_replica_count,
                min_free_bytes,
                max_payload_bytes,
            )
    }

    pub fn apply_prepared_unhealthy_root_placement_repair(
        &mut self,
        prepared: PreparedRootUnhealthyPlacementRepair,
    ) -> Result<RootPlacementRepairReport, ObjectError> {
        self.meta_service
            .apply_prepared_unhealthy_placement_repair(prepared)
    }

    pub fn apply_prepared_root_capacity_rebalance(
        &mut self,
        prepared: PreparedRootCapacityRebalance,
    ) -> Result<RootCapacityRebalanceReport, ObjectError> {
        self.meta_service
            .apply_prepared_capacity_rebalance(prepared)
    }

}
