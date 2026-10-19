// SPDX-License-Identifier: Apache-2.0
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
}
