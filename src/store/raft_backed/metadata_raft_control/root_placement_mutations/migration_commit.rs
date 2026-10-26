// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_commit_placement_migration(
        &mut self,
        plan: PlacementMigrationPlan,
        expected_plan_checksum: impl Into<String>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_commit_placement_migration_with_min_free(plan, expected_plan_checksum, 0)
    }

    pub fn propose_commit_placement_migration_with_min_free(
        &mut self,
        plan: PlacementMigrationPlan,
        expected_plan_checksum: impl Into<String>,
        min_free_bytes: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::CommitRootPlacementMigration {
            plan,
            expected_plan_checksum: expected_plan_checksum.into(),
            min_free_bytes,
        })
    }
}
