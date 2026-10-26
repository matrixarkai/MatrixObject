// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixMetaService {
    pub fn prepare_placement_migration_raft_entry(
        &self,
        plan: PlacementMigrationPlan,
        expected_plan_checksum: &str,
    ) -> Result<PreparedPlacementMigration, ObjectError> {
        self.prepare_placement_migration_raft_entry_with_min_free(plan, expected_plan_checksum, 0)
    }

    pub fn prepare_placement_migration_raft_entry_with_min_free(
        &self,
        plan: PlacementMigrationPlan,
        expected_plan_checksum: &str,
        min_free_bytes: u64,
    ) -> Result<PreparedPlacementMigration, ObjectError> {
        if plan.plan_checksum != expected_plan_checksum
            || placement_migration_plan_checksum(&plan) != expected_plan_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "root prepared placement migration plan checksum mismatch".to_string(),
            ));
        }
        self.clone().commit_placement_migration_with_min_free(
            plan.clone(),
            &expected_plan_checksum,
            min_free_bytes,
        )?;
        let expected_plan_checksum = expected_plan_checksum.to_string();
        let request_checksum = placement_migration_request_checksum(
            &plan,
            &expected_plan_checksum,
            min_free_bytes,
        );
        let mut prepared = PreparedPlacementMigration {
            plan,
            expected_plan_checksum,
            min_free_bytes,
            before_state_fingerprint: self.root_state_fingerprint(),
            root_read_index: 0,
            root_raft_term: 0,
            root_read_proof_checksum: String::new(),
            request_checksum,
            expected_previous_commit_index: self.placement_commit_index.saturating_sub(1),
            prepared_commit_index: self.placement_commit_index,
            raft_entry_checksum: String::new(),
        };
        prepared.raft_entry_checksum = prepared_placement_migration_checksum(&prepared);
        Ok(prepared)
    }

    pub fn apply_prepared_placement_migration(
        &mut self,
        prepared: PreparedPlacementMigration,
    ) -> Result<ObjectPlacement, ObjectError> {
        if prepared.expected_previous_commit_index != self.placement_commit_index.saturating_sub(1) {
            return Err(ObjectError::PreconditionFailed(
                "root prepared placement migration previous commit fence mismatch".to_string(),
            ));
        }
        if prepared.prepared_commit_index != self.placement_commit_index {
            return Err(ObjectError::PreconditionFailed(
                "root prepared placement migration commit index must be contiguous".to_string(),
            ));
        }
        if prepared.prepared_commit_index
            != prepared.expected_previous_commit_index.saturating_add(1)
        {
            return Err(ObjectError::PreconditionFailed(
                "root prepared placement migration commit index must be adjacent".to_string(),
            ));
        }
        let expected_checksum =
            prepared_placement_migration_checksum(&PreparedPlacementMigration {
                raft_entry_checksum: String::new(),
                ..prepared.clone()
            });
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(
                "root prepared placement migration raft entry checksum mismatch".to_string(),
            ));
        }
        if prepared.request_checksum
            != placement_migration_request_checksum(
                &prepared.plan,
                &prepared.expected_plan_checksum,
                prepared.min_free_bytes,
            )
        {
            return Err(ObjectError::Corruption(
                "root prepared placement migration request checksum mismatch".to_string(),
            ));
        }
        if prepared.before_state_fingerprint != self.root_state_fingerprint() {
            return Err(ObjectError::PreconditionFailed(
                "root prepared placement migration state changed before apply".to_string(),
            ));
        }
        self.commit_placement_migration_with_min_free(
            prepared.plan,
            &prepared.expected_plan_checksum,
            prepared.min_free_bytes,
        )
    }
}
