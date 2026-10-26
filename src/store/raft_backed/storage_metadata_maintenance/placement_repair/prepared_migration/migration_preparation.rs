// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn prepare_placement_migration_raft_entry(
        &self,
        plan: PlacementMigrationPlan,
        expected_plan_checksum: impl AsRef<str>,
    ) -> Result<PreparedPlacementMigration, ObjectError> {
        self.store
            .prepare_placement_migration_raft_entry(plan, expected_plan_checksum.as_ref())
    }

    pub fn prepare_placement_migration_raft_entry_with_min_free(
        &self,
        plan: PlacementMigrationPlan,
        expected_plan_checksum: impl AsRef<str>,
        min_free_bytes: u64,
    ) -> Result<PreparedPlacementMigration, ObjectError> {
        self.store
            .prepare_placement_migration_raft_entry_with_min_free(
                plan,
                expected_plan_checksum.as_ref(),
                min_free_bytes,
            )
    }

    pub fn prepare_placement_migration_raft_entry_linearizable(
        &self,
        plan: PlacementMigrationPlan,
        expected_plan_checksum: impl AsRef<str>,
    ) -> Result<PreparedPlacementMigration, ObjectError> {
        self.prepare_placement_migration_raft_entry_with_min_free_linearizable(
            plan,
            expected_plan_checksum,
            0,
        )
    }

    pub fn prepare_placement_migration_raft_entry_with_min_free_linearizable(
        &self,
        plan: PlacementMigrationPlan,
        expected_plan_checksum: impl AsRef<str>,
        min_free_bytes: u64,
    ) -> Result<PreparedPlacementMigration, ObjectError> {
        let proof = self.ensure_placement_read_index("prepare root placement migration")?;
        let mut prepared = self.prepare_placement_migration_raft_entry_with_min_free(
            plan,
            expected_plan_checksum,
            min_free_bytes,
        )?;
        prepared.root_read_index = proof.requested_root_read_index;
        prepared.root_raft_term = proof.root_raft_term;
        prepared.root_read_proof_checksum = proof.proof_checksum;
        prepared.raft_entry_checksum = prepared_placement_migration_checksum(&prepared);
        Ok(prepared)
    }

    pub fn prepare_placement_migration_envelope_linearizable(
        &self,
        plan: PlacementMigrationPlan,
        expected_plan_checksum: impl AsRef<str>,
    ) -> Result<PreparedPlacementMigrationEnvelope, ObjectError> {
        let prepared = self.prepare_placement_migration_raft_entry_linearizable(
            plan,
            expected_plan_checksum,
        )?;
        Ok(prepared_placement_migration_envelope(&prepared))
    }

    pub fn prepare_placement_migration_envelope_with_min_free_linearizable(
        &self,
        plan: PlacementMigrationPlan,
        expected_plan_checksum: impl AsRef<str>,
        min_free_bytes: u64,
    ) -> Result<PreparedPlacementMigrationEnvelope, ObjectError> {
        let prepared = self
            .prepare_placement_migration_raft_entry_with_min_free_linearizable(
                plan,
                expected_plan_checksum,
                min_free_bytes,
            )?;
        Ok(prepared_placement_migration_envelope(&prepared))
    }

    pub fn prepare_placement_migration_envelope_with_read_proof_linearizable(
        &self,
        plan: PlacementMigrationPlan,
        expected_plan_checksum: impl AsRef<str>,
    ) -> Result<ProvenPreparedPlacementMigrationEnvelope, ObjectError> {
        let proof = self.ensure_placement_read_index("prepare root placement migration envelope")?;
        let mut prepared =
            self.prepare_placement_migration_raft_entry(plan, expected_plan_checksum)?;
        prepared.root_read_index = proof.requested_root_read_index;
        prepared.root_raft_term = proof.root_raft_term;
        prepared.root_read_proof_checksum = proof.proof_checksum.clone();
        prepared.raft_entry_checksum = prepared_placement_migration_checksum(&prepared);
        Ok(ProvenPreparedPlacementMigrationEnvelope {
            envelope: prepared_placement_migration_envelope(&prepared),
            proof,
        })
    }
}
