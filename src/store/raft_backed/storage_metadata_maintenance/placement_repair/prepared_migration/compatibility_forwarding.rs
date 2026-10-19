// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_root_placement_migration_prepared(
        &mut self,
        plan: PlacementMigrationPlan,
        expected_plan_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared =
            self.prepare_root_placement_migration_raft_entry(plan, expected_plan_checksum)?;
        self.propose_prepared_root_placement_migration(prepared)
    }

    pub fn propose_root_placement_migration_prepared_linearizable(
        &mut self,
        plan: PlacementMigrationPlan,
        expected_plan_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_root_placement_migration_raft_entry_linearizable(
            plan,
            expected_plan_checksum,
        )?;
        self.propose_prepared_root_placement_migration_linearizable(prepared)
    }

    pub fn propose_root_placement_migration_prepared_with_envelope_linearizable(
        &mut self,
        plan: PlacementMigrationPlan,
        expected_plan_checksum: impl AsRef<str>,
    ) -> Result<CommittedRootPlacementMigration, ObjectError> {
        let prepared = self.prepare_root_placement_migration_raft_entry_linearizable(
            plan,
            expected_plan_checksum,
        )?;
        self.propose_prepared_root_placement_migration_with_envelope_linearizable(prepared)
    }

    pub fn propose_root_placement_migration_prepared_with_envelope_and_read_proof_linearizable(
        &mut self,
        plan: PlacementMigrationPlan,
        expected_plan_checksum: impl AsRef<str>,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<CommittedRootPlacementMigration>, ObjectError> {
        self.propose_root_placement_migration_prepared_with_envelope_and_read_proof_linearizable_committed_at(
            plan,
            expected_plan_checksum,
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_root_placement_migration_prepared_with_envelope_and_read_proof_linearizable_committed_at(
        &mut self,
        plan: PlacementMigrationPlan,
        expected_plan_checksum: impl AsRef<str>,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<CommittedRootPlacementMigration>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "propose root placement migration envelope",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        let committed = self.propose_root_placement_migration_prepared_with_envelope_linearizable(
            plan,
            expected_plan_checksum,
        )?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "propose root placement migration envelope post-commit",
            now_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        Ok(proven_metadata_read(committed, &readiness))
    }

    pub fn propose_root_placement_migration_prepared_with_envelope_and_route_proof_linearizable_fenced(
        &mut self,
        plan: PlacementMigrationPlan,
        expected_plan_checksum: impl AsRef<str>,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenRouteFencedMetadataRead<CommittedRootPlacementMigration>, ObjectError> {
        self.propose_root_placement_migration_prepared_with_envelope_and_route_proof_linearizable_fenced_committed_at(
            plan,
            expected_plan_checksum,
            now_ms,
            expected_page_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_root_placement_migration_prepared_with_envelope_and_route_proof_linearizable_fenced_committed_at(
        &mut self,
        plan: PlacementMigrationPlan,
        expected_plan_checksum: impl AsRef<str>,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenRouteFencedMetadataRead<CommittedRootPlacementMigration>, ObjectError> {
        let expected_checksum = expected_page_route_control_checksum.as_ref().to_string();
        self.ensure_page_route_control_ready(
            "root placement migration route proof",
            &expected_checksum,
        )?;
        let route_control_proof = self.store.chunk_service.route_control_proof();
        let proven = self
            .propose_root_placement_migration_prepared_with_envelope_and_read_proof_linearizable_committed_at(
                plan,
                expected_plan_checksum,
                now_ms,
                min_read_index,
                min_commit_index,
            )?;
        Ok(ProvenRouteFencedMetadataRead {
            value: proven.value,
            proof: proven.proof,
            route_control_proof,
            expected_route_control_checksum: expected_checksum,
        })
    }
}
