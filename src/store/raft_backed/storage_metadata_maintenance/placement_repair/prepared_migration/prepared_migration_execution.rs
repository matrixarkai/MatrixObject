// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_prepared_root_placement_migration(
        &mut self,
        prepared: PreparedRootPlacementMigration,
    ) -> Result<ObjectMetadata, ObjectError> {
        if !prepared.root_read_proof_checksum.is_empty() {
            let proof = self.ensure_root_read_index("propose prepared root placement migration")?;
            if proof.requested_root_read_index != prepared.root_read_index
                || proof.root_raft_term != prepared.root_raft_term
                || proof.proof_checksum != prepared.root_read_proof_checksum
            {
                return Err(ObjectError::PreconditionFailed(
                    "prepared root placement migration read proof fence mismatch".to_string(),
                ));
            }
        }
        if self.raft.commit_index() != self.store.last_applied_index()
            || prepared.expected_previous_commit_index
                != self.store.meta_service.placement_commit_index.saturating_sub(1)
        {
            return Err(ObjectError::PreconditionFailed(
                "prepared root placement migration requires raft/root commit alignment".to_string(),
            ));
        }
        self.store
            .clone()
            .apply_command(ObjectCommand::ApplyPreparedRootPlacementMigration {
                prepared: prepared.clone(),
            })?;
        let entry = self
            .raft
            .append(ObjectCommand::ApplyPreparedRootPlacementMigration { prepared })?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::Corruption(
                "prepared root placement migration raft index is not contiguous".to_string(),
            ));
        }
        self.store.apply_raft_entry(&entry)
    }

    pub fn propose_prepared_root_placement_migration_linearizable(
        &mut self,
        prepared: PreparedRootPlacementMigration,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_root_read_index("propose prepared root placement migration")?;
        self.propose_prepared_root_placement_migration(prepared)
    }

    pub fn propose_prepared_root_placement_migration_with_envelope_linearizable(
        &mut self,
        prepared: PreparedRootPlacementMigration,
    ) -> Result<CommittedRootPlacementMigration, ObjectError> {
        self.ensure_root_read_index("propose prepared root placement migration envelope")?;
        let envelope_source = prepared.clone();
        let metadata = self.propose_prepared_root_placement_migration(prepared)?;
        let envelope = committed_root_placement_migration_envelope(
            &envelope_source,
            &metadata,
            self.raft.commit_index(),
        );
        Ok(CommittedRootPlacementMigration { metadata, envelope })
    }

    pub fn propose_prepared_root_placement_migration_with_envelope_and_read_proof_linearizable(
        &mut self,
        prepared: PreparedRootPlacementMigration,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<CommittedRootPlacementMigration>, ObjectError> {
        self.propose_prepared_root_placement_migration_with_envelope_and_read_proof_linearizable_committed_at(
            prepared,
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_prepared_root_placement_migration_with_envelope_and_read_proof_linearizable_committed_at(
        &mut self,
        prepared: PreparedRootPlacementMigration,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<CommittedRootPlacementMigration>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "propose prepared root placement migration envelope",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        let committed =
            self.propose_prepared_root_placement_migration_with_envelope_linearizable(prepared)?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "propose prepared root placement migration envelope post-commit",
            now_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        Ok(proven_metadata_read(committed, &readiness))
    }

    pub fn propose_prepared_root_placement_migration_with_envelope_and_route_proof_linearizable_fenced(
        &mut self,
        prepared: PreparedRootPlacementMigration,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenRouteFencedMetadataRead<CommittedRootPlacementMigration>, ObjectError> {
        self.propose_prepared_root_placement_migration_with_envelope_and_route_proof_linearizable_fenced_committed_at(
            prepared,
            now_ms,
            expected_page_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_prepared_root_placement_migration_with_envelope_and_route_proof_linearizable_fenced_committed_at(
        &mut self,
        prepared: PreparedRootPlacementMigration,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenRouteFencedMetadataRead<CommittedRootPlacementMigration>, ObjectError> {
        let expected_checksum = expected_page_route_control_checksum.as_ref().to_string();
        self.ensure_page_route_control_ready(
            "prepared root placement migration route proof",
            &expected_checksum,
        )?;
        let route_control_proof = self.store.chunk_service.route_control_proof();
        let proven = self
            .propose_prepared_root_placement_migration_with_envelope_and_read_proof_linearizable_committed_at(
                prepared,
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

    pub fn propose_proven_root_placement_migration_envelope_fenced(
        &mut self,
        proven_envelope: ProvenPreparedRootPlacementMigrationEnvelope,
        expected_envelope_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
        expected_plan_checksum: impl AsRef<str>,
    ) -> Result<CommittedRootPlacementMigration, ObjectError> {
        if self.store.last_applied_index() < proven_envelope.proof.root_applied_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven root placement migration proof is not applied: applied={} required={}",
                self.store.last_applied_index(),
                proven_envelope.proof.root_applied_index
            )));
        }
        let expected_envelope_checksum = expected_envelope_checksum.as_ref();
        if expected_envelope_checksum.is_empty()
            || proven_envelope.envelope.envelope_checksum != expected_envelope_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven root placement migration envelope checksum mismatch".to_string(),
            ));
        }
        let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
        if expected_raft_entry_checksum.is_empty()
            || proven_envelope.envelope.raft_entry_checksum != expected_raft_entry_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven root placement migration raft entry checksum mismatch".to_string(),
            ));
        }
        let expected_plan_checksum = expected_plan_checksum.as_ref();
        if expected_plan_checksum.is_empty()
            || proven_envelope.envelope.expected_plan_checksum != expected_plan_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven root placement migration plan checksum mismatch".to_string(),
            ));
        }
        let plan = PlacementMigrationPlan {
            object_id: proven_envelope.envelope.object_id.clone(),
            version: proven_envelope.envelope.version,
            length: proven_envelope.envelope.length,
            source_node_id: proven_envelope.envelope.source_node_id.clone(),
            target_node_id: proven_envelope.envelope.target_node_id.clone(),
            before_node_ids: proven_envelope.envelope.before_node_ids.clone(),
            after_node_ids: proven_envelope.envelope.after_node_ids.clone(),
            reason: proven_envelope.envelope.reason.clone(),
            plan_checksum: expected_plan_checksum.to_string(),
        };
        let prepared = self.prepare_root_placement_migration_raft_entry_linearizable(
            plan,
            expected_plan_checksum,
        )?;
        let actual_envelope = prepared_root_placement_migration_envelope(&prepared);
        if actual_envelope.envelope_checksum != proven_envelope.envelope.envelope_checksum {
            return Err(ObjectError::PreconditionFailed(
                "proven root placement migration envelope changed before proposal".to_string(),
            ));
        }
        prepared.verify_expected_raft_entry_checksum(expected_raft_entry_checksum)?;
        self.propose_prepared_root_placement_migration_with_envelope_linearizable(prepared)
    }

    pub fn propose_proven_root_placement_migration_envelope_fenced_with_read_proof_linearizable(
        &mut self,
        proven_envelope: ProvenPreparedRootPlacementMigrationEnvelope,
        expected_envelope_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
        expected_plan_checksum: impl AsRef<str>,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<CommittedRootPlacementMigration>, ObjectError> {
        self.propose_proven_root_placement_migration_envelope_fenced_with_read_proof_linearizable_committed_at(
            proven_envelope,
            expected_envelope_checksum,
            expected_raft_entry_checksum,
            expected_plan_checksum,
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_proven_root_placement_migration_envelope_fenced_with_read_proof_linearizable_committed_at(
        &mut self,
        proven_envelope: ProvenPreparedRootPlacementMigrationEnvelope,
        expected_envelope_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
        expected_plan_checksum: impl AsRef<str>,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<CommittedRootPlacementMigration>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "propose proven root placement migration envelope",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        let committed = self.propose_proven_root_placement_migration_envelope_fenced(
            proven_envelope,
            expected_envelope_checksum,
            expected_raft_entry_checksum,
            expected_plan_checksum,
        )?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "propose proven root placement migration envelope post-commit",
            now_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        Ok(proven_metadata_read(committed, &readiness))
    }

    pub fn propose_proven_root_placement_migration_envelope_fenced_with_route_proof_linearizable(
        &mut self,
        proven_envelope: ProvenPreparedRootPlacementMigrationEnvelope,
        expected_envelope_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
        expected_plan_checksum: impl AsRef<str>,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenRouteFencedMetadataRead<CommittedRootPlacementMigration>, ObjectError> {
        self.propose_proven_root_placement_migration_envelope_fenced_with_route_proof_linearizable_committed_at(
            proven_envelope,
            expected_envelope_checksum,
            expected_raft_entry_checksum,
            expected_plan_checksum,
            now_ms,
            expected_page_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_proven_root_placement_migration_envelope_fenced_with_route_proof_linearizable_committed_at(
        &mut self,
        proven_envelope: ProvenPreparedRootPlacementMigrationEnvelope,
        expected_envelope_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
        expected_plan_checksum: impl AsRef<str>,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenRouteFencedMetadataRead<CommittedRootPlacementMigration>, ObjectError> {
        let expected_checksum = expected_page_route_control_checksum.as_ref().to_string();
        self.ensure_page_route_control_ready(
            "proven root placement migration route proof",
            &expected_checksum,
        )?;
        let route_control_proof = self.store.chunk_service.route_control_proof();
        let proven = self
            .propose_proven_root_placement_migration_envelope_fenced_with_read_proof_linearizable_committed_at(
                proven_envelope,
                expected_envelope_checksum,
                expected_raft_entry_checksum,
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
