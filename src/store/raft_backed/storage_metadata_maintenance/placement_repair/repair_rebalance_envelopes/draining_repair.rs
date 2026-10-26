// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_prepared_draining_placement_repair(
        &mut self,
        prepared: PreparedPlacementRepair,
    ) -> Result<ObjectMetadata, ObjectError> {
        if self.raft.commit_index() != self.store.last_applied_index()
            || prepared.expected_previous_commit_index
                != self.store.meta_service.placement_commit_index.saturating_sub(1)
        {
            return Err(ObjectError::PreconditionFailed(
                "prepared root placement repair requires raft/root commit alignment".to_string(),
            ));
        }
        self.store
            .clone()
            .apply_command(ObjectCommand::ApplyPreparedRootPlacementRepair {
                prepared: prepared.clone(),
            })?;
        let entry = self
            .raft
            .append(ObjectCommand::ApplyPreparedRootPlacementRepair { prepared })?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::Corruption(
                "prepared root placement repair raft index is not contiguous".to_string(),
            ));
        }
        self.store.apply_raft_entry(&entry)
    }

    pub fn propose_prepared_draining_placement_repair_linearizable(
        &mut self,
        prepared: PreparedPlacementRepair,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_placement_read_index("propose prepared draining root placement repair")?;
        self.propose_prepared_draining_placement_repair(prepared)
    }

    pub fn propose_prepared_draining_placement_repair_with_read_proof_linearizable(
        &mut self,
        prepared: PreparedPlacementRepair,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        self.propose_prepared_draining_placement_repair_with_read_proof_linearizable_committed_at(
            prepared,
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_prepared_draining_placement_repair_with_read_proof_linearizable_committed_at(
        &mut self,
        prepared: PreparedPlacementRepair,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "propose prepared draining root placement repair",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        let metadata =
            self.propose_prepared_draining_placement_repair_linearizable(prepared)?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "propose prepared draining root placement repair post-commit",
            now_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        Ok(proven_metadata_read(metadata, &readiness))
    }

    pub fn propose_draining_placement_repair_prepared(
        &mut self,
        max_objects: usize,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_draining_placement_repair_raft_entry(max_objects);
        self.propose_prepared_draining_placement_repair(prepared)
    }

    pub fn propose_draining_placement_repair_prepared_linearizable(
        &mut self,
        max_objects: usize,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared =
            self.prepare_draining_placement_repair_raft_entry_linearizable(max_objects)?;
        self.propose_prepared_draining_placement_repair_linearizable(prepared)
    }

    pub fn propose_draining_placement_repair_prepared_with_read_proof_linearizable(
        &mut self,
        max_objects: usize,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        self.propose_draining_placement_repair_prepared_with_read_proof_linearizable_committed_at(
            max_objects,
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_draining_placement_repair_prepared_with_read_proof_linearizable_committed_at(
        &mut self,
        max_objects: usize,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "propose draining root placement repair",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        let metadata =
            self.propose_draining_placement_repair_prepared_linearizable(max_objects)?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "propose draining root placement repair post-commit",
            now_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        Ok(proven_metadata_read(metadata, &readiness))
    }

}
