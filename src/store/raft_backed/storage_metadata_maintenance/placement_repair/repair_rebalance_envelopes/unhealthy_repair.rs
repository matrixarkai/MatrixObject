impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_prepared_unhealthy_root_placement_repair(
        &mut self,
        prepared: PreparedRootUnhealthyPlacementRepair,
    ) -> Result<ObjectMetadata, ObjectError> {
        if self.raft.commit_index() != self.store.last_applied_index()
            || prepared.expected_previous_commit_index
                != self.store.meta_service.placement_commit_index.saturating_sub(1)
        {
            return Err(ObjectError::PreconditionFailed(
                "prepared root unhealthy placement repair requires raft/root commit alignment"
                    .to_string(),
            ));
        }
        self.store.clone().apply_command(
            ObjectCommand::ApplyPreparedRootUnhealthyPlacementRepair {
                prepared: prepared.clone(),
            },
        )?;
        let entry = self
            .raft
            .append(ObjectCommand::ApplyPreparedRootUnhealthyPlacementRepair { prepared })?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::Corruption(
                "prepared root unhealthy placement repair raft index is not contiguous".to_string(),
            ));
        }
        self.store.apply_raft_entry(&entry)
    }

    pub fn propose_prepared_unhealthy_root_placement_repair_linearizable(
        &mut self,
        prepared: PreparedRootUnhealthyPlacementRepair,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_root_read_index("propose prepared unhealthy root placement repair")?;
        self.propose_prepared_unhealthy_root_placement_repair(prepared)
    }

    pub fn propose_prepared_unhealthy_root_placement_repair_with_read_proof_linearizable(
        &mut self,
        prepared: PreparedRootUnhealthyPlacementRepair,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        self.propose_prepared_unhealthy_root_placement_repair_with_read_proof_linearizable_committed_at(
            prepared,
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_prepared_unhealthy_root_placement_repair_with_read_proof_linearizable_committed_at(
        &mut self,
        prepared: PreparedRootUnhealthyPlacementRepair,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "propose prepared unhealthy root placement repair",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        let metadata =
            self.propose_prepared_unhealthy_root_placement_repair_linearizable(prepared)?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "propose prepared unhealthy root placement repair post-commit",
            now_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        Ok(proven_metadata_read(metadata, &readiness))
    }

    pub fn propose_unhealthy_root_placement_repair_prepared(
        &mut self,
        marker: Option<ObjectId>,
        max_objects: usize,
        expected_replica_count: usize,
        min_free_bytes: u64,
        max_payload_bytes: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_unhealthy_root_placement_repair_raft_entry(
            marker.as_ref(),
            max_objects,
            expected_replica_count,
            min_free_bytes,
            max_payload_bytes,
        )?;
        self.propose_prepared_unhealthy_root_placement_repair(prepared)
    }

    pub fn propose_unhealthy_root_placement_repair_prepared_linearizable(
        &mut self,
        marker: Option<ObjectId>,
        max_objects: usize,
        expected_replica_count: usize,
        min_free_bytes: u64,
        max_payload_bytes: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_unhealthy_root_placement_repair_raft_entry_linearizable(
            marker.as_ref(),
            max_objects,
            expected_replica_count,
            min_free_bytes,
            max_payload_bytes,
        )?;
        self.propose_prepared_unhealthy_root_placement_repair_linearizable(prepared)
    }

    pub fn propose_unhealthy_root_placement_repair_prepared_with_read_proof_linearizable(
        &mut self,
        marker: Option<ObjectId>,
        max_objects: usize,
        expected_replica_count: usize,
        min_free_bytes: u64,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        self.propose_unhealthy_root_placement_repair_prepared_with_read_proof_linearizable_committed_at(
            marker,
            max_objects,
            expected_replica_count,
            min_free_bytes,
            max_payload_bytes,
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_unhealthy_root_placement_repair_prepared_with_read_proof_linearizable_committed_at(
        &mut self,
        marker: Option<ObjectId>,
        max_objects: usize,
        expected_replica_count: usize,
        min_free_bytes: u64,
        max_payload_bytes: u64,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "propose unhealthy root placement repair",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        let metadata = self.propose_unhealthy_root_placement_repair_prepared_linearizable(
            marker,
            max_objects,
            expected_replica_count,
            min_free_bytes,
            max_payload_bytes,
        )?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "propose unhealthy root placement repair post-commit",
            now_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        Ok(proven_metadata_read(metadata, &readiness))
    }

}
