impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_repair_unhealthy_root_placements(
        &mut self,
        marker: Option<ObjectId>,
        max_objects: usize,
        expected_replica_count: usize,
        min_free_bytes: u64,
        max_payload_bytes: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::RepairUnhealthyRootPlacements {
            marker,
            max_objects,
            expected_replica_count,
            min_free_bytes,
            max_payload_bytes,
        })
    }

    pub fn prepare_unhealthy_root_placement_repair_raft_entry(
        &self,
        marker: Option<&ObjectId>,
        max_objects: usize,
        expected_replica_count: usize,
        min_free_bytes: u64,
        max_payload_bytes: u64,
    ) -> Result<PreparedRootUnhealthyPlacementRepair, ObjectError> {
        self.store
            .prepare_unhealthy_root_placement_repair_raft_entry(
                marker,
                max_objects,
                expected_replica_count,
                min_free_bytes,
                max_payload_bytes,
            )
    }

    pub fn prepare_unhealthy_root_placement_repair_raft_entry_linearizable(
        &self,
        marker: Option<&ObjectId>,
        max_objects: usize,
        expected_replica_count: usize,
        min_free_bytes: u64,
        max_payload_bytes: u64,
    ) -> Result<PreparedRootUnhealthyPlacementRepair, ObjectError> {
        self.ensure_root_read_index("prepare unhealthy root placement repair")?;
        self.prepare_unhealthy_root_placement_repair_raft_entry(
            marker,
            max_objects,
            expected_replica_count,
            min_free_bytes,
            max_payload_bytes,
        )
    }

    pub fn prepare_draining_root_placement_repair_envelope_linearizable(
        &self,
        max_objects: usize,
    ) -> Result<PreparedRootPlacementRepairEnvelope, ObjectError> {
        let prepared =
            self.prepare_draining_root_placement_repair_raft_entry_linearizable(max_objects)?;
        Ok(prepared_draining_root_placement_repair_envelope(&prepared))
    }

    pub fn prepare_draining_root_placement_repair_envelope_with_read_proof_linearizable(
        &self,
        max_objects: usize,
    ) -> Result<ProvenPreparedRootPlacementRepairEnvelope, ObjectError> {
        let proof =
            self.ensure_root_read_index("prepare draining root placement repair envelope")?;
        let prepared = self.prepare_draining_root_placement_repair_raft_entry(max_objects);
        Ok(ProvenPreparedRootPlacementRepairEnvelope {
            envelope: prepared_draining_root_placement_repair_envelope(&prepared),
            proof,
        })
    }

    pub fn prepare_unhealthy_root_placement_repair_envelope_linearizable(
        &self,
        marker: Option<&ObjectId>,
        max_objects: usize,
        expected_replica_count: usize,
        min_free_bytes: u64,
        max_payload_bytes: u64,
    ) -> Result<PreparedRootPlacementRepairEnvelope, ObjectError> {
        let prepared = self.prepare_unhealthy_root_placement_repair_raft_entry_linearizable(
            marker,
            max_objects,
            expected_replica_count,
            min_free_bytes,
            max_payload_bytes,
        )?;
        Ok(prepared_unhealthy_root_placement_repair_envelope(&prepared))
    }

    pub fn prepare_unhealthy_root_placement_repair_envelope_with_read_proof_linearizable(
        &self,
        marker: Option<&ObjectId>,
        max_objects: usize,
        expected_replica_count: usize,
        min_free_bytes: u64,
        max_payload_bytes: u64,
    ) -> Result<ProvenPreparedRootPlacementRepairEnvelope, ObjectError> {
        let proof =
            self.ensure_root_read_index("prepare unhealthy root placement repair envelope")?;
        let prepared = self.prepare_unhealthy_root_placement_repair_raft_entry(
            marker,
            max_objects,
            expected_replica_count,
            min_free_bytes,
            max_payload_bytes,
        )?;
        Ok(ProvenPreparedRootPlacementRepairEnvelope {
            envelope: prepared_unhealthy_root_placement_repair_envelope(&prepared),
            proof,
        })
    }

    pub fn prepare_root_capacity_rebalance_envelope_linearizable(
        &self,
        max_objects: usize,
        min_free_bytes: u64,
    ) -> Result<PreparedRootCapacityRebalanceEnvelope, ObjectError> {
        let prepared = self
            .prepare_root_capacity_rebalance_raft_entry_linearizable(max_objects, min_free_bytes)?;
        Ok(prepared_root_capacity_rebalance_envelope(&prepared))
    }

    pub fn prepare_root_capacity_rebalance_envelope_with_read_proof_linearizable(
        &self,
        max_objects: usize,
        min_free_bytes: u64,
    ) -> Result<ProvenPreparedRootCapacityRebalanceEnvelope, ObjectError> {
        let proof = self.ensure_root_read_index("prepare root capacity rebalance envelope")?;
        let prepared = self.prepare_root_capacity_rebalance_raft_entry(max_objects, min_free_bytes);
        Ok(ProvenPreparedRootCapacityRebalanceEnvelope {
            envelope: prepared_root_capacity_rebalance_envelope(&prepared),
            proof,
        })
    }

    pub fn propose_rebalance_root_capacity(
        &mut self,
        max_objects: usize,
        min_free_bytes: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::RebalanceRootCapacity {
            max_objects,
            min_free_bytes,
        })
    }

    pub fn prepare_root_capacity_rebalance_raft_entry(
        &self,
        max_objects: usize,
        min_free_bytes: u64,
    ) -> PreparedRootCapacityRebalance {
        self.store
            .prepare_root_capacity_rebalance_raft_entry(max_objects, min_free_bytes)
    }

    pub fn prepare_root_capacity_rebalance_raft_entry_linearizable(
        &self,
        max_objects: usize,
        min_free_bytes: u64,
    ) -> Result<PreparedRootCapacityRebalance, ObjectError> {
        self.ensure_root_read_index("prepare root capacity rebalance")?;
        Ok(self.prepare_root_capacity_rebalance_raft_entry(max_objects, min_free_bytes))
    }

    pub fn prepare_draining_root_placement_repair_raft_entry(
        &self,
        max_objects: usize,
    ) -> PreparedRootPlacementRepair {
        self.store
            .prepare_draining_root_placement_repair_raft_entry(max_objects)
    }

    pub fn prepare_draining_root_placement_repair_raft_entry_linearizable(
        &self,
        max_objects: usize,
    ) -> Result<PreparedRootPlacementRepair, ObjectError> {
        self.ensure_root_read_index("prepare draining root placement repair")?;
        Ok(self.prepare_draining_root_placement_repair_raft_entry(max_objects))
    }
}
