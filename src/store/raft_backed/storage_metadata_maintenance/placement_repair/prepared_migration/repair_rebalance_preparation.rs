// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_repair_unhealthy_placements(
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

    pub fn prepare_unhealthy_placement_repair_raft_entry(
        &self,
        marker: Option<&ObjectId>,
        max_objects: usize,
        expected_replica_count: usize,
        min_free_bytes: u64,
        max_payload_bytes: u64,
    ) -> Result<PreparedUnhealthyPlacementRepair, ObjectError> {
        self.store
            .prepare_unhealthy_placement_repair_raft_entry(
                marker,
                max_objects,
                expected_replica_count,
                min_free_bytes,
                max_payload_bytes,
            )
    }

    pub fn prepare_unhealthy_placement_repair_raft_entry_linearizable(
        &self,
        marker: Option<&ObjectId>,
        max_objects: usize,
        expected_replica_count: usize,
        min_free_bytes: u64,
        max_payload_bytes: u64,
    ) -> Result<PreparedUnhealthyPlacementRepair, ObjectError> {
        self.ensure_placement_read_index("prepare unhealthy root placement repair")?;
        self.prepare_unhealthy_placement_repair_raft_entry(
            marker,
            max_objects,
            expected_replica_count,
            min_free_bytes,
            max_payload_bytes,
        )
    }

    pub fn prepare_draining_placement_repair_envelope_linearizable(
        &self,
        max_objects: usize,
    ) -> Result<PreparedPlacementRepairEnvelope, ObjectError> {
        let prepared =
            self.prepare_draining_placement_repair_raft_entry_linearizable(max_objects)?;
        Ok(prepared_draining_placement_repair_envelope(&prepared))
    }

    pub fn prepare_draining_placement_repair_envelope_with_read_proof_linearizable(
        &self,
        max_objects: usize,
    ) -> Result<ProvenPreparedPlacementRepairEnvelope, ObjectError> {
        let proof =
            self.ensure_placement_read_index("prepare draining root placement repair envelope")?;
        let prepared = self.prepare_draining_placement_repair_raft_entry(max_objects);
        Ok(ProvenPreparedPlacementRepairEnvelope {
            envelope: prepared_draining_placement_repair_envelope(&prepared),
            proof,
        })
    }

    pub fn prepare_unhealthy_placement_repair_envelope_linearizable(
        &self,
        marker: Option<&ObjectId>,
        max_objects: usize,
        expected_replica_count: usize,
        min_free_bytes: u64,
        max_payload_bytes: u64,
    ) -> Result<PreparedPlacementRepairEnvelope, ObjectError> {
        let prepared = self.prepare_unhealthy_placement_repair_raft_entry_linearizable(
            marker,
            max_objects,
            expected_replica_count,
            min_free_bytes,
            max_payload_bytes,
        )?;
        Ok(prepared_unhealthy_placement_repair_envelope(&prepared))
    }

    pub fn prepare_unhealthy_placement_repair_envelope_with_read_proof_linearizable(
        &self,
        marker: Option<&ObjectId>,
        max_objects: usize,
        expected_replica_count: usize,
        min_free_bytes: u64,
        max_payload_bytes: u64,
    ) -> Result<ProvenPreparedPlacementRepairEnvelope, ObjectError> {
        let proof =
            self.ensure_placement_read_index("prepare unhealthy root placement repair envelope")?;
        let prepared = self.prepare_unhealthy_placement_repair_raft_entry(
            marker,
            max_objects,
            expected_replica_count,
            min_free_bytes,
            max_payload_bytes,
        )?;
        Ok(ProvenPreparedPlacementRepairEnvelope {
            envelope: prepared_unhealthy_placement_repair_envelope(&prepared),
            proof,
        })
    }

    pub fn prepare_placement_capacity_rebalance_envelope_linearizable(
        &self,
        max_objects: usize,
        min_free_bytes: u64,
    ) -> Result<PreparedPlacementCapacityRebalanceEnvelope, ObjectError> {
        let prepared = self
            .prepare_placement_capacity_rebalance_raft_entry_linearizable(max_objects, min_free_bytes)?;
        Ok(prepared_placement_capacity_rebalance_envelope(&prepared))
    }

    pub fn prepare_placement_capacity_rebalance_envelope_with_read_proof_linearizable(
        &self,
        max_objects: usize,
        min_free_bytes: u64,
    ) -> Result<ProvenPreparedPlacementCapacityRebalanceEnvelope, ObjectError> {
        let proof = self.ensure_placement_read_index("prepare root capacity rebalance envelope")?;
        let prepared = self.prepare_placement_capacity_rebalance_raft_entry(max_objects, min_free_bytes);
        Ok(ProvenPreparedPlacementCapacityRebalanceEnvelope {
            envelope: prepared_placement_capacity_rebalance_envelope(&prepared),
            proof,
        })
    }

    pub fn propose_rebalance_placement_capacity(
        &mut self,
        max_objects: usize,
        min_free_bytes: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::RebalanceRootCapacity {
            max_objects,
            min_free_bytes,
        })
    }

    pub fn prepare_placement_capacity_rebalance_raft_entry(
        &self,
        max_objects: usize,
        min_free_bytes: u64,
    ) -> PreparedPlacementCapacityRebalance {
        self.store
            .prepare_placement_capacity_rebalance_raft_entry(max_objects, min_free_bytes)
    }

    pub fn prepare_placement_capacity_rebalance_raft_entry_linearizable(
        &self,
        max_objects: usize,
        min_free_bytes: u64,
    ) -> Result<PreparedPlacementCapacityRebalance, ObjectError> {
        self.ensure_placement_read_index("prepare root capacity rebalance")?;
        Ok(self.prepare_placement_capacity_rebalance_raft_entry(max_objects, min_free_bytes))
    }

    pub fn prepare_draining_placement_repair_raft_entry(
        &self,
        max_objects: usize,
    ) -> PreparedPlacementRepair {
        self.store
            .prepare_draining_placement_repair_raft_entry(max_objects)
    }

    pub fn prepare_draining_placement_repair_raft_entry_linearizable(
        &self,
        max_objects: usize,
    ) -> Result<PreparedPlacementRepair, ObjectError> {
        self.ensure_placement_read_index("prepare draining root placement repair")?;
        Ok(self.prepare_draining_placement_repair_raft_entry(max_objects))
    }
}
