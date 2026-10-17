impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_repair_draining_root_placements(
        &mut self,
        max_objects: usize,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::RepairDrainingRootPlacements { max_objects })
    }

    pub fn plan_object_decommission_draining_replicas(
        &self,
        max_objects: usize,
    ) -> RootPlacementRepairReport {
        self.store
            .meta_service
            .plan_draining_placement_repairs(max_objects)
    }

    pub fn plan_object_decommission_draining_replicas_with_read_proof_linearizable(
        &self,
        max_objects: usize,
    ) -> Result<ProvenRootRead<RootPlacementRepairReport>, ObjectError> {
        let proof = self.ensure_root_read_index("plan object decommission")?;
        let value = self.plan_object_decommission_draining_replicas(max_objects);
        Ok(ProvenRootRead { value, proof })
    }
}
