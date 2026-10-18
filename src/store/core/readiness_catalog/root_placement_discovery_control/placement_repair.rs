impl MatrixObjectStore {

    pub fn plan_draining_root_placement_repairs(
        &self,
        max_objects: usize,
    ) -> RootPlacementRepairReport {
        self.meta_service
            .plan_draining_placement_repairs(max_objects)
    }


    pub fn repair_draining_root_placements(
        &mut self,
        max_objects: usize,
    ) -> RootPlacementRepairReport {
        self.meta_service.repair_draining_placements(max_objects)
    }


    pub fn plan_unhealthy_root_placement_repairs(
        &self,
        marker: Option<&ObjectId>,
        max_objects: usize,
        expected_replica_count: usize,
        min_free_bytes: u64,
        max_payload_bytes: u64,
    ) -> Result<RootPlacementRepairReport, ObjectError> {
        self.meta_service.plan_unhealthy_placement_repairs(
            marker,
            max_objects,
            expected_replica_count,
            min_free_bytes,
            max_payload_bytes,
        )
    }


    pub fn repair_unhealthy_root_placements(
        &mut self,
        marker: Option<&ObjectId>,
        max_objects: usize,
        expected_replica_count: usize,
        min_free_bytes: u64,
        max_payload_bytes: u64,
    ) -> Result<RootPlacementRepairReport, ObjectError> {
        self.meta_service.repair_unhealthy_placements(
            marker,
            max_objects,
            expected_replica_count,
            min_free_bytes,
            max_payload_bytes,
        )
    }


    pub fn prepare_draining_root_placement_repair_raft_entry(
        &self,
        max_objects: usize,
    ) -> PreparedRootPlacementRepair {
        self.meta_service
            .prepare_draining_placement_repair_raft_entry(max_objects)
    }


    pub fn apply_prepared_draining_root_placement_repair(
        &mut self,
        prepared: PreparedRootPlacementRepair,
    ) -> Result<RootPlacementRepairReport, ObjectError> {
        self.meta_service
            .apply_prepared_draining_placement_repair(prepared)
    }


    pub fn prepare_unhealthy_root_placement_repair_raft_entry(
        &self,
        marker: Option<&ObjectId>,
        max_objects: usize,
        expected_replica_count: usize,
        min_free_bytes: u64,
        max_payload_bytes: u64,
    ) -> Result<PreparedRootUnhealthyPlacementRepair, ObjectError> {
        self.meta_service
            .prepare_unhealthy_placement_repair_raft_entry(
                marker,
                max_objects,
                expected_replica_count,
                min_free_bytes,
                max_payload_bytes,
            )
    }


    pub fn apply_prepared_unhealthy_root_placement_repair(
        &mut self,
        prepared: PreparedRootUnhealthyPlacementRepair,
    ) -> Result<RootPlacementRepairReport, ObjectError> {
        self.meta_service
            .apply_prepared_unhealthy_placement_repair(prepared)
    }
}
