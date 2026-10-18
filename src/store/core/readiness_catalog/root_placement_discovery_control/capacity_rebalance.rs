impl MatrixObjectStore {

    pub fn plan_root_capacity_rebalance(
        &self,
        max_objects: usize,
        min_free_bytes: u64,
    ) -> RootCapacityRebalanceReport {
        self.meta_service
            .plan_capacity_rebalance(max_objects, min_free_bytes)
    }


    pub fn rebalance_root_capacity(
        &mut self,
        max_objects: usize,
        min_free_bytes: u64,
    ) -> RootCapacityRebalanceReport {
        self.meta_service
            .rebalance_capacity(max_objects, min_free_bytes)
    }


    pub fn prepare_root_capacity_rebalance_raft_entry(
        &self,
        max_objects: usize,
        min_free_bytes: u64,
    ) -> PreparedRootCapacityRebalance {
        self.meta_service
            .prepare_capacity_rebalance_raft_entry(max_objects, min_free_bytes)
    }


    pub fn apply_prepared_root_capacity_rebalance(
        &mut self,
        prepared: PreparedRootCapacityRebalance,
    ) -> Result<RootCapacityRebalanceReport, ObjectError> {
        self.meta_service
            .apply_prepared_capacity_rebalance(prepared)
    }
}
