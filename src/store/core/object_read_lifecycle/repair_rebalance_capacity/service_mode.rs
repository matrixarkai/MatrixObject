impl MatrixObjectStore {
    pub fn configure_chunk_replica_service_mode(
        &mut self,
        replica_index: usize,
        accepting_writes: Option<bool>,
        draining: Option<bool>,
        frozen: Option<bool>,
    ) -> Result<(), ObjectError> {
        self.page_service.configure_chunk_replica_service_mode(
            replica_index,
            accepting_writes,
            draining,
            frozen,
        )
    }

    pub fn configure_all_eligible_chunk_replica_service_mode(
        &mut self,
        accepting_writes: Option<bool>,
        draining: Option<bool>,
        frozen: Option<bool>,
    ) -> Result<u64, ObjectError> {
        self.page_service
            .configure_all_eligible_chunk_replica_service_mode(accepting_writes, draining, frozen)
    }
}
