impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn prepare_page_batch_write_raft_entry(
        &self,
        writes: Vec<ChunkRangeWrite>,
        idempotency_key: &str,
    ) -> Result<PreparedPageBatchWrite, ObjectError> {
        let expected_route_control_checksum = self
            .store
            .chunk_service
            .route_control_proof()
            .state_checksum;
        self.store.prepare_page_batch_write_raft_entry_fenced(
            writes,
            idempotency_key,
            self.store.last_applied_index(),
            Some(&expected_route_control_checksum),
        )
    }

    pub fn prepare_page_batch_write_raft_entry_bounded(
        &self,
        writes: Vec<ChunkRangeWrite>,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
        idempotency_key: &str,
    ) -> Result<PreparedPageBatchWrite, ObjectError> {
        self.prepare_page_batch_write_raft_entry_bounded_from(
            writes,
            max_write_bytes,
            max_chunk_write_count,
            0,
            idempotency_key,
        )
    }

    pub fn prepare_page_batch_write_raft_entry_bounded_from(
        &self,
        writes: Vec<ChunkRangeWrite>,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
        write_start_range_index: u64,
        idempotency_key: &str,
    ) -> Result<PreparedPageBatchWrite, ObjectError> {
        self.store
            .prepare_page_batch_write_raft_entry_bounded_from(
                writes,
                max_write_bytes,
                max_chunk_write_count,
                write_start_range_index,
                idempotency_key,
                self.store.last_applied_index(),
            )
    }

    pub fn prepare_page_batch_write_raft_entry_bounded_fenced_from(
        &self,
        writes: Vec<ChunkRangeWrite>,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
        write_start_range_index: u64,
        idempotency_key: &str,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedPageBatchWrite, ObjectError> {
        self.store
            .prepare_page_batch_write_raft_entry_bounded_fenced_from(
                writes,
                max_write_bytes,
                max_chunk_write_count,
                write_start_range_index,
                idempotency_key,
                self.store.last_applied_index(),
                expected_route_control_checksum,
            )
    }
}
