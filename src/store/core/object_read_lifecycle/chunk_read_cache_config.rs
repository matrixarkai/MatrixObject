impl MatrixObjectStore {
    pub fn configure_chunk_read_cache(
        &mut self,
        max_bytes_per_replica: u64,
    ) -> Result<(), ObjectError> {
        self.configure_chunk_read_cache_with_stream_bypass(
            max_bytes_per_replica,
            self.chunk_read_cache_max_cacheable_read_bytes_per_replica,
        )
    }

    pub fn configure_chunk_read_cache_with_stream_bypass(
        &mut self,
        max_bytes_per_replica: u64,
        max_cacheable_read_bytes_per_replica: u64,
    ) -> Result<(), ObjectError> {
        self.chunk_read_cache_max_bytes_per_replica = max_bytes_per_replica;
        self.chunk_read_cache_max_cacheable_read_bytes_per_replica =
            max_cacheable_read_bytes_per_replica;
        self.chunk_service
            .set_read_cache_max_bytes_per_replica(max_bytes_per_replica);
        self.chunk_service
            .set_read_cache_max_cacheable_read_bytes_per_replica(
                max_cacheable_read_bytes_per_replica,
            );
        Ok(())
    }

    pub fn configure_chunk_replica_read_cache(
        &mut self,
        replica_index: usize,
        max_bytes_per_replica: u64,
    ) -> Result<(), ObjectError> {
        self.configure_chunk_replica_read_cache_with_stream_bypass(
            replica_index,
            max_bytes_per_replica,
            self.chunk_read_cache_max_cacheable_read_bytes_per_replica,
        )
    }

    pub fn configure_chunk_replica_read_cache_with_stream_bypass(
        &mut self,
        replica_index: usize,
        max_bytes_per_replica: u64,
        max_cacheable_read_bytes_per_replica: u64,
    ) -> Result<(), ObjectError> {
        self.chunk_service
            .set_read_cache_max_bytes_for_replica(replica_index, max_bytes_per_replica)?;
        self.chunk_service
            .set_read_cache_max_cacheable_read_bytes_for_replica(
                replica_index,
                max_cacheable_read_bytes_per_replica,
            )
    }
}
