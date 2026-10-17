impl MatrixObjectStore {
    pub fn set_read_cache_max_bytes_per_replica(&self, max_bytes: u64) {
        self.page_service
            .set_read_cache_max_bytes_per_replica(max_bytes);
    }

    pub fn configure_chunk_read_cache_control(
        &mut self,
        max_bytes_per_replica: u64,
    ) -> Result<ChunkReadCacheControlReport, ObjectError> {
        self.chunk_read_cache_max_bytes_per_replica = max_bytes_per_replica;
        self.page_service
            .configure_chunk_read_cache_control(max_bytes_per_replica)
    }

    pub fn clear_chunk_read_cache_control(
        &self,
    ) -> Result<ChunkReadCacheControlReport, ObjectError> {
        self.page_service.clear_chunk_read_cache_control()
    }

    pub fn prewarm_chunk_read_cache_control(
        &mut self,
        chunk_id: ChunkId,
        offset: u64,
        length: u64,
        max_bytes: u64,
    ) -> Result<ChunkReadCacheControlReport, ObjectError> {
        self.page_service
            .prewarm_chunk_read_cache_control(chunk_id, offset, length, max_bytes)
    }

    pub fn prewarm_chunk_read_cache_control_batch(
        &mut self,
        ranges: Vec<ChunkReadCacheRange>,
        max_bytes: u64,
    ) -> Result<ChunkReadCacheControlReport, ObjectError> {
        self.page_service
            .prewarm_chunk_read_cache_control_batch(ranges, max_bytes)
    }

    pub fn invalidate_chunk_read_cache_control(
        &mut self,
        chunk_id: ChunkId,
        offset: u64,
        length: u64,
        max_bytes: u64,
    ) -> Result<ChunkReadCacheControlReport, ObjectError> {
        self.page_service
            .invalidate_chunk_read_cache_control(chunk_id, offset, length, max_bytes)
    }

    pub fn invalidate_chunk_read_cache_control_batch(
        &mut self,
        ranges: Vec<ChunkReadCacheRange>,
        max_bytes: u64,
    ) -> Result<ChunkReadCacheControlReport, ObjectError> {
        self.page_service
            .invalidate_chunk_read_cache_control_batch(ranges, max_bytes)
    }

    pub fn read_cache_stats(&self) -> ChunkReadCacheStats {
        self.page_service.read_cache_stats()
    }

    pub fn clear_read_cache(&self) {
        self.page_service.clear_read_cache();
    }

    pub fn page_storage_readiness(
        &self,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        now_ms: u64,
    ) -> PageStorageReadiness {
        self.page_service
            .storage_readiness(min_capacity_free_bytes, min_reclaimable_bytes, now_ms)
    }

    pub fn seal_page_slab(&mut self, slab_id: SlabId) -> Result<bool, ObjectError> {
        self.page_service.seal_slab(slab_id)
    }

    pub fn seal_page_slab_fenced(
        &mut self,
        slab_id: SlabId,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<bool, ObjectError> {
        self.page_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        self.page_service.seal_slab(slab_id)
    }

    pub fn list_page_sealed_slabs(
        &self,
        marker: Option<&SlabId>,
        limit: usize,
    ) -> Result<SealedSlabPage, ObjectError> {
        self.page_service.list_sealed_slabs(marker, limit)
    }
}
