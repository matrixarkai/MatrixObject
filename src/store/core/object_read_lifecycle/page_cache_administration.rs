impl MatrixObjectStore {
    pub fn set_read_cache_max_bytes_per_replica(&self, max_bytes: u64) {
        self.chunk_service
            .set_read_cache_max_bytes_per_replica(max_bytes);
    }

    pub fn configure_chunk_read_cache_control(
        &mut self,
        max_bytes_per_replica: u64,
    ) -> Result<ReplicaReadCacheControlReport, ObjectError> {
        self.chunk_read_cache_max_bytes_per_replica = max_bytes_per_replica;
        self.chunk_service
            .configure_chunk_read_cache_control(max_bytes_per_replica)
    }

    pub fn clear_chunk_read_cache_control(
        &self,
    ) -> Result<ReplicaReadCacheControlReport, ObjectError> {
        self.chunk_service.clear_chunk_read_cache_control()
    }

    pub fn prewarm_chunk_read_cache_control(
        &mut self,
        chunk_id: PageId,
        offset: u64,
        length: u64,
        max_bytes: u64,
    ) -> Result<ReplicaReadCacheControlReport, ObjectError> {
        self.chunk_service
            .prewarm_chunk_read_cache_control(chunk_id, offset, length, max_bytes)
    }

    pub fn prewarm_chunk_read_cache_control_batch(
        &mut self,
        ranges: Vec<ReplicaReadCacheRange>,
        max_bytes: u64,
    ) -> Result<ReplicaReadCacheControlReport, ObjectError> {
        self.chunk_service
            .prewarm_chunk_read_cache_control_batch(ranges, max_bytes)
    }

    pub fn invalidate_chunk_read_cache_control(
        &mut self,
        chunk_id: PageId,
        offset: u64,
        length: u64,
        max_bytes: u64,
    ) -> Result<ReplicaReadCacheControlReport, ObjectError> {
        self.chunk_service
            .invalidate_chunk_read_cache_control(chunk_id, offset, length, max_bytes)
    }

    pub fn invalidate_chunk_read_cache_control_batch(
        &mut self,
        ranges: Vec<ReplicaReadCacheRange>,
        max_bytes: u64,
    ) -> Result<ReplicaReadCacheControlReport, ObjectError> {
        self.chunk_service
            .invalidate_chunk_read_cache_control_batch(ranges, max_bytes)
    }

    pub fn read_cache_stats(&self) -> ReplicaReadCacheStats {
        self.chunk_service.read_cache_stats()
    }

    pub fn clear_read_cache(&self) {
        self.chunk_service.clear_read_cache();
    }

    pub fn page_storage_readiness(
        &self,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        now_ms: u64,
    ) -> ChunkStorageReadiness {
        self.chunk_service
            .storage_readiness(min_capacity_free_bytes, min_reclaimable_bytes, now_ms)
    }

    pub fn seal_page_slab(&mut self, slab_id: SlabId) -> Result<bool, ObjectError> {
        self.chunk_service.seal_slab(slab_id)
    }

    pub fn seal_page_slab_fenced(
        &mut self,
        slab_id: SlabId,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<bool, ObjectError> {
        self.chunk_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        self.chunk_service.seal_slab(slab_id)
    }

    pub fn list_page_sealed_slabs(
        &self,
        marker: Option<&SlabId>,
        limit: usize,
    ) -> Result<SealedSlabPage, ObjectError> {
        self.chunk_service.list_sealed_slabs(marker, limit)
    }
}
