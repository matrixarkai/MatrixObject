impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn configure_chunk_read_cache_control_with_read_proof_linearizable_fenced(
        &mut self,
        max_bytes_per_replica: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ProvenStorageRead<ChunkReadCacheControlReport>, ObjectError> {
        self.configure_chunk_read_cache_control_with_read_proof_linearizable_fenced_committed_at(
            max_bytes_per_replica,
            expected_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn configure_chunk_read_cache_control_with_read_proof_linearizable_fenced_committed_at(
        &mut self,
        max_bytes_per_replica: u64,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenStorageRead<ChunkReadCacheControlReport>, ObjectError> {
        let requested_read_index = self
            .raft
            .commit_index()
            .max(min_read_index)
            .max(min_commit_index);
        self.ensure_storage_control_read_index_committed_at(
            "objectstore chunk read-cache configure",
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        let value = self
            .store
            .configure_chunk_read_cache_control(max_bytes_per_replica)?;
        let proof = self.page_storage_read_proof_committed(requested_read_index, min_commit_index);
        Ok(ProvenStorageRead { value, proof })
    }

    pub fn clear_chunk_read_cache_control_with_read_proof_linearizable_fenced(
        &mut self,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ProvenStorageRead<ChunkReadCacheControlReport>, ObjectError> {
        self.clear_chunk_read_cache_control_with_read_proof_linearizable_fenced_committed_at(
            expected_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn clear_chunk_read_cache_control_with_read_proof_linearizable_fenced_committed_at(
        &mut self,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenStorageRead<ChunkReadCacheControlReport>, ObjectError> {
        let requested_read_index = self
            .raft
            .commit_index()
            .max(min_read_index)
            .max(min_commit_index);
        self.ensure_storage_control_read_index_committed_at(
            "objectstore chunk read-cache clear",
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        let value = self.store.clear_chunk_read_cache_control()?;
        let proof = self.page_storage_read_proof_committed(requested_read_index, min_commit_index);
        Ok(ProvenStorageRead { value, proof })
    }

    pub fn prewarm_chunk_read_cache_control_with_read_proof_linearizable_fenced(
        &mut self,
        chunk_id: PageId,
        offset: u64,
        length: u64,
        max_bytes: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ProvenStorageRead<ChunkReadCacheControlReport>, ObjectError> {
        self.prewarm_chunk_read_cache_control_with_read_proof_linearizable_fenced_committed_at(
            chunk_id,
            offset,
            length,
            max_bytes,
            expected_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prewarm_chunk_read_cache_control_with_read_proof_linearizable_fenced_committed_at(
        &mut self,
        chunk_id: PageId,
        offset: u64,
        length: u64,
        max_bytes: u64,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenStorageRead<ChunkReadCacheControlReport>, ObjectError> {
        self.prewarm_chunk_read_cache_control_batch_with_read_proof_linearizable_fenced_committed_at(
            vec![ChunkReadCacheRange {
                chunk_id,
                offset,
                length,
            }],
            max_bytes,
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
        )
    }

    pub fn prewarm_chunk_read_cache_control_batch_with_read_proof_linearizable_fenced(
        &mut self,
        ranges: Vec<ChunkReadCacheRange>,
        max_bytes: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ProvenStorageRead<ChunkReadCacheControlReport>, ObjectError> {
        self.prewarm_chunk_read_cache_control_batch_with_read_proof_linearizable_fenced_committed_at(
            ranges,
            max_bytes,
            expected_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prewarm_chunk_read_cache_control_batch_with_read_proof_linearizable_fenced_committed_at(
        &mut self,
        ranges: Vec<ChunkReadCacheRange>,
        max_bytes: u64,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenStorageRead<ChunkReadCacheControlReport>, ObjectError> {
        let requested_read_index = self
            .raft
            .commit_index()
            .max(min_read_index)
            .max(min_commit_index);
        self.ensure_storage_control_read_index_committed_at(
            "objectstore chunk read-cache prewarm",
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        let value = self
            .store
            .prewarm_chunk_read_cache_control_batch(ranges, max_bytes)?;
        let proof = self.page_storage_read_proof_committed(requested_read_index, min_commit_index);
        Ok(ProvenStorageRead { value, proof })
    }

    pub fn invalidate_chunk_read_cache_control_with_read_proof_linearizable_fenced(
        &mut self,
        chunk_id: PageId,
        offset: u64,
        length: u64,
        max_bytes: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ProvenStorageRead<ChunkReadCacheControlReport>, ObjectError> {
        self.invalidate_chunk_read_cache_control_with_read_proof_linearizable_fenced_committed_at(
            chunk_id,
            offset,
            length,
            max_bytes,
            expected_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn invalidate_chunk_read_cache_control_with_read_proof_linearizable_fenced_committed_at(
        &mut self,
        chunk_id: PageId,
        offset: u64,
        length: u64,
        max_bytes: u64,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenStorageRead<ChunkReadCacheControlReport>, ObjectError> {
        self.invalidate_chunk_read_cache_control_batch_with_read_proof_linearizable_fenced_committed_at(
            vec![ChunkReadCacheRange {
                chunk_id,
                offset,
                length,
            }],
            max_bytes,
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
        )
    }

    pub fn invalidate_chunk_read_cache_control_batch_with_read_proof_linearizable_fenced(
        &mut self,
        ranges: Vec<ChunkReadCacheRange>,
        max_bytes: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ProvenStorageRead<ChunkReadCacheControlReport>, ObjectError> {
        self.invalidate_chunk_read_cache_control_batch_with_read_proof_linearizable_fenced_committed_at(
            ranges,
            max_bytes,
            expected_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn invalidate_chunk_read_cache_control_batch_with_read_proof_linearizable_fenced_committed_at(
        &mut self,
        ranges: Vec<ChunkReadCacheRange>,
        max_bytes: u64,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenStorageRead<ChunkReadCacheControlReport>, ObjectError> {
        let requested_read_index = self
            .raft
            .commit_index()
            .max(min_read_index)
            .max(min_commit_index);
        self.ensure_storage_control_read_index_committed_at(
            "objectstore chunk read-cache invalidate",
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        let value = self
            .store
            .invalidate_chunk_read_cache_control_batch(ranges, max_bytes)?;
        let proof = self.page_storage_read_proof_committed(requested_read_index, min_commit_index);
        Ok(ProvenStorageRead { value, proof })
    }
}
