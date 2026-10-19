impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn prewarm_chunk_range_cache_with_read_proof_linearizable_fenced(
        &mut self,
        chunk_id: &ChunkId,
        offset: u64,
        length: u64,
        max_bytes: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ProvenStorageRead<CacheControlStats>, ObjectError> {
        self.prewarm_chunk_range_cache_with_read_proof_linearizable_fenced_committed_at(
            chunk_id,
            offset,
            length,
            max_bytes,
            expected_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prewarm_chunk_range_cache_with_route_proof_linearizable_fenced(
        &mut self,
        chunk_id: &ChunkId,
        offset: u64,
        length: u64,
        max_bytes: u64,
        expected_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenPageRouteFencedStorageRead<CacheControlStats>, ObjectError> {
        self.prewarm_chunk_range_cache_with_route_proof_linearizable_fenced_committed_at(
            chunk_id,
            offset,
            length,
            max_bytes,
            expected_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prewarm_chunk_range_cache_with_route_proof_linearizable_fenced_committed_at(
        &mut self,
        chunk_id: &ChunkId,
        offset: u64,
        length: u64,
        max_bytes: u64,
        expected_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenPageRouteFencedStorageRead<CacheControlStats>, ObjectError> {
        let expected_checksum = expected_route_control_checksum.as_ref().to_string();
        let route_control_proof = self.store.chunk_service.route_control_proof();
        let proven = self
            .prewarm_chunk_range_cache_with_read_proof_linearizable_fenced_committed_at(
                chunk_id,
                offset,
                length,
                max_bytes,
                Some(&expected_checksum),
                min_read_index,
                min_commit_index,
            )?;
        Ok(ProvenPageRouteFencedStorageRead {
            value: proven.value,
            proof: proven.proof,
            route_control_proof,
            expected_route_control_checksum: expected_checksum,
        })
    }

    pub fn prewarm_chunk_range_cache_with_read_proof_linearizable_fenced_committed_at(
        &mut self,
        chunk_id: &ChunkId,
        offset: u64,
        length: u64,
        max_bytes: u64,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenStorageRead<CacheControlStats>, ObjectError> {
        let requested_read_index = self
            .raft
            .commit_index()
            .max(min_read_index)
            .max(min_commit_index);
        let request = ChunkRangeRequest {
            chunk_id: chunk_id.clone(),
            offset,
            length,
        };
        self.can_accept_page_batch_read_bounded_linearizable_fenced_committed_at(
            vec![request],
            max_bytes,
            0,
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        let value = self
            .store
            .chunk_service
            .prewarm_chunk_range_cache(chunk_id, offset, length, max_bytes)?;
        let proof = self.page_storage_read_proof_committed(requested_read_index, min_commit_index);
        Ok(ProvenStorageRead { value, proof })
    }

}
