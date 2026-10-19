// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn invalidate_chunk_range_cache_with_read_proof_linearizable_fenced(
        &mut self,
        chunk_id: &ChunkId,
        offset: u64,
        length: u64,
        max_bytes: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ProvenStorageRead<CacheControlStats>, ObjectError> {
        self.invalidate_chunk_range_cache_with_read_proof_linearizable_fenced_committed_at(
            chunk_id,
            offset,
            length,
            max_bytes,
            expected_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn invalidate_chunk_range_cache_with_route_proof_linearizable_fenced(
        &mut self,
        chunk_id: &ChunkId,
        offset: u64,
        length: u64,
        max_bytes: u64,
        expected_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenPageRouteFencedStorageRead<CacheControlStats>, ObjectError> {
        self.invalidate_chunk_range_cache_with_route_proof_linearizable_fenced_committed_at(
            chunk_id,
            offset,
            length,
            max_bytes,
            expected_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn invalidate_chunk_range_cache_with_route_proof_linearizable_fenced_committed_at(
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
            .invalidate_chunk_range_cache_with_read_proof_linearizable_fenced_committed_at(
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

    pub fn invalidate_chunk_range_cache_with_read_proof_linearizable_fenced_committed_at(
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
        if self.raft.commit_index() < min_commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "page cache invalidation requires raft commit index at least {} but raft is at {}",
                min_commit_index,
                self.raft.commit_index()
            )));
        }
        if self.raft.commit_index() < min_read_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "page cache invalidation requires raft commit index at least {} but raft is at {}",
                min_read_index,
                self.raft.commit_index()
            )));
        }
        if self.store.last_applied_index() < requested_read_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "page cache invalidation requires applied commit index {} but store is at {}",
                requested_read_index,
                self.store.last_applied_index()
            )));
        }
        self.store
            .chunk_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        let value = self
            .store
            .chunk_service
            .invalidate_chunk_range_cache_bounded(chunk_id, offset, length, max_bytes)?;
        let proof = self.page_storage_read_proof_committed(requested_read_index, min_commit_index);
        Ok(ProvenStorageRead { value, proof })
    }

}
