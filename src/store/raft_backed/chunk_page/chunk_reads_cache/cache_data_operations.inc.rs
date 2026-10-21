// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn prewarm_chunk_ranges_with_read_proof_linearizable_fenced(
        &mut self,
        replica_index: usize,
        page_id: &PageId,
        ranges: Vec<DataRange>,
        max_bytes: u64,
        expected_control_checksum: Option<&str>,
    ) -> Result<ProvenStorageRead<CacheControlStats>, ObjectError> {
        self.prewarm_chunk_ranges_with_read_proof_linearizable_fenced_committed_at(
            replica_index,
            page_id,
            ranges,
            max_bytes,
            expected_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prewarm_chunk_ranges_with_control_proof_linearizable_fenced(
        &mut self,
        replica_index: usize,
        page_id: &PageId,
        ranges: Vec<DataRange>,
        max_bytes: u64,
        expected_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenChunkControlFencedStorageRead<CacheControlStats>, ObjectError> {
        self.prewarm_chunk_ranges_with_control_proof_linearizable_fenced_committed_at(
            replica_index,
            page_id,
            ranges,
            max_bytes,
            expected_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prewarm_chunk_ranges_with_control_proof_linearizable_fenced_committed_at(
        &mut self,
        replica_index: usize,
        page_id: &PageId,
        ranges: Vec<DataRange>,
        max_bytes: u64,
        expected_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenChunkControlFencedStorageRead<CacheControlStats>, ObjectError> {
        let expected_checksum = expected_control_checksum.as_ref().to_string();
        let control_proof = self
            .store
            .chunk_service
            .replicas
            .get(replica_index)
            .ok_or_else(|| {
                ObjectError::InvalidArgument("replica index is out of range".to_string())
            })?
            .control_proof();
        let proven = self.prewarm_chunk_ranges_with_read_proof_linearizable_fenced_committed_at(
            replica_index,
            page_id,
            ranges,
            max_bytes,
            Some(&expected_checksum),
            min_read_index,
            min_commit_index,
        )?;
        Ok(ProvenChunkControlFencedStorageRead {
            value: proven.value,
            proof: proven.proof,
            control_proof,
            expected_control_checksum: expected_checksum,
        })
    }

    pub fn prewarm_chunk_ranges_with_read_proof_linearizable_fenced_committed_at(
        &mut self,
        replica_index: usize,
        page_id: &PageId,
        ranges: Vec<DataRange>,
        max_bytes: u64,
        expected_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenStorageRead<CacheControlStats>, ObjectError> {
        let requested_read_index = self
            .raft
            .commit_index()
            .max(min_read_index)
            .max(min_commit_index);
        self.can_accept_chunk_batch_read_linearizable_fenced_committed_at(
            replica_index,
            page_id,
            ranges.clone(),
            max_bytes,
            expected_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        let value = {
            let replica = self
                .store
                .chunk_service
                .replicas
                .get_mut(replica_index)
                .ok_or_else(|| {
                    ObjectError::InvalidArgument("replica index is out of range".to_string())
                })?;
            replica.prewarm_chunk_ranges(page_id, ranges, max_bytes)?
        };
        let proof = self.chunk_storage_read_proof_committed(
            replica_index,
            requested_read_index,
            min_commit_index,
        )?;
        Ok(ProvenStorageRead { value, proof })
    }

    pub fn invalidate_chunk_cache_with_read_proof_linearizable_fenced(
        &mut self,
        replica_index: usize,
        page_id: &PageId,
        offset: u64,
        length: u64,
        max_bytes: u64,
        expected_control_checksum: Option<&str>,
    ) -> Result<ProvenStorageRead<CacheControlStats>, ObjectError> {
        self.invalidate_chunk_cache_with_read_proof_linearizable_fenced_committed_at(
            replica_index,
            page_id,
            offset,
            length,
            max_bytes,
            expected_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn invalidate_chunk_cache_with_control_proof_linearizable_fenced(
        &mut self,
        replica_index: usize,
        page_id: &PageId,
        offset: u64,
        length: u64,
        max_bytes: u64,
        expected_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenChunkControlFencedStorageRead<CacheControlStats>, ObjectError> {
        self.invalidate_chunk_cache_with_control_proof_linearizable_fenced_committed_at(
            replica_index,
            page_id,
            offset,
            length,
            max_bytes,
            expected_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn invalidate_chunk_cache_with_control_proof_linearizable_fenced_committed_at(
        &mut self,
        replica_index: usize,
        page_id: &PageId,
        offset: u64,
        length: u64,
        max_bytes: u64,
        expected_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenChunkControlFencedStorageRead<CacheControlStats>, ObjectError> {
        let expected_checksum = expected_control_checksum.as_ref().to_string();
        let control_proof = self
            .store
            .chunk_service
            .replicas
            .get(replica_index)
            .ok_or_else(|| {
                ObjectError::InvalidArgument("replica index is out of range".to_string())
            })?
            .control_proof();
        let proven = self.invalidate_chunk_cache_with_read_proof_linearizable_fenced_committed_at(
            replica_index,
            page_id,
            offset,
            length,
            max_bytes,
            Some(&expected_checksum),
            min_read_index,
            min_commit_index,
        )?;
        Ok(ProvenChunkControlFencedStorageRead {
            value: proven.value,
            proof: proven.proof,
            control_proof,
            expected_control_checksum: expected_checksum,
        })
    }

    pub fn invalidate_chunk_cache_with_read_proof_linearizable_fenced_committed_at(
        &mut self,
        replica_index: usize,
        page_id: &PageId,
        offset: u64,
        length: u64,
        max_bytes: u64,
        expected_control_checksum: Option<&str>,
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
                "chunk cache invalidation requires raft commit index at least {} but raft is at {}",
                min_commit_index,
                self.raft.commit_index()
            )));
        }
        if self.raft.commit_index() < min_read_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "chunk cache invalidation requires raft commit index at least {} but raft is at {}",
                min_read_index,
                self.raft.commit_index()
            )));
        }
        if self.store.last_applied_index() < requested_read_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "chunk cache invalidation requires applied commit index {} but store is at {}",
                requested_read_index,
                self.store.last_applied_index()
            )));
        }
        let value = {
            let replica = self
                .store
                .chunk_service
                .replicas
                .get_mut(replica_index)
                .ok_or_else(|| {
                    ObjectError::InvalidArgument("replica index is out of range".to_string())
                })?;
            replica.check_expected_control_checksum(expected_control_checksum)?;
            replica.invalidate_chunk_cache_bounded(page_id, offset, length, max_bytes)
        };
        let proof = self.chunk_storage_read_proof_committed(
            replica_index,
            requested_read_index,
            min_commit_index,
        )?;
        Ok(ProvenStorageRead { value, proof })
    }
}
