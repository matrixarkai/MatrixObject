impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn read_chunk_with_read_proof_linearizable_fenced(
        &self,
        replica_index: usize,
        chunk_id: &PageId,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        expected_control_checksum: Option<&str>,
    ) -> Result<ProvenStorageRead<ChunkReadResult>, ObjectError> {
        self.read_chunk_with_read_proof_linearizable_fenced_committed_at(
            replica_index,
            chunk_id,
            offset,
            length,
            max_payload_bytes,
            expected_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn read_chunk_with_control_proof_linearizable_fenced(
        &self,
        replica_index: usize,
        chunk_id: &PageId,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        expected_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenChunkControlFencedStorageRead<ChunkReadResult>, ObjectError> {
        self.read_chunk_with_control_proof_linearizable_fenced_committed_at(
            replica_index,
            chunk_id,
            offset,
            length,
            max_payload_bytes,
            expected_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn read_chunk_with_control_proof_linearizable_fenced_committed_at(
        &self,
        replica_index: usize,
        chunk_id: &PageId,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        expected_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenChunkControlFencedStorageRead<ChunkReadResult>, ObjectError> {
        let expected_checksum = expected_control_checksum.as_ref().to_string();
        let replica = self
            .store
            .page_service
            .replicas
            .get(replica_index)
            .ok_or_else(|| {
                ObjectError::InvalidArgument("replica index is out of range".to_string())
            })?;
        let control_proof = replica.control_proof();
        let proven = self.read_chunk_with_read_proof_linearizable_fenced_committed_at(
            replica_index,
            chunk_id,
            offset,
            length,
            max_payload_bytes,
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

    pub fn read_chunk_with_read_proof_linearizable_fenced_committed_at(
        &self,
        replica_index: usize,
        chunk_id: &PageId,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        expected_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenStorageRead<ChunkReadResult>, ObjectError> {
        let requested_read_index = self
            .raft
            .commit_index()
            .max(min_read_index)
            .max(min_commit_index);
        self.can_accept_chunk_batch_read_linearizable_fenced_committed_at(
            replica_index,
            chunk_id,
            vec![DataRange { offset, length }],
            max_payload_bytes,
            expected_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        let replica = self
            .store
            .page_service
            .replicas
            .get(replica_index)
            .ok_or_else(|| {
                ObjectError::InvalidArgument("replica index is out of range".to_string())
            })?;
        let value = replica.read_chunk_bounded(chunk_id, offset, length, max_payload_bytes)?;
        let proof = self.chunk_storage_read_proof_committed(
            replica_index,
            requested_read_index,
            min_commit_index,
        )?;
        Ok(ProvenStorageRead { value, proof })
    }

    pub fn read_chunk_ranges_with_read_proof_linearizable_fenced(
        &self,
        replica_index: usize,
        chunk_id: &PageId,
        ranges: Vec<DataRange>,
        max_payload_bytes: u64,
        expected_control_checksum: Option<&str>,
    ) -> Result<ProvenStorageRead<(Vec<ChunkRangeReadResult>, ChunkReadBatchStats)>, ObjectError>
    {
        self.read_chunk_ranges_with_read_proof_linearizable_fenced_committed_at(
            replica_index,
            chunk_id,
            ranges,
            max_payload_bytes,
            expected_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn read_chunk_ranges_with_control_proof_linearizable_fenced(
        &self,
        replica_index: usize,
        chunk_id: &PageId,
        ranges: Vec<DataRange>,
        max_payload_bytes: u64,
        expected_control_checksum: impl AsRef<str>,
    ) -> Result<
        ProvenChunkControlFencedStorageRead<(Vec<ChunkRangeReadResult>, ChunkReadBatchStats)>,
        ObjectError,
    > {
        self.read_chunk_ranges_with_control_proof_linearizable_fenced_committed_at(
            replica_index,
            chunk_id,
            ranges,
            max_payload_bytes,
            expected_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn read_chunk_ranges_with_control_proof_linearizable_fenced_committed_at(
        &self,
        replica_index: usize,
        chunk_id: &PageId,
        ranges: Vec<DataRange>,
        max_payload_bytes: u64,
        expected_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<
        ProvenChunkControlFencedStorageRead<(Vec<ChunkRangeReadResult>, ChunkReadBatchStats)>,
        ObjectError,
    > {
        let expected_checksum = expected_control_checksum.as_ref().to_string();
        let replica = self
            .store
            .page_service
            .replicas
            .get(replica_index)
            .ok_or_else(|| {
                ObjectError::InvalidArgument("replica index is out of range".to_string())
            })?;
        let control_proof = replica.control_proof();
        let proven = self.read_chunk_ranges_with_read_proof_linearizable_fenced_committed_at(
            replica_index,
            chunk_id,
            ranges,
            max_payload_bytes,
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

    pub fn read_chunk_ranges_with_read_proof_linearizable_fenced_committed_at(
        &self,
        replica_index: usize,
        chunk_id: &PageId,
        ranges: Vec<DataRange>,
        max_payload_bytes: u64,
        expected_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenStorageRead<(Vec<ChunkRangeReadResult>, ChunkReadBatchStats)>, ObjectError>
    {
        let requested_read_index = self
            .raft
            .commit_index()
            .max(min_read_index)
            .max(min_commit_index);
        self.can_accept_chunk_batch_read_linearizable_fenced_committed_at(
            replica_index,
            chunk_id,
            ranges.clone(),
            max_payload_bytes,
            expected_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        let replica = self
            .store
            .page_service
            .replicas
            .get(replica_index)
            .ok_or_else(|| {
                ObjectError::InvalidArgument("replica index is out of range".to_string())
            })?;
        let value = replica.read_chunk_ranges(chunk_id, ranges, max_payload_bytes)?;
        let proof = self.chunk_storage_read_proof_committed(
            replica_index,
            requested_read_index,
            min_commit_index,
        )?;
        Ok(ProvenStorageRead { value, proof })
    }
}
