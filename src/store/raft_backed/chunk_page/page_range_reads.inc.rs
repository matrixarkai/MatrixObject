// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn read_chunk_range_with_read_proof_linearizable_fenced(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ProvenStorageRead<(Vec<u8>, ChunkReadStats)>, ObjectError> {
        self.read_chunk_range_with_read_proof_linearizable_fenced_committed_at(
            chunk_id,
            offset,
            length,
            max_payload_bytes,
            max_chunk_read_count,
            expected_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn read_chunk_range_with_route_proof_linearizable_fenced(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        expected_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenPageRouteFencedStorageRead<(Vec<u8>, ChunkReadStats)>, ObjectError> {
        self.read_chunk_range_with_route_proof_linearizable_fenced_committed_at(
            chunk_id,
            offset,
            length,
            max_payload_bytes,
            max_chunk_read_count,
            expected_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn read_chunk_range_with_route_proof_linearizable_fenced_committed_at(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        expected_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenPageRouteFencedStorageRead<(Vec<u8>, ChunkReadStats)>, ObjectError> {
        let expected_checksum = expected_route_control_checksum.as_ref().to_string();
        let route_control_proof = self.store.chunk_service.route_control_proof();
        let proven = self.read_chunk_range_with_read_proof_linearizable_fenced_committed_at(
            chunk_id,
            offset,
            length,
            max_payload_bytes,
            max_chunk_read_count,
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

    pub fn read_chunk_range_with_read_proof_linearizable_fenced_committed_at(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenStorageRead<(Vec<u8>, ChunkReadStats)>, ObjectError> {
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
            max_payload_bytes,
            max_chunk_read_count,
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        let value = self
            .store
            .chunk_service
            .read_chunk_range_bounded_by_chunks(
                chunk_id,
                offset,
                length,
                max_payload_bytes,
                max_chunk_read_count,
            )?;
        let proof = self.page_storage_read_proof_committed(requested_read_index, min_commit_index);
        Ok(ProvenStorageRead { value, proof })
    }

    pub fn read_slab_ranges_with_read_proof_linearizable_fenced(
        &mut self,
        requests: Vec<ChunkRangeRequest>,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ProvenStorageRead<(Vec<ChunkRangeReadResult>, ChunkBatchStats)>, ObjectError> {
        self.read_slab_ranges_with_read_proof_linearizable_fenced_committed_at(
            requests,
            max_payload_bytes,
            max_chunk_read_count,
            expected_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn read_chunk_ranges_with_route_proof_linearizable_fenced(
        &mut self,
        requests: Vec<ChunkRangeRequest>,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        expected_route_control_checksum: impl AsRef<str>,
    ) -> Result<
        ProvenPageRouteFencedStorageRead<(Vec<ChunkRangeReadResult>, ChunkBatchStats)>,
        ObjectError,
    > {
        self.read_chunk_ranges_with_route_proof_linearizable_fenced_committed_at(
            requests,
            max_payload_bytes,
            max_chunk_read_count,
            expected_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn read_chunk_ranges_with_route_proof_linearizable_fenced_committed_at(
        &mut self,
        requests: Vec<ChunkRangeRequest>,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        expected_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<
        ProvenPageRouteFencedStorageRead<(Vec<ChunkRangeReadResult>, ChunkBatchStats)>,
        ObjectError,
    > {
        let expected_checksum = expected_route_control_checksum.as_ref().to_string();
        let route_control_proof = self.store.chunk_service.route_control_proof();
        let proven = self.read_slab_ranges_with_read_proof_linearizable_fenced_committed_at(
            requests,
            max_payload_bytes,
            max_chunk_read_count,
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

    pub fn read_slab_ranges_with_read_proof_linearizable_fenced_committed_at(
        &mut self,
        requests: Vec<ChunkRangeRequest>,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenStorageRead<(Vec<ChunkRangeReadResult>, ChunkBatchStats)>, ObjectError> {
        let requested_read_index = self
            .raft
            .commit_index()
            .max(min_read_index)
            .max(min_commit_index);
        self.can_accept_page_batch_read_bounded_linearizable_fenced_committed_at_borrowed(
            &requests,
            max_payload_bytes,
            max_chunk_read_count,
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        let value = self
            .store
            .chunk_service
            .read_chunk_ranges_bounded_by_chunks(
                requests,
                max_payload_bytes,
                max_chunk_read_count,
            )?;
        let proof = self.page_storage_read_proof_committed(requested_read_index, min_commit_index);
        Ok(ProvenStorageRead { value, proof })
    }

}
