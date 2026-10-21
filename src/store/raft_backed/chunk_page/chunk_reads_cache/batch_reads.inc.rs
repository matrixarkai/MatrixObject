// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn read_chunk_ranges_batch_with_read_proof_linearizable_fenced_committed_at(
        &self,
        replica_index: usize,
        page_id: &PageId,
        ranges: Vec<DataRange>,
        max_payload_bytes: u64,
        expected_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenStorageRead<ReplicaReadBatchRead>, ObjectError> {
        let proven = self.read_chunk_ranges_with_read_proof_linearizable_fenced_committed_at(
            replica_index,
            page_id,
            ranges,
            max_payload_bytes,
            expected_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        let (ranges, stats) = proven.value;
        let proof_checksum =
            chunk_read_batch_read_proof_checksum(page_id, &ranges, &stats, &proven.proof);
        Ok(ProvenStorageRead {
            value: ReplicaReadBatchRead {
                ranges,
                stats,
                min_read_index,
                min_commit_index,
                proof_format: CHUNK_READ_BATCH_PROOF_FORMAT.to_string(),
                proof_checksum,
            },
            proof: proven.proof,
        })
    }

    pub fn read_chunk_ranges_batch_with_read_proof_linearizable_fenced(
        &self,
        replica_index: usize,
        page_id: &PageId,
        ranges: Vec<DataRange>,
        max_payload_bytes: u64,
        expected_control_checksum: Option<&str>,
    ) -> Result<ProvenStorageRead<ReplicaReadBatchRead>, ObjectError> {
        self.read_chunk_ranges_batch_with_read_proof_linearizable_fenced_committed_at(
            replica_index,
            page_id,
            ranges,
            max_payload_bytes,
            expected_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn read_chunk_ranges_batch_with_read_proof_linearizable_fenced_at(
        &self,
        replica_index: usize,
        page_id: &PageId,
        ranges: Vec<DataRange>,
        max_payload_bytes: u64,
        expected_control_checksum: Option<&str>,
        min_read_index: u64,
    ) -> Result<ProvenStorageRead<ReplicaReadBatchRead>, ObjectError> {
        self.read_chunk_ranges_batch_with_read_proof_linearizable_fenced_committed_at(
            replica_index,
            page_id,
            ranges,
            max_payload_bytes,
            expected_control_checksum,
            min_read_index,
            0,
        )
    }

    pub fn read_chunk_ranges_batch_with_control_proof_linearizable_fenced(
        &self,
        replica_index: usize,
        page_id: &PageId,
        ranges: Vec<DataRange>,
        max_payload_bytes: u64,
        expected_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenChunkControlFencedStorageRead<ReplicaReadBatchRead>, ObjectError> {
        self.read_chunk_ranges_batch_with_control_proof_linearizable_fenced_committed_at(
            replica_index,
            page_id,
            ranges,
            max_payload_bytes,
            expected_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn read_chunk_ranges_batch_with_control_proof_linearizable_fenced_at(
        &self,
        replica_index: usize,
        page_id: &PageId,
        ranges: Vec<DataRange>,
        max_payload_bytes: u64,
        expected_control_checksum: impl AsRef<str>,
        min_read_index: u64,
    ) -> Result<ProvenChunkControlFencedStorageRead<ReplicaReadBatchRead>, ObjectError> {
        self.read_chunk_ranges_batch_with_control_proof_linearizable_fenced_committed_at(
            replica_index,
            page_id,
            ranges,
            max_payload_bytes,
            expected_control_checksum,
            min_read_index,
            0,
        )
    }

    pub fn read_chunk_ranges_batch_with_control_proof_linearizable_fenced_committed_at(
        &self,
        replica_index: usize,
        page_id: &PageId,
        ranges: Vec<DataRange>,
        max_payload_bytes: u64,
        expected_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenChunkControlFencedStorageRead<ReplicaReadBatchRead>, ObjectError> {
        let expected_checksum = expected_control_checksum.as_ref().to_string();
        let replica = self
            .store
            .chunk_service
            .replicas
            .get(replica_index)
            .ok_or_else(|| {
                ObjectError::InvalidArgument("replica index is out of range".to_string())
            })?;
        let control_proof = replica.control_proof();
        let proven = self
            .read_chunk_ranges_batch_with_read_proof_linearizable_fenced_committed_at(
                replica_index,
                page_id,
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

    pub fn read_chunk_ranges_batch_strict_linearizable_fenced(
        &self,
        replica_index: usize,
        page_id: &PageId,
        ranges: Vec<DataRange>,
        max_payload_bytes: u64,
        expected_control_checksum: Option<&str>,
        expected_chunk_read_batch_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenStorageRead<ReplicaReadBatchRead>, ObjectError> {
        self.read_chunk_ranges_batch_strict_linearizable_fenced_committed_at(
            replica_index,
            page_id,
            ranges,
            max_payload_bytes,
            expected_control_checksum,
            expected_chunk_read_batch_proof_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn read_chunk_ranges_batch_strict_linearizable_fenced_committed_at(
        &self,
        replica_index: usize,
        page_id: &PageId,
        ranges: Vec<DataRange>,
        max_payload_bytes: u64,
        expected_control_checksum: Option<&str>,
        expected_chunk_read_batch_proof_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenStorageRead<ReplicaReadBatchRead>, ObjectError> {
        let proven = self
            .read_chunk_ranges_batch_with_read_proof_linearizable_fenced_committed_at(
                replica_index,
                page_id,
                ranges,
                max_payload_bytes,
                expected_control_checksum,
                min_read_index,
                min_commit_index,
            )?;
        proven.require_chunk_read_batch_proof_checksum(
            expected_chunk_read_batch_proof_checksum.as_ref(),
        )
    }
}
