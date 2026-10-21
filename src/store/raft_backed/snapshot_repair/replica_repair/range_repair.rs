// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn prepare_repair_page_range_raft_entry(
        &self,
        chunk_id: ChunkId,
        offset: u64,
        length: u64,
        max_repair_chunks: usize,
        start_chunk_index: u64,
    ) -> Result<PreparedPageRangeRepair, ObjectError> {
        self.store.prepare_page_range_repair_raft_entry(
            chunk_id,
            offset,
            length,
            max_repair_chunks,
            start_chunk_index,
            self.store.last_applied_index(),
        )
    }

    pub fn prepare_repair_page_range_raft_entry_linearizable_fenced(
        &self,
        chunk_id: ChunkId,
        offset: u64,
        length: u64,
        max_repair_chunks: usize,
        start_chunk_index: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<PreparedPageRangeRepair, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready(
            "block range repair prepare",
            expected_page_route_control_checksum,
        )?;
        let mut prepared = self.prepare_repair_page_range_raft_entry(
            chunk_id,
            offset,
            length,
            max_repair_chunks,
            start_chunk_index,
        )?;
        prepared.report.expected_route_control_checksum =
            expected_page_route_control_checksum.to_string();
        prepared.expected_route_control_checksum =
            expected_page_route_control_checksum.to_string();
        prepared.raft_entry_checksum = prepared_page_range_repair_checksum(&prepared);
        Ok(prepared)
    }

    pub fn prepare_repair_page_range_raft_entry_with_read_proof_linearizable_fenced(
        &self,
        chunk_id: ChunkId,
        offset: u64,
        length: u64,
        max_repair_chunks: usize,
        start_chunk_index: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenStorageRead<PreparedPageRangeRepair>, ObjectError> {
        self.prepare_repair_page_range_raft_entry_with_read_proof_linearizable_fenced_committed_at(
            chunk_id,
            offset,
            length,
            max_repair_chunks,
            start_chunk_index,
            expected_page_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prepare_repair_page_range_raft_entry_with_read_proof_linearizable_fenced_committed_at(
        &self,
        chunk_id: ChunkId,
        offset: u64,
        length: u64,
        max_repair_chunks: usize,
        start_chunk_index: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenStorageRead<PreparedPageRangeRepair>, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        let requested_read_index = self
            .raft
            .commit_index()
            .max(min_read_index)
            .max(min_commit_index);
        self.ensure_storage_control_read_index_committed_at(
            "prepare block range repair",
            Some(expected_page_route_control_checksum),
            min_read_index,
            min_commit_index,
        )?;
        let prepared = self.prepare_repair_page_range_raft_entry_linearizable_fenced(
            chunk_id,
            offset,
            length,
            max_repair_chunks,
            start_chunk_index,
            expected_page_route_control_checksum,
        )?;
        let proof = self.page_storage_read_proof_committed(requested_read_index, min_commit_index);
        Ok(ProvenStorageRead {
            value: prepared,
            proof,
        })
    }

    pub fn prepare_repair_page_range_envelope_linearizable_fenced(
        &self,
        chunk_id: ChunkId,
        offset: u64,
        length: u64,
        max_repair_chunks: usize,
        start_chunk_index: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<PreparedPageRepairEnvelope, ObjectError> {
        let prepared = self.prepare_repair_page_range_raft_entry_linearizable_fenced(
            chunk_id,
            offset,
            length,
            max_repair_chunks,
            start_chunk_index,
            expected_page_route_control_checksum,
        )?;
        Ok(prepared_page_range_repair_envelope(&prepared))
    }

    pub fn prepare_repair_page_range_envelope_with_read_proof_linearizable_fenced(
        &self,
        chunk_id: ChunkId,
        offset: u64,
        length: u64,
        max_repair_chunks: usize,
        start_chunk_index: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenPreparedPageRepairEnvelope, ObjectError> {
        self.prepare_repair_page_range_envelope_with_read_proof_linearizable_fenced_committed_at(
            chunk_id,
            offset,
            length,
            max_repair_chunks,
            start_chunk_index,
            expected_page_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prepare_repair_page_range_envelope_with_read_proof_linearizable_fenced_committed_at(
        &self,
        chunk_id: ChunkId,
        offset: u64,
        length: u64,
        max_repair_chunks: usize,
        start_chunk_index: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenPreparedPageRepairEnvelope, ObjectError> {
        let prepared = self
            .prepare_repair_page_range_raft_entry_with_read_proof_linearizable_fenced_committed_at(
                chunk_id,
                offset,
                length,
                max_repair_chunks,
                start_chunk_index,
                expected_page_route_control_checksum,
                min_read_index,
                min_commit_index,
            )?;
        Ok(ProvenPreparedPageRepairEnvelope {
            envelope: prepared_page_range_repair_envelope(&prepared.value),
            proof: prepared.proof,
        })
    }
}
