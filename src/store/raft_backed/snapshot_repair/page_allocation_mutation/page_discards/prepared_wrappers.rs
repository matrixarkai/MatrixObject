// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_page_batch_discard_prepared(
        &mut self,
        requests: Vec<ChunkRangeRequest>,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_page_batch_discard_raft_entry(requests, idempotency_key)?;
        self.propose_prepared_page_batch_discard(prepared)
    }

    pub fn propose_page_batch_discard_prepared_bounded(
        &mut self,
        requests: Vec<ChunkRangeRequest>,
        max_discard_bytes: u64,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_page_batch_discard_raft_entry_bounded(
            requests,
            max_discard_bytes,
            idempotency_key,
        )?;
        self.propose_prepared_page_batch_discard(prepared)
    }

    pub fn propose_page_batch_discard_prepared_bounded_from(
        &mut self,
        requests: Vec<ChunkRangeRequest>,
        max_discard_bytes: u64,
        discard_start_range_index: u64,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_page_batch_discard_raft_entry_bounded_from(
            requests,
            max_discard_bytes,
            discard_start_range_index,
            idempotency_key,
        )?;
        self.propose_prepared_page_batch_discard(prepared)
    }

    pub fn propose_page_batch_discard_prepared_bounded_fenced(
        &mut self,
        requests: Vec<ChunkRangeRequest>,
        max_discard_bytes: u64,
        idempotency_key: &str,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_storage_control_read_index(
            "prepare block batch discard",
            expected_route_control_checksum,
        )?;
        let prepared = self
            .store
            .prepare_page_batch_discard_raft_entry_bounded_fenced(
                requests,
                max_discard_bytes,
                idempotency_key,
                self.store.last_applied_index(),
                expected_route_control_checksum,
            )?;
        let expected_raft_entry_checksum = prepared.raft_entry_checksum.clone();
        self.propose_prepared_page_batch_discard_linearizable_fenced(
            prepared,
            expected_route_control_checksum,
            &expected_raft_entry_checksum,
        )
    }

    pub fn propose_page_batch_discard_prepared_bounded_from_fenced(
        &mut self,
        requests: Vec<ChunkRangeRequest>,
        max_discard_bytes: u64,
        discard_start_range_index: u64,
        idempotency_key: &str,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_storage_control_read_index(
            "prepare block batch discard",
            expected_route_control_checksum,
        )?;
        let prepared = self
            .store
            .prepare_page_batch_discard_raft_entry_bounded_from_fenced(
                requests,
                max_discard_bytes,
                discard_start_range_index,
                idempotency_key,
                self.store.last_applied_index(),
                expected_route_control_checksum,
            )?;
        let expected_raft_entry_checksum = prepared.raft_entry_checksum.clone();
        self.propose_prepared_page_batch_discard_linearizable_fenced(
            prepared,
            expected_route_control_checksum,
            &expected_raft_entry_checksum,
        )
    }
}
