impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn prepare_page_batch_discard_raft_entry(
        &self,
        requests: Vec<ChunkRangeRequest>,
        idempotency_key: &str,
    ) -> Result<PreparedPageBatchDiscard, ObjectError> {
        let expected_route_control_checksum = self
            .store
            .chunk_service
            .route_control_proof()
            .state_checksum;
        self.store.prepare_page_batch_discard_raft_entry_fenced(
            requests,
            idempotency_key,
            self.store.last_applied_index(),
            Some(&expected_route_control_checksum),
        )
    }

    pub fn prepare_page_batch_discard_raft_entry_bounded(
        &self,
        requests: Vec<ChunkRangeRequest>,
        max_discard_bytes: u64,
        idempotency_key: &str,
    ) -> Result<PreparedPageBatchDiscard, ObjectError> {
        self.store.prepare_page_batch_discard_raft_entry_bounded(
            requests,
            max_discard_bytes,
            idempotency_key,
            self.store.last_applied_index(),
        )
    }

    pub fn prepare_page_batch_discard_raft_entry_bounded_from(
        &self,
        requests: Vec<ChunkRangeRequest>,
        max_discard_bytes: u64,
        discard_start_range_index: u64,
        idempotency_key: &str,
    ) -> Result<PreparedPageBatchDiscard, ObjectError> {
        self.store
            .prepare_page_batch_discard_raft_entry_bounded_from(
                requests,
                max_discard_bytes,
                discard_start_range_index,
                idempotency_key,
                self.store.last_applied_index(),
            )
    }

    pub fn prepare_page_batch_discard_raft_entry_bounded_with_read_proof_linearizable_fenced(
        &self,
        requests: Vec<ChunkRangeRequest>,
        max_discard_bytes: u64,
        idempotency_key: &str,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ProvenStorageRead<PreparedPageBatchDiscard>, ObjectError> {
        self.prepare_page_batch_discard_raft_entry_bounded_with_read_proof_linearizable_fenced_committed_at(
            requests,
            max_discard_bytes,
            idempotency_key,
            expected_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prepare_page_batch_discard_raft_entry_bounded_with_read_proof_linearizable_fenced_committed_at(
        &self,
        requests: Vec<ChunkRangeRequest>,
        max_discard_bytes: u64,
        idempotency_key: &str,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenStorageRead<PreparedPageBatchDiscard>, ObjectError> {
        let requested_read_index = self
            .raft
            .commit_index()
            .max(min_read_index)
            .max(min_commit_index);
        self.ensure_storage_control_read_index_committed_at(
            "prepare block batch discard",
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
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
        let proof = self.page_storage_read_proof_committed(requested_read_index, min_commit_index);
        Ok(ProvenStorageRead {
            value: prepared,
            proof,
        })
    }

    pub fn prepare_page_batch_discard_raft_entry_bounded_from_with_read_proof_linearizable_fenced(
        &self,
        requests: Vec<ChunkRangeRequest>,
        max_discard_bytes: u64,
        discard_start_range_index: u64,
        idempotency_key: &str,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ProvenStorageRead<PreparedPageBatchDiscard>, ObjectError> {
        self.prepare_page_batch_discard_raft_entry_bounded_from_with_read_proof_linearizable_fenced_committed_at(
            requests,
            max_discard_bytes,
            discard_start_range_index,
            idempotency_key,
            expected_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prepare_page_batch_discard_raft_entry_bounded_from_with_read_proof_linearizable_fenced_committed_at(
        &self,
        requests: Vec<ChunkRangeRequest>,
        max_discard_bytes: u64,
        discard_start_range_index: u64,
        idempotency_key: &str,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenStorageRead<PreparedPageBatchDiscard>, ObjectError> {
        let requested_read_index = self
            .raft
            .commit_index()
            .max(min_read_index)
            .max(min_commit_index);
        self.ensure_storage_control_read_index_committed_at(
            "prepare block batch discard",
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
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
        let proof = self.page_storage_read_proof_committed(requested_read_index, min_commit_index);
        Ok(ProvenStorageRead {
            value: prepared,
            proof,
        })
    }

}
