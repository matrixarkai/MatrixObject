impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn prepare_chunk_batch_write_raft_entry(
        &self,
        replica_index: usize,
        chunk_id: PageId,
        writes: Vec<(u64, Vec<u8>)>,
        idempotency_key: &str,
    ) -> Result<PreparedChunkBatchWrite, ObjectError> {
        self.store.prepare_chunk_batch_write_raft_entry(
            replica_index,
            chunk_id,
            writes,
            idempotency_key,
            self.store.last_applied_index(),
        )
    }

    pub fn prepare_chunk_batch_write_raft_entry_bounded(
        &self,
        replica_index: usize,
        chunk_id: PageId,
        writes: Vec<(u64, Vec<u8>)>,
        max_write_bytes: u64,
        idempotency_key: &str,
    ) -> Result<PreparedChunkBatchWrite, ObjectError> {
        self.prepare_chunk_batch_write_raft_entry_bounded_from(
            replica_index,
            chunk_id,
            writes,
            max_write_bytes,
            0,
            idempotency_key,
        )
    }

    pub fn prepare_chunk_batch_write_raft_entry_bounded_from(
        &self,
        replica_index: usize,
        chunk_id: PageId,
        writes: Vec<(u64, Vec<u8>)>,
        max_write_bytes: u64,
        write_start_range_index: u64,
        idempotency_key: &str,
    ) -> Result<PreparedChunkBatchWrite, ObjectError> {
        self.store
            .prepare_chunk_batch_write_raft_entry_bounded_from(
                replica_index,
                chunk_id,
                writes,
                max_write_bytes,
                write_start_range_index,
                idempotency_key,
                self.store.last_applied_index(),
            )
    }

    pub fn prepare_chunk_batch_write_raft_entry_bounded_fenced(
        &self,
        replica_index: usize,
        chunk_id: PageId,
        writes: Vec<(u64, Vec<u8>)>,
        max_write_bytes: u64,
        idempotency_key: &str,
        expected_control_checksum: Option<&str>,
    ) -> Result<PreparedChunkBatchWrite, ObjectError> {
        self.prepare_chunk_batch_write_raft_entry_bounded_fenced_from(
            replica_index,
            chunk_id,
            writes,
            max_write_bytes,
            0,
            idempotency_key,
            expected_control_checksum,
        )
    }

    pub fn prepare_chunk_batch_write_raft_entry_bounded_fenced_from(
        &self,
        replica_index: usize,
        chunk_id: PageId,
        writes: Vec<(u64, Vec<u8>)>,
        max_write_bytes: u64,
        write_start_range_index: u64,
        idempotency_key: &str,
        expected_control_checksum: Option<&str>,
    ) -> Result<PreparedChunkBatchWrite, ObjectError> {
        self.store
            .prepare_chunk_batch_write_raft_entry_bounded_fenced_from(
                replica_index,
                chunk_id,
                writes,
                max_write_bytes,
                write_start_range_index,
                idempotency_key,
                self.store.last_applied_index(),
                expected_control_checksum,
            )
    }

    pub fn prepare_chunk_batch_write_raft_entry_bounded_with_read_proof_linearizable_fenced(
        &self,
        replica_index: usize,
        chunk_id: PageId,
        writes: Vec<(u64, Vec<u8>)>,
        max_write_bytes: u64,
        idempotency_key: &str,
        expected_control_checksum: Option<&str>,
    ) -> Result<ProvenStorageRead<PreparedChunkBatchWrite>, ObjectError> {
        self.prepare_chunk_batch_write_raft_entry_bounded_from_with_read_proof_linearizable_fenced(
            replica_index,
            chunk_id,
            writes,
            max_write_bytes,
            0,
            idempotency_key,
            expected_control_checksum,
        )
    }

    pub fn prepare_chunk_batch_write_raft_entry_bounded_from_with_read_proof_linearizable_fenced(
        &self,
        replica_index: usize,
        chunk_id: PageId,
        writes: Vec<(u64, Vec<u8>)>,
        max_write_bytes: u64,
        write_start_range_index: u64,
        idempotency_key: &str,
        expected_control_checksum: Option<&str>,
    ) -> Result<ProvenStorageRead<PreparedChunkBatchWrite>, ObjectError> {
        self.prepare_chunk_batch_write_raft_entry_bounded_with_read_proof_linearizable_fenced_committed_at(
            replica_index,
            chunk_id,
            writes,
            max_write_bytes,
            write_start_range_index,
            idempotency_key,
            expected_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prepare_chunk_batch_write_raft_entry_bounded_with_read_proof_linearizable_fenced_committed_at(
        &self,
        replica_index: usize,
        chunk_id: PageId,
        writes: Vec<(u64, Vec<u8>)>,
        max_write_bytes: u64,
        write_start_range_index: u64,
        idempotency_key: &str,
        expected_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenStorageRead<PreparedChunkBatchWrite>, ObjectError> {
        let requested_read_index = self
            .raft
            .commit_index()
            .max(min_read_index)
            .max(min_commit_index);
        if self.raft.commit_index() < min_commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "prepare chunk batch write requires raft commit index at least {} but raft is at {}",
                min_commit_index,
                self.raft.commit_index()
            )));
        }
        if self.raft.commit_index() < min_read_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "prepare chunk batch write requires raft commit index at least {} but raft is at {}",
                min_read_index,
                self.raft.commit_index()
            )));
        }
        if self.store.last_applied_index() < requested_read_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "prepare chunk batch write requires applied commit index {} but store is at {}",
                requested_read_index,
                self.store.last_applied_index()
            )));
        }
        let prepared = self.prepare_chunk_batch_write_raft_entry_bounded_fenced_from(
            replica_index,
            chunk_id,
            writes,
            max_write_bytes,
            write_start_range_index,
            idempotency_key,
            expected_control_checksum,
        )?;
        let proof = self.chunk_storage_read_proof_committed(
            replica_index,
            requested_read_index,
            min_commit_index,
        )?;
        Ok(ProvenStorageRead {
            value: prepared,
            proof,
        })
    }
}
