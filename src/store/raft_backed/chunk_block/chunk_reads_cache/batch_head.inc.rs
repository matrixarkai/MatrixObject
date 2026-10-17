impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn head_chunk_batch_linearizable(
        &self,
        replica_index: usize,
        chunk_ids: Vec<ChunkId>,
    ) -> Result<(Vec<ChunkHead>, ChunkHeadBatchStats, u64), ObjectError> {
        self.head_chunk_batch_linearizable_at(replica_index, chunk_ids, self.raft.commit_index())
    }

    pub fn head_chunk_batch_linearizable_at(
        &self,
        replica_index: usize,
        chunk_ids: Vec<ChunkId>,
        min_read_index: u64,
    ) -> Result<(Vec<ChunkHead>, ChunkHeadBatchStats, u64), ObjectError> {
        self.head_chunk_batch_linearizable_committed_at(replica_index, chunk_ids, min_read_index, 0)
    }

    pub fn head_chunk_batch_linearizable_committed_at(
        &self,
        replica_index: usize,
        chunk_ids: Vec<ChunkId>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<(Vec<ChunkHead>, ChunkHeadBatchStats, u64), ObjectError> {
        let required_read_index = self
            .raft
            .commit_index()
            .max(min_read_index)
            .max(min_commit_index);
        if self.raft.commit_index() < min_commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "chunk head batch requires raft commit index at least {} but raft is at {}",
                min_commit_index,
                self.raft.commit_index()
            )));
        }
        if self.raft.commit_index() < min_read_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "chunk head batch requires raft commit index at least {} but raft is at {}",
                min_read_index,
                self.raft.commit_index()
            )));
        }
        if self.store.last_applied_index() < required_read_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "chunk head batch requires applied commit index {} but store is at {}",
                required_read_index,
                self.store.last_applied_index()
            )));
        }
        let replica = self
            .store
            .page_service
            .replicas
            .get(replica_index)
            .ok_or_else(|| {
                ObjectError::InvalidArgument("replica index is out of range".to_string())
            })?;
        let (heads, stats) = replica.head_chunk_batch(chunk_ids)?;
        Ok((heads, stats, required_read_index))
    }

    pub fn head_chunk_batch_with_read_proof_linearizable_at(
        &self,
        replica_index: usize,
        chunk_ids: Vec<ChunkId>,
        min_read_index: u64,
    ) -> Result<ProvenStorageRead<ChunkHeadBatchRead>, ObjectError> {
        self.head_chunk_batch_with_read_proof_linearizable_committed_at(
            replica_index,
            chunk_ids,
            min_read_index,
            0,
        )
    }

    pub fn head_chunk_batch_with_read_proof_linearizable_committed_at(
        &self,
        replica_index: usize,
        chunk_ids: Vec<ChunkId>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenStorageRead<ChunkHeadBatchRead>, ObjectError> {
        let requested_chunk_ids = chunk_ids.clone();
        let (heads, stats, requested_read_index) = self
            .head_chunk_batch_linearizable_committed_at(
                replica_index,
                chunk_ids,
                min_read_index,
                min_commit_index,
            )?;
        let proof = self.chunk_storage_read_proof_committed(
            replica_index,
            requested_read_index,
            min_commit_index,
        )?;
        let proof_checksum =
            chunk_head_batch_read_proof_checksum(&requested_chunk_ids, &heads, &stats, &proof);
        Ok(ProvenStorageRead {
            value: ChunkHeadBatchRead {
                heads,
                stats,
                min_read_index,
                min_commit_index,
                proof_format: CHUNK_HEAD_BATCH_PROOF_FORMAT.to_string(),
                proof_checksum,
            },
            proof,
        })
    }

    pub fn head_chunk_batch_strict_linearizable(
        &self,
        replica_index: usize,
        chunk_ids: Vec<ChunkId>,
        expected_chunk_head_batch_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenStorageRead<ChunkHeadBatchRead>, ObjectError> {
        self.head_chunk_batch_strict_linearizable_committed_at(
            replica_index,
            chunk_ids,
            expected_chunk_head_batch_proof_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn head_chunk_batch_strict_linearizable_at(
        &self,
        replica_index: usize,
        chunk_ids: Vec<ChunkId>,
        expected_chunk_head_batch_proof_checksum: impl AsRef<str>,
        min_read_index: u64,
    ) -> Result<ProvenStorageRead<ChunkHeadBatchRead>, ObjectError> {
        self.head_chunk_batch_strict_linearizable_committed_at(
            replica_index,
            chunk_ids,
            expected_chunk_head_batch_proof_checksum,
            min_read_index,
            0,
        )
    }

    pub fn head_chunk_batch_strict_linearizable_committed_at(
        &self,
        replica_index: usize,
        chunk_ids: Vec<ChunkId>,
        expected_chunk_head_batch_proof_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenStorageRead<ChunkHeadBatchRead>, ObjectError> {
        let proven = self.head_chunk_batch_with_read_proof_linearizable_committed_at(
            replica_index,
            chunk_ids,
            min_read_index,
            min_commit_index,
        )?;
        proven.require_chunk_head_batch_proof_checksum(
            expected_chunk_head_batch_proof_checksum.as_ref(),
        )
    }
}
