impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn can_accept_chunk_batch_discard_linearizable(
        &self,
        replica_index: usize,
        chunk_id: &ChunkId,
        ranges: Vec<DataRange>,
        max_discard_bytes: u64,
    ) -> Result<ChunkDiscardAdmission, ObjectError> {
        self.can_accept_chunk_batch_discard_linearizable_committed_at(
            replica_index,
            chunk_id,
            ranges,
            max_discard_bytes,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn can_accept_chunk_batch_discard_linearizable_committed_at(
        &self,
        replica_index: usize,
        chunk_id: &ChunkId,
        ranges: Vec<DataRange>,
        max_discard_bytes: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ChunkDiscardAdmission, ObjectError> {
        self.can_accept_chunk_batch_discard_linearizable_fenced_committed_at(
            replica_index,
            chunk_id,
            ranges,
            max_discard_bytes,
            None,
            min_read_index,
            min_commit_index,
        )
    }

    pub fn can_accept_chunk_batch_discard_linearizable_fenced(
        &self,
        replica_index: usize,
        chunk_id: &ChunkId,
        ranges: Vec<DataRange>,
        max_discard_bytes: u64,
        expected_control_checksum: Option<&str>,
    ) -> Result<ChunkDiscardAdmission, ObjectError> {
        self.can_accept_chunk_batch_discard_linearizable_fenced_committed_at(
            replica_index,
            chunk_id,
            ranges,
            max_discard_bytes,
            expected_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn can_accept_chunk_batch_discard_linearizable_fenced_committed_at(
        &self,
        replica_index: usize,
        chunk_id: &ChunkId,
        ranges: Vec<DataRange>,
        max_discard_bytes: u64,
        expected_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ChunkDiscardAdmission, ObjectError> {
        let required_read_index = self
            .raft
            .commit_index()
            .max(min_read_index)
            .max(min_commit_index);
        if self.raft.commit_index() < min_commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "chunk batch discard admission requires raft commit index at least {} but raft is at {}",
                min_commit_index,
                self.raft.commit_index()
            )));
        }
        if self.raft.commit_index() < min_read_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "chunk batch discard admission requires raft commit index at least {} but raft is at {}",
                min_read_index,
                self.raft.commit_index()
            )));
        }
        if self.store.last_applied_index() < required_read_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "chunk batch discard admission requires applied commit index {} but store is at {}",
                required_read_index,
                self.store.last_applied_index()
            )));
        }
        let mut admission = self.store.can_accept_chunk_batch_discard_fenced(
            replica_index,
            chunk_id,
            ranges,
            max_discard_bytes,
            expected_control_checksum,
        )?;
        admission.min_read_index = required_read_index;
        admission.min_commit_index = min_commit_index;
        Ok(admission)
    }

    pub fn can_accept_chunk_batch_discard_with_read_proof_linearizable_fenced(
        &self,
        replica_index: usize,
        chunk_id: &ChunkId,
        ranges: Vec<DataRange>,
        max_discard_bytes: u64,
        expected_control_checksum: Option<&str>,
    ) -> Result<ProvenAdmissionRead<ChunkDiscardAdmission>, ObjectError> {
        self.can_accept_chunk_batch_discard_with_read_proof_linearizable_fenced_committed_at(
            replica_index,
            chunk_id,
            ranges,
            max_discard_bytes,
            expected_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn can_accept_chunk_batch_discard_with_control_proof_linearizable_fenced(
        &self,
        replica_index: usize,
        chunk_id: &ChunkId,
        ranges: Vec<DataRange>,
        max_discard_bytes: u64,
        expected_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenChunkControlFencedAdmissionRead<ChunkDiscardAdmission>, ObjectError> {
        self.can_accept_chunk_batch_discard_with_control_proof_linearizable_fenced_committed_at(
            replica_index,
            chunk_id,
            ranges,
            max_discard_bytes,
            expected_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn can_accept_chunk_batch_discard_with_control_proof_linearizable_fenced_committed_at(
        &self,
        replica_index: usize,
        chunk_id: &ChunkId,
        ranges: Vec<DataRange>,
        max_discard_bytes: u64,
        expected_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenChunkControlFencedAdmissionRead<ChunkDiscardAdmission>, ObjectError> {
        let expected_checksum = expected_control_checksum.as_ref().to_string();
        let control_proof = self
            .store
            .page_service
            .replicas
            .get(replica_index)
            .ok_or_else(|| {
                ObjectError::InvalidArgument("replica index is out of range".to_string())
            })?
            .control_proof();
        let proven = self
            .can_accept_chunk_batch_discard_with_read_proof_linearizable_fenced_committed_at(
                replica_index,
                chunk_id,
                ranges,
                max_discard_bytes,
                Some(&expected_checksum),
                min_read_index,
                min_commit_index,
            )?;
        Ok(ProvenChunkControlFencedAdmissionRead {
            value: proven.value,
            proof: proven.proof,
            control_proof,
            expected_control_checksum: expected_checksum,
        })
    }

    pub fn can_accept_chunk_batch_discard_with_read_proof_linearizable_fenced_committed_at(
        &self,
        replica_index: usize,
        chunk_id: &ChunkId,
        ranges: Vec<DataRange>,
        max_discard_bytes: u64,
        expected_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenAdmissionRead<ChunkDiscardAdmission>, ObjectError> {
        let requested_read_index = self
            .raft
            .commit_index()
            .max(min_read_index)
            .max(min_commit_index);
        let value = self.can_accept_chunk_batch_discard_linearizable_fenced_committed_at(
            replica_index,
            chunk_id,
            ranges,
            max_discard_bytes,
            expected_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        let proof = self.chunk_admission_read_proof_committed(
            replica_index,
            requested_read_index,
            min_commit_index,
        )?;
        Ok(ProvenAdmissionRead { value, proof })
    }
}
