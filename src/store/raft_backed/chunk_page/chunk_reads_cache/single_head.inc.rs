// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn head_chunk_linearizable(
        &self,
        replica_index: usize,
        page_id: &PageId,
    ) -> Result<(ReplicaHead, u64), ObjectError> {
        self.head_chunk_linearizable_at(replica_index, page_id, self.raft.commit_index())
    }

    pub fn head_chunk_linearizable_at(
        &self,
        replica_index: usize,
        page_id: &PageId,
        min_read_index: u64,
    ) -> Result<(ReplicaHead, u64), ObjectError> {
        self.head_chunk_linearizable_committed_at(replica_index, page_id, min_read_index, 0)
    }

    pub fn head_chunk_linearizable_committed_at(
        &self,
        replica_index: usize,
        page_id: &PageId,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<(ReplicaHead, u64), ObjectError> {
        let required_read_index = self
            .raft
            .commit_index()
            .max(min_read_index)
            .max(min_commit_index);
        if self.raft.commit_index() < min_commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "chunk head requires raft commit index at least {} but raft is at {}",
                min_commit_index,
                self.raft.commit_index()
            )));
        }
        if self.raft.commit_index() < min_read_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "chunk head requires raft commit index at least {} but raft is at {}",
                min_read_index,
                self.raft.commit_index()
            )));
        }
        if self.store.last_applied_index() < required_read_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "chunk head requires applied commit index {} but store is at {}",
                required_read_index,
                self.store.last_applied_index()
            )));
        }
        let replica = self
            .store
            .chunk_service
            .replicas
            .get(replica_index)
            .ok_or_else(|| {
                ObjectError::InvalidArgument("replica index is out of range".to_string())
            })?;
        let head = replica.head_chunk(page_id)?;
        Ok((head, required_read_index))
    }

    pub fn head_chunk_with_read_proof_linearizable_at(
        &self,
        replica_index: usize,
        page_id: &PageId,
        min_read_index: u64,
    ) -> Result<ProvenStorageRead<ReplicaHead>, ObjectError> {
        self.head_chunk_with_read_proof_linearizable_committed_at(
            replica_index,
            page_id,
            min_read_index,
            0,
        )
    }

    pub fn head_chunk_with_read_proof_linearizable_committed_at(
        &self,
        replica_index: usize,
        page_id: &PageId,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenStorageRead<ReplicaHead>, ObjectError> {
        let (value, requested_read_index) = self.head_chunk_linearizable_committed_at(
            replica_index,
            page_id,
            min_read_index,
            min_commit_index,
        )?;
        let proof = self.chunk_storage_read_proof_committed(
            replica_index,
            requested_read_index,
            min_commit_index,
        )?;
        Ok(ProvenStorageRead { value, proof })
    }
}
