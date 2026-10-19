// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn chunk_replica_storage_readiness_linearizable(
        &self,
        replica_index: usize,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        now_ms: u64,
    ) -> Result<(ReplicaStorageReadiness, u64), ObjectError> {
        self.chunk_replica_storage_readiness_linearizable_at(
            replica_index,
            min_capacity_free_bytes,
            min_reclaimable_bytes,
            now_ms,
            self.raft.commit_index(),
        )
    }

    pub fn chunk_replica_storage_readiness_linearizable_at(
        &self,
        replica_index: usize,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        now_ms: u64,
        min_read_index: u64,
    ) -> Result<(ReplicaStorageReadiness, u64), ObjectError> {
        self.chunk_replica_storage_readiness_linearizable_committed_at(
            replica_index,
            min_capacity_free_bytes,
            min_reclaimable_bytes,
            now_ms,
            min_read_index,
            0,
        )
    }

    pub fn chunk_replica_storage_readiness_linearizable_committed_at(
        &self,
        replica_index: usize,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<(ReplicaStorageReadiness, u64), ObjectError> {
        let required_read_index = self
            .raft
            .commit_index()
            .max(min_read_index)
            .max(min_commit_index);
        if self.raft.commit_index() < min_commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "chunk storage readiness requires raft commit index at least {} but raft is at {}",
                min_commit_index,
                self.raft.commit_index()
            )));
        }
        if self.raft.commit_index() < min_read_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "chunk storage readiness requires raft commit index at least {} but raft is at {}",
                min_read_index,
                self.raft.commit_index()
            )));
        }
        if self.store.last_applied_index() < required_read_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "chunk storage readiness requires applied commit index {} but store is at {}",
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
        Ok((
            replica.storage_readiness(min_capacity_free_bytes, min_reclaimable_bytes, now_ms),
            required_read_index,
        ))
    }

    pub fn chunk_replica_storage_readiness_with_read_proof_linearizable_at(
        &self,
        replica_index: usize,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        now_ms: u64,
        min_read_index: u64,
    ) -> Result<ProvenStorageRead<ReplicaStorageReadiness>, ObjectError> {
        self.chunk_replica_storage_readiness_with_read_proof_linearizable_committed_at(
            replica_index,
            min_capacity_free_bytes,
            min_reclaimable_bytes,
            now_ms,
            min_read_index,
            0,
        )
    }

    pub fn chunk_replica_storage_readiness_with_read_proof_linearizable_committed_at(
        &self,
        replica_index: usize,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenStorageRead<ReplicaStorageReadiness>, ObjectError> {
        let (value, requested_read_index) = self
            .chunk_replica_storage_readiness_linearizable_committed_at(
                replica_index,
                min_capacity_free_bytes,
                min_reclaimable_bytes,
                now_ms,
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
