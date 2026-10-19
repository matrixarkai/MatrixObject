impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn export_chunk_replica_snapshot_chunk_with_read_proof_linearizable_fenced(
        &self,
        replica_index: usize,
        offset: u64,
        max_payload_bytes: usize,
        expected_control_checksum: Option<&str>,
    ) -> Result<ProvenStorageRead<ReplicaServiceSnapshotChunk>, ObjectError> {
        self.export_chunk_replica_snapshot_chunk_with_read_proof_linearizable_fenced_committed_at(
            replica_index,
            offset,
            max_payload_bytes,
            expected_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn export_chunk_replica_snapshot_chunk_with_control_proof_linearizable_fenced(
        &self,
        replica_index: usize,
        offset: u64,
        max_payload_bytes: usize,
        expected_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenChunkControlFencedStorageRead<ReplicaServiceSnapshotChunk>, ObjectError> {
        self.export_chunk_replica_snapshot_chunk_with_control_proof_linearizable_fenced_committed_at(
            replica_index,
            offset,
            max_payload_bytes,
            expected_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn export_chunk_replica_snapshot_chunk_with_control_proof_linearizable_fenced_committed_at(
        &self,
        replica_index: usize,
        offset: u64,
        max_payload_bytes: usize,
        expected_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenChunkControlFencedStorageRead<ReplicaServiceSnapshotChunk>, ObjectError> {
        let expected_checksum = expected_control_checksum.as_ref().to_string();
        let control_proof = self
            .store
            .chunk_service
            .replicas
            .get(replica_index)
            .ok_or_else(|| {
                ObjectError::InvalidArgument("replica index is out of range".to_string())
            })?
            .control_proof();
        let proven = self
            .export_chunk_replica_snapshot_chunk_with_read_proof_linearizable_fenced_committed_at(
                replica_index,
                offset,
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

    pub fn export_chunk_replica_snapshot_chunk_with_read_proof_linearizable_fenced_committed_at(
        &self,
        replica_index: usize,
        offset: u64,
        max_payload_bytes: usize,
        expected_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenStorageRead<ReplicaServiceSnapshotChunk>, ObjectError> {
        let requested_read_index = self
            .raft
            .commit_index()
            .max(min_read_index)
            .max(min_commit_index);
        if self.raft.commit_index() < min_commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "chunk replica snapshot export requires raft commit index at least {} but raft is at {}",
                min_commit_index,
                self.raft.commit_index()
            )));
        }
        if self.raft.commit_index() < min_read_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "chunk replica snapshot export requires raft commit index at least {} but raft is at {}",
                min_read_index,
                self.raft.commit_index()
            )));
        }
        if self.store.last_applied_index() < requested_read_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "chunk replica snapshot export requires applied commit index {} but store is at {}",
                requested_read_index,
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
        replica.check_expected_control_checksum(expected_control_checksum)?;
        let value = self.store.export_chunk_replica_snapshot_chunk(
            replica_index,
            offset,
            max_payload_bytes,
        )?;
        let proof = self.chunk_storage_read_proof_committed(
            replica_index,
            requested_read_index,
            min_commit_index,
        )?;
        Ok(ProvenStorageRead { value, proof })
    }
}
