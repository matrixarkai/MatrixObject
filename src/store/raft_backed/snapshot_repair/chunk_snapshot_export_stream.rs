// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn export_chunk_replica_snapshot_chunk_stream_with_read_proof_linearizable_fenced<F>(
        &self,
        replica_index: usize,
        max_payload_bytes: usize,
        expected_control_checksum: Option<&str>,
        emit: F,
    ) -> Result<(), ObjectError>
    where
        F: FnMut(ProvenStorageRead<ReplicaServiceSnapshotChunk>) -> Result<(), ObjectError>,
    {
        self.export_chunk_replica_snapshot_chunk_stream_with_read_proof_linearizable_fenced_committed_at(
            replica_index,
            max_payload_bytes,
            expected_control_checksum,
            self.raft.commit_index(),
            0,
            emit,
        )
    }

    pub fn export_chunk_replica_snapshot_chunk_stream_with_control_proof_linearizable_fenced<F>(
        &self,
        replica_index: usize,
        max_payload_bytes: usize,
        expected_control_checksum: impl AsRef<str>,
        emit: F,
    ) -> Result<(), ObjectError>
    where
        F: FnMut(
            ProvenChunkControlFencedStorageRead<ReplicaServiceSnapshotChunk>,
        ) -> Result<(), ObjectError>,
    {
        self.export_chunk_replica_snapshot_chunk_stream_with_control_proof_linearizable_fenced_committed_at(
            replica_index,
            max_payload_bytes,
            expected_control_checksum,
            self.raft.commit_index(),
            0,
            emit,
        )
    }

    pub fn export_chunk_replica_snapshot_chunk_stream_with_control_proof_linearizable_fenced_committed_at<
        F,
    >(
        &self,
        replica_index: usize,
        max_payload_bytes: usize,
        expected_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
        mut emit: F,
    ) -> Result<(), ObjectError>
    where
        F: FnMut(
            ProvenChunkControlFencedStorageRead<ReplicaServiceSnapshotChunk>,
        ) -> Result<(), ObjectError>,
    {
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
        replica.check_expected_control_checksum(Some(&expected_checksum))?;
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
        let proof = self.chunk_storage_read_proof_committed(
            replica_index,
            requested_read_index,
            min_commit_index,
        )?;
        self.store
            .chunk_service
            .export_chunk_replica_snapshot_chunk_stream(
                replica_index,
                max_payload_bytes,
                |value| {
                    emit(ProvenChunkControlFencedStorageRead {
                        value,
                        proof: proof.clone(),
                        control_proof: control_proof.clone(),
                        expected_control_checksum: expected_checksum.clone(),
                    })?;
                    Ok(())
                },
            )?;
        Ok(())
    }

    pub fn export_chunk_replica_snapshot_chunk_stream_with_read_proof_linearizable_fenced_committed_at<
        F,
    >(
        &self,
        replica_index: usize,
        max_payload_bytes: usize,
        expected_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
        mut emit: F,
    ) -> Result<(), ObjectError>
    where
        F: FnMut(ProvenStorageRead<ReplicaServiceSnapshotChunk>) -> Result<(), ObjectError>,
    {
        let replica = self
            .store
            .chunk_service
            .replicas
            .get(replica_index)
            .ok_or_else(|| {
                ObjectError::InvalidArgument("replica index is out of range".to_string())
            })?;
        replica.check_expected_control_checksum(expected_control_checksum)?;
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
        let proof = self.chunk_storage_read_proof_committed(
            replica_index,
            requested_read_index,
            min_commit_index,
        )?;
        self.store
            .chunk_service
            .export_chunk_replica_snapshot_chunk_stream(
                replica_index,
                max_payload_bytes,
                |value| {
                    emit(ProvenStorageRead {
                        value,
                        proof: proof.clone(),
                    })?;
                    Ok(())
                },
            )?;
        Ok(())
    }
}
