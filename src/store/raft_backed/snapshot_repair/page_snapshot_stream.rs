// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn export_page_snapshot_chunk_stream_with_read_proof_linearizable_fenced<F>(
        &self,
        max_payload_bytes: usize,
        expected_route_control_checksum: Option<&str>,
        emit: F,
    ) -> Result<(), ObjectError>
    where
        F: FnMut(ProvenStorageRead<ChunkServiceSnapshotChunk>) -> Result<(), ObjectError>,
    {
        self.export_page_snapshot_chunk_stream_with_read_proof_linearizable_fenced_committed_at(
            max_payload_bytes,
            expected_route_control_checksum,
            self.raft.commit_index(),
            0,
            emit,
        )
    }

    pub fn export_page_snapshot_chunk_stream_with_route_proof_linearizable_fenced<F>(
        &self,
        max_payload_bytes: usize,
        expected_route_control_checksum: impl AsRef<str>,
        emit: F,
    ) -> Result<(), ObjectError>
    where
        F: FnMut(
            ProvenPageRouteFencedStorageRead<ChunkServiceSnapshotChunk>,
        ) -> Result<(), ObjectError>,
    {
        self.export_page_snapshot_chunk_stream_with_route_proof_linearizable_fenced_committed_at(
            max_payload_bytes,
            expected_route_control_checksum,
            self.raft.commit_index(),
            0,
            emit,
        )
    }

    pub fn export_page_snapshot_chunk_stream_with_route_proof_linearizable_fenced_committed_at<F>(
        &self,
        max_payload_bytes: usize,
        expected_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
        mut emit: F,
    ) -> Result<(), ObjectError>
    where
        F: FnMut(
            ProvenPageRouteFencedStorageRead<ChunkServiceSnapshotChunk>,
        ) -> Result<(), ObjectError>,
    {
        let expected_checksum = expected_route_control_checksum.as_ref().to_string();
        let route_control_proof = self.store.chunk_service.route_control_proof();
        self.store
            .chunk_service
            .check_expected_route_control_checksum(Some(&expected_checksum))?;
        let requested_read_index = self
            .raft
            .commit_index()
            .max(min_read_index)
            .max(min_commit_index);
        if self.raft.commit_index() < min_commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "page snapshot export requires raft commit index at least {} but raft is at {}",
                min_commit_index,
                self.raft.commit_index()
            )));
        }
        if self.raft.commit_index() < min_read_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "page snapshot export requires raft commit index at least {} but raft is at {}",
                min_read_index,
                self.raft.commit_index()
            )));
        }
        if self.store.last_applied_index() < requested_read_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "page snapshot export requires applied commit index {} but store is at {}",
                requested_read_index,
                self.store.last_applied_index()
            )));
        }
        let proof = self.page_storage_read_proof_committed(requested_read_index, min_commit_index);
        self.store
            .chunk_service
            .export_snapshot_chunk_stream(max_payload_bytes, |value| {
                emit(ProvenPageRouteFencedStorageRead {
                    value,
                    proof: proof.clone(),
                    route_control_proof: route_control_proof.clone(),
                    expected_route_control_checksum: expected_checksum.clone(),
                })?;
                Ok(())
            })?;
        Ok(())
    }

    pub fn export_page_snapshot_chunk_stream_with_read_proof_linearizable_fenced_committed_at<F>(
        &self,
        max_payload_bytes: usize,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
        mut emit: F,
    ) -> Result<(), ObjectError>
    where
        F: FnMut(ProvenStorageRead<ChunkServiceSnapshotChunk>) -> Result<(), ObjectError>,
    {
        let requested_read_index = self
            .raft
            .commit_index()
            .max(min_read_index)
            .max(min_commit_index);
        if self.raft.commit_index() < min_commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "page snapshot export requires raft commit index at least {} but raft is at {}",
                min_commit_index,
                self.raft.commit_index()
            )));
        }
        if self.raft.commit_index() < min_read_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "page snapshot export requires raft commit index at least {} but raft is at {}",
                min_read_index,
                self.raft.commit_index()
            )));
        }
        if self.store.last_applied_index() < requested_read_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "page snapshot export requires applied commit index {} but store is at {}",
                requested_read_index,
                self.store.last_applied_index()
            )));
        }
        self.store
            .chunk_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        let proof = self.page_storage_read_proof_committed(requested_read_index, min_commit_index);
        self.store
            .chunk_service
            .export_snapshot_chunk_stream(max_payload_bytes, |value| {
                emit(ProvenStorageRead {
                    value,
                    proof: proof.clone(),
                })?;
                Ok(())
            })?;
        Ok(())
    }
}
