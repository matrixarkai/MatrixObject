// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn export_chunk_replica_snapshot_chunks_with_read_proof_linearizable_fenced(
        &self,
        replica_index: usize,
        max_payload_bytes: usize,
        expected_control_checksum: Option<&str>,
    ) -> Result<Vec<ProvenStorageRead<ReplicaServiceSnapshotChunk>>, ObjectError> {
        self.export_chunk_replica_snapshot_chunks_with_read_proof_linearizable_fenced_committed_at(
            replica_index,
            max_payload_bytes,
            expected_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn export_chunk_replica_snapshot_chunks_with_control_proof_linearizable_fenced(
        &self,
        replica_index: usize,
        max_payload_bytes: usize,
        expected_control_checksum: impl AsRef<str>,
    ) -> Result<Vec<ProvenChunkControlFencedStorageRead<ReplicaServiceSnapshotChunk>>, ObjectError>
    {
        self.export_chunk_replica_snapshot_chunks_with_control_proof_linearizable_fenced_committed_at(
            replica_index,
            max_payload_bytes,
            expected_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn export_chunk_replica_snapshot_chunks_with_control_proof_linearizable_fenced_committed_at(
        &self,
        replica_index: usize,
        max_payload_bytes: usize,
        expected_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<Vec<ProvenChunkControlFencedStorageRead<ReplicaServiceSnapshotChunk>>, ObjectError>
    {
        let mut pages = Vec::new();
        self.export_chunk_replica_snapshot_chunk_stream_with_control_proof_linearizable_fenced_committed_at(
            replica_index,
            max_payload_bytes,
            expected_control_checksum,
            min_read_index,
            min_commit_index,
            |chunk| {
                if pages.is_empty() {
                    pages.reserve(snapshot_chunk_vec_capacity(
                        chunk.value.total_bytes,
                        max_payload_bytes,
                    ));
                }
                pages.push(chunk);
                Ok(())
            },
        )?;
        Ok(pages)
    }

    pub fn export_chunk_replica_snapshot_chunks_with_read_proof_linearizable_fenced_committed_at(
        &self,
        replica_index: usize,
        max_payload_bytes: usize,
        expected_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<Vec<ProvenStorageRead<ReplicaServiceSnapshotChunk>>, ObjectError> {
        let mut pages = Vec::new();
        self.export_chunk_replica_snapshot_chunk_stream_with_read_proof_linearizable_fenced_committed_at(
            replica_index,
            max_payload_bytes,
            expected_control_checksum,
            min_read_index,
            min_commit_index,
            |chunk| {
                if pages.is_empty() {
                    pages.reserve(snapshot_chunk_vec_capacity(
                        chunk.value.total_bytes,
                        max_payload_bytes,
                    ));
                }
                pages.push(chunk);
                Ok(())
            },
        )?;
        Ok(pages)
    }
}
