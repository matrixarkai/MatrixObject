// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_repair_audited_page_replicas(
        &mut self,
        after_chunk_id: Option<PageId>,
        max_chunks: usize,
        max_payload_bytes: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::RepairAuditedPageReplicas {
            after_chunk_id,
            max_chunks,
            max_payload_bytes,
        })
    }

    pub fn audit_page_replicas_linearizable(
        &self,
        after_chunk_id: Option<PageId>,
        max_chunks: usize,
        max_payload_bytes: u64,
    ) -> Result<ChunkReplicaAuditChunk, ObjectError> {
        self.audit_page_replicas_linearizable_fenced(
            after_chunk_id,
            max_chunks,
            max_payload_bytes,
            "",
        )
    }

    pub fn audit_page_replicas_linearizable_fenced(
        &self,
        after_chunk_id: Option<PageId>,
        max_chunks: usize,
        max_payload_bytes: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ChunkReplicaAuditChunk, ObjectError> {
        self.ensure_page_route_control_ready(
            "block replica audit",
            expected_page_route_control_checksum.as_ref(),
        )?;
        self.store
            .audit_page_replicas(after_chunk_id.as_ref(), max_chunks, max_payload_bytes)
    }

    pub fn audit_page_replicas_with_read_proof_linearizable_fenced(
        &self,
        after_chunk_id: Option<PageId>,
        max_chunks: usize,
        max_payload_bytes: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenStorageRead<ChunkReplicaAuditChunk>, ObjectError> {
        self.audit_page_replicas_with_read_proof_linearizable_fenced_committed_at(
            after_chunk_id,
            max_chunks,
            max_payload_bytes,
            expected_page_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn audit_page_replicas_with_read_proof_linearizable_fenced_committed_at(
        &self,
        after_chunk_id: Option<PageId>,
        max_chunks: usize,
        max_payload_bytes: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenStorageRead<ChunkReplicaAuditChunk>, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        let requested_read_index = self
            .raft
            .commit_index()
            .max(min_read_index)
            .max(min_commit_index);
        self.ensure_page_route_control_ready_at(
            "block replica audit",
            expected_page_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        let value = self.audit_page_replicas_linearizable_fenced(
            after_chunk_id,
            max_chunks,
            max_payload_bytes,
            expected_page_route_control_checksum,
        )?;
        let proof = self.page_storage_read_proof_committed(requested_read_index, min_commit_index);
        Ok(ProvenStorageRead { value, proof })
    }

    fn ensure_page_route_control_ready(
        &self,
        operation: &str,
        expected_page_route_control_checksum: &str,
    ) -> Result<(), ObjectError> {
        self.ensure_page_route_control_ready_at(
            operation,
            expected_page_route_control_checksum,
            0,
            0,
        )
    }

    fn ensure_put_options_page_route_control_ready(
        &self,
        operation: &str,
        expected_page_route_control_checksum: &str,
        options: &PutOptions,
    ) -> Result<(), ObjectError> {
        self.ensure_page_route_control_ready_at(
            operation,
            expected_page_route_control_checksum,
            options.min_read_index,
            options.min_commit_index,
        )
    }

    fn ensure_page_route_control_ready_at(
        &self,
        operation: &str,
        expected_page_route_control_checksum: &str,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<(), ObjectError> {
        let commit_index = self.raft.commit_index();
        if commit_index < min_commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "{} commit index is not satisfied: committed={} required={}",
                operation, commit_index, min_commit_index
            )));
        }
        let required_read_index = commit_index.max(min_read_index).max(min_commit_index);
        if self.store.last_applied_index() < required_read_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "{} read index is not satisfied: applied={} required={}",
                operation,
                self.store.last_applied_index(),
                required_read_index
            )));
        }
        if !expected_page_route_control_checksum.is_empty() {
            let actual_checksum = self
                .store
                .chunk_service
                .route_control_proof()
                .state_checksum;
            if expected_page_route_control_checksum != actual_checksum {
                return Err(ObjectError::PreconditionFailed(format!(
                    "{} route control checksum mismatch: expected {} actual {}",
                    operation, expected_page_route_control_checksum, actual_checksum
                )));
            }
        }
        Ok(())
    }

    fn route_fenced_metadata_read<T>(
        &self,
        value: T,
        readiness: &MetadataRaftReadiness,
        expected_page_route_control_checksum: &str,
    ) -> ProvenRouteFencedMetadataRead<T> {
        ProvenRouteFencedMetadataRead {
            value,
            proof: object_read_proof_from_readiness(readiness),
            route_control_proof: self.store.chunk_service.route_control_proof(),
            expected_route_control_checksum: expected_page_route_control_checksum.to_string(),
        }
    }

}
