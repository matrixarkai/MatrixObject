impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    fn page_admission_read_proof_committed(
        &self,
        requested_read_index: u64,
        requested_commit_index: u64,
    ) -> AdmissionReadProof {
        AdmissionReadProof {
            requested_admission_read_index: requested_read_index,
            requested_admission_commit_index: requested_commit_index,
            admission_applied_index: self.store.last_applied_index(),
            admission_read_index_lag: requested_read_index
                .saturating_sub(self.store.last_applied_index()),
            admission_commit_index_lag: requested_commit_index
                .saturating_sub(self.raft.commit_index()),
            control_kind: "block-route".to_string(),
            control_checksum: self
                .store
                .page_service
                .route_control_proof()
                .state_checksum,
        }
    }

    fn chunk_admission_read_proof_committed(
        &self,
        replica_index: usize,
        requested_read_index: u64,
        requested_commit_index: u64,
    ) -> Result<AdmissionReadProof, ObjectError> {
        let replica = self
            .store
            .page_service
            .replicas
            .get(replica_index)
            .ok_or_else(|| {
                ObjectError::InvalidArgument("replica index out of range".to_string())
            })?;
        Ok(AdmissionReadProof {
            requested_admission_read_index: requested_read_index,
            requested_admission_commit_index: requested_commit_index,
            admission_applied_index: self.store.last_applied_index(),
            admission_read_index_lag: requested_read_index
                .saturating_sub(self.store.last_applied_index()),
            admission_commit_index_lag: requested_commit_index
                .saturating_sub(self.raft.commit_index()),
            control_kind: "chunk-service".to_string(),
            control_checksum: replica.control_proof().state_checksum,
        })
    }

    fn page_storage_read_proof(&self, requested_read_index: u64) -> StorageReadProof {
        self.page_storage_read_proof_committed(requested_read_index, 0)
    }

    fn page_storage_read_proof_committed(
        &self,
        requested_read_index: u64,
        requested_commit_index: u64,
    ) -> StorageReadProof {
        StorageReadProof {
            requested_storage_read_index: requested_read_index,
            requested_storage_commit_index: requested_commit_index,
            storage_applied_index: self.store.last_applied_index(),
            storage_read_index_lag: requested_read_index
                .saturating_sub(self.store.last_applied_index()),
            storage_commit_index_lag: requested_commit_index
                .saturating_sub(self.raft.commit_index()),
            control_kind: "block-route".to_string(),
            control_checksum: self
                .store
                .page_service
                .route_control_proof()
                .state_checksum,
        }
    }

    fn chunk_storage_read_proof_committed(
        &self,
        replica_index: usize,
        requested_read_index: u64,
        requested_commit_index: u64,
    ) -> Result<StorageReadProof, ObjectError> {
        let replica = self
            .store
            .page_service
            .replicas
            .get(replica_index)
            .ok_or_else(|| {
                ObjectError::InvalidArgument("replica index out of range".to_string())
            })?;
        Ok(StorageReadProof {
            requested_storage_read_index: requested_read_index,
            requested_storage_commit_index: requested_commit_index,
            storage_applied_index: self.store.last_applied_index(),
            storage_read_index_lag: requested_read_index
                .saturating_sub(self.store.last_applied_index()),
            storage_commit_index_lag: requested_commit_index
                .saturating_sub(self.raft.commit_index()),
            control_kind: "chunk-service".to_string(),
            control_checksum: replica.control_proof().state_checksum,
        })
    }

    fn validate_storage_snapshot_install_proof(
        proof: &StorageReadProof,
        expected_control_kind: &str,
        expected_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<(), ObjectError> {
        if proof.control_kind != expected_control_kind {
            return Err(ObjectError::PreconditionFailed(format!(
                "storage snapshot proof kind mismatch: expected {} got {}",
                expected_control_kind, proof.control_kind
            )));
        }
        if let Some(expected) = expected_control_checksum {
            if proof.control_checksum != expected {
                return Err(ObjectError::PreconditionFailed(format!(
                    "storage snapshot proof control checksum mismatch: expected {} got {}",
                    expected, proof.control_checksum
                )));
            }
        }
        if proof.requested_storage_read_index < min_read_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "storage snapshot proof read fence {} is below required {}",
                proof.requested_storage_read_index, min_read_index
            )));
        }
        if proof.requested_storage_commit_index < min_commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "storage snapshot proof commit fence {} is below required {}",
                proof.requested_storage_commit_index, min_commit_index
            )));
        }
        if proof.storage_read_index_lag != 0 {
            return Err(ObjectError::PreconditionFailed(format!(
                "storage snapshot proof has read lag {}",
                proof.storage_read_index_lag
            )));
        }
        if proof.storage_commit_index_lag != 0 {
            return Err(ObjectError::PreconditionFailed(format!(
                "storage snapshot proof has commit lag {}",
                proof.storage_commit_index_lag
            )));
        }
        Ok(())
    }
}
