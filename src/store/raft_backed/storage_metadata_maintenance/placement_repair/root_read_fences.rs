impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    fn ensure_root_read_index(&self, operation: &str) -> Result<RootReadProof, ObjectError> {
        self.ensure_root_read_index_committed_at(operation, self.raft.commit_index(), 0)
    }

    fn ensure_root_read_index_committed_at(
        &self,
        operation: &str,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<RootReadProof, ObjectError> {
        let commit_index = self.raft.commit_index();
        if commit_index < min_commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "{} root commit index is below required: commit={} required={}",
                operation, commit_index, min_commit_index
            )));
        }
        if commit_index < min_read_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "{} root read index is below required: commit={} required={}",
                operation, commit_index, min_read_index
            )));
        }
        let requested_index = commit_index.max(min_read_index).max(min_commit_index);
        let applied_index = self.store.last_applied_index();
        if applied_index < requested_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "{} root read index is not satisfied: applied={} required={}",
                operation, applied_index, requested_index
            )));
        }
        let mut proof = RootReadProof {
            proof_format: "MOS-RUST-ROOT-READ-PROOF2".to_string(),
            operation: operation.to_string(),
            requested_root_read_index: requested_index,
            requested_root_commit_index: requested_index.max(min_commit_index),
            root_applied_index: applied_index,
            root_read_index_lag: requested_index.saturating_sub(applied_index),
            root_raft_term: self.raft.term(),
            root_state_fingerprint: self.store.meta_service.root_state_fingerprint(),
            proof_checksum: String::new(),
        };
        proof.proof_checksum = root_read_proof_checksum(&proof);
        Ok(proof)
    }

    fn ensure_metadata_read_index(
        &self,
        operation: &str,
        now_ms: u64,
    ) -> Result<MetadataRaftReadiness, ObjectError> {
        self.ensure_metadata_read_index_committed_at(operation, now_ms, self.raft.commit_index(), 0)
    }

    fn ensure_metadata_read_index_committed_at(
        &self,
        operation: &str,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<MetadataRaftReadiness, ObjectError> {
        let commit_index = self.raft.commit_index();
        if commit_index < min_commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "{} metadata control commit index is not satisfied: committed={} required={}",
                operation, commit_index, min_commit_index
            )));
        }
        let required_read_index = commit_index.max(min_read_index).max(min_commit_index);
        if self.store.last_applied_index() < required_read_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "{} metadata control read index is not satisfied: applied={} required={}",
                operation,
                self.store.last_applied_index(),
                required_read_index
            )));
        }
        let readiness = self.metadata_raft_readiness(required_read_index, now_ms);
        if !readiness.readable {
            return Err(ObjectError::PreconditionFailed(format!(
                "{} metadata control proof is not readable: reason={} applied={} required={}",
                operation,
                readiness.reason,
                readiness.applied_commit_index,
                readiness.min_read_index
            )));
        }
        Ok(readiness)
    }

    fn ensure_metadata_requested_read_index(
        &self,
        operation: &str,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<MetadataRaftReadiness, ObjectError> {
        if self.raft.commit_index() < min_commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "{} metadata control commit index is not satisfied: committed={} required={}",
                operation,
                self.raft.commit_index(),
                min_commit_index
            )));
        }
        if self.store.last_applied_index() < min_read_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "{} metadata control read index is not satisfied: applied={} required={}",
                operation,
                self.store.last_applied_index(),
                min_read_index
            )));
        }
        let readiness = self.metadata_raft_readiness(min_read_index, now_ms);
        if !readiness.readable {
            return Err(ObjectError::PreconditionFailed(format!(
                "{} metadata control proof is not readable: reason={} applied={} required={}",
                operation,
                readiness.reason,
                readiness.applied_commit_index,
                readiness.min_read_index
            )));
        }
        Ok(readiness)
    }

    fn metadata_read_options_committed_at(
        &self,
        operation: &str,
        now_ms: u64,
        min_commit_index: u64,
    ) -> Result<MetadataReadOptions, ObjectError> {
        let readiness = self.ensure_metadata_read_index_committed_at(
            operation,
            now_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        Ok(MetadataReadOptions {
            min_read_index: readiness.min_read_index,
            now_ms,
            ..MetadataReadOptions::default()
        })
    }
}
