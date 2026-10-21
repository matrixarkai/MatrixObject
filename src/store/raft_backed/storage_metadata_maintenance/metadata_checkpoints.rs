// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_commit_meta_change_checkpoint(
        &mut self,
        checkpoint: MetadataChangeCheckpoint,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::CommitMetaChangeCheckpoint { checkpoint })
    }

    pub fn prepare_commit_meta_change_checkpoint_raft_entry(
        &self,
        checkpoint: MetadataChangeCheckpoint,
        idempotency_key: &str,
    ) -> Result<PreparedMetadataChangeCheckpoint, ObjectError> {
        self.store.prepare_metadata_change_checkpoint_raft_entry(
            checkpoint,
            idempotency_key,
            self.store.last_applied_index(),
        )
    }

    pub fn prepare_commit_meta_change_checkpoint_raft_entry_with_read_proof_linearizable_fenced(
        &self,
        checkpoint: MetadataChangeCheckpoint,
        idempotency_key: &str,
        expected_metadata_control_proof_checksum: &str,
    ) -> Result<ProvenMetadataRead<PreparedMetadataChangeCheckpoint>, ObjectError> {
        self.prepare_commit_meta_change_checkpoint_raft_entry_with_read_proof_linearizable_fenced_committed_at(
            checkpoint,
            idempotency_key,
            expected_metadata_control_proof_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prepare_commit_meta_change_checkpoint_raft_entry_with_read_proof_linearizable_fenced_committed_at(
        &self,
        checkpoint: MetadataChangeCheckpoint,
        idempotency_key: &str,
        expected_metadata_control_proof_checksum: &str,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<PreparedMetadataChangeCheckpoint>, ObjectError> {
        let now_ms = checkpoint.update_time_ms;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "prepare proven metadata change checkpoint",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        let expected_metadata_control_proof_checksum =
            if expected_metadata_control_proof_checksum.is_empty() {
                readiness.metadata_control_proof_checksum.as_str()
            } else {
                expected_metadata_control_proof_checksum
            };
        let prepared =
            self.prepare_commit_meta_change_checkpoint_raft_entry(checkpoint, idempotency_key)?;
        if prepared.metadata_control_proof_checksum != expected_metadata_control_proof_checksum {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata change checkpoint control proof checksum mismatch".to_string(),
            ));
        }
        Ok(proven_metadata_read(prepared, &readiness))
    }

    pub fn propose_prepared_meta_change_checkpoint(
        &mut self,
        prepared: PreparedMetadataChangeCheckpoint,
    ) -> Result<ObjectMetadata, ObjectError> {
        if self.raft.commit_index() != self.store.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata change checkpoint requires raft/store commit alignment"
                    .to_string(),
            ));
        }
        self.store
            .clone()
            .apply_command(ObjectCommand::ApplyPreparedMetaChangeCheckpoint {
                prepared: prepared.clone(),
            })?;
        let entry = self
            .raft
            .append(ObjectCommand::ApplyPreparedMetaChangeCheckpoint { prepared })?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata change checkpoint raft index is not contiguous".to_string(),
            ));
        }
        self.store.apply_raft_entry(&entry)
    }

    pub fn propose_proven_meta_change_checkpoint_fenced(
        &mut self,
        proven_prepared: ProvenMetadataRead<PreparedMetadataChangeCheckpoint>,
        expected_metadata_control_proof_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_metadata_control_proof_checksum =
            expected_metadata_control_proof_checksum.as_ref();
        if expected_metadata_control_proof_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven metadata change checkpoint requires expected metadata control proof checksum"
                    .to_string(),
            ));
        }
        if proven_prepared.value.metadata_control_proof_checksum
            != expected_metadata_control_proof_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven metadata change checkpoint control proof checksum mismatch".to_string(),
            ));
        }
        let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
        if expected_raft_entry_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven metadata change checkpoint requires expected raft entry checksum"
                    .to_string(),
            ));
        }
        proven_prepared
            .value
            .verify_expected_raft_entry_checksum(expected_raft_entry_checksum)?;
        if self.store.last_metadata_commit_index() < proven_prepared.proof.metadata_commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven metadata change checkpoint proof is not applied: applied={} required={}",
                self.store.last_metadata_commit_index(),
                proven_prepared.proof.metadata_commit_index
            )));
        }
        self.propose_prepared_meta_change_checkpoint(proven_prepared.value)
    }

    pub fn propose_commit_meta_change_checkpoint_prepared(
        &mut self,
        checkpoint: MetadataChangeCheckpoint,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared =
            self.prepare_commit_meta_change_checkpoint_raft_entry(checkpoint, idempotency_key)?;
        self.propose_prepared_meta_change_checkpoint(prepared)
    }

}
