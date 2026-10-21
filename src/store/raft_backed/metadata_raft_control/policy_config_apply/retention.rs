// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_prepared_meta_retention_config(
        &mut self,
        prepared: PreparedMetadataRetentionConfig,
    ) -> Result<ObjectMetadata, ObjectError> {
        if self.raft.commit_index() != self.store.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata retention requires raft/store commit alignment".to_string(),
            ));
        }
        if !prepared.metadata_control_proof_checksum.is_empty() {
            self.store.check_expected_metadata_control_proof(
                &prepared.metadata_control_proof_checksum,
                prepared.metadata_control_proof_now_ms,
            )?;
        }
        self.store
            .clone()
            .apply_command(ObjectCommand::ApplyPreparedMetaRetentionConfig {
                prepared: prepared.clone(),
            })?;
        let entry = self
            .raft
            .append(ObjectCommand::ApplyPreparedMetaRetentionConfig { prepared })?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata retention raft index is not contiguous".to_string(),
            ));
        }
        self.store.apply_raft_entry(&entry)
    }

    pub fn propose_prepared_meta_retention_config_linearizable(
        &mut self,
        prepared: PreparedMetadataRetentionConfig,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_metadata_read_index("propose prepared metadata retention", now_ms)?;
        if prepared.metadata_control_proof_checksum.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "prepared metadata retention requires a metadata control proof checksum"
                    .to_string(),
            ));
        }
        self.propose_prepared_meta_retention_config(prepared)
    }

    pub fn propose_proven_meta_retention_config_fenced(
        &mut self,
        proven_prepared: ProvenMetadataRead<PreparedMetadataRetentionConfig>,
        expected_metadata_control_proof_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_metadata_control_proof_checksum =
            expected_metadata_control_proof_checksum.as_ref();
        if expected_metadata_control_proof_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven metadata retention requires expected metadata control proof checksum"
                    .to_string(),
            ));
        }
        if proven_prepared.value.metadata_control_proof_checksum
            != expected_metadata_control_proof_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven metadata retention control proof checksum mismatch".to_string(),
            ));
        }
        let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
        if expected_raft_entry_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven metadata retention requires expected raft entry checksum".to_string(),
            ));
        }
        proven_prepared
            .value
            .verify_expected_raft_entry_checksum(expected_raft_entry_checksum)?;
        if self.store.last_metadata_commit_index() < proven_prepared.proof.metadata_commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven metadata retention proof is not applied: applied={} required={}",
                self.store.last_metadata_commit_index(),
                proven_prepared.proof.metadata_commit_index
            )));
        }
        let now_ms = proven_prepared.value.metadata_control_proof_now_ms;
        self.propose_prepared_meta_retention_config_linearizable(proven_prepared.value, now_ms)
    }

}
