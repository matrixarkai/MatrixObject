// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn prepare_object_metadata_commit_raft_entry(
        &self,
        current_key: &str,
        current_value: &str,
        expected_current_version: u64,
        bucket: &str,
        key: &str,
        object_value: &str,
        delete_object: bool,
        expected_object_version: u64,
        lease_owner: Option<&str>,
        lease_epoch: Option<u64>,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<PreparedObjectMetadataCommit, ObjectError> {
        self.store.prepare_object_metadata_commit_raft_entry(
            current_key,
            current_value,
            expected_current_version,
            bucket,
            key,
            object_value,
            delete_object,
            expected_object_version,
            lease_owner,
            lease_epoch,
            now_ms,
            idempotency_key,
            self.store.last_applied_index(),
        )
    }

    pub fn prepare_object_metadata_commit_raft_entry_with_read_proof_linearizable_fenced(
        &self,
        current_key: &str,
        current_value: &str,
        expected_current_version: u64,
        bucket: &str,
        key: &str,
        object_value: &str,
        delete_object: bool,
        expected_object_version: u64,
        lease_owner: Option<&str>,
        lease_epoch: Option<u64>,
        now_ms: u64,
        idempotency_key: &str,
        expected_metadata_control_proof_checksum: &str,
    ) -> Result<ProvenMetadataRead<PreparedObjectMetadataCommit>, ObjectError> {
        self.prepare_object_metadata_commit_raft_entry_with_read_proof_linearizable_fenced_committed_at(
            current_key,
            current_value,
            expected_current_version,
            bucket,
            key,
            object_value,
            delete_object,
            expected_object_version,
            lease_owner,
            lease_epoch,
            now_ms,
            idempotency_key,
            expected_metadata_control_proof_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prepare_object_metadata_commit_raft_entry_with_read_proof_linearizable_fenced_committed_at(
        &self,
        current_key: &str,
        current_value: &str,
        expected_current_version: u64,
        bucket: &str,
        key: &str,
        object_value: &str,
        delete_object: bool,
        expected_object_version: u64,
        lease_owner: Option<&str>,
        lease_epoch: Option<u64>,
        now_ms: u64,
        idempotency_key: &str,
        expected_metadata_control_proof_checksum: &str,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<PreparedObjectMetadataCommit>, ObjectError> {
        let readiness = self.ensure_metadata_read_index_committed_at(
            "prepare proven object metadata commit",
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
        let prepared = self.prepare_object_metadata_commit_raft_entry(
            current_key,
            current_value,
            expected_current_version,
            bucket,
            key,
            object_value,
            delete_object,
            expected_object_version,
            lease_owner,
            lease_epoch,
            now_ms,
            idempotency_key,
        )?;
        prepared.verify_prepared_object_metadata_commit()?;
        if prepared.batch.metadata_control_proof_checksum
            != expected_metadata_control_proof_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "prepared object metadata commit control proof checksum mismatch".to_string(),
            ));
        }
        Ok(proven_metadata_read(prepared, &readiness))
    }

    pub fn propose_prepared_object_metadata_commit(
        &mut self,
        prepared: PreparedObjectMetadataCommit,
    ) -> Result<ObjectMetadata, ObjectError> {
        prepared.verify_prepared_object_metadata_commit()?;
        if self.raft.commit_index() != self.store.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "prepared object metadata commit requires raft/store commit alignment".to_string(),
            ));
        }
        self.store
            .clone()
            .apply_prepared_object_metadata_commit(prepared.clone())?;
        let entry = self.raft.append(ObjectCommand::ApplyPreparedMetaBatch {
            prepared: prepared.batch,
        })?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::PreconditionFailed(
                "prepared object metadata commit raft index is not contiguous".to_string(),
            ));
        }
        self.store.apply_raft_entry(&entry)
    }

    pub fn propose_proven_object_metadata_commit_fenced(
        &mut self,
        proven_prepared: ProvenMetadataRead<PreparedObjectMetadataCommit>,
        expected_metadata_control_proof_checksum: impl AsRef<str>,
        expected_object_commit_checksum: impl AsRef<str>,
        expected_batch_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_metadata_control_proof_checksum =
            expected_metadata_control_proof_checksum.as_ref();
        if expected_metadata_control_proof_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven object metadata commit requires expected metadata control proof checksum"
                    .to_string(),
            ));
        }
        let expected_object_commit_checksum = expected_object_commit_checksum.as_ref();
        if expected_object_commit_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven object metadata commit requires expected checksum".to_string(),
            ));
        }
        let expected_batch_raft_entry_checksum = expected_batch_raft_entry_checksum.as_ref();
        if expected_batch_raft_entry_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven object metadata commit requires expected batch raft entry checksum"
                    .to_string(),
            ));
        }
        proven_prepared.value.verify_expected_fences(
            expected_metadata_control_proof_checksum,
            expected_object_commit_checksum,
            expected_batch_raft_entry_checksum,
            "",
        )?;
        if self.store.last_metadata_commit_index() < proven_prepared.proof.metadata_commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven object metadata commit proof is not applied: applied={} required={}",
                self.store.last_metadata_commit_index(),
                proven_prepared.proof.metadata_commit_index
            )));
        }
        self.propose_prepared_object_metadata_commit(proven_prepared.value)
    }

    pub fn propose_proven_object_metadata_commit_strict_fenced(
        &mut self,
        proven_prepared: ProvenMetadataRead<PreparedObjectMetadataCommit>,
        expected_metadata_control_proof_checksum: impl AsRef<str>,
        expected_object_commit_checksum: impl AsRef<str>,
        expected_batch_raft_entry_checksum: impl AsRef<str>,
        expected_object_request_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_object_request_checksum = expected_object_request_checksum.as_ref();
        if expected_object_request_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "strict proven object metadata commit requires expected object request checksum"
                    .to_string(),
            ));
        }
        proven_prepared
            .value
            .verify_expected_object_request_checksum(expected_object_request_checksum)?;
        self.propose_proven_object_metadata_commit_fenced(
            proven_prepared,
            expected_metadata_control_proof_checksum,
            expected_object_commit_checksum,
            expected_batch_raft_entry_checksum,
        )
    }

    pub fn propose_object_metadata_commit_prepared(
        &mut self,
        current_key: &str,
        current_value: &str,
        expected_current_version: u64,
        bucket: &str,
        key: &str,
        object_value: &str,
        delete_object: bool,
        expected_object_version: u64,
        lease_owner: Option<&str>,
        lease_epoch: Option<u64>,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_object_metadata_commit_raft_entry(
            current_key,
            current_value,
            expected_current_version,
            bucket,
            key,
            object_value,
            delete_object,
            expected_object_version,
            lease_owner,
            lease_epoch,
            now_ms,
            idempotency_key,
        )?;
        self.propose_prepared_object_metadata_commit(prepared)
    }

    pub fn propose_commit_metadata_batch_prepared(
        &mut self,
        mutations: Vec<MetadataMutation>,
        lease_owner: Option<String>,
        lease_epoch: Option<u64>,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_commit_metadata_batch_raft_entry(
            mutations,
            lease_owner.as_deref(),
            lease_epoch,
            now_ms,
            idempotency_key,
        )?;
        self.propose_prepared_metadata_batch(prepared)
    }

    pub fn propose_commit_metadata_batch_prepared_bounded(
        &mut self,
        mutations: Vec<MetadataMutation>,
        max_mutations: usize,
        max_payload_bytes: u64,
        lease_owner: Option<String>,
        lease_epoch: Option<u64>,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_commit_metadata_batch_raft_entry_bounded(
            mutations,
            max_mutations,
            max_payload_bytes,
            lease_owner.as_deref(),
            lease_epoch,
            now_ms,
            idempotency_key,
        )?;
        self.propose_prepared_metadata_batch(prepared)
    }

}
