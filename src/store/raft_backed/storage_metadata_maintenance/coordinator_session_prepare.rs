// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn prepare_meta_coordinator_session_raft_entry(
        &self,
        operation: &str,
        session_id: &str,
        owner: &str,
        endpoint: &str,
        epoch: u64,
        ttl_ms: u64,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<PreparedMetadataCoordinatorSession, ObjectError> {
        self.store.prepare_meta_coordinator_session_raft_entry(
            operation,
            session_id,
            owner,
            endpoint,
            epoch,
            ttl_ms,
            now_ms,
            idempotency_key,
            self.store.last_applied_index(),
        )
    }

    pub fn prepare_meta_coordinator_session_raft_entry_with_read_proof_linearizable_fenced(
        &self,
        operation: &str,
        session_id: &str,
        owner: &str,
        endpoint: &str,
        epoch: u64,
        ttl_ms: u64,
        now_ms: u64,
        idempotency_key: &str,
        expected_metadata_control_proof_checksum: &str,
    ) -> Result<ProvenMetadataRead<PreparedMetadataCoordinatorSession>, ObjectError> {
        self.prepare_meta_coordinator_session_raft_entry_with_read_proof_linearizable_fenced_committed_at(
            operation,
            session_id,
            owner,
            endpoint,
            epoch,
            ttl_ms,
            now_ms,
            idempotency_key,
            expected_metadata_control_proof_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prepare_meta_coordinator_session_raft_entry_with_read_proof_linearizable_fenced_committed_at(
        &self,
        operation: &str,
        session_id: &str,
        owner: &str,
        endpoint: &str,
        epoch: u64,
        ttl_ms: u64,
        now_ms: u64,
        idempotency_key: &str,
        expected_metadata_control_proof_checksum: &str,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<PreparedMetadataCoordinatorSession>, ObjectError> {
        let readiness = self.ensure_metadata_read_index_committed_at(
            "prepare proven metadata coordinator session",
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
        let prepared = self
            .store
            .prepare_meta_coordinator_session_raft_entry_fenced(
                operation,
                session_id,
                owner,
                endpoint,
                epoch,
                ttl_ms,
                now_ms,
                idempotency_key,
                self.store.last_applied_index(),
                expected_metadata_control_proof_checksum,
            )?;
        Ok(proven_metadata_read(prepared, &readiness))
    }

    pub fn propose_prepared_meta_coordinator_session(
        &mut self,
        prepared: PreparedMetadataCoordinatorSession,
    ) -> Result<ObjectMetadata, ObjectError> {
        if self.raft.commit_index() != self.store.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata coordinator session requires raft/store commit alignment"
                    .to_string(),
            ));
        }
        self.store
            .clone()
            .apply_command(ObjectCommand::ApplyPreparedMetaCoordinatorSession {
                prepared: prepared.clone(),
            })?;
        let entry = self
            .raft
            .append(ObjectCommand::ApplyPreparedMetaCoordinatorSession { prepared })?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata coordinator session raft index is not contiguous".to_string(),
            ));
        }
        self.store.apply_raft_entry(&entry)
    }

    pub fn propose_prepared_meta_coordinator_session_linearizable(
        &mut self,
        prepared: PreparedMetadataCoordinatorSession,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_metadata_read_index("propose prepared metadata coordinator session", now_ms)?;
        if prepared.metadata_control_proof_checksum.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "prepared metadata coordinator session requires a metadata control proof checksum"
                    .to_string(),
            ));
        }
        self.propose_prepared_meta_coordinator_session(prepared)
    }

    pub fn propose_proven_meta_coordinator_session_fenced(
        &mut self,
        proven_prepared: ProvenMetadataRead<PreparedMetadataCoordinatorSession>,
        expected_metadata_control_proof_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_metadata_control_proof_checksum =
            expected_metadata_control_proof_checksum.as_ref();
        if expected_metadata_control_proof_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven metadata coordinator session requires expected metadata control proof checksum"
                    .to_string(),
            ));
        }
        if proven_prepared.value.metadata_control_proof_checksum
            != expected_metadata_control_proof_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven metadata coordinator session control proof checksum mismatch".to_string(),
            ));
        }
        let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
        if expected_raft_entry_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven metadata coordinator session requires expected raft entry checksum"
                    .to_string(),
            ));
        }
        proven_prepared
            .value
            .verify_expected_raft_entry_checksum(expected_raft_entry_checksum)?;
        if self.store.last_metadata_commit_index() < proven_prepared.proof.metadata_commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven metadata coordinator session proof is not applied: applied={} required={}",
                self.store.last_metadata_commit_index(),
                proven_prepared.proof.metadata_commit_index
            )));
        }
        let now_ms = proven_prepared.value.metadata_control_proof_now_ms;
        self.propose_prepared_meta_coordinator_session_linearizable(proven_prepared.value, now_ms)
    }

    pub fn propose_register_meta_coordinator_session_prepared(
        &mut self,
        session_id: &str,
        owner: &str,
        endpoint: &str,
        ttl_ms: u64,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_meta_coordinator_session_raft_entry(
            "register",
            session_id,
            owner,
            endpoint,
            0,
            ttl_ms,
            now_ms,
            idempotency_key,
        )?;
        self.propose_prepared_meta_coordinator_session(prepared)
    }

    pub fn propose_renew_meta_coordinator_session_prepared(
        &mut self,
        session_id: &str,
        owner: &str,
        epoch: u64,
        ttl_ms: u64,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_meta_coordinator_session_raft_entry(
            "renew",
            session_id,
            owner,
            "",
            epoch,
            ttl_ms,
            now_ms,
            idempotency_key,
        )?;
        self.propose_prepared_meta_coordinator_session(prepared)
    }

    pub fn propose_release_meta_coordinator_session_prepared(
        &mut self,
        session_id: &str,
        owner: &str,
        epoch: u64,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_meta_coordinator_session_raft_entry(
            "release",
            session_id,
            owner,
            "",
            epoch,
            0,
            0,
            idempotency_key,
        )?;
        self.propose_prepared_meta_coordinator_session(prepared)
    }

}
