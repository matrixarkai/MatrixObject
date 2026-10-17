impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_prepared_meta_coordinator_session_expiry(
        &mut self,
        prepared: PreparedMetadataCoordinatorSessionExpiry,
    ) -> Result<ObjectMetadata, ObjectError> {
        if self.raft.commit_index() != self.store.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata coordinator session expiry requires raft/store commit alignment"
                    .to_string(),
            ));
        }
        self.store.clone().apply_command(
            ObjectCommand::ApplyPreparedMetaCoordinatorSessionExpiry {
                prepared: prepared.clone(),
            },
        )?;
        let entry = self
            .raft
            .append(ObjectCommand::ApplyPreparedMetaCoordinatorSessionExpiry { prepared })?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata coordinator session expiry raft index is not contiguous"
                    .to_string(),
            ));
        }
        self.store.apply_raft_entry(&entry)
    }

    pub fn propose_proven_meta_coordinator_session_expiry_envelope_fenced(
        &mut self,
        proven_envelope: ProvenMetadataRead<PreparedMetadataCoordinatorSessionExpiryEnvelope>,
        prepared: PreparedMetadataCoordinatorSessionExpiry,
        expected_metadata_control_proof_checksum: impl AsRef<str>,
        expected_envelope_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_metadata_control_proof_checksum =
            expected_metadata_control_proof_checksum.as_ref();
        if expected_metadata_control_proof_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven metadata coordinator session expiry requires expected metadata control proof checksum"
                    .to_string(),
            ));
        }
        if proven_envelope.value.metadata_control_proof_checksum
            != expected_metadata_control_proof_checksum
            || prepared.metadata_control_proof_checksum != expected_metadata_control_proof_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven metadata coordinator session expiry control proof checksum mismatch"
                    .to_string(),
            ));
        }
        let expected_envelope_checksum = expected_envelope_checksum.as_ref();
        if expected_envelope_checksum.is_empty()
            || proven_envelope.value.envelope_checksum != expected_envelope_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven metadata coordinator session expiry envelope checksum mismatch".to_string(),
            ));
        }
        let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
        if expected_raft_entry_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven metadata coordinator session expiry requires expected raft entry checksum"
                    .to_string(),
            ));
        }
        if proven_envelope.value.raft_entry_checksum != expected_raft_entry_checksum {
            return Err(ObjectError::PreconditionFailed(
                "proven metadata coordinator session expiry raft entry checksum mismatch"
                    .to_string(),
            ));
        }
        prepared.verify_expected_raft_entry_checksum(expected_raft_entry_checksum)?;
        if self.store.last_metadata_commit_index() < proven_envelope.proof.metadata_commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven metadata coordinator session expiry proof is not applied: applied={} required={}",
                self.store.last_metadata_commit_index(),
                proven_envelope.proof.metadata_commit_index
            )));
        }
        let actual_envelope = prepared_metadata_coordinator_session_expiry_envelope(&prepared)?;
        if actual_envelope != proven_envelope.value {
            return Err(ObjectError::PreconditionFailed(
                "proven metadata coordinator session expiry prepared image mismatch".to_string(),
            ));
        }
        self.propose_prepared_meta_coordinator_session_expiry(prepared)
    }

    pub fn propose_expire_meta_coordinator_sessions_prepared(
        &mut self,
        now_ms: u64,
        limit: usize,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_expire_meta_coordinator_sessions_prepared_including_fenced(
            now_ms, limit, false,
        )
    }

    pub fn propose_expire_meta_coordinator_sessions_prepared_including_fenced(
        &mut self,
        now_ms: u64,
        limit: usize,
        include_fenced: bool,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_expire_meta_coordinator_sessions_raft_entry_including_fenced(
            now_ms,
            limit,
            include_fenced,
        )?;
        self.propose_prepared_meta_coordinator_session_expiry(prepared)
    }

    pub fn propose_expire_meta_coordinator_sessions_prepared_bounded(
        &mut self,
        now_ms: u64,
        limit: usize,
        max_payload_bytes: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_expire_meta_coordinator_sessions_prepared_bounded_including_fenced(
            now_ms,
            limit,
            max_payload_bytes,
            false,
        )
    }

    pub fn propose_expire_meta_coordinator_sessions_prepared_bounded_including_fenced(
        &mut self,
        now_ms: u64,
        limit: usize,
        max_payload_bytes: u64,
        include_fenced: bool,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self
            .prepare_expire_meta_coordinator_sessions_raft_entry_bounded_including_fenced(
                now_ms,
                limit,
                max_payload_bytes,
                include_fenced,
            )?;
        self.propose_prepared_meta_coordinator_session_expiry(prepared)
    }
}
