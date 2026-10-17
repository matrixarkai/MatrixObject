impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn prepare_metadata_coordinator_config_raft_entry(
        &self,
        config: MetadataCoordinatorConfig,
        idempotency_key: &str,
    ) -> Result<PreparedMetadataCoordinatorConfig, ObjectError> {
        self.store.prepare_metadata_coordinator_config_raft_entry(
            config,
            idempotency_key,
            self.store.last_applied_index(),
        )
    }

    pub fn prepare_metadata_coordinator_config_raft_entry_fenced(
        &self,
        config: MetadataCoordinatorConfig,
        idempotency_key: &str,
        expected_metadata_control_proof_checksum: &str,
        now_ms: u64,
    ) -> Result<PreparedMetadataCoordinatorConfig, ObjectError> {
        self.store
            .prepare_metadata_coordinator_config_raft_entry_fenced(
                config,
                idempotency_key,
                self.store.last_applied_index(),
                expected_metadata_control_proof_checksum,
                now_ms,
            )
    }

    pub fn propose_prepared_metadata_coordinator_config(
        &mut self,
        prepared: PreparedMetadataCoordinatorConfig,
    ) -> Result<ObjectMetadata, ObjectError> {
        if self.raft.commit_index() != self.store.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata coordinator config requires raft/store commit alignment"
                    .to_string(),
            ));
        }
        self.store
            .clone()
            .apply_command(ObjectCommand::ApplyPreparedMetaCoordinator {
                prepared: prepared.clone(),
            })?;
        let entry = self
            .raft
            .append(ObjectCommand::ApplyPreparedMetaCoordinator { prepared })?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata coordinator config raft index is not contiguous".to_string(),
            ));
        }
        self.store.apply_raft_entry(&entry)
    }

    pub fn prepare_metadata_coordinator_config_raft_entry_with_read_proof_linearizable_fenced(
        &self,
        config: MetadataCoordinatorConfig,
        idempotency_key: &str,
        expected_metadata_control_proof_checksum: &str,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<PreparedMetadataCoordinatorConfig>, ObjectError> {
        self.prepare_metadata_coordinator_config_raft_entry_with_read_proof_linearizable_fenced_committed_at(
            config,
            idempotency_key,
            expected_metadata_control_proof_checksum,
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prepare_metadata_coordinator_config_raft_entry_with_read_proof_linearizable_fenced_committed_at(
        &self,
        config: MetadataCoordinatorConfig,
        idempotency_key: &str,
        expected_metadata_control_proof_checksum: &str,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<PreparedMetadataCoordinatorConfig>, ObjectError> {
        let readiness = self.ensure_metadata_read_index_committed_at(
            "prepare proven metadata coordinator config",
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
        let prepared = self.prepare_metadata_coordinator_config_raft_entry_fenced(
            config,
            idempotency_key,
            expected_metadata_control_proof_checksum,
            now_ms,
        )?;
        Ok(proven_metadata_read(prepared, &readiness))
    }

    pub fn propose_prepared_metadata_coordinator_config_linearizable(
        &mut self,
        prepared: PreparedMetadataCoordinatorConfig,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_metadata_read_index("propose prepared metadata coordinator config", now_ms)?;
        if prepared.metadata_control_proof_checksum.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "prepared metadata coordinator config requires a metadata control proof checksum"
                    .to_string(),
            ));
        }
        self.propose_prepared_metadata_coordinator_config(prepared)
    }

    pub fn propose_proven_metadata_coordinator_config_fenced(
        &mut self,
        proven_prepared: ProvenMetadataRead<PreparedMetadataCoordinatorConfig>,
        expected_metadata_control_proof_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_metadata_control_proof_checksum =
            expected_metadata_control_proof_checksum.as_ref();
        if expected_metadata_control_proof_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven metadata coordinator config requires expected metadata control proof checksum"
                    .to_string(),
            ));
        }
        if proven_prepared.value.metadata_control_proof_checksum
            != expected_metadata_control_proof_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven metadata coordinator config control proof checksum mismatch".to_string(),
            ));
        }
        let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
        if expected_raft_entry_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven metadata coordinator config requires expected raft entry checksum"
                    .to_string(),
            ));
        }
        proven_prepared
            .value
            .verify_expected_raft_entry_checksum(expected_raft_entry_checksum)?;
        if self.store.last_metadata_commit_index() < proven_prepared.proof.metadata_commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven metadata coordinator config proof is not applied: applied={} required={}",
                self.store.last_metadata_commit_index(),
                proven_prepared.proof.metadata_commit_index
            )));
        }
        let now_ms = proven_prepared.value.metadata_control_proof_now_ms;
        self.propose_prepared_metadata_coordinator_config_linearizable(
            proven_prepared.value,
            now_ms,
        )
    }

    pub fn propose_configure_meta_coordinator_prepared(
        &mut self,
        config: MetadataCoordinatorConfig,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared =
            self.prepare_metadata_coordinator_config_raft_entry(config, idempotency_key)?;
        self.propose_prepared_metadata_coordinator_config(prepared)
    }

}
