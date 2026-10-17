impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_acquire_metadata_lease(
        &mut self,
        namespace: &str,
        owner: &str,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::AcquireMetaLease {
            namespace: namespace.to_string(),
            owner: owner.to_string(),
            ttl_ms,
            now_ms,
        })
    }

    pub fn propose_acquire_metadata_lease_idempotent(
        &mut self,
        namespace: &str,
        owner: &str,
        ttl_ms: u64,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::AcquireMetaLeaseIdempotent {
            namespace: namespace.to_string(),
            owner: owner.to_string(),
            ttl_ms,
            now_ms,
            idempotency_key: idempotency_key.to_string(),
        })
    }

    pub fn propose_renew_metadata_lease(
        &mut self,
        namespace: &str,
        owner: &str,
        epoch: u64,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::RenewMetaLease {
            namespace: namespace.to_string(),
            owner: owner.to_string(),
            epoch,
            ttl_ms,
            now_ms,
        })
    }

    pub fn propose_renew_metadata_lease_idempotent(
        &mut self,
        namespace: &str,
        owner: &str,
        epoch: u64,
        ttl_ms: u64,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::RenewMetaLeaseIdempotent {
            namespace: namespace.to_string(),
            owner: owner.to_string(),
            epoch,
            ttl_ms,
            now_ms,
            idempotency_key: idempotency_key.to_string(),
        })
    }

    pub fn propose_release_metadata_lease(
        &mut self,
        namespace: &str,
        owner: &str,
        epoch: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::ReleaseMetaLease {
            namespace: namespace.to_string(),
            owner: owner.to_string(),
            epoch,
        })
    }

    pub fn propose_release_metadata_lease_idempotent(
        &mut self,
        namespace: &str,
        owner: &str,
        epoch: u64,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::ReleaseMetaLeaseIdempotent {
            namespace: namespace.to_string(),
            owner: owner.to_string(),
            epoch,
            idempotency_key: idempotency_key.to_string(),
        })
    }

    pub fn prepare_metadata_lease_raft_entry(
        &self,
        operation: &str,
        namespace: &str,
        owner: &str,
        epoch: u64,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<PreparedMetadataLease, ObjectError> {
        let readiness = self.ensure_metadata_read_index("prepare metadata lease", now_ms)?;
        let mut prepared = self.store.prepare_metadata_lease_raft_entry(
            operation,
            namespace,
            owner,
            epoch,
            ttl_ms,
            now_ms,
            self.store.last_applied_index(),
        )?;
        prepared.metadata_read_index = readiness.raft_log_commit_index;
        prepared.metadata_raft_term = readiness.raft_term;
        prepared.metadata_control_proof_checksum = readiness.metadata_control_proof_checksum;
        prepared.raft_entry_checksum = prepared_metadata_lease_checksum(&prepared);
        Ok(prepared)
    }

    pub fn prepare_metadata_lease_raft_entry_with_read_proof_linearizable_fenced(
        &self,
        operation: &str,
        namespace: &str,
        owner: &str,
        epoch: u64,
        ttl_ms: u64,
        now_ms: u64,
        expected_metadata_control_proof_checksum: &str,
    ) -> Result<ProvenMetadataRead<PreparedMetadataLease>, ObjectError> {
        let readiness = self.ensure_metadata_read_index("prepare proven metadata lease", now_ms)?;
        let expected_metadata_control_proof_checksum =
            if expected_metadata_control_proof_checksum.is_empty() {
                readiness.metadata_control_proof_checksum.as_str()
            } else {
                expected_metadata_control_proof_checksum
            };
        if readiness.metadata_control_proof_checksum != expected_metadata_control_proof_checksum {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata lease control proof checksum mismatch".to_string(),
            ));
        }
        let mut prepared = self.prepare_metadata_lease_raft_entry(
            operation, namespace, owner, epoch, ttl_ms, now_ms,
        )?;
        prepared.metadata_read_index = readiness.raft_log_commit_index;
        prepared.metadata_raft_term = readiness.raft_term;
        prepared.metadata_control_proof_checksum =
            expected_metadata_control_proof_checksum.to_string();
        prepared.raft_entry_checksum = prepared_metadata_lease_checksum(&prepared);
        Ok(proven_metadata_read(prepared, &readiness))
    }

    pub fn propose_prepared_metadata_lease(
        &mut self,
        prepared: PreparedMetadataLease,
    ) -> Result<ObjectMetadata, ObjectError> {
        if !prepared.metadata_control_proof_checksum.is_empty() {
            let readiness = self
                .ensure_metadata_read_index("propose prepared metadata lease", prepared.now_ms)?;
            if readiness.raft_log_commit_index != prepared.metadata_read_index
                || readiness.raft_term != prepared.metadata_raft_term
                || readiness.metadata_control_proof_checksum
                    != prepared.metadata_control_proof_checksum
            {
                return Err(ObjectError::PreconditionFailed(
                    "prepared metadata lease control proof fence mismatch".to_string(),
                ));
            }
        }
        if self.raft.commit_index() != self.store.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata lease requires raft/store commit alignment".to_string(),
            ));
        }
        self.store
            .clone()
            .apply_command(ObjectCommand::ApplyPreparedMetaLease {
                prepared: prepared.clone(),
            })?;
        let entry = self
            .raft
            .append(ObjectCommand::ApplyPreparedMetaLease { prepared })?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata lease raft index is not contiguous".to_string(),
            ));
        }
        self.store.apply_raft_entry(&entry)
    }

    pub fn propose_proven_metadata_lease_fenced(
        &mut self,
        proven_prepared: ProvenMetadataRead<PreparedMetadataLease>,
        expected_metadata_control_proof_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_metadata_control_proof_checksum =
            expected_metadata_control_proof_checksum.as_ref();
        if expected_metadata_control_proof_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven metadata lease requires expected metadata control proof checksum"
                    .to_string(),
            ));
        }
        if proven_prepared.value.metadata_control_proof_checksum
            != expected_metadata_control_proof_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven metadata lease control proof checksum mismatch".to_string(),
            ));
        }
        let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
        if expected_raft_entry_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven metadata lease requires expected raft entry checksum".to_string(),
            ));
        }
        proven_prepared
            .value
            .verify_expected_raft_entry_checksum(expected_raft_entry_checksum)?;
        if self.store.last_metadata_commit_index() < proven_prepared.proof.metadata_commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven metadata lease proof is not applied: applied={} required={}",
                self.store.last_metadata_commit_index(),
                proven_prepared.proof.metadata_commit_index
            )));
        }
        self.propose_prepared_metadata_lease(proven_prepared.value)
    }

    pub fn propose_acquire_metadata_lease_prepared(
        &mut self,
        namespace: &str,
        owner: &str,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared =
            self.prepare_metadata_lease_raft_entry("acquire", namespace, owner, 0, ttl_ms, now_ms)?;
        self.propose_prepared_metadata_lease(prepared)
    }

    pub fn propose_renew_metadata_lease_prepared(
        &mut self,
        namespace: &str,
        owner: &str,
        epoch: u64,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self
            .prepare_metadata_lease_raft_entry("renew", namespace, owner, epoch, ttl_ms, now_ms)?;
        self.propose_prepared_metadata_lease(prepared)
    }

    pub fn propose_release_metadata_lease_prepared(
        &mut self,
        namespace: &str,
        owner: &str,
        epoch: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared =
            self.prepare_metadata_lease_raft_entry("release", namespace, owner, epoch, 0, 0)?;
        self.propose_prepared_metadata_lease(prepared)
    }

}
