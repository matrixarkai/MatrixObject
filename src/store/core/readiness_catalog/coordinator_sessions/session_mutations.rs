impl MatrixObjectStore {
    pub fn register_meta_coordinator_session_idempotent(
        &mut self,
        session_id: &str,
        owner: &str,
        endpoint: &str,
        ttl_ms: u64,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<CoordinatorSessionReceipt, ObjectError> {
        let receipt = self.meta_service.register_coordinator_session_idempotent(
            session_id,
            owner,
            endpoint,
            ttl_ms,
            now_ms,
            idempotency_key,
        )?;
        if !receipt.deduped {
            self.invalidate_metadata_route_cache();
        }
        Ok(receipt)
    }

    pub fn renew_meta_coordinator_session(
        &mut self,
        session_id: &str,
        owner: &str,
        epoch: u64,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<CoordinatorSession, ObjectError> {
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let session = self
            .meta_service
            .renew_coordinator_session(session_id, owner, epoch, ttl_ms, now_ms)?;
        self.invalidate_metadata_route_cache();
        if self.options.persistent_snapshot_flush_on_commit {
            if let Err(err) = self.flush_persistent_snapshot() {
                if let Some(rollback) = rollback {
                    self.restore_object_mutation_rollback(rollback);
                }
                return Err(err);
            }
        }
        if let Err(err) = self.append_meta_coordinator_session_renew_journal_record(
            session_id, owner, epoch, ttl_ms, now_ms,
        ) {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(session)
    }

    pub fn renew_meta_coordinator_session_idempotent(
        &mut self,
        session_id: &str,
        owner: &str,
        epoch: u64,
        ttl_ms: u64,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<CoordinatorSessionReceipt, ObjectError> {
        let receipt = self.meta_service.renew_coordinator_session_idempotent(
            session_id,
            owner,
            epoch,
            ttl_ms,
            now_ms,
            idempotency_key,
        )?;
        if !receipt.deduped {
            self.invalidate_metadata_route_cache();
        }
        Ok(receipt)
    }

    pub fn release_meta_coordinator_session(
        &mut self,
        session_id: &str,
        owner: &str,
        epoch: u64,
    ) -> Result<CoordinatorSession, ObjectError> {
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let session = self
            .meta_service
            .release_coordinator_session(session_id, owner, epoch)?;
        self.invalidate_metadata_route_cache();
        if self.options.persistent_snapshot_flush_on_commit {
            if let Err(err) = self.flush_persistent_snapshot() {
                if let Some(rollback) = rollback {
                    self.restore_object_mutation_rollback(rollback);
                }
                return Err(err);
            }
        }
        if let Err(err) =
            self.append_meta_coordinator_session_release_journal_record(session_id, owner, epoch)
        {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(session)
    }

    pub fn release_meta_coordinator_session_idempotent(
        &mut self,
        session_id: &str,
        owner: &str,
        epoch: u64,
        idempotency_key: &str,
    ) -> Result<CoordinatorSessionReceipt, ObjectError> {
        let receipt = self.meta_service.release_coordinator_session_idempotent(
            session_id,
            owner,
            epoch,
            idempotency_key,
        )?;
        if !receipt.deduped {
            self.invalidate_metadata_route_cache();
        }
        Ok(receipt)
    }

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
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataCoordinatorSession, ObjectError> {
        self.meta_service.prepare_coordinator_session_raft_entry(
            operation,
            session_id,
            owner,
            endpoint,
            epoch,
            ttl_ms,
            now_ms,
            idempotency_key,
            expected_previous_commit_index,
        )
    }

    pub fn prepare_meta_coordinator_session_raft_entry_fenced(
        &self,
        operation: &str,
        session_id: &str,
        owner: &str,
        endpoint: &str,
        epoch: u64,
        ttl_ms: u64,
        now_ms: u64,
        idempotency_key: &str,
        expected_previous_commit_index: u64,
        expected_metadata_control_proof_checksum: &str,
    ) -> Result<PreparedMetadataCoordinatorSession, ObjectError> {
        self.check_expected_metadata_control_proof(
            expected_metadata_control_proof_checksum,
            now_ms,
        )?;
        let mut prepared = self.prepare_meta_coordinator_session_raft_entry(
            operation,
            session_id,
            owner,
            endpoint,
            epoch,
            ttl_ms,
            now_ms,
            idempotency_key,
            expected_previous_commit_index,
        )?;
        prepared.metadata_control_proof_checksum =
            expected_metadata_control_proof_checksum.to_string();
        prepared.metadata_control_proof_now_ms = now_ms;
        prepared.raft_entry_checksum =
            prepared_metadata_coordinator_session_checksum(&PreparedMetadataCoordinatorSession {
                raft_entry_checksum: String::new(),
                ..prepared.clone()
            });
        Ok(prepared)
    }

    pub fn apply_prepared_meta_coordinator_session(
        &mut self,
        prepared: PreparedMetadataCoordinatorSession,
    ) -> Result<CoordinatorSessionReceipt, ObjectError> {
        let receipt = self
            .meta_service
            .apply_prepared_coordinator_session(prepared)?;
        if !receipt.deduped {
            self.invalidate_metadata_route_cache();
        }
        Ok(receipt)
    }

}
