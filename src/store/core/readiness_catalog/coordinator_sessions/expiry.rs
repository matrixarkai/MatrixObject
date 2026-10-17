impl MatrixObjectStore {
    pub fn expire_meta_coordinator_sessions(
        &mut self,
        now_ms: u64,
        limit: usize,
    ) -> Result<Vec<CoordinatorSession>, ObjectError> {
        self.expire_meta_coordinator_sessions_including_fenced(now_ms, limit, false)
    }

    pub fn expire_meta_coordinator_sessions_including_fenced(
        &mut self,
        now_ms: u64,
        limit: usize,
        include_fenced: bool,
    ) -> Result<Vec<CoordinatorSession>, ObjectError> {
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let expired = self
            .meta_service
            .expire_coordinator_sessions_including_fenced(now_ms, limit, include_fenced);
        if !expired.is_empty() {
            self.invalidate_metadata_route_cache();
            if self.options.persistent_snapshot_flush_on_commit {
                if let Err(err) = self.flush_persistent_snapshot() {
                    if let Some(rollback) = rollback {
                        self.restore_object_mutation_rollback(rollback);
                    }
                    return Err(err);
                }
            }
            if let Err(err) = self.append_meta_coordinator_session_expiry_journal_record(
                now_ms,
                limit,
                include_fenced,
            ) {
                if let Some(rollback) = rollback {
                    self.restore_object_mutation_rollback(rollback);
                }
                return Err(err);
            }
        }
        Ok(expired)
    }

    pub fn prepare_meta_coordinator_session_expiry_raft_entry(
        &self,
        now_ms: u64,
        limit: usize,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataCoordinatorSessionExpiry, ObjectError> {
        self.prepare_meta_coordinator_session_expiry_raft_entry_including_fenced(
            now_ms,
            limit,
            false,
            expected_previous_commit_index,
        )
    }

    pub fn prepare_meta_coordinator_session_expiry_raft_entry_including_fenced(
        &self,
        now_ms: u64,
        limit: usize,
        include_fenced: bool,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataCoordinatorSessionExpiry, ObjectError> {
        self.meta_service
            .prepare_coordinator_session_expiry_raft_entry_including_fenced(
                now_ms,
                limit,
                include_fenced,
                expected_previous_commit_index,
            )
    }

    pub fn prepare_meta_coordinator_session_expiry_raft_entry_bounded(
        &self,
        now_ms: u64,
        limit: usize,
        max_payload_bytes: u64,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataCoordinatorSessionExpiry, ObjectError> {
        self.prepare_meta_coordinator_session_expiry_raft_entry_bounded_including_fenced(
            now_ms,
            limit,
            max_payload_bytes,
            false,
            expected_previous_commit_index,
        )
    }

    pub fn prepare_meta_coordinator_session_expiry_raft_entry_bounded_including_fenced(
        &self,
        now_ms: u64,
        limit: usize,
        max_payload_bytes: u64,
        include_fenced: bool,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataCoordinatorSessionExpiry, ObjectError> {
        self.meta_service
            .prepare_coordinator_session_expiry_raft_entry_bounded_including_fenced(
                now_ms,
                limit,
                max_payload_bytes,
                include_fenced,
                expected_previous_commit_index,
            )
    }

    pub fn apply_prepared_meta_coordinator_session_expiry(
        &mut self,
        prepared: PreparedMetadataCoordinatorSessionExpiry,
    ) -> Result<Vec<CoordinatorSession>, ObjectError> {
        let expired = self
            .meta_service
            .apply_prepared_coordinator_session_expiry(prepared)?;
        if !expired.is_empty() {
            self.invalidate_metadata_route_cache();
        }
        Ok(expired)
    }

}
