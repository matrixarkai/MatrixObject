// SPDX-License-Identifier: Apache-2.0
impl MatrixMetaService {
    pub fn prepare_coordinator_session_expiry_raft_entry(
        &self,
        now_ms: u64,
        limit: usize,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataCoordinatorSessionExpiry, ObjectError> {
        self.prepare_coordinator_session_expiry_raft_entry_including_fenced(
            now_ms,
            limit,
            false,
            expected_previous_commit_index,
        )
    }

    pub fn prepare_coordinator_session_expiry_raft_entry_including_fenced(
        &self,
        now_ms: u64,
        limit: usize,
        include_fenced: bool,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataCoordinatorSessionExpiry, ObjectError> {
        if expected_previous_commit_index != self.next_commit_index.saturating_sub(1) {
            return Err(ObjectError::PreconditionFailed(
                "metadata coordinator session expiry prepare previous commit fence mismatch"
                    .to_string(),
            ));
        }
        let expired =
            self.plan_expired_coordinator_sessions_including_fenced(now_ms, limit, include_fenced);
        let request_checksum =
            metadata_coordinator_session_expiry_request_checksum(now_ms, limit, include_fenced);
        let (metadata_control_proof_checksum, metadata_control_proof_bytes) =
            self.metadata_control_proof_digest_at(now_ms);
        let mut prepared = PreparedMetadataCoordinatorSessionExpiry {
            now_ms,
            limit,
            include_fenced,
            expired,
            metadata_control_proof_format: "MOSRS-META-SERVICE-CONTROL3".to_string(),
            metadata_control_proof_checksum,
            metadata_control_proof_bytes,
            request_checksum,
            expected_previous_commit_index,
            prepared_commit_index: expected_previous_commit_index.saturating_add(1),
            raft_entry_checksum: String::new(),
        };
        prepared.raft_entry_checksum =
            prepared_metadata_coordinator_session_expiry_checksum(&prepared);
        Ok(prepared)
    }

    pub fn prepare_coordinator_session_expiry_raft_entry_bounded(
        &self,
        now_ms: u64,
        limit: usize,
        max_payload_bytes: u64,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataCoordinatorSessionExpiry, ObjectError> {
        self.prepare_coordinator_session_expiry_raft_entry_bounded_including_fenced(
            now_ms,
            limit,
            max_payload_bytes,
            false,
            expected_previous_commit_index,
        )
    }

    pub fn prepare_coordinator_session_expiry_raft_entry_bounded_including_fenced(
        &self,
        now_ms: u64,
        limit: usize,
        max_payload_bytes: u64,
        include_fenced: bool,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataCoordinatorSessionExpiry, ObjectError> {
        if max_payload_bytes == 0 {
            return Err(ObjectError::InvalidArgument(
                "metadata coordinator session expiry max_payload_bytes must be positive"
                    .to_string(),
            ));
        }
        if expected_previous_commit_index != self.next_commit_index.saturating_sub(1) {
            return Err(ObjectError::PreconditionFailed(
                "metadata coordinator session expiry prepare previous commit fence mismatch"
                    .to_string(),
            ));
        }
        let planned =
            self.plan_expired_coordinator_sessions_including_fenced(now_ms, limit, include_fenced);
        let mut payload_bytes = 0_u64;
        let mut expired = Vec::new();
        for session in planned {
            let next_payload = payload_bytes
                .checked_add(coordinator_session_payload_bytes(&session)?)
                .ok_or_else(|| {
                    ObjectError::InvalidArgument(
                        "metadata coordinator session expiry payload bytes overflow".to_string(),
                    )
                })?;
            if next_payload > max_payload_bytes {
                if expired.is_empty() {
                    return Err(ObjectError::PreconditionFailed(
                        "metadata coordinator session expiry first session exceeds max_payload_bytes"
                            .to_string(),
                    ));
                }
                break;
            }
            payload_bytes = next_payload;
            expired.push(session);
        }
        let effective_limit = if expired.is_empty() {
            limit
        } else {
            expired.len()
        };
        let request_checksum = metadata_coordinator_session_expiry_request_checksum(
            now_ms,
            effective_limit,
            include_fenced,
        );
        let (metadata_control_proof_checksum, metadata_control_proof_bytes) =
            self.metadata_control_proof_digest_at(now_ms);
        let mut prepared = PreparedMetadataCoordinatorSessionExpiry {
            now_ms,
            limit: effective_limit,
            include_fenced,
            expired,
            metadata_control_proof_format: "MOSRS-META-SERVICE-CONTROL3".to_string(),
            metadata_control_proof_checksum,
            metadata_control_proof_bytes,
            request_checksum,
            expected_previous_commit_index,
            prepared_commit_index: expected_previous_commit_index.saturating_add(1),
            raft_entry_checksum: String::new(),
        };
        prepared.raft_entry_checksum =
            prepared_metadata_coordinator_session_expiry_checksum(&prepared);
        Ok(prepared)
    }

    pub fn apply_prepared_coordinator_session_expiry(
        &mut self,
        prepared: PreparedMetadataCoordinatorSessionExpiry,
    ) -> Result<Vec<CoordinatorSession>, ObjectError> {
        let take = if prepared.limit == 0 {
            usize::MAX
        } else {
            prepared.limit
        };
        if prepared.expired.len() > take {
            return Err(ObjectError::InvalidArgument(
                "metadata prepared coordinator session expiry exceeds limit".to_string(),
            ));
        }
        let expected_checksum = prepared_metadata_coordinator_session_expiry_checksum(
            &PreparedMetadataCoordinatorSessionExpiry {
                raft_entry_checksum: String::new(),
                ..prepared.clone()
            },
        );
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(
                "metadata prepared coordinator session expiry raft entry checksum mismatch"
                    .to_string(),
            ));
        }
        if prepared.request_checksum
            != metadata_coordinator_session_expiry_request_checksum(
                prepared.now_ms,
                prepared.limit,
                prepared.include_fenced,
            )
        {
            return Err(ObjectError::Corruption(
                "metadata prepared coordinator session expiry request checksum mismatch"
                    .to_string(),
            ));
        }
        if prepared.prepared_commit_index
            != prepared.expected_previous_commit_index.saturating_add(1)
        {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared coordinator session expiry commit index must be adjacent"
                    .to_string(),
            ));
        }
        let mut seen = BTreeSet::new();
        for session in &prepared.expired {
            validate_session_id(&session.session_id)?;
            validate_meta_key(&session.owner)?;
            validate_session_endpoint(&session.endpoint)?;
            if session.expire_time_ms > prepared.now_ms && !prepared.include_fenced {
                return Err(ObjectError::Corruption(
                    "metadata prepared coordinator session expiry contains live unfenced session"
                        .to_string(),
                ));
            }
            if !seen.insert(session.session_id.clone()) {
                return Err(ObjectError::Corruption(
                    "metadata prepared coordinator session expiry contains duplicate session"
                        .to_string(),
                ));
            }
        }
        if self.prepared_coordinator_session_expiry_already_applied(&prepared) {
            return Ok(prepared.expired);
        }
        if prepared.expected_previous_commit_index != self.next_commit_index.saturating_sub(1) {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared coordinator session expiry previous commit fence mismatch"
                    .to_string(),
            ));
        }
        self.check_expected_metadata_control_proof(
            &prepared.metadata_control_proof_checksum,
            prepared.now_ms,
        )?;
        for session in &prepared.expired {
            if !self.coordinator_session_expirable(
                session,
                prepared.now_ms,
                prepared.include_fenced,
            ) {
                return Err(ObjectError::Corruption(
                    "metadata prepared coordinator session expiry contains live unfenced session"
                        .to_string(),
                ));
            }
            let current = self
                .coordinator_sessions
                .get(&session.session_id)
                .ok_or_else(|| {
                    ObjectError::PreconditionFailed(
                        "metadata prepared coordinator session expiry session is missing"
                            .to_string(),
                    )
                })?;
            if current != session {
                return Err(ObjectError::PreconditionFailed(
                    "metadata prepared coordinator session expiry image mismatch".to_string(),
                ));
            }
        }
        let planned = self.plan_expired_coordinator_sessions_including_fenced(
            prepared.now_ms,
            prepared.limit,
            prepared.include_fenced,
        );
        if planned != prepared.expired {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared coordinator session expiry set changed before apply".to_string(),
            ));
        }
        for session in &prepared.expired {
            self.coordinator_sessions.remove(&session.session_id);
        }
        self.next_commit_index = prepared.prepared_commit_index.saturating_add(1);
        Ok(prepared.expired)
    }

    pub fn get_coordinator_session(
        &self,
        session_id: &str,
    ) -> Result<CoordinatorSession, ObjectError> {
        validate_session_id(session_id)?;
        self.coordinator_sessions
            .get(session_id)
            .cloned()
            .ok_or_else(|| ObjectError::NotFound("coordinator session missing".to_string()))
    }

    pub fn list_coordinator_sessions(&self, owner: Option<&str>) -> Vec<CoordinatorSession> {
        self.coordinator_sessions
            .values()
            .filter(|session| owner.map_or(true, |owner| session.owner == owner))
            .cloned()
            .collect()
    }

    pub fn check_shard_owner_serving(
        &self,
        key: &str,
        expected_owner: &str,
        expected_epoch: u64,
        now_ms: u64,
    ) -> Result<MetadataShard, ObjectError> {
        validate_meta_key(key)?;
        validate_meta_key(expected_owner)?;
        if expected_epoch == 0 {
            return Err(ObjectError::InvalidArgument(
                "metadata shard owner epoch must be positive".to_string(),
            ));
        }
        let shard = self.find_shard_for_key(key)?;
        if shard.draining {
            return Err(ObjectError::PreconditionFailed(
                "metadata shard is draining".to_string(),
            ));
        }
        if shard.owner_node_id != expected_owner || shard.epoch != expected_epoch {
            return Err(ObjectError::PreconditionFailed(
                "metadata shard owner fence mismatch".to_string(),
            ));
        }
        if self
            .active_fenced_owner_session(expected_owner, now_ms)
            .is_none()
        {
            return Err(ObjectError::PreconditionFailed(
                "metadata shard owner has no active endpoint-fenced coordinator session"
                    .to_string(),
            ));
        }
        Ok(shard)
    }

    fn coordinator_session_endpoint_is_fenced(&self, endpoint: &str) -> bool {
        validate_session_endpoint(endpoint).is_ok()
    }

    fn validate_coordinator_session_ttl(&self, ttl_ms: u64) -> Result<(), ObjectError> {
        if ttl_ms == 0 {
            return Err(ObjectError::InvalidArgument(
                "coordinator session ttl must be positive".to_string(),
            ));
        }
        if ttl_ms > self.coordinator_config.session_ttl_ms {
            return Err(ObjectError::InvalidArgument(
                "coordinator session ttl exceeds configured session ttl".to_string(),
            ));
        }
        Ok(())
    }

    fn validate_coordinator_session_endpoint_fence(
        &self,
        endpoint: &str,
    ) -> Result<(), ObjectError> {
        if self.coordinator_session_endpoint_is_fenced(endpoint) {
            return Ok(());
        }
        Err(ObjectError::PreconditionFailed(
            "coordinator session endpoint is invalid".to_string(),
        ))
    }

    fn active_fenced_owner_session(&self, owner: &str, now_ms: u64) -> Option<&CoordinatorSession> {
        self.coordinator_sessions
            .values()
            .filter(|session| {
                session.owner == owner
                    && session.expire_time_ms > now_ms
                    && self.coordinator_session_endpoint_is_fenced(&session.endpoint)
            })
            .max_by_key(|session| (session.epoch, session.expire_time_ms))
    }
}
