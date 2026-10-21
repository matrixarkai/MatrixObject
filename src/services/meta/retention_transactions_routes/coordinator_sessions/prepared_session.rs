// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixMetaService {
    pub fn prepare_coordinator_session_raft_entry(
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
        if expected_previous_commit_index != self.next_commit_index.saturating_sub(1) {
            return Err(ObjectError::PreconditionFailed(
                "metadata coordinator session prepare previous commit fence mismatch".to_string(),
            ));
        }
        validate_idempotency_key(idempotency_key)?;
        validate_session_id(session_id)?;
        validate_meta_key(owner)?;
        let request_checksum = metadata_coordinator_session_request_checksum(
            operation, session_id, owner, endpoint, epoch, ttl_ms, now_ms,
        );
        if let Some(existing) = self.coordinator_session_receipts.get(idempotency_key) {
            if existing.operation != operation || existing.request_checksum != request_checksum {
                return Err(ObjectError::PreconditionFailed(
                    "coordinator session idempotency key reused with different prepared request"
                        .to_string(),
                ));
            }
            let mut prepared = PreparedMetadataCoordinatorSession {
                operation: operation.to_string(),
                session: existing.session.clone(),
                released: existing.released,
                idempotency_key: idempotency_key.to_string(),
                request_checksum,
                metadata_control_proof_checksum: String::new(),
                metadata_control_proof_now_ms: 0,
                expected_previous_commit_index,
                prepared_commit_index: existing.commit_index,
                raft_entry_checksum: String::new(),
            };
            prepared.raft_entry_checksum =
                prepared_metadata_coordinator_session_checksum(&prepared);
            return Ok(prepared);
        }

        let (session, released) = match operation {
            "register" => {
                validate_session_endpoint(endpoint)?;
                self.check_coordinator_session_admission(session_id)?;
                self.validate_coordinator_session_ttl(ttl_ms)?;
                self.validate_coordinator_session_endpoint_fence(endpoint)?;
                if let Some(existing) = self.coordinator_sessions.get(session_id) {
                    if existing.expire_time_ms > now_ms && existing.owner != owner {
                        return Err(ObjectError::PreconditionFailed(
                            "coordinator session is held by another owner".to_string(),
                        ));
                    }
                }
                let next_epoch = self
                    .coordinator_sessions
                    .get(session_id)
                    .map_or(1, |session| session.epoch.saturating_add(1));
                (
                    CoordinatorSession {
                        session_id: session_id.to_string(),
                        owner: owner.to_string(),
                        endpoint: endpoint.to_string(),
                        epoch: next_epoch,
                        acquire_time_ms: now_ms,
                        last_renew_ms: now_ms,
                        expire_time_ms: now_ms.saturating_add(ttl_ms),
                        ttl_ms,
                    },
                    false,
                )
            }
            "renew" => {
                if epoch == 0 {
                    return Err(ObjectError::InvalidArgument(
                        "coordinator session renew requires epoch".to_string(),
                    ));
                }
                self.validate_coordinator_session_ttl(ttl_ms)?;
                let current = self.coordinator_sessions.get(session_id).ok_or_else(|| {
                    ObjectError::NotFound("coordinator session missing".to_string())
                })?;
                if current.owner != owner
                    || current.epoch != epoch
                    || current.expire_time_ms <= now_ms
                {
                    return Err(ObjectError::PreconditionFailed(
                        "coordinator session renew fence mismatch".to_string(),
                    ));
                }
                self.validate_coordinator_session_endpoint_fence(&current.endpoint)?;
                let mut session = current.clone();
                session.last_renew_ms = now_ms;
                session.expire_time_ms = now_ms.saturating_add(ttl_ms);
                session.ttl_ms = ttl_ms;
                (session, false)
            }
            "release" => {
                if epoch == 0 {
                    return Err(ObjectError::InvalidArgument(
                        "coordinator session release requires epoch".to_string(),
                    ));
                }
                let session = self
                    .coordinator_sessions
                    .get(session_id)
                    .cloned()
                    .ok_or_else(|| {
                        ObjectError::NotFound("coordinator session missing".to_string())
                    })?;
                if session.owner != owner || session.epoch != epoch {
                    return Err(ObjectError::PreconditionFailed(
                        "coordinator session release fence mismatch".to_string(),
                    ));
                }
                (session, true)
            }
            _ => {
                return Err(ObjectError::InvalidArgument(
                    "prepared coordinator session operation must be register, renew, or release"
                        .to_string(),
                ));
            }
        };

        let mut prepared = PreparedMetadataCoordinatorSession {
            operation: operation.to_string(),
            session,
            released,
            idempotency_key: idempotency_key.to_string(),
            request_checksum,
            metadata_control_proof_checksum: String::new(),
            metadata_control_proof_now_ms: 0,
            expected_previous_commit_index,
            prepared_commit_index: expected_previous_commit_index.saturating_add(1),
            raft_entry_checksum: String::new(),
        };
        prepared.raft_entry_checksum = prepared_metadata_coordinator_session_checksum(&prepared);
        Ok(prepared)
    }
}
