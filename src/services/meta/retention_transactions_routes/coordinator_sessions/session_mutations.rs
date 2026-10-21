// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixMetaService {
    pub fn register_coordinator_session_idempotent(
        &mut self,
        session_id: &str,
        owner: &str,
        endpoint: &str,
        ttl_ms: u64,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<CoordinatorSessionReceipt, ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        let operation = "register";
        let request_checksum = metadata_coordinator_session_request_checksum(
            operation, session_id, owner, endpoint, 0, ttl_ms, now_ms,
        );
        if let Some(existing) = self.coordinator_session_receipts.get(idempotency_key) {
            if existing.operation != operation || existing.request_checksum != request_checksum {
                return Err(ObjectError::PreconditionFailed(
                    "coordinator session idempotency key reused with different request".to_string(),
                ));
            }
            let mut receipt = existing.clone();
            receipt.deduped = true;
            return Ok(receipt);
        }
        let session =
            self.register_coordinator_session(session_id, owner, endpoint, ttl_ms, now_ms)?;
        let receipt = CoordinatorSessionReceipt {
            idempotency_key: idempotency_key.to_string(),
            operation: operation.to_string(),
            request_checksum,
            session,
            released: false,
            commit_index: self.next_commit_index.saturating_sub(1),
            update_time_ms: now_ms,
            deduped: false,
        };
        self.coordinator_session_receipts
            .insert(idempotency_key.to_string(), receipt.clone());
        Ok(receipt)
    }

    pub fn renew_coordinator_session(
        &mut self,
        session_id: &str,
        owner: &str,
        epoch: u64,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<CoordinatorSession, ObjectError> {
        validate_session_id(session_id)?;
        let endpoint = {
            let session = self
                .coordinator_sessions
                .get(session_id)
                .ok_or_else(|| ObjectError::NotFound("coordinator session missing".to_string()))?;
            if session.owner != owner || session.epoch != epoch || session.expire_time_ms <= now_ms
            {
                return Err(ObjectError::PreconditionFailed(
                    "coordinator session renew fence mismatch".to_string(),
                ));
            }
            session.endpoint.clone()
        };
        self.validate_coordinator_session_ttl(ttl_ms)?;
        self.validate_coordinator_session_endpoint_fence(&endpoint)?;
        let session = self
            .coordinator_sessions
            .get_mut(session_id)
            .ok_or_else(|| ObjectError::NotFound("coordinator session missing".to_string()))?;
        session.last_renew_ms = now_ms;
        session.expire_time_ms = now_ms.saturating_add(ttl_ms);
        session.ttl_ms = ttl_ms;
        self.next_commit_index = self.next_commit_index.saturating_add(1);
        Ok(session.clone())
    }

    pub fn renew_coordinator_session_idempotent(
        &mut self,
        session_id: &str,
        owner: &str,
        epoch: u64,
        ttl_ms: u64,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<CoordinatorSessionReceipt, ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        let operation = "renew";
        let request_checksum = metadata_coordinator_session_request_checksum(
            operation, session_id, owner, "", epoch, ttl_ms, now_ms,
        );
        if let Some(existing) = self.coordinator_session_receipts.get(idempotency_key) {
            if existing.operation != operation || existing.request_checksum != request_checksum {
                return Err(ObjectError::PreconditionFailed(
                    "coordinator session idempotency key reused with different request".to_string(),
                ));
            }
            let mut receipt = existing.clone();
            receipt.deduped = true;
            return Ok(receipt);
        }
        let session = self.renew_coordinator_session(session_id, owner, epoch, ttl_ms, now_ms)?;
        let receipt = CoordinatorSessionReceipt {
            idempotency_key: idempotency_key.to_string(),
            operation: operation.to_string(),
            request_checksum,
            session,
            released: false,
            commit_index: self.next_commit_index.saturating_sub(1),
            update_time_ms: now_ms,
            deduped: false,
        };
        self.coordinator_session_receipts
            .insert(idempotency_key.to_string(), receipt.clone());
        Ok(receipt)
    }

    pub fn release_coordinator_session(
        &mut self,
        session_id: &str,
        owner: &str,
        epoch: u64,
    ) -> Result<CoordinatorSession, ObjectError> {
        validate_session_id(session_id)?;
        let session = self
            .coordinator_sessions
            .get(session_id)
            .cloned()
            .ok_or_else(|| ObjectError::NotFound("coordinator session missing".to_string()))?;
        if session.owner != owner || session.epoch != epoch {
            return Err(ObjectError::PreconditionFailed(
                "coordinator session release fence mismatch".to_string(),
            ));
        }
        self.coordinator_sessions.remove(session_id);
        self.next_commit_index = self.next_commit_index.saturating_add(1);
        Ok(session)
    }

    pub fn release_coordinator_session_idempotent(
        &mut self,
        session_id: &str,
        owner: &str,
        epoch: u64,
        idempotency_key: &str,
    ) -> Result<CoordinatorSessionReceipt, ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        let operation = "release";
        let request_checksum = metadata_coordinator_session_request_checksum(
            operation, session_id, owner, "", epoch, 0, 0,
        );
        if let Some(existing) = self.coordinator_session_receipts.get(idempotency_key) {
            if existing.operation != operation || existing.request_checksum != request_checksum {
                return Err(ObjectError::PreconditionFailed(
                    "coordinator session idempotency key reused with different request".to_string(),
                ));
            }
            let mut receipt = existing.clone();
            receipt.deduped = true;
            return Ok(receipt);
        }
        let session = self.release_coordinator_session(session_id, owner, epoch)?;
        let update_time_ms = session.last_renew_ms;
        let receipt = CoordinatorSessionReceipt {
            idempotency_key: idempotency_key.to_string(),
            operation: operation.to_string(),
            request_checksum,
            session,
            released: true,
            commit_index: self.next_commit_index.saturating_sub(1),
            update_time_ms,
            deduped: false,
        };
        self.coordinator_session_receipts
            .insert(idempotency_key.to_string(), receipt.clone());
        Ok(receipt)
    }
}
