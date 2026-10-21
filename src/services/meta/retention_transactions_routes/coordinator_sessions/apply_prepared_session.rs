// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixMetaService {
    pub fn apply_prepared_coordinator_session(
        &mut self,
        prepared: PreparedMetadataCoordinatorSession,
    ) -> Result<CoordinatorSessionReceipt, ObjectError> {
        let expected_checksum =
            prepared_metadata_coordinator_session_checksum(&PreparedMetadataCoordinatorSession {
                raft_entry_checksum: String::new(),
                ..prepared.clone()
            });
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(
                "metadata prepared coordinator session raft entry checksum mismatch".to_string(),
            ));
        }
        let expected_request_checksum = metadata_coordinator_session_request_checksum(
            &prepared.operation,
            &prepared.session.session_id,
            &prepared.session.owner,
            if prepared.operation == "register" {
                &prepared.session.endpoint
            } else {
                ""
            },
            if prepared.operation == "register" {
                0
            } else {
                prepared.session.epoch
            },
            if prepared.released {
                0
            } else {
                prepared.session.ttl_ms
            },
            if prepared.released {
                0
            } else {
                prepared.session.last_renew_ms
            },
        );
        if prepared.request_checksum != expected_request_checksum {
            return Err(ObjectError::Corruption(
                "metadata prepared coordinator session request checksum mismatch".to_string(),
            ));
        }
        if prepared.prepared_commit_index
            != prepared.expected_previous_commit_index.saturating_add(1)
        {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared coordinator session commit index must be adjacent".to_string(),
            ));
        }
        if let Some(existing) = self
            .coordinator_session_receipts
            .get(&prepared.idempotency_key)
        {
            if existing.operation != prepared.operation
                || existing.request_checksum != prepared.request_checksum
                || existing.commit_index != prepared.prepared_commit_index
                || existing.session != prepared.session
                || existing.released != prepared.released
            {
                return Err(ObjectError::PreconditionFailed(
                    "metadata prepared coordinator session receipt mismatch".to_string(),
                ));
            }
            let mut receipt = existing.clone();
            receipt.deduped = true;
            return Ok(receipt);
        }
        if prepared.expected_previous_commit_index != self.next_commit_index.saturating_sub(1) {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared coordinator session previous commit fence mismatch".to_string(),
            ));
        }
        if !prepared.metadata_control_proof_checksum.is_empty() {
            self.check_expected_metadata_control_proof(
                &prepared.metadata_control_proof_checksum,
                prepared.metadata_control_proof_now_ms,
            )?;
        }
        if prepared.prepared_commit_index != self.next_commit_index {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared coordinator session commit index must be contiguous".to_string(),
            ));
        }
        validate_idempotency_key(&prepared.idempotency_key)?;
        validate_session_id(&prepared.session.session_id)?;
        validate_meta_key(&prepared.session.owner)?;
        validate_session_endpoint(&prepared.session.endpoint)?;
        if !prepared.released {
            self.validate_coordinator_session_ttl(prepared.session.ttl_ms)?;
        }
        if prepared.operation == "register" || prepared.operation == "renew" {
            self.validate_coordinator_session_endpoint_fence(&prepared.session.endpoint)?;
        }
        match prepared.operation.as_str() {
            "register" => {
                let current = self.coordinator_sessions.get(&prepared.session.session_id);
                if let Some(current) = current {
                    if current.expire_time_ms > prepared.session.acquire_time_ms
                        && current.owner != prepared.session.owner
                    {
                        return Err(ObjectError::PreconditionFailed(
                            "metadata prepared coordinator session register target is held by another owner"
                                .to_string(),
                        ));
                    }
                }
                let current_epoch = current.map_or(0, |session| session.epoch);
                if prepared.session.epoch != current_epoch.saturating_add(1) {
                    return Err(ObjectError::PreconditionFailed(
                        "metadata prepared coordinator session register epoch changed".to_string(),
                    ));
                }
                self.coordinator_sessions.insert(
                    prepared.session.session_id.clone(),
                    prepared.session.clone(),
                );
            }
            "renew" => {
                let current = self
                    .coordinator_sessions
                    .get(&prepared.session.session_id)
                    .ok_or_else(|| {
                        ObjectError::PreconditionFailed(
                            "metadata prepared coordinator session renew target missing"
                                .to_string(),
                        )
                    })?;
                if current.owner != prepared.session.owner
                    || current.epoch != prepared.session.epoch
                    || current.acquire_time_ms != prepared.session.acquire_time_ms
                    || current.endpoint != prepared.session.endpoint
                    || current.last_renew_ms > prepared.session.last_renew_ms
                {
                    return Err(ObjectError::PreconditionFailed(
                        "metadata prepared coordinator session renew image mismatch".to_string(),
                    ));
                }
                self.coordinator_sessions.insert(
                    prepared.session.session_id.clone(),
                    prepared.session.clone(),
                );
            }
            "release" => {
                let current = self
                    .coordinator_sessions
                    .get(&prepared.session.session_id)
                    .ok_or_else(|| {
                        ObjectError::PreconditionFailed(
                            "metadata prepared coordinator session release target missing"
                                .to_string(),
                        )
                    })?;
                if current != &prepared.session || !prepared.released {
                    return Err(ObjectError::PreconditionFailed(
                        "metadata prepared coordinator session release image mismatch".to_string(),
                    ));
                }
                self.coordinator_sessions
                    .remove(&prepared.session.session_id);
            }
            _ => {
                return Err(ObjectError::InvalidArgument(
                    "prepared coordinator session operation must be register, renew, or release"
                        .to_string(),
                ));
            }
        }
        self.next_commit_index = prepared.prepared_commit_index.saturating_add(1);
        let receipt = CoordinatorSessionReceipt {
            idempotency_key: prepared.idempotency_key.clone(),
            operation: prepared.operation.clone(),
            request_checksum: prepared.request_checksum.clone(),
            session: prepared.session.clone(),
            released: prepared.released,
            commit_index: prepared.prepared_commit_index,
            update_time_ms: if prepared.released {
                prepared.session.last_renew_ms
            } else {
                prepared.session.last_renew_ms
            },
            deduped: false,
        };
        self.coordinator_session_receipts
            .insert(prepared.idempotency_key, receipt.clone());
        Ok(receipt)
    }
}
