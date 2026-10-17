impl MatrixMetaService {
    pub fn prepare_transaction_raft_entry(
        &self,
        operation: &str,
        transaction_id: &str,
        owner: &str,
        ttl_ms: u64,
        mutations: Vec<MetadataMutation>,
        now_ms: u64,
        idempotency_key: &str,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataTransaction, ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        validate_meta_key(transaction_id)?;
        validate_meta_key(owner)?;
        if expected_previous_commit_index != self.next_commit_index.saturating_sub(1) {
            return Err(ObjectError::PreconditionFailed(
                "metadata transaction prepare previous commit fence mismatch".to_string(),
            ));
        }
        let request_checksum = metadata_transaction_request_checksum(
            operation,
            transaction_id,
            owner,
            ttl_ms,
            &mutations,
            now_ms,
        );
        if let Some(existing) = self.transaction_receipts.get(idempotency_key) {
            if existing.operation != operation || existing.request_checksum != request_checksum {
                return Err(ObjectError::PreconditionFailed(
                    "metadata transaction idempotency key reused with different request"
                        .to_string(),
                ));
            }
            let mut receipt = existing.clone();
            receipt.deduped = true;
            let (metadata_control_proof_checksum, metadata_control_proof_bytes) =
                self.metadata_control_proof_digest_at(now_ms);
            let mut prepared = PreparedMetadataTransaction {
                receipt,
                metadata_control_proof_format: "MOSRS-META-SERVICE-CONTROL3".to_string(),
                metadata_control_proof_checksum,
                metadata_control_proof_bytes,
                metadata_control_proof_now_ms: now_ms,
                expected_previous_commit_index,
                prepared_commit_index: expected_previous_commit_index.saturating_add(1),
                raft_entry_checksum: String::new(),
            };
            prepared.raft_entry_checksum = prepared_metadata_transaction_checksum(&prepared);
            return Ok(prepared);
        }

        let prepared_commit_index = expected_previous_commit_index.saturating_add(1);
        let (transaction, records) = match operation {
            "begin" => {
                if ttl_ms == 0 {
                    return Err(ObjectError::InvalidArgument(
                        "metadata transaction ttl must be positive".to_string(),
                    ));
                }
                if self.transactions.contains_key(transaction_id) {
                    return Err(ObjectError::PreconditionFailed(
                        "metadata transaction already exists".to_string(),
                    ));
                }
                self.check_open_transaction_admission(transaction_id)?;
                let read_versions = self.preflight_mutations(&mutations, None, None, now_ms)?;
                (
                    MetadataTransaction {
                        transaction_id: transaction_id.to_string(),
                        owner: owner.to_string(),
                        expire_time_ms: now_ms.saturating_add(ttl_ms),
                        state: "pending".to_string(),
                        mutations,
                        read_versions,
                        begin_time_ms: now_ms,
                        update_time_ms: now_ms,
                    },
                    Vec::new(),
                )
            }
            "renew" => {
                if ttl_ms == 0 {
                    return Err(ObjectError::InvalidArgument(
                        "metadata transaction ttl must be positive".to_string(),
                    ));
                }
                let mut transaction =
                    self.transactions
                        .get(transaction_id)
                        .cloned()
                        .ok_or_else(|| {
                            ObjectError::NotFound("metadata transaction missing".to_string())
                        })?;
                if transaction.owner != owner
                    || transaction.state != "pending"
                    || transaction.expire_time_ms <= now_ms
                {
                    return Err(ObjectError::PreconditionFailed(
                        "metadata transaction renew fence mismatch".to_string(),
                    ));
                }
                transaction.expire_time_ms = now_ms.saturating_add(ttl_ms);
                transaction.update_time_ms = now_ms;
                (transaction, Vec::new())
            }
            "commit" => {
                let mut transaction =
                    self.transactions
                        .get(transaction_id)
                        .cloned()
                        .ok_or_else(|| {
                            ObjectError::NotFound("metadata transaction missing".to_string())
                        })?;
                if transaction.owner != owner
                    || transaction.state != "pending"
                    || transaction.expire_time_ms <= now_ms
                {
                    return Err(ObjectError::PreconditionFailed(
                        "metadata transaction commit fence mismatch".to_string(),
                    ));
                }
                self.check_transaction_read_versions(&transaction)?;
                let mut projected = self.clone();
                let records =
                    projected.commit_batch(transaction.mutations.clone(), None, None, now_ms)?;
                transaction.state = "committed".to_string();
                transaction.update_time_ms = now_ms;
                (transaction, records)
            }
            "abort" => {
                let mut transaction =
                    self.transactions
                        .get(transaction_id)
                        .cloned()
                        .ok_or_else(|| {
                            ObjectError::NotFound("metadata transaction missing".to_string())
                        })?;
                if transaction.owner != owner || transaction.state != "pending" {
                    return Err(ObjectError::PreconditionFailed(
                        "metadata transaction abort fence mismatch".to_string(),
                    ));
                }
                transaction.state = "aborted".to_string();
                transaction.update_time_ms = now_ms;
                (transaction, Vec::new())
            }
            _ => {
                return Err(ObjectError::InvalidArgument(
                    "metadata transaction prepare operation must be begin, renew, commit, or abort"
                        .to_string(),
                ))
            }
        };
        let receipt = MetadataTransactionReceipt {
            idempotency_key: idempotency_key.to_string(),
            operation: operation.to_string(),
            request_checksum,
            transaction,
            records,
            commit_index: prepared_commit_index,
            update_time_ms: now_ms,
            deduped: false,
        };
        let (metadata_control_proof_checksum, metadata_control_proof_bytes) =
            self.metadata_control_proof_digest_at(now_ms);
        let mut prepared = PreparedMetadataTransaction {
            receipt,
            metadata_control_proof_format: "MOSRS-META-SERVICE-CONTROL3".to_string(),
            metadata_control_proof_checksum,
            metadata_control_proof_bytes,
            metadata_control_proof_now_ms: now_ms,
            expected_previous_commit_index,
            prepared_commit_index,
            raft_entry_checksum: String::new(),
        };
        prepared.raft_entry_checksum = prepared_metadata_transaction_checksum(&prepared);
        Ok(prepared)
    }

}
