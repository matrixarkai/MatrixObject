impl MatrixMetaService {
    pub fn apply_prepared_transaction(
        &mut self,
        prepared: PreparedMetadataTransaction,
    ) -> Result<MetadataTransactionReceipt, ObjectError> {
        validate_idempotency_key(&prepared.receipt.idempotency_key)?;
        let expected_checksum =
            prepared_metadata_transaction_checksum(&PreparedMetadataTransaction {
                raft_entry_checksum: String::new(),
                ..prepared.clone()
            });
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(
                "metadata prepared transaction raft entry checksum mismatch".to_string(),
            ));
        }
        let request_checksum = metadata_transaction_request_checksum(
            &prepared.receipt.operation,
            &prepared.receipt.transaction.transaction_id,
            &prepared.receipt.transaction.owner,
            if prepared.receipt.operation == "begin" || prepared.receipt.operation == "renew" {
                prepared
                    .receipt
                    .transaction
                    .expire_time_ms
                    .saturating_sub(prepared.receipt.update_time_ms)
            } else {
                0
            },
            if prepared.receipt.operation == "begin" {
                &prepared.receipt.transaction.mutations
            } else {
                &[]
            },
            prepared.receipt.update_time_ms,
        );
        if request_checksum != prepared.receipt.request_checksum {
            return Err(ObjectError::Corruption(
                "metadata prepared transaction request checksum mismatch".to_string(),
            ));
        }
        if prepared.prepared_commit_index
            != prepared.expected_previous_commit_index.saturating_add(1)
        {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared transaction commit index must be adjacent".to_string(),
            ));
        }
        if let Some(existing) = self
            .transaction_receipts
            .get(&prepared.receipt.idempotency_key)
        {
            if existing.operation != prepared.receipt.operation
                || existing.request_checksum != prepared.receipt.request_checksum
                || existing.commit_index != prepared.receipt.commit_index
            {
                return Err(ObjectError::PreconditionFailed(
                    "metadata prepared transaction idempotency key reused with different request"
                        .to_string(),
                ));
            }
            let mut receipt = existing.clone();
            receipt.deduped = true;
            return Ok(receipt);
        }
        if prepared.expected_previous_commit_index != self.next_commit_index.saturating_sub(1) {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared transaction previous commit fence mismatch".to_string(),
            ));
        }
        if prepared.prepared_commit_index != self.next_commit_index {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared transaction commit index must be contiguous".to_string(),
            ));
        }
        self.check_expected_metadata_control_proof(
            &prepared.metadata_control_proof_checksum,
            prepared.metadata_control_proof_now_ms,
        )?;
        if prepared.receipt.commit_index != prepared.prepared_commit_index {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared transaction receipt commit index mismatch".to_string(),
            ));
        }
        if prepared.receipt.operation == "begin" || prepared.receipt.operation == "commit" {
            let write_bytes =
                Self::metadata_write_payload_bytes(&prepared.receipt.transaction.mutations)?
                    .saturating_add(prepared.receipt.transaction.transaction_id.len() as u64)
                    .saturating_add(prepared.receipt.transaction.owner.len() as u64);
            self.admit_metadata_qos(1, write_bytes, prepared.receipt.update_time_ms)?;
        }

        match prepared.receipt.operation.as_str() {
            "begin" => {
                if self
                    .transactions
                    .contains_key(&prepared.receipt.transaction.transaction_id)
                {
                    return Err(ObjectError::PreconditionFailed(
                        "metadata prepared transaction already exists".to_string(),
                    ));
                }
            }
            "renew" => {
                let current = self
                    .transactions
                    .get(&prepared.receipt.transaction.transaction_id)
                    .ok_or_else(|| {
                        ObjectError::NotFound("metadata transaction missing".to_string())
                    })?;
                if current.owner != prepared.receipt.transaction.owner
                    || current.state != "pending"
                    || current.expire_time_ms <= prepared.receipt.update_time_ms
                {
                    return Err(ObjectError::PreconditionFailed(
                        "metadata prepared transaction renew fence mismatch".to_string(),
                    ));
                }
            }
            "commit" => {
                let current = self
                    .transactions
                    .get(&prepared.receipt.transaction.transaction_id)
                    .ok_or_else(|| {
                        ObjectError::NotFound("metadata transaction missing".to_string())
                    })?;
                if current.owner != prepared.receipt.transaction.owner
                    || current.state != "pending"
                    || current.expire_time_ms <= prepared.receipt.update_time_ms
                {
                    return Err(ObjectError::PreconditionFailed(
                        "metadata prepared transaction commit fence mismatch".to_string(),
                    ));
                }
                self.check_transaction_read_versions(current)?;
                for record in &prepared.receipt.records {
                    let current_version = self
                        .records
                        .get(&record.key)
                        .map_or(0, |existing| existing.version);
                    if record.version != current_version.saturating_add(1)
                        || record.update_time_ms != prepared.receipt.update_time_ms
                    {
                        return Err(ObjectError::Corruption(
                            "metadata prepared transaction record image mismatch".to_string(),
                        ));
                    }
                }
                for record in &prepared.receipt.records {
                    self.file_record(record.key.clone(), record.clone());
                    self.append_metadata_change(MetadataChange {
                        sequence: self.next_change_sequence,
                        commit_index: prepared.prepared_commit_index,
                        key: record.key.clone(),
                        version: record.version,
                        deleted: record.deleted,
                        update_time_ms: record.update_time_ms,
                    });
                    self.next_change_sequence = self.next_change_sequence.saturating_add(1);
                }
            }
            "abort" => {
                let current = self
                    .transactions
                    .get(&prepared.receipt.transaction.transaction_id)
                    .ok_or_else(|| {
                        ObjectError::NotFound("metadata transaction missing".to_string())
                    })?;
                if current.owner != prepared.receipt.transaction.owner || current.state != "pending"
                {
                    return Err(ObjectError::PreconditionFailed(
                        "metadata prepared transaction abort fence mismatch".to_string(),
                    ));
                }
            }
            _ => return Err(ObjectError::InvalidArgument(
                "metadata prepared transaction operation must be begin, renew, commit, or abort"
                    .to_string(),
            )),
        }
        self.transactions.insert(
            prepared.receipt.transaction.transaction_id.clone(),
            prepared.receipt.transaction.clone(),
        );
        self.next_commit_index = prepared.prepared_commit_index.saturating_add(1);
        self.transaction_receipts.insert(
            prepared.receipt.idempotency_key.clone(),
            prepared.receipt.clone(),
        );
        Ok(prepared.receipt)
    }

}
