impl MatrixMetaService {
    pub fn begin_transaction_idempotent(
        &mut self,
        transaction_id: &str,
        owner: &str,
        ttl_ms: u64,
        mutations: Vec<MetadataMutation>,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<MetadataTransactionReceipt, ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        let request_checksum = metadata_transaction_request_checksum(
            "begin",
            transaction_id,
            owner,
            ttl_ms,
            &mutations,
            now_ms,
        );
        if let Some(existing) = self.transaction_receipts.get(idempotency_key) {
            if existing.operation != "begin" || existing.request_checksum != request_checksum {
                return Err(ObjectError::PreconditionFailed(
                    "metadata transaction begin idempotency key reused with different request"
                        .to_string(),
                ));
            }
            let mut receipt = existing.clone();
            receipt.deduped = true;
            return Ok(receipt);
        }
        let transaction =
            self.begin_transaction(transaction_id, owner, ttl_ms, mutations, now_ms)?;
        let receipt = MetadataTransactionReceipt {
            idempotency_key: idempotency_key.to_string(),
            operation: "begin".to_string(),
            request_checksum,
            transaction: transaction.clone(),
            records: Vec::new(),
            commit_index: self.next_commit_index.saturating_sub(1),
            update_time_ms: now_ms,
            deduped: false,
        };
        self.transaction_receipts
            .insert(idempotency_key.to_string(), receipt.clone());
        Ok(receipt)
    }

    pub fn commit_transaction(
        &mut self,
        transaction_id: &str,
        owner: &str,
        now_ms: u64,
    ) -> Result<Vec<MetadataRecord>, ObjectError> {
        let transaction = self
            .transactions
            .get(transaction_id)
            .cloned()
            .ok_or_else(|| ObjectError::NotFound("metadata transaction missing".to_string()))?;
        if transaction.owner != owner
            || transaction.state != "pending"
            || transaction.expire_time_ms <= now_ms
        {
            return Err(ObjectError::PreconditionFailed(
                "metadata transaction commit fence mismatch".to_string(),
            ));
        }
        self.check_transaction_read_versions(&transaction)?;
        let records = self.commit_batch(transaction.mutations.clone(), None, None, now_ms)?;
        let committed = self.transactions.get_mut(transaction_id).ok_or_else(|| {
            ObjectError::NotFound("metadata transaction missing after commit".to_string())
        })?;
        committed.state = "committed".to_string();
        committed.update_time_ms = now_ms;
        self.next_commit_index = self.next_commit_index.saturating_add(1);
        Ok(records)
    }

    pub fn renew_transaction(
        &mut self,
        transaction_id: &str,
        owner: &str,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<MetadataTransaction, ObjectError> {
        validate_meta_key(transaction_id)?;
        validate_meta_key(owner)?;
        if ttl_ms == 0 {
            return Err(ObjectError::InvalidArgument(
                "metadata transaction ttl must be positive".to_string(),
            ));
        }
        let transaction = self
            .transactions
            .get_mut(transaction_id)
            .ok_or_else(|| ObjectError::NotFound("metadata transaction missing".to_string()))?;
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
        self.next_commit_index = self.next_commit_index.saturating_add(1);
        Ok(transaction.clone())
    }

    pub fn renew_transaction_idempotent(
        &mut self,
        transaction_id: &str,
        owner: &str,
        ttl_ms: u64,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<MetadataTransactionReceipt, ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        let request_checksum = metadata_transaction_request_checksum(
            "renew",
            transaction_id,
            owner,
            ttl_ms,
            &[],
            now_ms,
        );
        if let Some(existing) = self.transaction_receipts.get(idempotency_key) {
            if existing.operation != "renew" || existing.request_checksum != request_checksum {
                return Err(ObjectError::PreconditionFailed(
                    "metadata transaction renew idempotency key reused with different request"
                        .to_string(),
                ));
            }
            let mut receipt = existing.clone();
            receipt.deduped = true;
            return Ok(receipt);
        }
        let transaction = self.renew_transaction(transaction_id, owner, ttl_ms, now_ms)?;
        let receipt = MetadataTransactionReceipt {
            idempotency_key: idempotency_key.to_string(),
            operation: "renew".to_string(),
            request_checksum,
            transaction,
            records: Vec::new(),
            commit_index: self.next_commit_index.saturating_sub(1),
            update_time_ms: now_ms,
            deduped: false,
        };
        self.transaction_receipts
            .insert(idempotency_key.to_string(), receipt.clone());
        Ok(receipt)
    }

    pub fn commit_transaction_idempotent(
        &mut self,
        transaction_id: &str,
        owner: &str,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<MetadataTransactionReceipt, ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        let request_checksum =
            metadata_transaction_request_checksum("commit", transaction_id, owner, 0, &[], now_ms);
        if let Some(existing) = self.transaction_receipts.get(idempotency_key) {
            if existing.operation != "commit" || existing.request_checksum != request_checksum {
                return Err(ObjectError::PreconditionFailed(
                    "metadata transaction commit idempotency key reused with different request"
                        .to_string(),
                ));
            }
            let mut receipt = existing.clone();
            receipt.deduped = true;
            return Ok(receipt);
        }
        let records = self.commit_transaction(transaction_id, owner, now_ms)?;
        let transaction = self
            .transactions
            .get(transaction_id)
            .cloned()
            .ok_or_else(|| {
                ObjectError::NotFound("metadata transaction missing after commit".to_string())
            })?;
        let receipt = MetadataTransactionReceipt {
            idempotency_key: idempotency_key.to_string(),
            operation: "commit".to_string(),
            request_checksum,
            transaction,
            records,
            commit_index: self.next_commit_index.saturating_sub(1),
            update_time_ms: now_ms,
            deduped: false,
        };
        self.transaction_receipts
            .insert(idempotency_key.to_string(), receipt.clone());
        Ok(receipt)
    }


}
