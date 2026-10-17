impl MatrixMetaService {
    pub fn abort_transaction(
        &mut self,
        transaction_id: &str,
        owner: &str,
        now_ms: u64,
    ) -> Result<MetadataTransaction, ObjectError> {
        let transaction = self
            .transactions
            .get_mut(transaction_id)
            .ok_or_else(|| ObjectError::NotFound("metadata transaction missing".to_string()))?;
        if transaction.owner != owner || transaction.state != "pending" {
            return Err(ObjectError::PreconditionFailed(
                "metadata transaction abort fence mismatch".to_string(),
            ));
        }
        transaction.state = "aborted".to_string();
        transaction.update_time_ms = now_ms;
        self.next_commit_index = self.next_commit_index.saturating_add(1);
        Ok(transaction.clone())
    }

    pub fn abort_transaction_idempotent(
        &mut self,
        transaction_id: &str,
        owner: &str,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<MetadataTransactionReceipt, ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        let request_checksum =
            metadata_transaction_request_checksum("abort", transaction_id, owner, 0, &[], now_ms);
        if let Some(existing) = self.transaction_receipts.get(idempotency_key) {
            if existing.operation != "abort" || existing.request_checksum != request_checksum {
                return Err(ObjectError::PreconditionFailed(
                    "metadata transaction abort idempotency key reused with different request"
                        .to_string(),
                ));
            }
            let mut receipt = existing.clone();
            receipt.deduped = true;
            return Ok(receipt);
        }
        let transaction = self.abort_transaction(transaction_id, owner, now_ms)?;
        let receipt = MetadataTransactionReceipt {
            idempotency_key: idempotency_key.to_string(),
            operation: "abort".to_string(),
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

    pub fn list_transaction_receipts(
        &self,
        marker: Option<&str>,
        limit: usize,
        operation: Option<&str>,
        transaction_id: Option<&str>,
        max_payload_bytes: u64,
    ) -> Result<MetadataTransactionReceiptPage, ObjectError> {
        if let Some(marker) = marker {
            validate_idempotency_key(marker)?;
        }
        if let Some(operation) = operation {
            match operation {
                "begin" | "renew" | "commit" | "abort" => {}
                _ => {
                    return Err(ObjectError::InvalidArgument(
                        "metadata transaction receipt operation filter must be begin, renew, commit, or abort"
                            .to_string(),
                    ));
                }
            }
        }
        if let Some(transaction_id) = transaction_id {
            validate_meta_key(transaction_id)?;
        }
        let limit = limit.clamp(1, k_max_metadata_receipt_list_limit());
        let mut page = MetadataTransactionReceiptPage {
            receipts: Vec::new(),
            marker: marker.map(str::to_string),
            next_marker: None,
            operation: operation.map(str::to_string),
            transaction_id: transaction_id.map(str::to_string),
            total_receipt_count: self.transaction_receipts.len() as u64,
            max_payload_bytes,
            ..MetadataTransactionReceiptPage::default()
        };
        let iter: Box<dyn Iterator<Item = (&String, &MetadataTransactionReceipt)> + '_> =
            if let Some(marker) = marker {
                Box::new(self.transaction_receipts.range((
                    std::ops::Bound::Excluded(marker.to_string()),
                    std::ops::Bound::Unbounded,
                )))
            } else {
                Box::new(self.transaction_receipts.iter())
            };
        for (idempotency_key, receipt) in iter {
            page.scanned_count = page.scanned_count.saturating_add(1);
            if operation.is_some_and(|operation| receipt.operation != operation) {
                page.skipped_operation_count = page.skipped_operation_count.saturating_add(1);
                continue;
            }
            if transaction_id
                .is_some_and(|transaction_id| receipt.transaction.transaction_id != transaction_id)
            {
                page.skipped_transaction_count = page.skipped_transaction_count.saturating_add(1);
                continue;
            }
            if page.receipts.len() >= limit {
                page.truncated = true;
                break;
            }
            let item_payload = metadata_transaction_receipt_payload_bytes(receipt);
            if max_payload_bytes > 0
                && page.payload_bytes.saturating_add(item_payload) > max_payload_bytes
            {
                page.budget_limited = true;
                if page.receipts.is_empty() {
                    return Err(ObjectError::InvalidArgument(format!(
                        "metadata transaction receipt list first receipt exceeds max_payload_bytes: {idempotency_key}"
                    )));
                }
                page.truncated = true;
                break;
            }
            page.payload_bytes = page.payload_bytes.saturating_add(item_payload);
            page.next_marker = Some(idempotency_key.clone());
            page.receipts.push(receipt.clone());
            page.matched_count = page.matched_count.saturating_add(1);
        }
        page.receipt_count = page.receipts.len() as u64;
        if !page.truncated {
            page.next_marker = None;
        }
        Ok(page)
    }
}
