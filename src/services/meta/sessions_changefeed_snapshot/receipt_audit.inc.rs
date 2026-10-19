// SPDX-License-Identifier: Apache-2.0
impl MatrixMetaService {
    pub fn list_batch_receipts(
        &self,
        marker: Option<&str>,
        limit: usize,
        owner_fenced: Option<bool>,
        expected_owner: Option<&str>,
        expected_epoch: Option<u64>,
        lease_owner: Option<&str>,
        record_key_prefix: Option<&str>,
        max_payload_bytes: u64,
    ) -> Result<MetadataBatchReceiptPage, ObjectError> {
        if let Some(marker) = marker {
            validate_idempotency_key(marker)?;
        }
        if let Some(expected_owner) = expected_owner {
            validate_meta_key(expected_owner)?;
        }
        if let Some(lease_owner) = lease_owner {
            validate_meta_key(lease_owner)?;
        }
        if let Some(record_key_prefix) = record_key_prefix {
            validate_meta_key(record_key_prefix)?;
        }
        let limit = limit.clamp(1, k_max_metadata_receipt_list_limit());
        let mut page = MetadataBatchReceiptPage {
            receipts: Vec::new(),
            marker: marker.map(str::to_string),
            next_marker: None,
            owner_fenced,
            expected_owner: expected_owner.map(str::to_string),
            expected_epoch,
            lease_owner: lease_owner.map(str::to_string),
            record_key_prefix: record_key_prefix.map(str::to_string),
            total_receipt_count: self.batch_request_receipts.len() as u64,
            max_payload_bytes,
            ..MetadataBatchReceiptPage::default()
        };
        let iter: Box<dyn Iterator<Item = (&String, &MetadataBatchReceipt)> + '_> =
            if let Some(marker) = marker {
                Box::new(self.batch_request_receipts.range((
                    std::ops::Bound::Excluded(marker.to_string()),
                    std::ops::Bound::Unbounded,
                )))
            } else {
                Box::new(self.batch_request_receipts.iter())
            };
        for (idempotency_key, receipt) in iter {
            page.scanned_count = page.scanned_count.saturating_add(1);
            if owner_fenced.is_some_and(|expected| receipt.owner_fenced != expected) {
                page.skipped_owner_fence_count = page.skipped_owner_fence_count.saturating_add(1);
                continue;
            }
            if expected_owner.is_some_and(|expected_owner| receipt.expected_owner != expected_owner)
            {
                page.skipped_expected_owner_count =
                    page.skipped_expected_owner_count.saturating_add(1);
                continue;
            }
            if expected_epoch.is_some_and(|expected_epoch| receipt.expected_epoch != expected_epoch)
            {
                page.skipped_expected_epoch_count =
                    page.skipped_expected_epoch_count.saturating_add(1);
                continue;
            }
            if lease_owner
                .is_some_and(|lease_owner| receipt.lease_owner.as_deref() != Some(lease_owner))
            {
                page.skipped_lease_owner_count = page.skipped_lease_owner_count.saturating_add(1);
                continue;
            }
            if record_key_prefix.is_some_and(|prefix| {
                !receipt
                    .records
                    .iter()
                    .any(|record| record.key.starts_with(prefix))
            }) {
                page.skipped_record_prefix_count =
                    page.skipped_record_prefix_count.saturating_add(1);
                continue;
            }
            if page.receipts.len() >= limit {
                page.truncated = true;
                break;
            }
            let item_payload = metadata_batch_receipt_payload_bytes(receipt);
            if max_payload_bytes > 0
                && page.payload_bytes.saturating_add(item_payload) > max_payload_bytes
            {
                page.budget_limited = true;
                if page.receipts.is_empty() {
                    return Err(ObjectError::InvalidArgument(format!(
                        "metadata batch receipt list first receipt exceeds max_payload_bytes: {idempotency_key}"
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

    pub fn list_mutation_receipts(
        &self,
        marker: Option<&str>,
        limit: usize,
        method: Option<&str>,
        request_checksum: Option<&str>,
        owner_fenced: Option<bool>,
        record_key_prefix: Option<&str>,
        max_payload_bytes: u64,
    ) -> Result<MetadataMutationReceiptPage, ObjectError> {
        if let Some(marker) = marker {
            validate_meta_key(marker)?;
        }
        if let Some(record_key_prefix) = record_key_prefix {
            validate_meta_key(record_key_prefix)?;
        }
        let limit = limit.clamp(1, k_max_metadata_receipt_list_limit());
        let receipts = self.metadata_mutation_receipt_audit_items();
        let mut page = MetadataMutationReceiptPage {
            marker: marker.map(str::to_string),
            next_marker: None,
            method: method.map(str::to_string),
            request_checksum: request_checksum.map(str::to_string),
            owner_fenced,
            record_key_prefix: record_key_prefix.map(str::to_string),
            total_receipt_count: receipts.len() as u64,
            max_payload_bytes,
            ..MetadataMutationReceiptPage::default()
        };
        for receipt in receipts {
            if marker.is_some_and(|marker| receipt.audit_key.as_str() <= marker) {
                continue;
            }
            page.scanned_count = page.scanned_count.saturating_add(1);
            if method.is_some_and(|method| receipt.method != method) {
                page.skipped_method_count = page.skipped_method_count.saturating_add(1);
                continue;
            }
            if request_checksum.is_some_and(|checksum| receipt.request_checksum != checksum) {
                page.skipped_checksum_count = page.skipped_checksum_count.saturating_add(1);
                continue;
            }
            if owner_fenced.is_some_and(|expected| receipt.owner_fenced != expected) {
                page.skipped_owner_fence_count = page.skipped_owner_fence_count.saturating_add(1);
                continue;
            }
            if record_key_prefix.is_some_and(|prefix| {
                !receipt
                    .record_keys
                    .iter()
                    .any(|key| key.starts_with(prefix))
            }) {
                page.skipped_record_prefix_count =
                    page.skipped_record_prefix_count.saturating_add(1);
                continue;
            }
            page.matched_count = page.matched_count.saturating_add(1);
            if page.receipts.len() >= limit {
                page.truncated = true;
                break;
            }
            let item_payload = metadata_mutation_receipt_payload_bytes(&receipt);
            if max_payload_bytes > 0
                && page.payload_bytes.saturating_add(item_payload) > max_payload_bytes
            {
                page.budget_limited = true;
                if page.receipts.is_empty() {
                    return Err(ObjectError::InvalidArgument(format!(
                        "metadata mutation receipt list first receipt exceeds max_payload_bytes: {}",
                        receipt.audit_key
                    )));
                }
                page.truncated = true;
                break;
            }
            page.payload_bytes = page.payload_bytes.saturating_add(item_payload);
            page.next_marker = Some(receipt.audit_key.clone());
            page.receipts.push(receipt);
        }
        page.receipt_count = page.receipts.len() as u64;
        if !page.truncated {
            page.next_marker = None;
        }
        Ok(page)
    }

    fn metadata_mutation_receipt_audit_items(&self) -> Vec<MetadataMutationReceipt> {
        let mut receipts = Vec::new();
        for (idempotency_key, receipt) in &self.batch_request_receipts {
            receipts.push(MetadataMutationReceipt {
                audit_key: format!("batch/{idempotency_key}"),
                receipt_family: "batch".to_string(),
                idempotency_key: idempotency_key.clone(),
                method: "CommitMetaBatch".to_string(),
                request_checksum: receipt.request_checksum.clone(),
                commit_index: receipt.last_commit_index,
                update_time_ms: receipt.now_ms,
                owner_fenced: receipt.owner_fenced,
                expected_owner: receipt.expected_owner.clone(),
                expected_epoch: receipt.expected_epoch,
                lease_owner: receipt.lease_owner.clone(),
                record_keys: receipt
                    .records
                    .iter()
                    .map(|record| record.key.to_string())
                    .collect(),
            });
        }
        for (idempotency_key, receipt) in &self.transaction_receipts {
            let method = match receipt.operation.as_str() {
                "begin" => "BeginMetaTransaction",
                "renew" => "RenewMetaTransaction",
                "commit" => "CommitMetaTransaction",
                "abort" => "AbortMetaTransaction",
                _ => "MetaTransaction",
            };
            receipts.push(MetadataMutationReceipt {
                audit_key: format!("transaction/{idempotency_key}"),
                receipt_family: "transaction".to_string(),
                idempotency_key: idempotency_key.clone(),
                method: method.to_string(),
                request_checksum: receipt.request_checksum.clone(),
                commit_index: receipt.commit_index,
                update_time_ms: receipt.update_time_ms,
                owner_fenced: false,
                expected_owner: receipt.transaction.owner.clone(),
                expected_epoch: 0,
                lease_owner: None,
                record_keys: receipt
                    .records
                    .iter()
                    .map(|record| record.key.to_string())
                    .collect(),
            });
        }
        for (idempotency_key, receipt) in &self.lease_receipts {
            let method = match receipt.operation.as_str() {
                "acquire" => "AcquireMetaLease",
                "renew" => "RenewMetaLease",
                "release" => "ReleaseMetaLease",
                _ => "MetaLease",
            };
            receipts.push(MetadataMutationReceipt {
                audit_key: format!("lease/{idempotency_key}"),
                receipt_family: "lease".to_string(),
                idempotency_key: idempotency_key.clone(),
                method: method.to_string(),
                request_checksum: receipt.request_checksum.clone(),
                commit_index: receipt.commit_index,
                update_time_ms: receipt.update_time_ms,
                owner_fenced: false,
                expected_owner: receipt.lease.owner.clone(),
                expected_epoch: receipt.lease.epoch,
                lease_owner: Some(receipt.lease.owner.clone()),
                record_keys: vec![receipt.lease.namespace.clone()],
            });
        }
        for (idempotency_key, receipt) in &self.coordinator_config_receipts {
            receipts.push(MetadataMutationReceipt {
                audit_key: format!("coordinator-config/{idempotency_key}"),
                receipt_family: "coordinator-config".to_string(),
                idempotency_key: idempotency_key.clone(),
                method: "ConfigureMetaCoordinator".to_string(),
                request_checksum: receipt.request_checksum.clone(),
                commit_index: receipt.commit_index,
                update_time_ms: receipt.update_time_ms,
                owner_fenced: false,
                expected_owner: String::new(),
                expected_epoch: receipt.config.revision,
                lease_owner: None,
                record_keys: vec![receipt.config.namespace_path.clone()],
            });
        }
        for (idempotency_key, receipt) in &self.coordinator_session_receipts {
            let method = match receipt.operation.as_str() {
                "register" => "RegisterMetaCoordinatorSession",
                "renew" => "RenewMetaCoordinatorSession",
                "release" => "ReleaseMetaCoordinatorSession",
                _ => "MetaCoordinatorSession",
            };
            receipts.push(MetadataMutationReceipt {
                audit_key: format!("coordinator-session/{idempotency_key}"),
                receipt_family: "coordinator-session".to_string(),
                idempotency_key: idempotency_key.clone(),
                method: method.to_string(),
                request_checksum: receipt.request_checksum.clone(),
                commit_index: receipt.commit_index,
                update_time_ms: receipt.update_time_ms,
                owner_fenced: false,
                expected_owner: receipt.session.owner.clone(),
                expected_epoch: receipt.session.epoch,
                lease_owner: None,
                record_keys: vec![receipt.session.session_id.clone()],
            });
        }
        for (idempotency_key, receipt) in &self.shard_receipts {
            let method = match receipt.operation.as_str() {
                "configure" => "ConfigureMetaShard",
                "split" => "SplitMetaShard",
                "merge" => "MergeMetaShards",
                "move" => "MigrateMetaShard",
                _ => "MetaShardTopology",
            };
            receipts.push(MetadataMutationReceipt {
                audit_key: format!("shard/{idempotency_key}"),
                receipt_family: "shard".to_string(),
                idempotency_key: idempotency_key.clone(),
                method: method.to_string(),
                request_checksum: receipt.request_checksum.clone(),
                commit_index: receipt.commit_index,
                update_time_ms: receipt.update_time_ms,
                owner_fenced: false,
                expected_owner: receipt
                    .shards
                    .first()
                    .map_or_else(String::new, |shard| shard.owner_node_id.clone()),
                expected_epoch: receipt
                    .shards
                    .iter()
                    .map(|shard| shard.epoch)
                    .max()
                    .unwrap_or(0),
                lease_owner: None,
                record_keys: receipt
                    .shards
                    .iter()
                    .map(|shard| shard.shard_id.clone())
                    .collect(),
            });
        }
        receipts.sort_by(|left, right| left.audit_key.cmp(&right.audit_key));
        receipts
    }
}
