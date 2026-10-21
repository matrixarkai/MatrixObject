// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixMetaService {
    pub fn list_lease_receipts(
        &self,
        marker: Option<&str>,
        limit: usize,
        lease_namespace: Option<&str>,
        owner: Option<&str>,
        released: Option<bool>,
    ) -> Result<MetadataLeaseReceiptPage, ObjectError> {
        if let Some(marker) = marker {
            validate_idempotency_key(marker)?;
        }
        if let Some(namespace) = lease_namespace {
            validate_meta_key(namespace)?;
        }
        if let Some(owner) = owner {
            validate_meta_key(owner)?;
        }
        let limit = limit.clamp(1, k_max_metadata_receipt_list_limit());
        let mut page = MetadataLeaseReceiptPage {
            total_receipt_count: self.lease_receipts.len() as u64,
            limit: limit as u64,
            lease_namespace: lease_namespace.map(str::to_string),
            owner: owner.map(str::to_string),
            released,
            ..MetadataLeaseReceiptPage::default()
        };
        let iter: Box<dyn Iterator<Item = (&String, &MetadataLeaseReceipt)> + '_> =
            if let Some(marker) = marker {
                Box::new(self.lease_receipts.range((
                    std::ops::Bound::Excluded(marker.to_string()),
                    std::ops::Bound::Unbounded,
                )))
            } else {
                Box::new(self.lease_receipts.iter())
            };
        for (idempotency_key, receipt) in iter {
            page.scanned_count = page.scanned_count.saturating_add(1);
            if lease_namespace.is_some_and(|namespace| receipt.lease.namespace != namespace) {
                continue;
            }
            if owner.is_some_and(|owner| receipt.lease.owner != owner) {
                continue;
            }
            if released.is_some_and(|expected| receipt.released != expected) {
                continue;
            }
            page.receipts.push(receipt.clone());
            if page.receipts.len() >= limit {
                page.truncated = true;
                page.next_marker = Some(idempotency_key.clone());
                break;
            }
        }
        page.receipt_count = page.receipts.len() as u64;
        Ok(page)
    }

    pub fn list_shard_receipts(
        &self,
        marker: Option<&str>,
        limit: usize,
        operation: Option<&str>,
        shard_id: Option<&str>,
    ) -> Result<MetadataShardReceiptPage, ObjectError> {
        if let Some(marker) = marker {
            validate_idempotency_key(marker)?;
        }
        if let Some(operation) = operation {
            if !matches!(operation, "configure" | "split" | "merge" | "move") {
                return Err(ObjectError::InvalidArgument(
                    "metadata shard receipt operation filter must be configure, split, merge, or move"
                        .to_string(),
                ));
            }
        }
        if let Some(shard_id) = shard_id {
            validate_meta_key(shard_id)?;
        }
        let limit = limit.clamp(1, k_max_metadata_receipt_list_limit());
        let mut page = MetadataShardReceiptPage {
            total_receipt_count: self.shard_receipts.len() as u64,
            limit: limit as u64,
            operation: operation.map(str::to_string),
            shard_id: shard_id.map(str::to_string),
            ..MetadataShardReceiptPage::default()
        };
        let iter: Box<dyn Iterator<Item = (&String, &MetadataShardReceipt)> + '_> =
            if let Some(marker) = marker {
                Box::new(self.shard_receipts.range((
                    std::ops::Bound::Excluded(marker.to_string()),
                    std::ops::Bound::Unbounded,
                )))
            } else {
                Box::new(self.shard_receipts.iter())
            };
        for (idempotency_key, receipt) in iter {
            page.scanned_count = page.scanned_count.saturating_add(1);
            if operation.is_some_and(|operation| receipt.operation != operation) {
                continue;
            }
            if shard_id.is_some_and(|shard_id| {
                !receipt
                    .shards
                    .iter()
                    .any(|shard| shard.shard_id == shard_id)
            }) {
                continue;
            }
            if page.receipts.len() >= limit {
                page.truncated = true;
                page.next_marker = Some(idempotency_key.clone());
                break;
            }
            page.receipts.push(receipt.clone());
        }
        page.receipt_count = page.receipts.len() as u64;
        Ok(page)
    }

}
