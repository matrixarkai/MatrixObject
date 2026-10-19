impl MatrixChunkService {
    pub fn list_repair_receipts(
        &self,
        marker: Option<&str>,
        limit: usize,
        page_id: Option<&PageId>,
    ) -> Result<ReplicaRepairReceiptPage, ObjectError> {
        if let Some(marker) = marker {
            validate_idempotency_key(marker)?;
        }
        let limit = limit.clamp(1, k_max_storage_receipt_list_limit());
        let mut page = ReplicaRepairReceiptPage {
            total_receipt_count: self.repair_request_receipts.len() as u64,
            limit: limit as u64,
            page_id: page_id.cloned(),
            ..ReplicaRepairReceiptPage::default()
        };
        let iter: Box<dyn Iterator<Item = (&String, &ReplicaRepairReceipt)> + '_> =
            if let Some(marker) = marker {
                Box::new(self.repair_request_receipts.range((
                    std::ops::Bound::Excluded(marker.to_string()),
                    std::ops::Bound::Unbounded,
                )))
            } else {
                Box::new(self.repair_request_receipts.iter())
            };
        for (idempotency_key, receipt) in iter {
            page.scanned_count = page.scanned_count.saturating_add(1);
            if page_id.is_some_and(|page_id| &receipt.page_id != page_id) {
                page.skipped_chunk_count = page.skipped_chunk_count.saturating_add(1);
                continue;
            }
            if page.receipts.len() >= limit {
                page.truncated = true;
                break;
            }
            page.next_marker = Some(idempotency_key.clone());
            page.receipts.push(receipt.clone());
        }
        page.receipt_count = page.receipts.len() as u64;
        Ok(page)
    }

    pub fn list_range_repair_receipts(
        &self,
        marker: Option<&str>,
        limit: usize,
        slab_id: Option<&SlabId>,
    ) -> Result<PageRangeRepairReceiptPage, ObjectError> {
        if let Some(marker) = marker {
            validate_idempotency_key(marker)?;
        }
        let limit = limit.clamp(1, k_max_storage_receipt_list_limit());
        let mut page = PageRangeRepairReceiptPage {
            total_receipt_count: self.range_repair_request_receipts.len() as u64,
            limit: limit as u64,
            slab_id: slab_id.cloned(),
            ..PageRangeRepairReceiptPage::default()
        };
        let iter: Box<dyn Iterator<Item = (&String, &ChunkRangeRepairReceipt)> + '_> =
            if let Some(marker) = marker {
                Box::new(self.range_repair_request_receipts.range((
                    std::ops::Bound::Excluded(marker.to_string()),
                    std::ops::Bound::Unbounded,
                )))
            } else {
                Box::new(self.range_repair_request_receipts.iter())
            };
        for (idempotency_key, receipt) in iter {
            page.scanned_count = page.scanned_count.saturating_add(1);
            if slab_id.is_some_and(|slab_id| &receipt.slab_id != slab_id) {
                page.skipped_slab_count = page.skipped_slab_count.saturating_add(1);
                continue;
            }
            if page.receipts.len() >= limit {
                page.truncated = true;
                break;
            }
            page.next_marker = Some(idempotency_key.clone());
            page.receipts.push(receipt.clone());
        }
        page.receipt_count = page.receipts.len() as u64;
        Ok(page)
    }

    pub fn list_audit_repair_receipts(
        &self,
        marker: Option<&str>,
        limit: usize,
        after_chunk_id: Option<&PageId>,
    ) -> Result<PageAuditRepairReceiptPage, ObjectError> {
        if let Some(marker) = marker {
            validate_idempotency_key(marker)?;
        }
        let limit = limit.clamp(1, k_max_storage_receipt_list_limit());
        let mut page = PageAuditRepairReceiptPage {
            total_receipt_count: self.audit_repair_request_receipts.len() as u64,
            limit: limit as u64,
            after_chunk_id: after_chunk_id.cloned(),
            ..PageAuditRepairReceiptPage::default()
        };
        let iter: Box<dyn Iterator<Item = (&String, &ChunkAuditRepairReceipt)> + '_> =
            if let Some(marker) = marker {
                Box::new(self.audit_repair_request_receipts.range((
                    std::ops::Bound::Excluded(marker.to_string()),
                    std::ops::Bound::Unbounded,
                )))
            } else {
                Box::new(self.audit_repair_request_receipts.iter())
            };
        for (idempotency_key, receipt) in iter {
            page.scanned_count = page.scanned_count.saturating_add(1);
            if after_chunk_id.is_some_and(|after_chunk_id| {
                receipt.after_chunk_id.as_ref() != Some(after_chunk_id)
            }) {
                page.skipped_after_chunk_count = page.skipped_after_chunk_count.saturating_add(1);
                continue;
            }
            if page.receipts.len() >= limit {
                page.truncated = true;
                break;
            }
            page.next_marker = Some(idempotency_key.clone());
            page.receipts.push(receipt.clone());
        }
        page.receipt_count = page.receipts.len() as u64;
        Ok(page)
    }

}
