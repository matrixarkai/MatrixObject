impl MatrixReplicaService {
    pub fn list_write_receipts(
        &self,
        marker: Option<&str>,
        limit: usize,
        page_id: Option<&PageId>,
    ) -> Result<ReplicaWriteReceiptPage, ObjectError> {
        if let Some(marker) = marker {
            validate_idempotency_key(marker)?;
        }
        let limit = limit.clamp(1, k_max_storage_receipt_list_limit());
        let mut page = ReplicaWriteReceiptPage {
            total_receipt_count: self.write_request_receipts.len() as u64,
            limit: limit as u64,
            page_id: page_id.cloned(),
            ..ReplicaWriteReceiptPage::default()
        };
        let iter: Box<dyn Iterator<Item = (&String, &ReplicaWriteReceipt)> + '_> =
            if let Some(marker) = marker {
                Box::new(self.write_request_receipts.range((
                    std::ops::Bound::Excluded(marker.to_string()),
                    std::ops::Bound::Unbounded,
                )))
            } else {
                Box::new(self.write_request_receipts.iter())
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

    pub fn list_batch_write_receipts(
        &self,
        marker: Option<&str>,
        limit: usize,
        page_id: Option<&PageId>,
    ) -> Result<ReplicaBatchWriteReceiptPage, ObjectError> {
        if let Some(marker) = marker {
            validate_idempotency_key(marker)?;
        }
        let limit = limit.clamp(1, k_max_storage_receipt_list_limit());
        let mut page = ReplicaBatchWriteReceiptPage {
            total_receipt_count: self.batch_write_request_receipts.len() as u64,
            limit: limit as u64,
            page_id: page_id.cloned(),
            ..ReplicaBatchWriteReceiptPage::default()
        };
        let iter: Box<dyn Iterator<Item = (&String, &ReplicaBatchWriteReceipt)> + '_> =
            if let Some(marker) = marker {
                Box::new(self.batch_write_request_receipts.range((
                    std::ops::Bound::Excluded(marker.to_string()),
                    std::ops::Bound::Unbounded,
                )))
            } else {
                Box::new(self.batch_write_request_receipts.iter())
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

    pub fn list_discard_receipts(
        &self,
        marker: Option<&str>,
        limit: usize,
        page_id: Option<&PageId>,
    ) -> Result<ReplicaDiscardReceiptPage, ObjectError> {
        if let Some(marker) = marker {
            validate_idempotency_key(marker)?;
        }
        let limit = limit.clamp(1, k_max_storage_receipt_list_limit());
        let mut page = ReplicaDiscardReceiptPage {
            total_receipt_count: self.discard_request_receipts.len() as u64,
            limit: limit as u64,
            page_id: page_id.cloned(),
            ..ReplicaDiscardReceiptPage::default()
        };
        let iter: Box<dyn Iterator<Item = (&String, &ReplicaDiscardReceipt)> + '_> =
            if let Some(marker) = marker {
                Box::new(self.discard_request_receipts.range((
                    std::ops::Bound::Excluded(marker.to_string()),
                    std::ops::Bound::Unbounded,
                )))
            } else {
                Box::new(self.discard_request_receipts.iter())
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

    pub fn list_batch_discard_receipts(
        &self,
        marker: Option<&str>,
        limit: usize,
        page_id: Option<&PageId>,
    ) -> Result<ReplicaBatchDiscardReceiptPage, ObjectError> {
        if let Some(marker) = marker {
            validate_idempotency_key(marker)?;
        }
        let limit = limit.clamp(1, k_max_storage_receipt_list_limit());
        let mut page = ReplicaBatchDiscardReceiptPage {
            total_receipt_count: self.batch_discard_request_receipts.len() as u64,
            limit: limit as u64,
            page_id: page_id.cloned(),
            ..ReplicaBatchDiscardReceiptPage::default()
        };
        let iter: Box<dyn Iterator<Item = (&String, &ReplicaBatchDiscardReceipt)> + '_> =
            if let Some(marker) = marker {
                Box::new(self.batch_discard_request_receipts.range((
                    std::ops::Bound::Excluded(marker.to_string()),
                    std::ops::Bound::Unbounded,
                )))
            } else {
                Box::new(self.batch_discard_request_receipts.iter())
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
}
