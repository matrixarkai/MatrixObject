impl MatrixReplicaService {
    pub fn list_write_receipts(
        &self,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&PageId>,
    ) -> Result<ChunkWriteReceiptPage, ObjectError> {
        if let Some(marker) = marker {
            validate_idempotency_key(marker)?;
        }
        let limit = limit.clamp(1, k_max_storage_receipt_list_limit());
        let mut page = ChunkWriteReceiptPage {
            total_receipt_count: self.write_request_receipts.len() as u64,
            limit: limit as u64,
            chunk_id: chunk_id.cloned(),
            ..ChunkWriteReceiptPage::default()
        };
        let iter: Box<dyn Iterator<Item = (&String, &ChunkWriteReceipt)> + '_> =
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
            if chunk_id.is_some_and(|chunk_id| &receipt.chunk_id != chunk_id) {
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
        chunk_id: Option<&PageId>,
    ) -> Result<ChunkBatchWriteReceiptPage, ObjectError> {
        if let Some(marker) = marker {
            validate_idempotency_key(marker)?;
        }
        let limit = limit.clamp(1, k_max_storage_receipt_list_limit());
        let mut page = ChunkBatchWriteReceiptPage {
            total_receipt_count: self.batch_write_request_receipts.len() as u64,
            limit: limit as u64,
            chunk_id: chunk_id.cloned(),
            ..ChunkBatchWriteReceiptPage::default()
        };
        let iter: Box<dyn Iterator<Item = (&String, &ChunkBatchWriteReceipt)> + '_> =
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
            if chunk_id.is_some_and(|chunk_id| &receipt.chunk_id != chunk_id) {
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
        chunk_id: Option<&PageId>,
    ) -> Result<ChunkDiscardReceiptPage, ObjectError> {
        if let Some(marker) = marker {
            validate_idempotency_key(marker)?;
        }
        let limit = limit.clamp(1, k_max_storage_receipt_list_limit());
        let mut page = ChunkDiscardReceiptPage {
            total_receipt_count: self.discard_request_receipts.len() as u64,
            limit: limit as u64,
            chunk_id: chunk_id.cloned(),
            ..ChunkDiscardReceiptPage::default()
        };
        let iter: Box<dyn Iterator<Item = (&String, &ChunkDiscardReceipt)> + '_> =
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
            if chunk_id.is_some_and(|chunk_id| &receipt.chunk_id != chunk_id) {
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
        chunk_id: Option<&PageId>,
    ) -> Result<ChunkBatchDiscardReceiptPage, ObjectError> {
        if let Some(marker) = marker {
            validate_idempotency_key(marker)?;
        }
        let limit = limit.clamp(1, k_max_storage_receipt_list_limit());
        let mut page = ChunkBatchDiscardReceiptPage {
            total_receipt_count: self.batch_discard_request_receipts.len() as u64,
            limit: limit as u64,
            chunk_id: chunk_id.cloned(),
            ..ChunkBatchDiscardReceiptPage::default()
        };
        let iter: Box<dyn Iterator<Item = (&String, &ChunkBatchDiscardReceipt)> + '_> =
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
            if chunk_id.is_some_and(|chunk_id| &receipt.chunk_id != chunk_id) {
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
        chunk_id: Option<&PageId>,
    ) -> Result<ChunkRepairReceiptPage, ObjectError> {
        if let Some(marker) = marker {
            validate_idempotency_key(marker)?;
        }
        let limit = limit.clamp(1, k_max_storage_receipt_list_limit());
        let mut page = ChunkRepairReceiptPage {
            total_receipt_count: self.repair_request_receipts.len() as u64,
            limit: limit as u64,
            chunk_id: chunk_id.cloned(),
            ..ChunkRepairReceiptPage::default()
        };
        let iter: Box<dyn Iterator<Item = (&String, &ChunkRepairReceipt)> + '_> =
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
            if chunk_id.is_some_and(|chunk_id| &receipt.chunk_id != chunk_id) {
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
