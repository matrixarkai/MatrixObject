impl MatrixChunkService {
    pub fn list_discard_receipts(
        &self,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
    ) -> Result<PageDiscardReceiptPage, ObjectError> {
        self.list_discard_receipts_for_chunk(marker, limit, chunk_id, None)
    }

    pub fn list_discard_receipts_for_chunk(
        &self,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
        page_index: Option<u64>,
    ) -> Result<PageDiscardReceiptPage, ObjectError> {
        if let Some(marker) = marker {
            validate_idempotency_key(marker)?;
        }
        let limit = limit.clamp(1, k_max_storage_receipt_list_limit());
        let mut page = PageDiscardReceiptPage {
            total_receipt_count: self.discard_request_receipts.len() as u64,
            limit: limit as u64,
            chunk_id: chunk_id.cloned(),
            page_index,
            page_size: page_index.map(|_| self.page_size),
            ..PageDiscardReceiptPage::default()
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
                page.skipped_slab_count = page.skipped_slab_count.saturating_add(1);
                continue;
            }
            if page_index.is_some_and(|page_index| {
                !page_range_overlaps_chunk(
                    receipt.offset,
                    receipt.length,
                    page_index,
                    self.page_size,
                )
            }) {
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
    ) -> Result<PageBatchDiscardReceiptPage, ObjectError> {
        if let Some(marker) = marker {
            validate_idempotency_key(marker)?;
        }
        let limit = limit.clamp(1, k_max_storage_receipt_list_limit());
        let mut page = PageBatchDiscardReceiptPage {
            total_receipt_count: self.batch_discard_request_receipts.len() as u64,
            limit: limit as u64,
            ..PageBatchDiscardReceiptPage::default()
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
