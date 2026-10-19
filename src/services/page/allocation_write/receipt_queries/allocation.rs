impl MatrixPageService {
    pub fn list_allocation_receipts(
        &self,
        marker: Option<&str>,
        limit: usize,
        slab_id: Option<&SlabId>,
    ) -> Result<PageAllocationReceiptPage, ObjectError> {
        self.list_allocation_receipts_for_chunk(marker, limit, slab_id, None)
    }

    pub fn list_allocation_receipts_for_chunk(
        &self,
        marker: Option<&str>,
        limit: usize,
        slab_id: Option<&SlabId>,
        page_index: Option<u64>,
    ) -> Result<PageAllocationReceiptPage, ObjectError> {
        if let Some(marker) = marker {
            validate_idempotency_key(marker)?;
        }
        let limit = limit.clamp(1, k_max_storage_receipt_list_limit());
        let mut page = PageAllocationReceiptPage {
            total_receipt_count: self.allocation_request_receipts.len() as u64,
            limit: limit as u64,
            slab_id: slab_id.cloned(),
            page_index,
            page_size: page_index.map(|_| self.page_size),
            ..PageAllocationReceiptPage::default()
        };
        let iter: Box<dyn Iterator<Item = (&String, &PageAllocation)> + '_> =
            if let Some(marker) = marker {
                Box::new(self.allocation_request_receipts.range((
                    std::ops::Bound::Excluded(marker.to_string()),
                    std::ops::Bound::Unbounded,
                )))
            } else {
                Box::new(self.allocation_request_receipts.iter())
            };
        for (idempotency_key, allocation) in iter {
            page.scanned_count = page.scanned_count.saturating_add(1);
            if slab_id.is_some_and(|slab_id| &allocation.slab_id != slab_id) {
                page.skipped_slab_count = page.skipped_slab_count.saturating_add(1);
                continue;
            }
            if page_index.is_some_and(|page_index| {
                !allocation_overlaps_chunk(allocation, page_index, self.page_size)
            }) {
                page.skipped_chunk_count = page.skipped_chunk_count.saturating_add(1);
                continue;
            }
            if page.receipts.len() >= limit {
                page.truncated = true;
                break;
            }
            page.next_marker = Some(idempotency_key.clone());
            page.receipts.push(PageAllocationReceipt {
                allocation: allocation.clone(),
                idempotency_key: idempotency_key.clone(),
                released: false,
                deduped: false,
            });
        }
        page.receipt_count = page.receipts.len() as u64;
        Ok(page)
    }

    pub fn list_release_allocation_receipts(
        &self,
        marker: Option<&str>,
        limit: usize,
        slab_id: Option<&SlabId>,
    ) -> Result<PageReleaseAllocationReceiptPage, ObjectError> {
        self.list_release_allocation_receipts_for_chunk(marker, limit, slab_id, None)
    }

    pub fn list_release_allocation_receipts_for_chunk(
        &self,
        marker: Option<&str>,
        limit: usize,
        slab_id: Option<&SlabId>,
        page_index: Option<u64>,
    ) -> Result<PageReleaseAllocationReceiptPage, ObjectError> {
        if let Some(marker) = marker {
            validate_idempotency_key(marker)?;
        }
        let limit = limit.clamp(1, k_max_storage_receipt_list_limit());
        let mut page = PageReleaseAllocationReceiptPage {
            total_receipt_count: self.release_allocation_request_receipts.len() as u64,
            limit: limit as u64,
            slab_id: slab_id.cloned(),
            page_index,
            page_size: page_index.map(|_| self.page_size),
            ..PageReleaseAllocationReceiptPage::default()
        };
        let iter: Box<dyn Iterator<Item = (&String, &PageAllocation)> + '_> =
            if let Some(marker) = marker {
                Box::new(self.release_allocation_request_receipts.range((
                    std::ops::Bound::Excluded(marker.to_string()),
                    std::ops::Bound::Unbounded,
                )))
            } else {
                Box::new(self.release_allocation_request_receipts.iter())
            };
        for (idempotency_key, allocation) in iter {
            page.scanned_count = page.scanned_count.saturating_add(1);
            if slab_id.is_some_and(|slab_id| &allocation.slab_id != slab_id) {
                page.skipped_slab_count = page.skipped_slab_count.saturating_add(1);
                continue;
            }
            if page_index.is_some_and(|page_index| {
                !allocation_overlaps_chunk(allocation, page_index, self.page_size)
            }) {
                page.skipped_chunk_count = page.skipped_chunk_count.saturating_add(1);
                continue;
            }
            if page.receipts.len() >= limit {
                page.truncated = true;
                break;
            }
            page.next_marker = Some(idempotency_key.clone());
            page.receipts.push(PageAllocationReceipt {
                allocation: allocation.clone(),
                idempotency_key: idempotency_key.clone(),
                released: true,
                deduped: false,
            });
        }
        page.receipt_count = page.receipts.len() as u64;
        Ok(page)
    }

    pub fn list_batch_allocation_receipts(
        &self,
        marker: Option<&str>,
        limit: usize,
        slab_id: Option<&SlabId>,
    ) -> Result<PageBatchAllocationReceiptPage, ObjectError> {
        self.list_batch_allocation_receipts_for_chunk(marker, limit, slab_id, None)
    }

    pub fn list_batch_allocation_receipts_for_chunk(
        &self,
        marker: Option<&str>,
        limit: usize,
        slab_id: Option<&SlabId>,
        page_index: Option<u64>,
    ) -> Result<PageBatchAllocationReceiptPage, ObjectError> {
        if let Some(marker) = marker {
            validate_idempotency_key(marker)?;
        }
        let limit = limit.clamp(1, k_max_storage_receipt_list_limit());
        let mut page = PageBatchAllocationReceiptPage {
            total_receipt_count: self.batch_allocation_request_receipts.len() as u64,
            limit: limit as u64,
            slab_id: slab_id.cloned(),
            page_index,
            page_size: page_index.map(|_| self.page_size),
            ..PageBatchAllocationReceiptPage::default()
        };
        let iter: Box<dyn Iterator<Item = (&String, &Vec<PageAllocation>)> + '_> =
            if let Some(marker) = marker {
                Box::new(self.batch_allocation_request_receipts.range((
                    std::ops::Bound::Excluded(marker.to_string()),
                    std::ops::Bound::Unbounded,
                )))
            } else {
                Box::new(self.batch_allocation_request_receipts.iter())
            };
        for (idempotency_key, allocations) in iter {
            page.scanned_count = page.scanned_count.saturating_add(1);
            if slab_id.is_some_and(|slab_id| {
                !allocations
                    .iter()
                    .any(|allocation| &allocation.slab_id == slab_id)
            }) {
                page.skipped_slab_count = page.skipped_slab_count.saturating_add(1);
                continue;
            }
            if page_index.is_some_and(|page_index| {
                !allocations.iter().any(|allocation| {
                    allocation_overlaps_chunk(allocation, page_index, self.page_size)
                })
            }) {
                page.skipped_chunk_count = page.skipped_chunk_count.saturating_add(1);
                continue;
            }
            if page.receipts.len() >= limit {
                page.truncated = true;
                break;
            }
            page.next_marker = Some(idempotency_key.clone());
            page.receipts.push(PageBatchAllocationReceipt {
                allocations: allocations.clone(),
                idempotency_key: idempotency_key.clone(),
                deduped: false,
            });
        }
        page.receipt_count = page.receipts.len() as u64;
        Ok(page)
    }

}
