// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixChunkService {
    pub fn list_allocation_receipts(
        &self,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
    ) -> Result<PageAllocationReceiptPage, ObjectError> {
        self.list_allocation_receipts_for_chunk(marker, limit, chunk_id, None)
    }

    pub fn list_allocation_receipts_for_chunk(
        &self,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
        page_index: Option<u64>,
    ) -> Result<PageAllocationReceiptPage, ObjectError> {
        if let Some(marker) = marker {
            validate_idempotency_key(marker)?;
        }
        let limit = limit.clamp(1, k_max_storage_receipt_list_limit());
        let mut page = PageAllocationReceiptPage {
            total_receipt_count: self.allocation_request_receipts.len() as u64,
            limit: limit as u64,
            chunk_id: chunk_id.cloned(),
            page_index,
            page_size: page_index.map(|_| self.page_size),
            ..PageAllocationReceiptPage::default()
        };
        let iter: Box<dyn Iterator<Item = (&String, &ChunkAllocation)> + '_> =
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
            if chunk_id.is_some_and(|chunk_id| &allocation.chunk_id != chunk_id) {
                page.skipped_chunk_count = page.skipped_chunk_count.saturating_add(1);
                continue;
            }
            if page_index.is_some_and(|page_index| {
                !allocation_overlaps_chunk(allocation, page_index, self.page_size)
            }) {
                page.skipped_page_count = page.skipped_page_count.saturating_add(1);
                continue;
            }
            if page.receipts.len() >= limit {
                page.truncated = true;
                break;
            }
            page.next_marker = Some(idempotency_key.clone());
            page.receipts.push(ChunkAllocationReceipt {
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
        chunk_id: Option<&ChunkId>,
    ) -> Result<PageReleaseAllocationReceiptPage, ObjectError> {
        self.list_release_allocation_receipts_for_chunk(marker, limit, chunk_id, None)
    }

    pub fn list_release_allocation_receipts_for_chunk(
        &self,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
        page_index: Option<u64>,
    ) -> Result<PageReleaseAllocationReceiptPage, ObjectError> {
        if let Some(marker) = marker {
            validate_idempotency_key(marker)?;
        }
        let limit = limit.clamp(1, k_max_storage_receipt_list_limit());
        let mut page = PageReleaseAllocationReceiptPage {
            total_receipt_count: self.release_allocation_request_receipts.len() as u64,
            limit: limit as u64,
            chunk_id: chunk_id.cloned(),
            page_index,
            page_size: page_index.map(|_| self.page_size),
            ..PageReleaseAllocationReceiptPage::default()
        };
        let iter: Box<dyn Iterator<Item = (&String, &ChunkAllocation)> + '_> =
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
            if chunk_id.is_some_and(|chunk_id| &allocation.chunk_id != chunk_id) {
                page.skipped_chunk_count = page.skipped_chunk_count.saturating_add(1);
                continue;
            }
            if page_index.is_some_and(|page_index| {
                !allocation_overlaps_chunk(allocation, page_index, self.page_size)
            }) {
                page.skipped_page_count = page.skipped_page_count.saturating_add(1);
                continue;
            }
            if page.receipts.len() >= limit {
                page.truncated = true;
                break;
            }
            page.next_marker = Some(idempotency_key.clone());
            page.receipts.push(ChunkAllocationReceipt {
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
        chunk_id: Option<&ChunkId>,
    ) -> Result<PageBatchAllocationReceiptPage, ObjectError> {
        self.list_batch_allocation_receipts_for_chunk(marker, limit, chunk_id, None)
    }

    pub fn list_batch_allocation_receipts_for_chunk(
        &self,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
        page_index: Option<u64>,
    ) -> Result<PageBatchAllocationReceiptPage, ObjectError> {
        if let Some(marker) = marker {
            validate_idempotency_key(marker)?;
        }
        let limit = limit.clamp(1, k_max_storage_receipt_list_limit());
        let mut page = PageBatchAllocationReceiptPage {
            total_receipt_count: self.batch_allocation_request_receipts.len() as u64,
            limit: limit as u64,
            chunk_id: chunk_id.cloned(),
            page_index,
            page_size: page_index.map(|_| self.page_size),
            ..PageBatchAllocationReceiptPage::default()
        };
        let iter: Box<dyn Iterator<Item = (&String, &Vec<ChunkAllocation>)> + '_> =
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
            if chunk_id.is_some_and(|chunk_id| {
                !allocations
                    .iter()
                    .any(|allocation| &allocation.chunk_id == chunk_id)
            }) {
                page.skipped_chunk_count = page.skipped_chunk_count.saturating_add(1);
                continue;
            }
            if page_index.is_some_and(|page_index| {
                !allocations.iter().any(|allocation| {
                    allocation_overlaps_chunk(allocation, page_index, self.page_size)
                })
            }) {
                page.skipped_page_count = page.skipped_page_count.saturating_add(1);
                continue;
            }
            if page.receipts.len() >= limit {
                page.truncated = true;
                break;
            }
            page.next_marker = Some(idempotency_key.clone());
            page.receipts.push(ChunkBatchAllocationReceipt {
                allocations: allocations.clone(),
                idempotency_key: idempotency_key.clone(),
                deduped: false,
            });
        }
        page.receipt_count = page.receipts.len() as u64;
        Ok(page)
    }

}
