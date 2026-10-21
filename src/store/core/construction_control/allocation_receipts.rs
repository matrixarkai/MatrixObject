// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn list_page_allocation_receipts(
        &self,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
    ) -> Result<PageAllocationReceiptPage, ObjectError> {
        self.chunk_service
            .list_allocation_receipts(marker, limit, chunk_id)
    }

    pub fn list_page_allocation_receipts_for_chunk(
        &self,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
        page_index: Option<u64>,
    ) -> Result<PageAllocationReceiptPage, ObjectError> {
        self.chunk_service.list_allocation_receipts_for_chunk(
            marker,
            limit,
            chunk_id,
            page_index,
        )
    }

    pub fn list_page_release_allocation_receipts(
        &self,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
    ) -> Result<PageReleaseAllocationReceiptPage, ObjectError> {
        self.chunk_service
            .list_release_allocation_receipts(marker, limit, chunk_id)
    }

    pub fn list_page_release_allocation_receipts_for_chunk(
        &self,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
        page_index: Option<u64>,
    ) -> Result<PageReleaseAllocationReceiptPage, ObjectError> {
        self.chunk_service
            .list_release_allocation_receipts_for_chunk(marker, limit, chunk_id, page_index)
    }

    pub fn list_page_batch_allocation_receipts(
        &self,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
    ) -> Result<PageBatchAllocationReceiptPage, ObjectError> {
        self.chunk_service
            .list_batch_allocation_receipts(marker, limit, chunk_id)
    }

    pub fn list_page_batch_allocation_receipts_for_chunk(
        &self,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
        page_index: Option<u64>,
    ) -> Result<PageBatchAllocationReceiptPage, ObjectError> {
        self.chunk_service.list_batch_allocation_receipts_for_chunk(
            marker,
            limit,
            chunk_id,
            page_index,
        )
    }

}
