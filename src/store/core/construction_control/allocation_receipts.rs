impl MatrixObjectStore {
    pub fn list_page_allocation_receipts(
        &self,
        marker: Option<&str>,
        limit: usize,
        slab_id: Option<&SlabId>,
    ) -> Result<PageAllocationReceiptPage, ObjectError> {
        self.chunk_service
            .list_allocation_receipts(marker, limit, slab_id)
    }

    pub fn list_page_allocation_receipts_for_chunk(
        &self,
        marker: Option<&str>,
        limit: usize,
        slab_id: Option<&SlabId>,
        page_index: Option<u64>,
    ) -> Result<PageAllocationReceiptPage, ObjectError> {
        self.chunk_service.list_allocation_receipts_for_chunk(
            marker,
            limit,
            slab_id,
            page_index,
        )
    }

    pub fn list_page_release_allocation_receipts(
        &self,
        marker: Option<&str>,
        limit: usize,
        slab_id: Option<&SlabId>,
    ) -> Result<PageReleaseAllocationReceiptPage, ObjectError> {
        self.chunk_service
            .list_release_allocation_receipts(marker, limit, slab_id)
    }

    pub fn list_page_release_allocation_receipts_for_chunk(
        &self,
        marker: Option<&str>,
        limit: usize,
        slab_id: Option<&SlabId>,
        page_index: Option<u64>,
    ) -> Result<PageReleaseAllocationReceiptPage, ObjectError> {
        self.chunk_service
            .list_release_allocation_receipts_for_chunk(marker, limit, slab_id, page_index)
    }

    pub fn list_page_batch_allocation_receipts(
        &self,
        marker: Option<&str>,
        limit: usize,
        slab_id: Option<&SlabId>,
    ) -> Result<PageBatchAllocationReceiptPage, ObjectError> {
        self.chunk_service
            .list_batch_allocation_receipts(marker, limit, slab_id)
    }

    pub fn list_page_batch_allocation_receipts_for_chunk(
        &self,
        marker: Option<&str>,
        limit: usize,
        slab_id: Option<&SlabId>,
        page_index: Option<u64>,
    ) -> Result<PageBatchAllocationReceiptPage, ObjectError> {
        self.chunk_service.list_batch_allocation_receipts_for_chunk(
            marker,
            limit,
            slab_id,
            page_index,
        )
    }

}
