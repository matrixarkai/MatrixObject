impl MatrixObjectStore {
    pub fn list_page_write_receipts(
        &self,
        marker: Option<&str>,
        limit: usize,
        slab_id: Option<&SlabId>,
    ) -> Result<PageWriteReceiptPage, ObjectError> {
        self.chunk_service
            .list_write_receipts(marker, limit, slab_id)
    }

    pub fn list_page_write_receipts_for_chunk(
        &self,
        marker: Option<&str>,
        limit: usize,
        slab_id: Option<&SlabId>,
        page_index: Option<u64>,
    ) -> Result<PageWriteReceiptPage, ObjectError> {
        self.chunk_service
            .list_write_receipts_for_chunk(marker, limit, slab_id, page_index)
    }

    pub fn list_page_batch_write_receipts(
        &self,
        marker: Option<&str>,
        limit: usize,
    ) -> Result<PageBatchWriteReceiptPage, ObjectError> {
        self.chunk_service.list_batch_write_receipts(marker, limit)
    }

    pub fn list_page_discard_receipts(
        &self,
        marker: Option<&str>,
        limit: usize,
        slab_id: Option<&SlabId>,
    ) -> Result<PageDiscardReceiptPage, ObjectError> {
        self.chunk_service
            .list_discard_receipts(marker, limit, slab_id)
    }

    pub fn list_page_discard_receipts_for_chunk(
        &self,
        marker: Option<&str>,
        limit: usize,
        slab_id: Option<&SlabId>,
        page_index: Option<u64>,
    ) -> Result<PageDiscardReceiptPage, ObjectError> {
        self.chunk_service
            .list_discard_receipts_for_chunk(marker, limit, slab_id, page_index)
    }

    pub fn list_page_batch_discard_receipts(
        &self,
        marker: Option<&str>,
        limit: usize,
    ) -> Result<PageBatchDiscardReceiptPage, ObjectError> {
        self.chunk_service
            .list_batch_discard_receipts(marker, limit)
    }

    pub fn list_page_range_repair_receipts(
        &self,
        marker: Option<&str>,
        limit: usize,
        slab_id: Option<&SlabId>,
    ) -> Result<PageRangeRepairReceiptPage, ObjectError> {
        self.chunk_service
            .list_range_repair_receipts(marker, limit, slab_id)
    }

    pub fn list_page_audit_repair_receipts(
        &self,
        marker: Option<&str>,
        limit: usize,
        after_chunk_id: Option<&PageId>,
    ) -> Result<PageAuditRepairReceiptPage, ObjectError> {
        self.chunk_service
            .list_audit_repair_receipts(marker, limit, after_chunk_id)
    }

    pub fn list_page_repair_receipts(
        &self,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&PageId>,
    ) -> Result<ReplicaRepairReceiptPage, ObjectError> {
        self.chunk_service
            .list_repair_receipts(marker, limit, chunk_id)
    }
}
