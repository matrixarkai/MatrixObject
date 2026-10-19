impl MatrixChunkService {
    pub fn list_chunk_write_receipts(
        &self,
        replica_index: usize,
        marker: Option<&str>,
        limit: usize,
        page_id: Option<&PageId>,
    ) -> Result<ReplicaWriteReceiptPage, ObjectError> {
        self.replicas
            .get(replica_index)
            .ok_or_else(|| ObjectError::InvalidArgument("replica index out of range".to_string()))?
            .list_write_receipts(marker, limit, page_id)
    }

    pub fn list_chunk_batch_write_receipts(
        &self,
        replica_index: usize,
        marker: Option<&str>,
        limit: usize,
        page_id: Option<&PageId>,
    ) -> Result<ReplicaBatchWriteReceiptPage, ObjectError> {
        self.replicas
            .get(replica_index)
            .ok_or_else(|| ObjectError::InvalidArgument("replica index out of range".to_string()))?
            .list_batch_write_receipts(marker, limit, page_id)
    }

    pub fn list_chunk_discard_receipts(
        &self,
        replica_index: usize,
        marker: Option<&str>,
        limit: usize,
        page_id: Option<&PageId>,
    ) -> Result<ReplicaDiscardReceiptPage, ObjectError> {
        self.replicas
            .get(replica_index)
            .ok_or_else(|| ObjectError::InvalidArgument("replica index out of range".to_string()))?
            .list_discard_receipts(marker, limit, page_id)
    }

    pub fn list_chunk_batch_discard_receipts(
        &self,
        replica_index: usize,
        marker: Option<&str>,
        limit: usize,
        page_id: Option<&PageId>,
    ) -> Result<ReplicaBatchDiscardReceiptPage, ObjectError> {
        self.replicas
            .get(replica_index)
            .ok_or_else(|| ObjectError::InvalidArgument("replica index out of range".to_string()))?
            .list_batch_discard_receipts(marker, limit, page_id)
    }

    pub fn list_chunk_repair_receipts(
        &self,
        replica_index: usize,
        marker: Option<&str>,
        limit: usize,
        page_id: Option<&PageId>,
    ) -> Result<ReplicaRepairReceiptPage, ObjectError> {
        self.replicas
            .get(replica_index)
            .ok_or_else(|| ObjectError::InvalidArgument("replica index out of range".to_string()))?
            .list_repair_receipts(marker, limit, page_id)
    }
}
