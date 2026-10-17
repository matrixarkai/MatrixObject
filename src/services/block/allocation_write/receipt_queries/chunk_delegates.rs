impl MatrixPageService {
    pub fn list_chunk_write_receipts(
        &self,
        replica_index: usize,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
    ) -> Result<ChunkWriteReceiptPage, ObjectError> {
        self.replicas
            .get(replica_index)
            .ok_or_else(|| ObjectError::InvalidArgument("replica index out of range".to_string()))?
            .list_write_receipts(marker, limit, chunk_id)
    }

    pub fn list_chunk_batch_write_receipts(
        &self,
        replica_index: usize,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
    ) -> Result<ChunkBatchWriteReceiptPage, ObjectError> {
        self.replicas
            .get(replica_index)
            .ok_or_else(|| ObjectError::InvalidArgument("replica index out of range".to_string()))?
            .list_batch_write_receipts(marker, limit, chunk_id)
    }

    pub fn list_chunk_discard_receipts(
        &self,
        replica_index: usize,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
    ) -> Result<ChunkDiscardReceiptPage, ObjectError> {
        self.replicas
            .get(replica_index)
            .ok_or_else(|| ObjectError::InvalidArgument("replica index out of range".to_string()))?
            .list_discard_receipts(marker, limit, chunk_id)
    }

    pub fn list_chunk_batch_discard_receipts(
        &self,
        replica_index: usize,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
    ) -> Result<ChunkBatchDiscardReceiptPage, ObjectError> {
        self.replicas
            .get(replica_index)
            .ok_or_else(|| ObjectError::InvalidArgument("replica index out of range".to_string()))?
            .list_batch_discard_receipts(marker, limit, chunk_id)
    }

    pub fn list_chunk_repair_receipts(
        &self,
        replica_index: usize,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
    ) -> Result<ChunkRepairReceiptPage, ObjectError> {
        self.replicas
            .get(replica_index)
            .ok_or_else(|| ObjectError::InvalidArgument("replica index out of range".to_string()))?
            .list_repair_receipts(marker, limit, chunk_id)
    }
}
