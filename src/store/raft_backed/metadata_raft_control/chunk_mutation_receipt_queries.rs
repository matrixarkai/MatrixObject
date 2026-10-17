impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn list_chunk_write_receipts_linearizable(
        &self,
        replica_index: usize,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
        now_ms: u64,
    ) -> Result<(ChunkWriteReceiptPage, MetadataRaftReadiness), ObjectError> {
        let readiness = self.ensure_receipt_read_index("chunk write", now_ms)?;
        let page = self
            .store
            .list_chunk_write_receipts(replica_index, marker, limit, chunk_id)?;
        Ok((page, readiness))
    }

    pub fn list_chunk_write_receipts_with_read_proof_linearizable(
        &self,
        replica_index: usize,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
        now_ms: u64,
    ) -> Result<ProvenReceiptRead<ChunkWriteReceiptPage>, ObjectError> {
        let (page, readiness) = self.list_chunk_write_receipts_linearizable(
            replica_index,
            marker,
            limit,
            chunk_id,
            now_ms,
        )?;
        Ok(proven_storage_receipt_page_read(page, &readiness))
    }

    pub fn list_chunk_write_receipts_with_read_proof_linearizable_fenced(
        &self,
        replica_index: usize,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
        now_ms: u64,
        expected_chunk_write_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenReceiptRead<ChunkWriteReceiptPage>, ObjectError> {
        let proven = self.list_chunk_write_receipts_with_read_proof_linearizable(
            replica_index,
            marker,
            limit,
            chunk_id,
            now_ms,
        )?;
        proven.require_chunk_write_receipt_page_proof_checksum(
            expected_chunk_write_receipt_page_proof_checksum,
        )
    }

    pub fn list_chunk_batch_write_receipts_linearizable(
        &self,
        replica_index: usize,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
        now_ms: u64,
    ) -> Result<(ChunkBatchWriteReceiptPage, MetadataRaftReadiness), ObjectError> {
        let readiness = self.ensure_receipt_read_index("chunk batch write", now_ms)?;
        let page =
            self.store
                .list_chunk_batch_write_receipts(replica_index, marker, limit, chunk_id)?;
        Ok((page, readiness))
    }

    pub fn list_chunk_batch_write_receipts_with_read_proof_linearizable(
        &self,
        replica_index: usize,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
        now_ms: u64,
    ) -> Result<ProvenReceiptRead<ChunkBatchWriteReceiptPage>, ObjectError> {
        let (page, readiness) = self.list_chunk_batch_write_receipts_linearizable(
            replica_index,
            marker,
            limit,
            chunk_id,
            now_ms,
        )?;
        Ok(proven_storage_receipt_page_read(page, &readiness))
    }

    pub fn list_chunk_batch_write_receipts_with_read_proof_linearizable_fenced(
        &self,
        replica_index: usize,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
        now_ms: u64,
        expected_chunk_batch_write_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenReceiptRead<ChunkBatchWriteReceiptPage>, ObjectError> {
        let proven = self.list_chunk_batch_write_receipts_with_read_proof_linearizable(
            replica_index,
            marker,
            limit,
            chunk_id,
            now_ms,
        )?;
        proven.require_chunk_batch_write_receipt_page_proof_checksum(
            expected_chunk_batch_write_receipt_page_proof_checksum,
        )
    }

    pub fn list_chunk_discard_receipts_linearizable(
        &self,
        replica_index: usize,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
        now_ms: u64,
    ) -> Result<(ChunkDiscardReceiptPage, MetadataRaftReadiness), ObjectError> {
        let readiness = self.ensure_receipt_read_index("chunk discard", now_ms)?;
        let page =
            self.store
                .list_chunk_discard_receipts(replica_index, marker, limit, chunk_id)?;
        Ok((page, readiness))
    }

    pub fn list_chunk_discard_receipts_with_read_proof_linearizable(
        &self,
        replica_index: usize,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
        now_ms: u64,
    ) -> Result<ProvenReceiptRead<ChunkDiscardReceiptPage>, ObjectError> {
        let (page, readiness) = self.list_chunk_discard_receipts_linearizable(
            replica_index,
            marker,
            limit,
            chunk_id,
            now_ms,
        )?;
        Ok(proven_storage_receipt_page_read(page, &readiness))
    }
}
