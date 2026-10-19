impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn list_page_discard_receipts_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
        now_ms: u64,
    ) -> Result<(PageDiscardReceiptPage, MetadataRaftReadiness), ObjectError> {
        let readiness = self.ensure_receipt_read_index("block discard", now_ms)?;
        let page = self
            .store
            .list_page_discard_receipts(marker, limit, chunk_id)?;
        Ok((page, readiness))
    }

    pub fn list_page_discard_receipts_for_chunk_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
        page_index: Option<u64>,
        now_ms: u64,
    ) -> Result<(PageDiscardReceiptPage, MetadataRaftReadiness), ObjectError> {
        let readiness = self.ensure_receipt_read_index("block discard", now_ms)?;
        let page = self.store.list_page_discard_receipts_for_chunk(
            marker,
            limit,
            chunk_id,
            page_index,
        )?;
        Ok((page, readiness))
    }

    pub fn list_page_discard_receipts_with_read_proof_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
        now_ms: u64,
    ) -> Result<ProvenReceiptRead<PageDiscardReceiptPage>, ObjectError> {
        let (page, readiness) =
            self.list_page_discard_receipts_linearizable(marker, limit, chunk_id, now_ms)?;
        Ok(proven_storage_receipt_page_read(page, &readiness))
    }

    pub fn list_page_discard_receipts_for_chunk_with_read_proof_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
        page_index: Option<u64>,
        now_ms: u64,
    ) -> Result<ProvenReceiptRead<PageDiscardReceiptPage>, ObjectError> {
        let (page, readiness) = self.list_page_discard_receipts_for_chunk_linearizable(
            marker,
            limit,
            chunk_id,
            page_index,
            now_ms,
        )?;
        Ok(proven_storage_receipt_page_read(page, &readiness))
    }

    pub fn list_page_discard_receipts_with_read_proof_linearizable_fenced(
        &self,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
        now_ms: u64,
        expected_page_discard_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenReceiptRead<PageDiscardReceiptPage>, ObjectError> {
        let proven = self.list_page_discard_receipts_with_read_proof_linearizable(
            marker, limit, chunk_id, now_ms,
        )?;
        proven.require_page_discard_receipt_page_proof_checksum(
            expected_page_discard_receipt_page_proof_checksum,
        )
    }

    pub fn list_page_discard_receipts_for_chunk_with_read_proof_linearizable_fenced(
        &self,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
        page_index: Option<u64>,
        now_ms: u64,
        expected_page_discard_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenReceiptRead<PageDiscardReceiptPage>, ObjectError> {
        let proven = self.list_page_discard_receipts_for_chunk_with_read_proof_linearizable(
            marker,
            limit,
            chunk_id,
            page_index,
            now_ms,
        )?;
        proven.require_page_discard_receipt_page_proof_checksum(
            expected_page_discard_receipt_page_proof_checksum,
        )
    }

    pub fn list_page_batch_discard_receipts_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        now_ms: u64,
    ) -> Result<(PageBatchDiscardReceiptPage, MetadataRaftReadiness), ObjectError> {
        let readiness = self.ensure_receipt_read_index("block batch discard", now_ms)?;
        let page = self
            .store
            .list_page_batch_discard_receipts(marker, limit)?;
        Ok((page, readiness))
    }

    pub fn list_page_batch_discard_receipts_with_read_proof_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        now_ms: u64,
    ) -> Result<ProvenReceiptRead<PageBatchDiscardReceiptPage>, ObjectError> {
        let (page, readiness) =
            self.list_page_batch_discard_receipts_linearizable(marker, limit, now_ms)?;
        Ok(proven_storage_receipt_page_read(page, &readiness))
    }

    pub fn list_page_batch_discard_receipts_with_read_proof_linearizable_fenced(
        &self,
        marker: Option<&str>,
        limit: usize,
        now_ms: u64,
        expected_page_batch_discard_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenReceiptRead<PageBatchDiscardReceiptPage>, ObjectError> {
        let proven = self.list_page_batch_discard_receipts_with_read_proof_linearizable(
            marker, limit, now_ms,
        )?;
        proven.require_page_batch_discard_receipt_page_proof_checksum(
            expected_page_batch_discard_receipt_page_proof_checksum,
        )
    }

}
