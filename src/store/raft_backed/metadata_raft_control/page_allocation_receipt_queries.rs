impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn list_page_allocation_receipts_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
        now_ms: u64,
    ) -> Result<(PageAllocationReceiptPage, MetadataRaftReadiness), ObjectError> {
        let readiness = self.ensure_receipt_read_index("block allocation", now_ms)?;
        let page = self
            .store
            .list_page_allocation_receipts(marker, limit, chunk_id)?;
        Ok((page, readiness))
    }

    pub fn list_page_allocation_receipts_for_chunk_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
        page_index: Option<u64>,
        now_ms: u64,
    ) -> Result<(PageAllocationReceiptPage, MetadataRaftReadiness), ObjectError> {
        let readiness = self.ensure_receipt_read_index("block allocation", now_ms)?;
        let page = self.store.list_page_allocation_receipts_for_chunk(
            marker,
            limit,
            chunk_id,
            page_index,
        )?;
        Ok((page, readiness))
    }

    pub fn list_page_allocation_receipts_with_read_proof_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
        now_ms: u64,
    ) -> Result<ProvenReceiptRead<PageAllocationReceiptPage>, ObjectError> {
        let (page, readiness) =
            self.list_page_allocation_receipts_linearizable(marker, limit, chunk_id, now_ms)?;
        Ok(proven_storage_receipt_page_read(page, &readiness))
    }

    pub fn list_page_allocation_receipts_for_chunk_with_read_proof_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
        page_index: Option<u64>,
        now_ms: u64,
    ) -> Result<ProvenReceiptRead<PageAllocationReceiptPage>, ObjectError> {
        let (page, readiness) = self.list_page_allocation_receipts_for_chunk_linearizable(
            marker,
            limit,
            chunk_id,
            page_index,
            now_ms,
        )?;
        Ok(proven_storage_receipt_page_read(page, &readiness))
    }

    pub fn list_page_allocation_receipts_with_read_proof_linearizable_fenced(
        &self,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
        now_ms: u64,
        expected_page_allocation_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenReceiptRead<PageAllocationReceiptPage>, ObjectError> {
        let proven = self.list_page_allocation_receipts_with_read_proof_linearizable(
            marker, limit, chunk_id, now_ms,
        )?;
        proven.require_page_allocation_receipt_page_proof_checksum(
            expected_page_allocation_receipt_page_proof_checksum,
        )
    }

    pub fn list_page_allocation_receipts_for_chunk_with_read_proof_linearizable_fenced(
        &self,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
        page_index: Option<u64>,
        now_ms: u64,
        expected_page_allocation_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenReceiptRead<PageAllocationReceiptPage>, ObjectError> {
        let proven = self.list_page_allocation_receipts_for_chunk_with_read_proof_linearizable(
            marker,
            limit,
            chunk_id,
            page_index,
            now_ms,
        )?;
        proven.require_page_allocation_receipt_page_proof_checksum(
            expected_page_allocation_receipt_page_proof_checksum,
        )
    }

    pub fn list_page_release_allocation_receipts_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
        now_ms: u64,
    ) -> Result<(PageReleaseAllocationReceiptPage, MetadataRaftReadiness), ObjectError> {
        let readiness = self.ensure_receipt_read_index("block release allocation", now_ms)?;
        let page = self
            .store
            .list_page_release_allocation_receipts(marker, limit, chunk_id)?;
        Ok((page, readiness))
    }

    pub fn list_page_release_allocation_receipts_for_chunk_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
        page_index: Option<u64>,
        now_ms: u64,
    ) -> Result<(PageReleaseAllocationReceiptPage, MetadataRaftReadiness), ObjectError> {
        let readiness = self.ensure_receipt_read_index("block release allocation", now_ms)?;
        let page = self
            .store
            .list_page_release_allocation_receipts_for_chunk(
                marker,
                limit,
                chunk_id,
                page_index,
            )?;
        Ok((page, readiness))
    }

    pub fn list_page_release_allocation_receipts_with_read_proof_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
        now_ms: u64,
    ) -> Result<ProvenReceiptRead<PageReleaseAllocationReceiptPage>, ObjectError> {
        let (page, readiness) = self.list_page_release_allocation_receipts_linearizable(
            marker, limit, chunk_id, now_ms,
        )?;
        Ok(proven_storage_receipt_page_read(page, &readiness))
    }

    pub fn list_page_release_allocation_receipts_for_chunk_with_read_proof_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
        page_index: Option<u64>,
        now_ms: u64,
    ) -> Result<ProvenReceiptRead<PageReleaseAllocationReceiptPage>, ObjectError> {
        let (page, readiness) = self
            .list_page_release_allocation_receipts_for_chunk_linearizable(
                marker,
                limit,
                chunk_id,
                page_index,
                now_ms,
            )?;
        Ok(proven_storage_receipt_page_read(page, &readiness))
    }

    pub fn list_page_release_allocation_receipts_with_read_proof_linearizable_fenced(
        &self,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
        now_ms: u64,
        expected_page_allocation_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenReceiptRead<PageReleaseAllocationReceiptPage>, ObjectError> {
        let proven = self.list_page_release_allocation_receipts_with_read_proof_linearizable(
            marker, limit, chunk_id, now_ms,
        )?;
        proven.require_page_allocation_receipt_page_proof_checksum(
            expected_page_allocation_receipt_page_proof_checksum,
        )
    }

    pub fn list_page_release_allocation_receipts_for_chunk_with_read_proof_linearizable_fenced(
        &self,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
        page_index: Option<u64>,
        now_ms: u64,
        expected_page_allocation_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenReceiptRead<PageReleaseAllocationReceiptPage>, ObjectError> {
        let proven = self
            .list_page_release_allocation_receipts_for_chunk_with_read_proof_linearizable(
                marker,
                limit,
                chunk_id,
                page_index,
                now_ms,
            )?;
        proven.require_page_allocation_receipt_page_proof_checksum(
            expected_page_allocation_receipt_page_proof_checksum,
        )
    }

    pub fn list_page_batch_allocation_receipts_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
        now_ms: u64,
    ) -> Result<(PageBatchAllocationReceiptPage, MetadataRaftReadiness), ObjectError> {
        let readiness = self.ensure_receipt_read_index("block batch allocation", now_ms)?;
        let page = self
            .store
            .list_page_batch_allocation_receipts(marker, limit, chunk_id)?;
        Ok((page, readiness))
    }

    pub fn list_page_batch_allocation_receipts_for_chunk_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
        page_index: Option<u64>,
        now_ms: u64,
    ) -> Result<(PageBatchAllocationReceiptPage, MetadataRaftReadiness), ObjectError> {
        let readiness = self.ensure_receipt_read_index("block batch allocation", now_ms)?;
        let page = self.store.list_page_batch_allocation_receipts_for_chunk(
            marker,
            limit,
            chunk_id,
            page_index,
        )?;
        Ok((page, readiness))
    }

    pub fn list_page_batch_allocation_receipts_with_read_proof_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
        now_ms: u64,
    ) -> Result<ProvenReceiptRead<PageBatchAllocationReceiptPage>, ObjectError> {
        let (page, readiness) = self
            .list_page_batch_allocation_receipts_linearizable(marker, limit, chunk_id, now_ms)?;
        Ok(proven_storage_receipt_page_read(page, &readiness))
    }

    pub fn list_page_batch_allocation_receipts_for_chunk_with_read_proof_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
        page_index: Option<u64>,
        now_ms: u64,
    ) -> Result<ProvenReceiptRead<PageBatchAllocationReceiptPage>, ObjectError> {
        let (page, readiness) = self.list_page_batch_allocation_receipts_for_chunk_linearizable(
            marker,
            limit,
            chunk_id,
            page_index,
            now_ms,
        )?;
        Ok(proven_storage_receipt_page_read(page, &readiness))
    }

    pub fn list_page_batch_allocation_receipts_with_read_proof_linearizable_fenced(
        &self,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
        now_ms: u64,
        expected_page_batch_allocation_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenReceiptRead<PageBatchAllocationReceiptPage>, ObjectError> {
        let proven = self.list_page_batch_allocation_receipts_with_read_proof_linearizable(
            marker, limit, chunk_id, now_ms,
        )?;
        proven.require_page_batch_allocation_receipt_page_proof_checksum(
            expected_page_batch_allocation_receipt_page_proof_checksum,
        )
    }

    pub fn list_page_batch_allocation_receipts_for_chunk_with_read_proof_linearizable_fenced(
        &self,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
        page_index: Option<u64>,
        now_ms: u64,
        expected_page_batch_allocation_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenReceiptRead<PageBatchAllocationReceiptPage>, ObjectError> {
        let proven = self
            .list_page_batch_allocation_receipts_for_chunk_with_read_proof_linearizable(
                marker,
                limit,
                chunk_id,
                page_index,
                now_ms,
            )?;
        proven.require_page_batch_allocation_receipt_page_proof_checksum(
            expected_page_batch_allocation_receipt_page_proof_checksum,
        )
    }

}
