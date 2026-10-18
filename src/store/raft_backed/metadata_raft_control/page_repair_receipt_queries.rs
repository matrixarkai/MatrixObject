impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn list_page_range_repair_receipts_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        slab_id: Option<&SlabId>,
        now_ms: u64,
    ) -> Result<(PageRangeRepairReceiptPage, MetadataRaftReadiness), ObjectError> {
        let readiness = self.ensure_receipt_read_index("block range repair", now_ms)?;
        let page = self
            .store
            .list_page_range_repair_receipts(marker, limit, slab_id)?;
        Ok((page, readiness))
    }

    pub fn list_page_range_repair_receipts_with_read_proof_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        slab_id: Option<&SlabId>,
        now_ms: u64,
    ) -> Result<ProvenReceiptRead<PageRangeRepairReceiptPage>, ObjectError> {
        let (page, readiness) =
            self.list_page_range_repair_receipts_linearizable(marker, limit, slab_id, now_ms)?;
        Ok(proven_storage_receipt_page_read(page, &readiness))
    }

    pub fn list_page_range_repair_receipts_with_read_proof_linearizable_fenced(
        &self,
        marker: Option<&str>,
        limit: usize,
        slab_id: Option<&SlabId>,
        now_ms: u64,
        expected_page_range_repair_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenReceiptRead<PageRangeRepairReceiptPage>, ObjectError> {
        let proven = self.list_page_range_repair_receipts_with_read_proof_linearizable(
            marker, limit, slab_id, now_ms,
        )?;
        proven.require_page_range_repair_receipt_page_proof_checksum(
            expected_page_range_repair_receipt_page_proof_checksum,
        )
    }

    pub fn list_page_audit_repair_receipts_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        after_chunk_id: Option<&ChunkId>,
        now_ms: u64,
    ) -> Result<(PageAuditRepairReceiptPage, MetadataRaftReadiness), ObjectError> {
        let readiness = self.ensure_receipt_read_index("block audit repair", now_ms)?;
        let page = self
            .store
            .list_page_audit_repair_receipts(marker, limit, after_chunk_id)?;
        Ok((page, readiness))
    }

    pub fn list_page_audit_repair_receipts_with_read_proof_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        after_chunk_id: Option<&ChunkId>,
        now_ms: u64,
    ) -> Result<ProvenReceiptRead<PageAuditRepairReceiptPage>, ObjectError> {
        let (page, readiness) = self.list_page_audit_repair_receipts_linearizable(
            marker,
            limit,
            after_chunk_id,
            now_ms,
        )?;
        Ok(proven_storage_receipt_page_read(page, &readiness))
    }

    pub fn list_page_audit_repair_receipts_with_read_proof_linearizable_fenced(
        &self,
        marker: Option<&str>,
        limit: usize,
        after_chunk_id: Option<&ChunkId>,
        now_ms: u64,
        expected_page_audit_repair_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenReceiptRead<PageAuditRepairReceiptPage>, ObjectError> {
        let proven = self.list_page_audit_repair_receipts_with_read_proof_linearizable(
            marker,
            limit,
            after_chunk_id,
            now_ms,
        )?;
        proven.require_page_audit_repair_receipt_page_proof_checksum(
            expected_page_audit_repair_receipt_page_proof_checksum,
        )
    }

    pub fn list_page_repair_receipts_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
        now_ms: u64,
    ) -> Result<(ChunkRepairReceiptPage, MetadataRaftReadiness), ObjectError> {
        let readiness = self.ensure_receipt_read_index("block repair", now_ms)?;
        let page = self
            .store
            .list_page_repair_receipts(marker, limit, chunk_id)?;
        Ok((page, readiness))
    }

    pub fn list_page_repair_receipts_with_read_proof_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
        now_ms: u64,
    ) -> Result<ProvenReceiptRead<ChunkRepairReceiptPage>, ObjectError> {
        let (page, readiness) =
            self.list_page_repair_receipts_linearizable(marker, limit, chunk_id, now_ms)?;
        Ok(proven_storage_receipt_page_read(page, &readiness))
    }

    pub fn list_page_repair_receipts_with_read_proof_linearizable_fenced(
        &self,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
        now_ms: u64,
        expected_chunk_repair_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenReceiptRead<ChunkRepairReceiptPage>, ObjectError> {
        let proven = self.list_page_repair_receipts_with_read_proof_linearizable(
            marker, limit, chunk_id, now_ms,
        )?;
        proven.require_chunk_repair_receipt_page_proof_checksum(
            expected_chunk_repair_receipt_page_proof_checksum,
        )
    }

}
