impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn list_chunk_discard_receipts_with_read_proof_linearizable_fenced(
        &self,
        replica_index: usize,
        marker: Option<&str>,
        limit: usize,
        page_id: Option<&PageId>,
        now_ms: u64,
        expected_chunk_discard_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenReceiptRead<ReplicaDiscardReceiptPage>, ObjectError> {
        let proven = self.list_chunk_discard_receipts_with_read_proof_linearizable(
            replica_index,
            marker,
            limit,
            page_id,
            now_ms,
        )?;
        proven.require_chunk_discard_receipt_page_proof_checksum(
            expected_chunk_discard_receipt_page_proof_checksum,
        )
    }

    pub fn list_chunk_batch_discard_receipts_linearizable(
        &self,
        replica_index: usize,
        marker: Option<&str>,
        limit: usize,
        page_id: Option<&PageId>,
        now_ms: u64,
    ) -> Result<(ReplicaBatchDiscardReceiptPage, MetadataRaftReadiness), ObjectError> {
        let readiness = self.ensure_receipt_read_index("chunk batch discard", now_ms)?;
        let page =
            self.store
                .list_chunk_batch_discard_receipts(replica_index, marker, limit, page_id)?;
        Ok((page, readiness))
    }

    pub fn list_chunk_batch_discard_receipts_with_read_proof_linearizable(
        &self,
        replica_index: usize,
        marker: Option<&str>,
        limit: usize,
        page_id: Option<&PageId>,
        now_ms: u64,
    ) -> Result<ProvenReceiptRead<ReplicaBatchDiscardReceiptPage>, ObjectError> {
        let (page, readiness) = self.list_chunk_batch_discard_receipts_linearizable(
            replica_index,
            marker,
            limit,
            page_id,
            now_ms,
        )?;
        Ok(proven_storage_receipt_page_read(page, &readiness))
    }

    pub fn list_chunk_batch_discard_receipts_with_read_proof_linearizable_fenced(
        &self,
        replica_index: usize,
        marker: Option<&str>,
        limit: usize,
        page_id: Option<&PageId>,
        now_ms: u64,
        expected_chunk_batch_discard_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenReceiptRead<ReplicaBatchDiscardReceiptPage>, ObjectError> {
        let proven = self.list_chunk_batch_discard_receipts_with_read_proof_linearizable(
            replica_index,
            marker,
            limit,
            page_id,
            now_ms,
        )?;
        proven.require_chunk_batch_discard_receipt_page_proof_checksum(
            expected_chunk_batch_discard_receipt_page_proof_checksum,
        )
    }

    pub fn list_chunk_repair_receipts_linearizable(
        &self,
        replica_index: usize,
        marker: Option<&str>,
        limit: usize,
        page_id: Option<&PageId>,
        now_ms: u64,
    ) -> Result<(ReplicaRepairReceiptPage, MetadataRaftReadiness), ObjectError> {
        let readiness = self.ensure_receipt_read_index("chunk repair", now_ms)?;
        let page = self
            .store
            .list_chunk_repair_receipts(replica_index, marker, limit, page_id)?;
        Ok((page, readiness))
    }

    pub fn list_chunk_repair_receipts_with_read_proof_linearizable(
        &self,
        replica_index: usize,
        marker: Option<&str>,
        limit: usize,
        page_id: Option<&PageId>,
        now_ms: u64,
    ) -> Result<ProvenReceiptRead<ReplicaRepairReceiptPage>, ObjectError> {
        let (page, readiness) = self.list_chunk_repair_receipts_linearizable(
            replica_index,
            marker,
            limit,
            page_id,
            now_ms,
        )?;
        Ok(proven_storage_receipt_page_read(page, &readiness))
    }

    pub fn list_chunk_repair_receipts_with_read_proof_linearizable_fenced(
        &self,
        replica_index: usize,
        marker: Option<&str>,
        limit: usize,
        page_id: Option<&PageId>,
        now_ms: u64,
        expected_chunk_repair_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenReceiptRead<ReplicaRepairReceiptPage>, ObjectError> {
        let proven = self.list_chunk_repair_receipts_with_read_proof_linearizable(
            replica_index,
            marker,
            limit,
            page_id,
            now_ms,
        )?;
        proven.require_chunk_repair_receipt_page_proof_checksum(
            expected_chunk_repair_receipt_page_proof_checksum,
        )
    }

}
