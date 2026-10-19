impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn list_chunk_write_receipts_linearizable(
        &self,
        replica_index: usize,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&PageId>,
        now_ms: u64,
    ) -> Result<(ReplicaWriteReceiptPage, MetadataRaftReadiness), ObjectError> {
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
        chunk_id: Option<&PageId>,
        now_ms: u64,
    ) -> Result<ProvenReceiptRead<ReplicaWriteReceiptPage>, ObjectError> {
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
        chunk_id: Option<&PageId>,
        now_ms: u64,
        expected_chunk_write_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenReceiptRead<ReplicaWriteReceiptPage>, ObjectError> {
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
        chunk_id: Option<&PageId>,
        now_ms: u64,
    ) -> Result<(ReplicaBatchWriteReceiptPage, MetadataRaftReadiness), ObjectError> {
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
        chunk_id: Option<&PageId>,
        now_ms: u64,
    ) -> Result<ProvenReceiptRead<ReplicaBatchWriteReceiptPage>, ObjectError> {
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
        chunk_id: Option<&PageId>,
        now_ms: u64,
        expected_chunk_batch_write_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenReceiptRead<ReplicaBatchWriteReceiptPage>, ObjectError> {
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
        chunk_id: Option<&PageId>,
        now_ms: u64,
    ) -> Result<(ReplicaDiscardReceiptPage, MetadataRaftReadiness), ObjectError> {
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
        chunk_id: Option<&PageId>,
        now_ms: u64,
    ) -> Result<ProvenReceiptRead<ReplicaDiscardReceiptPage>, ObjectError> {
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
