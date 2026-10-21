// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn list_page_write_receipts_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
        now_ms: u64,
    ) -> Result<(PageWriteReceiptPage, MetadataRaftReadiness), ObjectError> {
        let readiness = self.ensure_receipt_read_index("block write", now_ms)?;
        let page = self
            .store
            .list_page_write_receipts(marker, limit, chunk_id)?;
        Ok((page, readiness))
    }

    pub fn list_page_write_receipts_for_chunk_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
        page_index: Option<u64>,
        now_ms: u64,
    ) -> Result<(PageWriteReceiptPage, MetadataRaftReadiness), ObjectError> {
        let readiness = self.ensure_receipt_read_index("block write", now_ms)?;
        let page = self.store.list_page_write_receipts_for_chunk(
            marker,
            limit,
            chunk_id,
            page_index,
        )?;
        Ok((page, readiness))
    }

    pub fn list_page_write_receipts_with_read_proof_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
        now_ms: u64,
    ) -> Result<ProvenReceiptRead<PageWriteReceiptPage>, ObjectError> {
        let (page, readiness) =
            self.list_page_write_receipts_linearizable(marker, limit, chunk_id, now_ms)?;
        Ok(proven_storage_receipt_page_read(page, &readiness))
    }

    pub fn list_page_write_receipts_for_chunk_with_read_proof_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
        page_index: Option<u64>,
        now_ms: u64,
    ) -> Result<ProvenReceiptRead<PageWriteReceiptPage>, ObjectError> {
        let (page, readiness) = self.list_page_write_receipts_for_chunk_linearizable(
            marker,
            limit,
            chunk_id,
            page_index,
            now_ms,
        )?;
        Ok(proven_storage_receipt_page_read(page, &readiness))
    }

    pub fn list_page_write_receipts_with_read_proof_linearizable_fenced(
        &self,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
        now_ms: u64,
        expected_page_write_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenReceiptRead<PageWriteReceiptPage>, ObjectError> {
        let proven = self.list_page_write_receipts_with_read_proof_linearizable(
            marker, limit, chunk_id, now_ms,
        )?;
        proven.require_page_write_receipt_page_proof_checksum(
            expected_page_write_receipt_page_proof_checksum,
        )
    }

    pub fn list_page_write_receipts_for_chunk_with_read_proof_linearizable_fenced(
        &self,
        marker: Option<&str>,
        limit: usize,
        chunk_id: Option<&ChunkId>,
        page_index: Option<u64>,
        now_ms: u64,
        expected_page_write_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenReceiptRead<PageWriteReceiptPage>, ObjectError> {
        let proven = self.list_page_write_receipts_for_chunk_with_read_proof_linearizable(
            marker,
            limit,
            chunk_id,
            page_index,
            now_ms,
        )?;
        proven.require_page_write_receipt_page_proof_checksum(
            expected_page_write_receipt_page_proof_checksum,
        )
    }

    pub fn list_page_batch_write_receipts_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        now_ms: u64,
    ) -> Result<(PageBatchWriteReceiptPage, MetadataRaftReadiness), ObjectError> {
        let readiness = self.ensure_receipt_read_index("block batch write", now_ms)?;
        let page = self.store.list_page_batch_write_receipts(marker, limit)?;
        Ok((page, readiness))
    }

    pub fn list_page_batch_write_receipts_with_read_proof_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        now_ms: u64,
    ) -> Result<ProvenReceiptRead<PageBatchWriteReceiptPage>, ObjectError> {
        let (page, readiness) =
            self.list_page_batch_write_receipts_linearizable(marker, limit, now_ms)?;
        Ok(proven_storage_receipt_page_read(page, &readiness))
    }

    pub fn list_page_batch_write_receipts_with_read_proof_linearizable_fenced(
        &self,
        marker: Option<&str>,
        limit: usize,
        now_ms: u64,
        expected_page_batch_write_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenReceiptRead<PageBatchWriteReceiptPage>, ObjectError> {
        let proven = self
            .list_page_batch_write_receipts_with_read_proof_linearizable(marker, limit, now_ms)?;
        proven.require_page_batch_write_receipt_page_proof_checksum(
            expected_page_batch_write_receipt_page_proof_checksum,
        )
    }

}
