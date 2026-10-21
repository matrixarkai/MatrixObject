// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn list_metadata_transaction_receipts_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        operation: Option<&str>,
        transaction_id: Option<&str>,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<(MetadataTransactionReceiptPage, MetadataRaftReadiness), ObjectError> {
        let commit_index = self.raft.commit_index();
        let readiness = self.metadata_raft_readiness(0, now_ms);
        if self.store.last_applied_index() < commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata transaction receipt read index is not satisfied: applied={} required={}",
                self.store.last_applied_index(),
                commit_index
            )));
        }
        let page = self.store.list_metadata_transaction_receipts(
            marker,
            limit,
            operation,
            transaction_id,
            max_payload_bytes,
        )?;
        Ok((page, readiness))
    }

    pub fn list_metadata_transaction_receipts_with_read_proof_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        operation: Option<&str>,
        transaction_id: Option<&str>,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<ProvenReceiptRead<MetadataTransactionReceiptPage>, ObjectError> {
        let (page, readiness) = self.list_metadata_transaction_receipts_linearizable(
            marker,
            limit,
            operation,
            transaction_id,
            max_payload_bytes,
            now_ms,
        )?;
        Ok(proven_receipt_read(page, &readiness))
    }

    pub fn list_metadata_transaction_receipts_with_read_proof_linearizable_fenced(
        &self,
        marker: Option<&str>,
        limit: usize,
        operation: Option<&str>,
        transaction_id: Option<&str>,
        max_payload_bytes: u64,
        now_ms: u64,
        expected_metadata_transaction_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenReceiptRead<MetadataTransactionReceiptPage>, ObjectError> {
        let proven = self.list_metadata_transaction_receipts_with_read_proof_linearizable(
            marker,
            limit,
            operation,
            transaction_id,
            max_payload_bytes,
            now_ms,
        )?;
        proven.require_transaction_receipt_page_proof_checksum(
            expected_metadata_transaction_receipt_page_proof_checksum,
        )
    }

    pub fn list_metadata_transaction_receipts_from_meta_shard_fenced(
        &self,
        marker: Option<&str>,
        limit: usize,
        operation: Option<&str>,
        transaction_id: &str,
        max_payload_bytes: u64,
        now_ms: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
    ) -> Result<(MetadataTransactionReceiptPage, MetadataRaftReadiness), ObjectError> {
        let (page, readiness) = self.list_metadata_transaction_receipts_linearizable(
            marker,
            limit,
            operation,
            Some(transaction_id),
            max_payload_bytes,
            now_ms,
        )?;
        let transaction_ids = page
            .receipts
            .iter()
            .map(|receipt| receipt.transaction.transaction_id.clone())
            .collect::<Vec<_>>();
        self.verify_transaction_scan_shard_owner(
            "metadata transaction receipt page shard-fenced read",
            transaction_id,
            &transaction_ids,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        Ok((page, readiness))
    }

    pub fn list_metadata_transaction_receipts_with_read_proof_from_meta_shard_fenced(
        &self,
        marker: Option<&str>,
        limit: usize,
        operation: Option<&str>,
        transaction_id: &str,
        max_payload_bytes: u64,
        now_ms: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        expected_metadata_transaction_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenReceiptRead<MetadataTransactionReceiptPage>, ObjectError> {
        let (page, readiness) = self.list_metadata_transaction_receipts_from_meta_shard_fenced(
            marker,
            limit,
            operation,
            transaction_id,
            max_payload_bytes,
            now_ms,
            expected_shard_owner,
            expected_shard_epoch,
        )?;
        let proven = proven_receipt_read(page, &readiness);
        proven.require_transaction_receipt_page_proof_checksum(
            expected_metadata_transaction_receipt_page_proof_checksum,
        )
    }

}
