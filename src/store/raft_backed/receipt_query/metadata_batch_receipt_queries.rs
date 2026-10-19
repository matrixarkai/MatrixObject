// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn list_metadata_batch_receipts_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        owner_fenced: Option<bool>,
        expected_owner: Option<&str>,
        expected_epoch: Option<u64>,
        lease_owner: Option<&str>,
        record_key_prefix: Option<&str>,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<(MetadataBatchReceiptPage, MetadataRaftReadiness), ObjectError> {
        let commit_index = self.raft.commit_index();
        let readiness = self.metadata_raft_readiness(0, now_ms);
        if self.store.last_applied_index() < commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata batch receipt read index is not satisfied: applied={} required={}",
                self.store.last_applied_index(),
                commit_index
            )));
        }
        let page = self.store.list_metadata_batch_receipts(
            marker,
            limit,
            owner_fenced,
            expected_owner,
            expected_epoch,
            lease_owner,
            record_key_prefix,
            max_payload_bytes,
        )?;
        Ok((page, readiness))
    }

    pub fn list_metadata_batch_receipts_with_read_proof_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        owner_fenced: Option<bool>,
        expected_owner: Option<&str>,
        expected_epoch: Option<u64>,
        lease_owner: Option<&str>,
        record_key_prefix: Option<&str>,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<ProvenReceiptRead<MetadataBatchReceiptPage>, ObjectError> {
        let (page, readiness) = self.list_metadata_batch_receipts_linearizable(
            marker,
            limit,
            owner_fenced,
            expected_owner,
            expected_epoch,
            lease_owner,
            record_key_prefix,
            max_payload_bytes,
            now_ms,
        )?;
        Ok(proven_receipt_read(page, &readiness))
    }

    pub fn list_metadata_batch_receipts_with_read_proof_linearizable_fenced(
        &self,
        marker: Option<&str>,
        limit: usize,
        owner_fenced: Option<bool>,
        expected_owner: Option<&str>,
        expected_epoch: Option<u64>,
        lease_owner: Option<&str>,
        record_key_prefix: Option<&str>,
        max_payload_bytes: u64,
        now_ms: u64,
        expected_metadata_batch_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenReceiptRead<MetadataBatchReceiptPage>, ObjectError> {
        let proven = self.list_metadata_batch_receipts_with_read_proof_linearizable(
            marker,
            limit,
            owner_fenced,
            expected_owner,
            expected_epoch,
            lease_owner,
            record_key_prefix,
            max_payload_bytes,
            now_ms,
        )?;
        proven.require_batch_receipt_page_proof_checksum(
            expected_metadata_batch_receipt_page_proof_checksum,
        )
    }

    fn verify_metadata_record_keys_shard_owner(
        &self,
        operation: &str,
        record_key_prefix: &str,
        record_keys: &[String],
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<(), ObjectError> {
        if record_key_prefix.is_empty() {
            return Err(ObjectError::InvalidArgument(format!(
                "{operation} requires a non-empty record key prefix for metadata-shard fencing"
            )));
        }
        self.store.meta_service.check_shard_owner_serving(
            record_key_prefix,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        for record_key in record_keys {
            self.store.meta_service.check_shard_owner_serving(
                record_key,
                expected_shard_owner,
                expected_shard_epoch,
                now_ms,
            )?;
        }
        Ok(())
    }

    pub fn list_metadata_batch_receipts_from_meta_shard_fenced(
        &self,
        marker: Option<&str>,
        limit: usize,
        owner_fenced: Option<bool>,
        expected_owner: Option<&str>,
        expected_epoch: Option<u64>,
        lease_owner: Option<&str>,
        record_key_prefix: &str,
        max_payload_bytes: u64,
        now_ms: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
    ) -> Result<(MetadataBatchReceiptPage, MetadataRaftReadiness), ObjectError> {
        let (page, readiness) = self.list_metadata_batch_receipts_linearizable(
            marker,
            limit,
            owner_fenced,
            expected_owner,
            expected_epoch,
            lease_owner,
            Some(record_key_prefix),
            max_payload_bytes,
            now_ms,
        )?;
        let record_keys = page
            .receipts
            .iter()
            .flat_map(|receipt| receipt.records.iter().map(|record| record.key.to_string()))
            .collect::<Vec<_>>();
        self.verify_metadata_record_keys_shard_owner(
            "metadata batch receipt page shard-fenced read",
            record_key_prefix,
            &record_keys,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        Ok((page, readiness))
    }

    pub fn list_metadata_batch_receipts_with_read_proof_from_meta_shard_fenced(
        &self,
        marker: Option<&str>,
        limit: usize,
        owner_fenced: Option<bool>,
        expected_owner: Option<&str>,
        expected_epoch: Option<u64>,
        lease_owner: Option<&str>,
        record_key_prefix: &str,
        max_payload_bytes: u64,
        now_ms: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        expected_metadata_batch_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenReceiptRead<MetadataBatchReceiptPage>, ObjectError> {
        let (page, readiness) = self.list_metadata_batch_receipts_from_meta_shard_fenced(
            marker,
            limit,
            owner_fenced,
            expected_owner,
            expected_epoch,
            lease_owner,
            record_key_prefix,
            max_payload_bytes,
            now_ms,
            expected_shard_owner,
            expected_shard_epoch,
        )?;
        let proven = proven_receipt_read(page, &readiness);
        proven.require_batch_receipt_page_proof_checksum(
            expected_metadata_batch_receipt_page_proof_checksum,
        )
    }

}
