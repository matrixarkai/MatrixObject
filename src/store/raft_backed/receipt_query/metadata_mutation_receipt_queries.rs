// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn list_metadata_mutation_receipts_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        method: Option<&str>,
        request_checksum: Option<&str>,
        owner_fenced: Option<bool>,
        record_key_prefix: Option<&str>,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<(MetadataMutationReceiptPage, MetadataRaftReadiness), ObjectError> {
        let commit_index = self.raft.commit_index();
        let readiness = self.metadata_raft_readiness(0, now_ms);
        if self.store.last_applied_index() < commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata mutation receipt read index is not satisfied: applied={} required={}",
                self.store.last_applied_index(),
                commit_index
            )));
        }
        let page = self.store.list_metadata_mutation_receipts(
            marker,
            limit,
            method,
            request_checksum,
            owner_fenced,
            record_key_prefix,
            max_payload_bytes,
        )?;
        Ok((page, readiness))
    }

    pub fn list_metadata_mutation_receipts_with_read_proof_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        method: Option<&str>,
        request_checksum: Option<&str>,
        owner_fenced: Option<bool>,
        record_key_prefix: Option<&str>,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<ProvenReceiptRead<MetadataMutationReceiptPage>, ObjectError> {
        let (page, readiness) = self.list_metadata_mutation_receipts_linearizable(
            marker,
            limit,
            method,
            request_checksum,
            owner_fenced,
            record_key_prefix,
            max_payload_bytes,
            now_ms,
        )?;
        Ok(proven_receipt_read(page, &readiness))
    }

    pub fn list_metadata_mutation_receipts_with_read_proof_linearizable_fenced(
        &self,
        marker: Option<&str>,
        limit: usize,
        method: Option<&str>,
        request_checksum: Option<&str>,
        owner_fenced: Option<bool>,
        record_key_prefix: Option<&str>,
        max_payload_bytes: u64,
        now_ms: u64,
        expected_metadata_mutation_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenReceiptRead<MetadataMutationReceiptPage>, ObjectError> {
        let proven = self.list_metadata_mutation_receipts_with_read_proof_linearizable(
            marker,
            limit,
            method,
            request_checksum,
            owner_fenced,
            record_key_prefix,
            max_payload_bytes,
            now_ms,
        )?;
        proven.require_mutation_receipt_page_proof_checksum(
            expected_metadata_mutation_receipt_page_proof_checksum,
        )
    }

    pub fn list_metadata_mutation_receipts_from_meta_shard_fenced(
        &self,
        marker: Option<&str>,
        limit: usize,
        method: Option<&str>,
        request_checksum: Option<&str>,
        owner_fenced: Option<bool>,
        record_key_prefix: &str,
        max_payload_bytes: u64,
        now_ms: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
    ) -> Result<(MetadataMutationReceiptPage, MetadataRaftReadiness), ObjectError> {
        let (page, readiness) = self.list_metadata_mutation_receipts_linearizable(
            marker,
            limit,
            method,
            request_checksum,
            owner_fenced,
            Some(record_key_prefix),
            max_payload_bytes,
            now_ms,
        )?;
        let record_keys = page
            .receipts
            .iter()
            .flat_map(|receipt| receipt.record_keys.iter().cloned())
            .collect::<Vec<_>>();
        self.verify_metadata_record_keys_shard_owner(
            "metadata mutation receipt page shard-fenced read",
            record_key_prefix,
            &record_keys,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        Ok((page, readiness))
    }

    pub fn list_metadata_mutation_receipts_with_read_proof_from_meta_shard_fenced(
        &self,
        marker: Option<&str>,
        limit: usize,
        method: Option<&str>,
        request_checksum: Option<&str>,
        owner_fenced: Option<bool>,
        record_key_prefix: &str,
        max_payload_bytes: u64,
        now_ms: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        expected_metadata_mutation_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenReceiptRead<MetadataMutationReceiptPage>, ObjectError> {
        let (page, readiness) = self.list_metadata_mutation_receipts_from_meta_shard_fenced(
            marker,
            limit,
            method,
            request_checksum,
            owner_fenced,
            record_key_prefix,
            max_payload_bytes,
            now_ms,
            expected_shard_owner,
            expected_shard_epoch,
        )?;
        let proven = proven_receipt_read(page, &readiness);
        proven.require_mutation_receipt_page_proof_checksum(
            expected_metadata_mutation_receipt_page_proof_checksum,
        )
    }
}
