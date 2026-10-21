// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn list_metadata_transactions_linearizable(
        &self,
        prefix: &str,
        marker: Option<&str>,
        limit: usize,
        include_terminal: bool,
        owner: Option<&str>,
        state: Option<&str>,
        only_expired: bool,
        now_ms: u64,
    ) -> Result<(MetadataTransactionPage, MetadataRaftReadiness), ObjectError> {
        let commit_index = self.raft.commit_index();
        let readiness = self.metadata_raft_readiness(0, now_ms);
        if self.store.last_applied_index() < commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata transaction page read index is not satisfied: applied={} required={}",
                self.store.last_applied_index(),
                commit_index
            )));
        }
        let page = self.store.list_metadata_transactions(
            prefix,
            marker,
            limit,
            include_terminal,
            owner,
            state,
            only_expired,
            now_ms,
        )?;
        Ok((page, readiness))
    }

    pub fn list_metadata_transactions_with_read_proof_linearizable(
        &self,
        prefix: &str,
        marker: Option<&str>,
        limit: usize,
        include_terminal: bool,
        owner: Option<&str>,
        state: Option<&str>,
        only_expired: bool,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<MetadataTransactionPage>, ObjectError> {
        let (page, readiness) = self.list_metadata_transactions_linearizable(
            prefix,
            marker,
            limit,
            include_terminal,
            owner,
            state,
            only_expired,
            now_ms,
        )?;
        Ok(proven_metadata_read(page, &readiness))
    }

    pub fn list_metadata_transactions_with_read_proof_linearizable_fenced(
        &self,
        prefix: &str,
        marker: Option<&str>,
        limit: usize,
        include_terminal: bool,
        owner: Option<&str>,
        state: Option<&str>,
        only_expired: bool,
        now_ms: u64,
        expected_metadata_transaction_page_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenMetadataRead<MetadataTransactionPage>, ObjectError> {
        let proven = self.list_metadata_transactions_with_read_proof_linearizable(
            prefix,
            marker,
            limit,
            include_terminal,
            owner,
            state,
            only_expired,
            now_ms,
        )?;
        proven.require_transaction_page_proof_checksum(
            expected_metadata_transaction_page_proof_checksum,
        )
    }

    fn verify_transaction_scan_shard_owner(
        &self,
        operation: &str,
        prefix: &str,
        transaction_ids: &[String],
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<(), ObjectError> {
        if prefix.is_empty() {
            return Err(ObjectError::InvalidArgument(format!(
                "{operation} requires a non-empty prefix for metadata-shard fencing"
            )));
        }
        self.store.meta_service.check_shard_owner_serving(
            prefix,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        for transaction_id in transaction_ids {
            self.store.meta_service.check_shard_owner_serving(
                transaction_id,
                expected_shard_owner,
                expected_shard_epoch,
                now_ms,
            )?;
        }
        Ok(())
    }

    pub fn list_metadata_transactions_from_meta_shard_fenced(
        &self,
        prefix: &str,
        marker: Option<&str>,
        limit: usize,
        include_terminal: bool,
        owner: Option<&str>,
        state: Option<&str>,
        only_expired: bool,
        now_ms: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
    ) -> Result<(MetadataTransactionPage, MetadataRaftReadiness), ObjectError> {
        let (page, readiness) = self.list_metadata_transactions_linearizable(
            prefix,
            marker,
            limit,
            include_terminal,
            owner,
            state,
            only_expired,
            now_ms,
        )?;
        let transaction_ids = page
            .transactions
            .iter()
            .map(|transaction| transaction.transaction_id.clone())
            .collect::<Vec<_>>();
        self.verify_transaction_scan_shard_owner(
            "metadata transaction page shard-fenced read",
            prefix,
            &transaction_ids,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        Ok((page, readiness))
    }

    pub fn list_metadata_transactions_with_read_proof_from_meta_shard_fenced(
        &self,
        prefix: &str,
        marker: Option<&str>,
        limit: usize,
        include_terminal: bool,
        owner: Option<&str>,
        state: Option<&str>,
        only_expired: bool,
        now_ms: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        expected_metadata_transaction_page_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenMetadataRead<MetadataTransactionPage>, ObjectError> {
        let (page, readiness) = self.list_metadata_transactions_from_meta_shard_fenced(
            prefix,
            marker,
            limit,
            include_terminal,
            owner,
            state,
            only_expired,
            now_ms,
            expected_shard_owner,
            expected_shard_epoch,
        )?;
        let proven = proven_metadata_read(page, &readiness);
        proven.require_transaction_page_proof_checksum(
            expected_metadata_transaction_page_proof_checksum,
        )
    }

    pub fn list_metadata_transaction_summaries_linearizable(
        &self,
        prefix: &str,
        marker: Option<&str>,
        limit: usize,
        include_terminal: bool,
        owner: Option<&str>,
        state: Option<&str>,
        only_expired: bool,
        now_ms: u64,
        max_payload_bytes: u64,
    ) -> Result<(MetadataTransactionSummaryPage, MetadataRaftReadiness), ObjectError> {
        let commit_index = self.raft.commit_index();
        let readiness = self.metadata_raft_readiness(0, now_ms);
        if self.store.last_applied_index() < commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata transaction summary read index is not satisfied: applied={} required={}",
                self.store.last_applied_index(),
                commit_index
            )));
        }
        let page = self.store.list_metadata_transaction_summaries(
            prefix,
            marker,
            limit,
            include_terminal,
            owner,
            state,
            only_expired,
            now_ms,
            max_payload_bytes,
        )?;
        Ok((page, readiness))
    }

    pub fn list_metadata_transaction_summaries_with_read_proof_linearizable(
        &self,
        prefix: &str,
        marker: Option<&str>,
        limit: usize,
        include_terminal: bool,
        owner: Option<&str>,
        state: Option<&str>,
        only_expired: bool,
        now_ms: u64,
        max_payload_bytes: u64,
    ) -> Result<ProvenMetadataRead<MetadataTransactionSummaryPage>, ObjectError> {
        let (page, readiness) = self.list_metadata_transaction_summaries_linearizable(
            prefix,
            marker,
            limit,
            include_terminal,
            owner,
            state,
            only_expired,
            now_ms,
            max_payload_bytes,
        )?;
        Ok(proven_metadata_read(page, &readiness))
    }

    pub fn list_metadata_transaction_summaries_with_read_proof_linearizable_fenced(
        &self,
        prefix: &str,
        marker: Option<&str>,
        limit: usize,
        include_terminal: bool,
        owner: Option<&str>,
        state: Option<&str>,
        only_expired: bool,
        now_ms: u64,
        max_payload_bytes: u64,
        expected_metadata_transaction_summary_page_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenMetadataRead<MetadataTransactionSummaryPage>, ObjectError> {
        let proven = self.list_metadata_transaction_summaries_with_read_proof_linearizable(
            prefix,
            marker,
            limit,
            include_terminal,
            owner,
            state,
            only_expired,
            now_ms,
            max_payload_bytes,
        )?;
        proven.require_transaction_summary_page_proof_checksum(
            expected_metadata_transaction_summary_page_proof_checksum,
        )
    }

    pub fn list_metadata_transaction_summaries_from_meta_shard_fenced(
        &self,
        prefix: &str,
        marker: Option<&str>,
        limit: usize,
        include_terminal: bool,
        owner: Option<&str>,
        state: Option<&str>,
        only_expired: bool,
        now_ms: u64,
        max_payload_bytes: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
    ) -> Result<(MetadataTransactionSummaryPage, MetadataRaftReadiness), ObjectError> {
        let (page, readiness) = self.list_metadata_transaction_summaries_linearizable(
            prefix,
            marker,
            limit,
            include_terminal,
            owner,
            state,
            only_expired,
            now_ms,
            max_payload_bytes,
        )?;
        let transaction_ids = page
            .transactions
            .iter()
            .map(|transaction| transaction.transaction_id.clone())
            .collect::<Vec<_>>();
        self.verify_transaction_scan_shard_owner(
            "metadata transaction summary page shard-fenced read",
            prefix,
            &transaction_ids,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        Ok((page, readiness))
    }

    pub fn list_metadata_transaction_summaries_with_read_proof_from_meta_shard_fenced(
        &self,
        prefix: &str,
        marker: Option<&str>,
        limit: usize,
        include_terminal: bool,
        owner: Option<&str>,
        state: Option<&str>,
        only_expired: bool,
        now_ms: u64,
        max_payload_bytes: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        expected_metadata_transaction_summary_page_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenMetadataRead<MetadataTransactionSummaryPage>, ObjectError> {
        let (page, readiness) = self.list_metadata_transaction_summaries_from_meta_shard_fenced(
            prefix,
            marker,
            limit,
            include_terminal,
            owner,
            state,
            only_expired,
            now_ms,
            max_payload_bytes,
            expected_shard_owner,
            expected_shard_epoch,
        )?;
        let proven = proven_metadata_read(page, &readiness);
        proven.require_transaction_summary_page_proof_checksum(
            expected_metadata_transaction_summary_page_proof_checksum,
        )
    }

}
