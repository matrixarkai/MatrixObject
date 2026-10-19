// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn list_raft_membership_receipts_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        target_voter_node_id: Option<&str>,
        target_voter_checksum: Option<&str>,
        quorum_preserved: Option<bool>,
        membership_plan_safe: Option<bool>,
        now_ms: u64,
    ) -> Result<(RaftMembershipReceiptPage, MetadataRaftReadiness), ObjectError> {
        let commit_index = self.raft.commit_index();
        let readiness = self.metadata_raft_readiness(0, now_ms);
        if self.store.last_applied_index() < commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "raft membership receipt read index is not satisfied: applied={} required={}",
                self.store.last_applied_index(),
                commit_index
            )));
        }
        let page = self.store.list_raft_membership_receipts(
            marker,
            limit,
            target_voter_node_id,
            target_voter_checksum,
            quorum_preserved,
            membership_plan_safe,
        )?;
        Ok((page, readiness))
    }

    pub fn list_raft_membership_receipts_with_read_proof_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        target_voter_node_id: Option<&str>,
        target_voter_checksum: Option<&str>,
        quorum_preserved: Option<bool>,
        membership_plan_safe: Option<bool>,
        now_ms: u64,
    ) -> Result<ProvenReceiptRead<RaftMembershipReceiptPage>, ObjectError> {
        let (page, readiness) = self.list_raft_membership_receipts_linearizable(
            marker,
            limit,
            target_voter_node_id,
            target_voter_checksum,
            quorum_preserved,
            membership_plan_safe,
            now_ms,
        )?;
        Ok(proven_receipt_read(page, &readiness))
    }

    pub fn list_raft_membership_receipts_with_read_proof_linearizable_fenced(
        &self,
        marker: Option<&str>,
        limit: usize,
        target_voter_node_id: Option<&str>,
        target_voter_checksum: Option<&str>,
        quorum_preserved: Option<bool>,
        membership_plan_safe: Option<bool>,
        now_ms: u64,
        expected_raft_membership_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenReceiptRead<RaftMembershipReceiptPage>, ObjectError> {
        let proven = self.list_raft_membership_receipts_with_read_proof_linearizable(
            marker,
            limit,
            target_voter_node_id,
            target_voter_checksum,
            quorum_preserved,
            membership_plan_safe,
            now_ms,
        )?;
        proven.require_raft_membership_receipt_page_proof_checksum(
            expected_raft_membership_receipt_page_proof_checksum,
        )
    }

    pub fn list_metadata_lease_receipts_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        lease_namespace: Option<&str>,
        owner: Option<&str>,
        released: Option<bool>,
        now_ms: u64,
    ) -> Result<(MetadataLeaseReceiptPage, MetadataRaftReadiness), ObjectError> {
        let commit_index = self.raft.commit_index();
        let readiness = self.metadata_raft_readiness(0, now_ms);
        if self.store.last_applied_index() < commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata lease receipt read index is not satisfied: applied={} required={}",
                self.store.last_applied_index(),
                commit_index
            )));
        }
        let page = self.store.list_metadata_lease_receipts(
            marker,
            limit,
            lease_namespace,
            owner,
            released,
        )?;
        Ok((page, readiness))
    }

    pub fn list_metadata_lease_receipts_with_read_proof_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        lease_namespace: Option<&str>,
        owner: Option<&str>,
        released: Option<bool>,
        now_ms: u64,
    ) -> Result<ProvenReceiptRead<MetadataLeaseReceiptPage>, ObjectError> {
        let (page, readiness) = self.list_metadata_lease_receipts_linearizable(
            marker,
            limit,
            lease_namespace,
            owner,
            released,
            now_ms,
        )?;
        Ok(proven_receipt_read(page, &readiness))
    }

    pub fn list_metadata_lease_receipts_with_read_proof_linearizable_fenced(
        &self,
        marker: Option<&str>,
        limit: usize,
        lease_namespace: Option<&str>,
        owner: Option<&str>,
        released: Option<bool>,
        now_ms: u64,
        expected_metadata_lease_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenReceiptRead<MetadataLeaseReceiptPage>, ObjectError> {
        let proven = self.list_metadata_lease_receipts_with_read_proof_linearizable(
            marker,
            limit,
            lease_namespace,
            owner,
            released,
            now_ms,
        )?;
        proven.require_metadata_lease_receipt_page_proof_checksum(
            expected_metadata_lease_receipt_page_proof_checksum,
        )
    }

    pub fn list_metadata_shard_receipts_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        operation: Option<&str>,
        shard_id: Option<&str>,
        now_ms: u64,
    ) -> Result<(MetadataShardReceiptPage, MetadataRaftReadiness), ObjectError> {
        let commit_index = self.raft.commit_index();
        let readiness = self.metadata_raft_readiness(0, now_ms);
        if self.store.last_applied_index() < commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata shard receipt read index is not satisfied: applied={} required={}",
                self.store.last_applied_index(),
                commit_index
            )));
        }
        let page = self
            .store
            .list_metadata_shard_receipts(marker, limit, operation, shard_id)?;
        Ok((page, readiness))
    }

    pub fn list_metadata_shard_receipts_with_read_proof_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        operation: Option<&str>,
        shard_id: Option<&str>,
        now_ms: u64,
    ) -> Result<ProvenReceiptRead<MetadataShardReceiptPage>, ObjectError> {
        let (page, readiness) = self.list_metadata_shard_receipts_linearizable(
            marker, limit, operation, shard_id, now_ms,
        )?;
        Ok(proven_receipt_read(page, &readiness))
    }

    pub fn list_metadata_shard_receipts_with_read_proof_linearizable_fenced(
        &self,
        marker: Option<&str>,
        limit: usize,
        operation: Option<&str>,
        shard_id: Option<&str>,
        now_ms: u64,
        expected_metadata_shard_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenReceiptRead<MetadataShardReceiptPage>, ObjectError> {
        let proven = self.list_metadata_shard_receipts_with_read_proof_linearizable(
            marker, limit, operation, shard_id, now_ms,
        )?;
        proven.require_metadata_shard_receipt_page_proof_checksum(
            expected_metadata_shard_receipt_page_proof_checksum,
        )
    }

    fn ensure_receipt_read_index(
        &self,
        operation: &str,
        now_ms: u64,
    ) -> Result<MetadataRaftReadiness, ObjectError> {
        let commit_index = self.raft.commit_index();
        let readiness = self.metadata_raft_readiness(0, now_ms);
        if self.store.last_applied_index() < commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "{} receipt read index is not satisfied: applied={} required={}",
                operation,
                self.store.last_applied_index(),
                commit_index
            )));
        }
        Ok(readiness)
    }

}
