// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn list_meta_coordinator_session_receipts_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        operation: Option<&str>,
        session_id: Option<&str>,
        owner: Option<&str>,
        released: Option<bool>,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<(CoordinatorSessionReceiptPage, MetadataRaftReadiness), ObjectError> {
        let commit_index = self.raft.commit_index();
        let readiness = self.metadata_raft_readiness(0, now_ms);
        if self.store.last_applied_index() < commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "coordinator session receipt read index is not satisfied: applied={} required={}",
                self.store.last_applied_index(),
                commit_index
            )));
        }
        let page = self.store.list_meta_coordinator_session_receipts(
            marker,
            limit,
            operation,
            session_id,
            owner,
            released,
            max_payload_bytes,
        )?;
        Ok((page, readiness))
    }

    pub fn list_meta_coordinator_session_receipts_with_read_proof_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        operation: Option<&str>,
        session_id: Option<&str>,
        owner: Option<&str>,
        released: Option<bool>,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<ProvenReceiptRead<CoordinatorSessionReceiptPage>, ObjectError> {
        let (page, readiness) = self.list_meta_coordinator_session_receipts_linearizable(
            marker,
            limit,
            operation,
            session_id,
            owner,
            released,
            max_payload_bytes,
            now_ms,
        )?;
        Ok(proven_receipt_read(page, &readiness))
    }

    pub fn list_meta_coordinator_session_receipts_with_read_proof_linearizable_fenced(
        &self,
        marker: Option<&str>,
        limit: usize,
        operation: Option<&str>,
        session_id: Option<&str>,
        owner: Option<&str>,
        released: Option<bool>,
        max_payload_bytes: u64,
        now_ms: u64,
        expected_coordinator_session_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenReceiptRead<CoordinatorSessionReceiptPage>, ObjectError> {
        let proven = self.list_meta_coordinator_session_receipts_with_read_proof_linearizable(
            marker,
            limit,
            operation,
            session_id,
            owner,
            released,
            max_payload_bytes,
            now_ms,
        )?;
        proven.require_coordinator_session_receipt_page_proof_checksum(
            expected_coordinator_session_receipt_page_proof_checksum,
        )
    }

    pub fn list_meta_coordinator_config_receipts_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        backend: Option<&str>,
        now_ms: u64,
    ) -> Result<(MetadataCoordinatorConfigReceiptPage, MetadataRaftReadiness), ObjectError> {
        let readiness = self.ensure_receipt_read_index("coordinator config", now_ms)?;
        let page = self
            .store
            .list_meta_coordinator_config_receipts(marker, limit, backend)?;
        Ok((page, readiness))
    }

    pub fn list_meta_coordinator_config_receipts_with_read_proof_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        backend: Option<&str>,
        now_ms: u64,
    ) -> Result<ProvenReceiptRead<MetadataCoordinatorConfigReceiptPage>, ObjectError> {
        let (page, readiness) = self
            .list_meta_coordinator_config_receipts_linearizable(marker, limit, backend, now_ms)?;
        Ok(proven_receipt_read(page, &readiness))
    }

    pub fn list_meta_coordinator_config_receipts_with_read_proof_linearizable_fenced(
        &self,
        marker: Option<&str>,
        limit: usize,
        backend: Option<&str>,
        now_ms: u64,
        expected_metadata_coordinator_config_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenReceiptRead<MetadataCoordinatorConfigReceiptPage>, ObjectError> {
        let proven = self.list_meta_coordinator_config_receipts_with_read_proof_linearizable(
            marker, limit, backend, now_ms,
        )?;
        proven.require_coordinator_config_receipt_page_proof_checksum(
            expected_metadata_coordinator_config_receipt_page_proof_checksum,
        )
    }

}
