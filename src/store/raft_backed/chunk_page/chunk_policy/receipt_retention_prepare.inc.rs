impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn prepare_configure_chunk_receipt_retention_raft_entry(
        &self,
        retention: usize,
    ) -> Result<PreparedChunkReceiptRetentionConfig, ObjectError> {
        self.store
            .prepare_chunk_receipt_retention_raft_entry(retention, self.store.last_applied_index())
    }

    pub fn prepare_configure_chunk_replica_receipt_retention_raft_entry(
        &self,
        replica_index: usize,
        retention: usize,
    ) -> Result<PreparedChunkReceiptRetentionConfig, ObjectError> {
        let admitted_route_checksum = self
            .store
            .page_service
            .route_control_proof()
            .state_checksum;
        self.store
            .prepare_chunk_replica_receipt_retention_raft_entry_fenced(
                replica_index,
                retention,
                self.store.last_applied_index(),
                Some(&admitted_route_checksum),
            )
    }

    pub fn prepare_configure_chunk_receipt_retention_raft_entry_linearizable_fenced(
        &self,
        retention: usize,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedChunkReceiptRetentionConfig, ObjectError> {
        let proof = self.ensure_storage_control_read_index(
            "prepare chunk receipt retention",
            expected_route_control_checksum,
        )?;
        let route_checksum =
            expected_route_control_checksum.unwrap_or(proof.state_checksum.as_str());
        self.store
            .prepare_chunk_receipt_retention_raft_entry_fenced(
                retention,
                self.store.last_applied_index(),
                Some(route_checksum),
            )
    }

    pub fn prepare_configure_chunk_receipt_retention_raft_entry_linearizable(
        &self,
        retention: usize,
    ) -> Result<PreparedChunkReceiptRetentionConfig, ObjectError> {
        self.prepare_configure_chunk_receipt_retention_raft_entry_linearizable_fenced(
            retention, None,
        )
    }

    pub fn prepare_configure_chunk_replica_receipt_retention_raft_entry_linearizable_fenced(
        &self,
        replica_index: usize,
        retention: usize,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedChunkReceiptRetentionConfig, ObjectError> {
        let proof = self.ensure_storage_control_read_index(
            "prepare chunk replica receipt retention",
            expected_route_control_checksum,
        )?;
        let route_checksum =
            expected_route_control_checksum.unwrap_or(proof.state_checksum.as_str());
        self.store
            .prepare_chunk_replica_receipt_retention_raft_entry_fenced(
                replica_index,
                retention,
                self.store.last_applied_index(),
                Some(route_checksum),
            )
    }

    pub fn prepare_configure_chunk_receipt_retention_raft_entry_with_read_proof_linearizable_fenced(
        &self,
        retention: usize,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ProvenStorageRead<PreparedChunkReceiptRetentionConfig>, ObjectError> {
        let requested_read_index = self.raft.commit_index();
        let prepared = self
            .prepare_configure_chunk_receipt_retention_raft_entry_linearizable_fenced(
                retention,
                expected_route_control_checksum,
            )?;
        let proof = self.page_storage_read_proof(requested_read_index);
        Ok(ProvenStorageRead {
            value: prepared,
            proof,
        })
    }

    pub fn prepare_configure_chunk_replica_receipt_retention_raft_entry_with_read_proof_linearizable_fenced(
        &self,
        replica_index: usize,
        retention: usize,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ProvenStorageRead<PreparedChunkReceiptRetentionConfig>, ObjectError> {
        let requested_read_index = self.raft.commit_index();
        let prepared = self
            .prepare_configure_chunk_replica_receipt_retention_raft_entry_linearizable_fenced(
                replica_index,
                retention,
                expected_route_control_checksum,
            )?;
        let proof = self.page_storage_read_proof(requested_read_index);
        Ok(ProvenStorageRead {
            value: prepared,
            proof,
        })
    }
}
