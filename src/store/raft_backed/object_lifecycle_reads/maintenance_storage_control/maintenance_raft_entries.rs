impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn prepare_run_meta_maintenance_raft_entry(
        &self,
        now_ms: u64,
        lease_prefix: &str,
        lease_limit: usize,
        session_limit: usize,
        transaction_limit: usize,
    ) -> Result<PreparedMetadataMaintenance, ObjectError> {
        self.prepare_run_meta_maintenance_raft_entry_committed_at(
            now_ms,
            lease_prefix,
            lease_limit,
            session_limit,
            transaction_limit,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prepare_run_meta_maintenance_raft_entry_committed_at(
        &self,
        now_ms: u64,
        lease_prefix: &str,
        lease_limit: usize,
        session_limit: usize,
        transaction_limit: usize,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<PreparedMetadataMaintenance, ObjectError> {
        let readiness = self.ensure_metadata_read_index_committed_at(
            "prepare metadata maintenance",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        let mut prepared = self.store.prepare_metadata_maintenance_raft_entry(
            now_ms,
            lease_prefix,
            lease_limit,
            session_limit,
            transaction_limit,
            self.store.last_metadata_commit_index(),
        )?;
        prepared.metadata_read_index = readiness.raft_log_commit_index;
        prepared.metadata_raft_term = readiness.raft_term;
        prepared.raft_entry_checksum = prepared_metadata_maintenance_checksum(&prepared);
        Ok(prepared)
    }

    pub fn prepare_run_meta_maintenance_raft_entry_with_payload_budget(
        &self,
        now_ms: u64,
        lease_prefix: &str,
        lease_limit: usize,
        session_limit: usize,
        transaction_limit: usize,
        max_payload_bytes: u64,
    ) -> Result<PreparedMetadataMaintenance, ObjectError> {
        self.prepare_run_meta_maintenance_raft_entry_with_payload_budget_committed_at(
            now_ms,
            lease_prefix,
            lease_limit,
            session_limit,
            transaction_limit,
            max_payload_bytes,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prepare_run_meta_maintenance_raft_entry_with_payload_budget_committed_at(
        &self,
        now_ms: u64,
        lease_prefix: &str,
        lease_limit: usize,
        session_limit: usize,
        transaction_limit: usize,
        max_payload_bytes: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<PreparedMetadataMaintenance, ObjectError> {
        let readiness = self.ensure_metadata_read_index_committed_at(
            "prepare bounded metadata maintenance",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        let mut prepared = self
            .store
            .prepare_metadata_maintenance_raft_entry_with_payload_budget(
                now_ms,
                lease_prefix,
                lease_limit,
                session_limit,
                transaction_limit,
                self.store.last_metadata_commit_index(),
                max_payload_bytes,
            )?;
        prepared.metadata_read_index = readiness.raft_log_commit_index;
        prepared.metadata_raft_term = readiness.raft_term;
        prepared.raft_entry_checksum = prepared_metadata_maintenance_checksum(&prepared);
        Ok(prepared)
    }
}
