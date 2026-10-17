impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn prepare_run_meta_maintenance_envelope_linearizable(
        &self,
        now_ms: u64,
        lease_prefix: &str,
        lease_limit: usize,
        session_limit: usize,
        transaction_limit: usize,
    ) -> Result<PreparedMetadataMaintenanceEnvelope, ObjectError> {
        let proven = self
            .prepare_run_meta_maintenance_envelope_with_payload_budget_and_read_proof_linearizable(
                now_ms,
                lease_prefix,
                lease_limit,
                session_limit,
                transaction_limit,
                0,
            )?;
        Ok(proven.value)
    }

    pub fn prepare_run_meta_maintenance_envelope_with_read_proof_linearizable(
        &self,
        now_ms: u64,
        lease_prefix: &str,
        lease_limit: usize,
        session_limit: usize,
        transaction_limit: usize,
    ) -> Result<ProvenMetadataRead<PreparedMetadataMaintenanceEnvelope>, ObjectError> {
        self.prepare_run_meta_maintenance_envelope_with_payload_budget_and_read_proof_linearizable_committed_at(
            now_ms,
            lease_prefix,
            lease_limit,
            session_limit,
            transaction_limit,
            0,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prepare_run_meta_maintenance_envelope_with_payload_budget_and_read_proof_linearizable(
        &self,
        now_ms: u64,
        lease_prefix: &str,
        lease_limit: usize,
        session_limit: usize,
        transaction_limit: usize,
        max_payload_bytes: u64,
    ) -> Result<ProvenMetadataRead<PreparedMetadataMaintenanceEnvelope>, ObjectError> {
        self.prepare_run_meta_maintenance_envelope_with_payload_budget_and_read_proof_linearizable_committed_at(
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

    pub fn prepare_run_meta_maintenance_envelope_with_payload_budget_and_read_proof_linearizable_committed_at(
        &self,
        now_ms: u64,
        lease_prefix: &str,
        lease_limit: usize,
        session_limit: usize,
        transaction_limit: usize,
        max_payload_bytes: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<PreparedMetadataMaintenanceEnvelope>, ObjectError> {
        let readiness = self.ensure_metadata_read_index_committed_at(
            "metadata maintenance prepare",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        let prepared = if max_payload_bytes == 0 {
            self.prepare_run_meta_maintenance_raft_entry_committed_at(
                now_ms,
                lease_prefix,
                lease_limit,
                session_limit,
                transaction_limit,
                min_read_index,
                min_commit_index,
            )?
        } else {
            self.prepare_run_meta_maintenance_raft_entry_with_payload_budget_committed_at(
                now_ms,
                lease_prefix,
                lease_limit,
                session_limit,
                transaction_limit,
                max_payload_bytes,
                min_read_index,
                min_commit_index,
            )?
        };
        Ok(proven_metadata_read(
            prepared_metadata_maintenance_envelope(&prepared),
            &readiness,
        ))
    }
}
