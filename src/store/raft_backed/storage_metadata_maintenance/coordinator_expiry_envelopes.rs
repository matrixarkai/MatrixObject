impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn prepare_expire_meta_coordinator_sessions_envelope_linearizable(
        &self,
        now_ms: u64,
        limit: usize,
    ) -> Result<PreparedMetadataCoordinatorSessionExpiryEnvelope, ObjectError> {
        self.prepare_expire_meta_coordinator_sessions_envelope_linearizable_including_fenced(
            now_ms, limit, false,
        )
    }

    pub fn prepare_expire_meta_coordinator_sessions_envelope_linearizable_including_fenced(
        &self,
        now_ms: u64,
        limit: usize,
        include_fenced: bool,
    ) -> Result<PreparedMetadataCoordinatorSessionExpiryEnvelope, ObjectError> {
        let proven = self
            .prepare_expire_meta_coordinator_sessions_envelope_with_read_proof_linearizable_including_fenced(
                now_ms,
                limit,
                include_fenced,
            )?;
        Ok(proven.value)
    }

    pub fn prepare_expire_meta_coordinator_sessions_envelope_with_read_proof_linearizable(
        &self,
        now_ms: u64,
        limit: usize,
    ) -> Result<ProvenMetadataRead<PreparedMetadataCoordinatorSessionExpiryEnvelope>, ObjectError>
    {
        self.prepare_expire_meta_coordinator_sessions_envelope_with_read_proof_linearizable_including_fenced(
            now_ms, limit, false,
        )
    }

    pub fn prepare_expire_meta_coordinator_sessions_envelope_with_read_proof_linearizable_including_fenced(
        &self,
        now_ms: u64,
        limit: usize,
        include_fenced: bool,
    ) -> Result<ProvenMetadataRead<PreparedMetadataCoordinatorSessionExpiryEnvelope>, ObjectError>
    {
        self.prepare_expire_meta_coordinator_sessions_envelope_with_read_proof_linearizable_committed_at_including_fenced(
            now_ms,
            limit,
            include_fenced,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prepare_expire_meta_coordinator_sessions_envelope_with_read_proof_linearizable_committed_at(
        &self,
        now_ms: u64,
        limit: usize,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<PreparedMetadataCoordinatorSessionExpiryEnvelope>, ObjectError>
    {
        self.prepare_expire_meta_coordinator_sessions_envelope_with_read_proof_linearizable_committed_at_including_fenced(
            now_ms,
            limit,
            false,
            min_read_index,
            min_commit_index,
        )
    }

    pub fn prepare_expire_meta_coordinator_sessions_envelope_with_read_proof_linearizable_committed_at_including_fenced(
        &self,
        now_ms: u64,
        limit: usize,
        include_fenced: bool,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<PreparedMetadataCoordinatorSessionExpiryEnvelope>, ObjectError>
    {
        let readiness = self.ensure_metadata_read_index_committed_at(
            "metadata coordinator session expiry",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        let prepared = self.prepare_expire_meta_coordinator_sessions_raft_entry_including_fenced(
            now_ms,
            limit,
            include_fenced,
        )?;
        Ok(proven_metadata_read(
            prepared_metadata_coordinator_session_expiry_envelope(&prepared)?,
            &readiness,
        ))
    }

    pub fn prepare_expire_meta_coordinator_sessions_envelope_bounded_with_read_proof_linearizable(
        &self,
        now_ms: u64,
        limit: usize,
        max_payload_bytes: u64,
    ) -> Result<ProvenMetadataRead<PreparedMetadataCoordinatorSessionExpiryEnvelope>, ObjectError>
    {
        self.prepare_expire_meta_coordinator_sessions_envelope_bounded_with_read_proof_linearizable_including_fenced(
            now_ms, limit, max_payload_bytes, false,
        )
    }

    pub fn prepare_expire_meta_coordinator_sessions_envelope_bounded_with_read_proof_linearizable_including_fenced(
        &self,
        now_ms: u64,
        limit: usize,
        max_payload_bytes: u64,
        include_fenced: bool,
    ) -> Result<ProvenMetadataRead<PreparedMetadataCoordinatorSessionExpiryEnvelope>, ObjectError>
    {
        self.prepare_expire_meta_coordinator_sessions_envelope_bounded_with_read_proof_linearizable_committed_at_including_fenced(
            now_ms,
            limit,
            max_payload_bytes,
            include_fenced,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prepare_expire_meta_coordinator_sessions_envelope_bounded_with_read_proof_linearizable_committed_at(
        &self,
        now_ms: u64,
        limit: usize,
        max_payload_bytes: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<PreparedMetadataCoordinatorSessionExpiryEnvelope>, ObjectError>
    {
        self.prepare_expire_meta_coordinator_sessions_envelope_bounded_with_read_proof_linearizable_committed_at_including_fenced(
            now_ms,
            limit,
            max_payload_bytes,
            false,
            min_read_index,
            min_commit_index,
        )
    }

    pub fn prepare_expire_meta_coordinator_sessions_envelope_bounded_with_read_proof_linearizable_committed_at_including_fenced(
        &self,
        now_ms: u64,
        limit: usize,
        max_payload_bytes: u64,
        include_fenced: bool,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<PreparedMetadataCoordinatorSessionExpiryEnvelope>, ObjectError>
    {
        let readiness = self.ensure_metadata_read_index_committed_at(
            "bounded metadata coordinator session expiry",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        let prepared = self
            .prepare_expire_meta_coordinator_sessions_raft_entry_bounded_including_fenced(
                now_ms,
                limit,
                max_payload_bytes,
                include_fenced,
            )?;
        Ok(proven_metadata_read(
            prepared_metadata_coordinator_session_expiry_envelope(&prepared)?,
            &readiness,
        ))
    }

}
