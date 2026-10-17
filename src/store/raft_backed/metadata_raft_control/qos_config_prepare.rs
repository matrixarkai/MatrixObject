impl<R: RustRaftLog> RaftBackedObjectStore<R> {
pub fn prepare_configure_meta_qos_raft_entry(
        &self,
        config: MetadataQosConfig,
    ) -> Result<PreparedMetadataQosConfig, ObjectError> {
        self.store
            .prepare_metadata_qos_config_raft_entry(config, self.store.last_metadata_commit_index())
    }

    pub fn prepare_configure_meta_qos_raft_entry_linearizable(
        &self,
        config: MetadataQosConfig,
        now_ms: u64,
    ) -> Result<PreparedMetadataQosConfig, ObjectError> {
        self.ensure_metadata_read_index("prepare metadata qos", now_ms)?;
        let proof = self.store.metadata_control_proof_at(now_ms);
        self.prepare_configure_meta_qos_raft_entry_fenced(config, &proof.state_checksum, now_ms)
    }

    pub fn prepare_configure_meta_qos_raft_entry_fenced(
        &self,
        config: MetadataQosConfig,
        expected_metadata_control_proof_checksum: &str,
        now_ms: u64,
    ) -> Result<PreparedMetadataQosConfig, ObjectError> {
        self.store.prepare_metadata_qos_config_raft_entry_fenced(
            config,
            self.store.last_metadata_commit_index(),
            expected_metadata_control_proof_checksum,
            now_ms,
        )
    }

    pub fn prepare_configure_meta_qos_raft_entry_linearizable_fenced(
        &self,
        config: MetadataQosConfig,
        expected_metadata_control_proof_checksum: &str,
        now_ms: u64,
    ) -> Result<PreparedMetadataQosConfig, ObjectError> {
        self.ensure_metadata_read_index("prepare fenced metadata qos", now_ms)?;
        self.prepare_configure_meta_qos_raft_entry_fenced(
            config,
            expected_metadata_control_proof_checksum,
            now_ms,
        )
    }

    pub fn prepare_configure_meta_qos_raft_entry_with_read_proof_linearizable_fenced(
        &self,
        config: MetadataQosConfig,
        expected_metadata_control_proof_checksum: &str,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<PreparedMetadataQosConfig>, ObjectError> {
        self.prepare_configure_meta_qos_raft_entry_with_read_proof_linearizable_fenced_committed_at(
            config,
            expected_metadata_control_proof_checksum,
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prepare_configure_meta_qos_raft_entry_with_read_proof_linearizable_fenced_committed_at(
        &self,
        config: MetadataQosConfig,
        expected_metadata_control_proof_checksum: &str,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<PreparedMetadataQosConfig>, ObjectError> {
        let readiness = self.ensure_metadata_read_index_committed_at(
            "prepare proven metadata qos",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        let expected_metadata_control_proof_checksum =
            if expected_metadata_control_proof_checksum.is_empty() {
                readiness.metadata_control_proof_checksum.as_str()
            } else {
                expected_metadata_control_proof_checksum
            };
        let prepared = self.prepare_configure_meta_qos_raft_entry_fenced(
            config,
            expected_metadata_control_proof_checksum,
            now_ms,
        )?;
        Ok(proven_metadata_read(prepared, &readiness))
    }
}
