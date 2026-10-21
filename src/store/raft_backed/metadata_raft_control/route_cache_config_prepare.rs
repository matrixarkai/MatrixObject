// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
pub fn prepare_configure_meta_route_cache_raft_entry(
        &self,
        config: MetadataRouteCacheConfig,
    ) -> Result<PreparedMetadataRouteCacheConfig, ObjectError> {
        self.store
            .prepare_metadata_route_cache_config_raft_entry(config, self.store.last_applied_index())
    }

    pub fn prepare_configure_meta_route_cache_raft_entry_linearizable(
        &self,
        config: MetadataRouteCacheConfig,
        now_ms: u64,
    ) -> Result<PreparedMetadataRouteCacheConfig, ObjectError> {
        self.ensure_metadata_read_index("prepare metadata route cache", now_ms)?;
        let proof = self.store.metadata_control_proof_at(now_ms);
        self.prepare_configure_meta_route_cache_raft_entry_fenced(
            config,
            &proof.state_checksum,
            now_ms,
        )
    }

    pub fn prepare_configure_meta_route_cache_raft_entry_fenced(
        &self,
        config: MetadataRouteCacheConfig,
        expected_metadata_control_proof_checksum: &str,
        now_ms: u64,
    ) -> Result<PreparedMetadataRouteCacheConfig, ObjectError> {
        self.store
            .prepare_metadata_route_cache_config_raft_entry_fenced(
                config,
                self.store.last_applied_index(),
                expected_metadata_control_proof_checksum,
                now_ms,
            )
    }

    pub fn prepare_configure_meta_route_cache_raft_entry_linearizable_fenced(
        &self,
        config: MetadataRouteCacheConfig,
        expected_metadata_control_proof_checksum: &str,
        now_ms: u64,
    ) -> Result<PreparedMetadataRouteCacheConfig, ObjectError> {
        self.ensure_metadata_read_index("prepare fenced metadata route cache", now_ms)?;
        self.prepare_configure_meta_route_cache_raft_entry_fenced(
            config,
            expected_metadata_control_proof_checksum,
            now_ms,
        )
    }

    pub fn prepare_configure_meta_route_cache_raft_entry_with_read_proof_linearizable_fenced(
        &self,
        config: MetadataRouteCacheConfig,
        expected_metadata_control_proof_checksum: &str,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<PreparedMetadataRouteCacheConfig>, ObjectError> {
        self.prepare_configure_meta_route_cache_raft_entry_with_read_proof_linearizable_fenced_committed_at(
            config,
            expected_metadata_control_proof_checksum,
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prepare_configure_meta_route_cache_raft_entry_with_read_proof_linearizable_fenced_committed_at(
        &self,
        config: MetadataRouteCacheConfig,
        expected_metadata_control_proof_checksum: &str,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<PreparedMetadataRouteCacheConfig>, ObjectError> {
        let readiness = self.ensure_metadata_read_index_committed_at(
            "prepare proven metadata route cache",
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
        let prepared = self.prepare_configure_meta_route_cache_raft_entry_fenced(
            config,
            expected_metadata_control_proof_checksum,
            now_ms,
        )?;
        Ok(proven_metadata_read(prepared, &readiness))
    }
}
