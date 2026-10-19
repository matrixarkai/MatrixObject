// SPDX-License-Identifier: Apache-2.0
impl MatrixObjectStore {
    fn record_consensus_read_observation(&self, readiness: &MetadataRaftReadiness) {
        if readiness.readable && readiness.read_lag == 0 && readiness.read_fence_applied_lag == 0 {
            self.consensus_read_cache_hits
                .set(self.consensus_read_cache_hits.get().saturating_add(1));
            *self.consensus_read_last_status.borrow_mut() = "cache_hit".to_string();
            return;
        }
        if readiness.read_lag != 0
            || readiness.read_fence_commit_lag != 0
            || readiness.read_fence_read_index_lag != 0
            || readiness.read_fence_applied_lag != 0
        {
            self.consensus_read_refreshes
                .set(self.consensus_read_refreshes.get().saturating_add(1));
        }
        *self.consensus_read_last_status.borrow_mut() = readiness.reason.clone();
    }

    pub fn metadata_control_proof_at(&self, now_ms: u64) -> MetadataControlProof {
        let readiness = self.metadata_raft_readiness(0, now_ms);
        let route_cache = self.metadata_route_cache_status_at(now_ms);
        let metadata_replication_quorum = raft_quorum(self.raft_voter_node_ids.len() as u64);
        let metadata_replication_acks = self
            .raft_healthy_peer_count
            .min(self.raft_voter_node_ids.len() as u64);
        metadata_control_proof(
            &readiness,
            &route_cache,
            self.raft_membership_epoch,
            self.raft_membership_receipts.len() as u64,
            metadata_replication_quorum,
            metadata_replication_acks,
        )
    }

    pub fn check_expected_metadata_control_proof(
        &self,
        expected_state_checksum: &str,
        now_ms: u64,
    ) -> Result<MetadataControlProof, ObjectError> {
        if expected_state_checksum.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "metadata control proof checksum expectation is empty".to_string(),
            ));
        }
        let proof = self.metadata_control_proof_at(now_ms);
        if proof.state_checksum != expected_state_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata control proof checksum mismatch: expected {} got {}",
                expected_state_checksum, proof.state_checksum
            )));
        }
        Ok(proof)
    }

    pub fn metadata_state_fingerprint(&self) -> String {
        self.meta_service.metadata_state_fingerprint()
    }

    pub fn metadata_route_cache_config(&self) -> MetadataRouteCacheConfig {
        MetadataRouteCacheConfig {
            entries: self.options.metadata_route_cache_entries,
            ttl_ms: self.options.metadata_route_cache_ttl_ms,
        }
    }

    pub fn configure_metadata_route_cache(
        &mut self,
        config: MetadataRouteCacheConfig,
    ) -> Result<MetadataRouteCacheConfig, ObjectError> {
        validate_metadata_route_cache_config(&config)?;
        self.options.metadata_route_cache_entries = config.entries;
        self.options.metadata_route_cache_ttl_ms = config.ttl_ms;
        self.invalidate_metadata_route_cache();
        Ok(config)
    }

    pub fn prepare_metadata_route_cache_config_raft_entry(
        &self,
        config: MetadataRouteCacheConfig,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataRouteCacheConfig, ObjectError> {
        if expected_previous_commit_index != self.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata route cache expected previous commit {} does not match current {}",
                expected_previous_commit_index,
                self.last_applied_index()
            )));
        }
        validate_metadata_route_cache_config(&config)?;
        let before_state_fingerprint = self.export_snapshot()?.snapshot_checksum;
        let mut projected = self.clone();
        projected.configure_metadata_route_cache(config.clone())?;
        let after_state_fingerprint = projected.export_snapshot()?.snapshot_checksum;
        let request_checksum = metadata_route_cache_config_request_checksum(&config);
        let mut prepared = PreparedMetadataRouteCacheConfig {
            config,
            before_state_fingerprint,
            after_state_fingerprint,
            metadata_control_proof_format: String::new(),
            metadata_control_proof_checksum: String::new(),
            metadata_control_proof_bytes: 0,
            metadata_control_proof_now_ms: 0,
            request_checksum,
            expected_previous_commit_index,
            prepared_commit_index: expected_previous_commit_index.saturating_add(1),
            raft_entry_checksum: String::new(),
        };
        prepared.raft_entry_checksum = prepared_metadata_route_cache_config_checksum(&prepared);
        Ok(prepared)
    }

    pub fn prepare_metadata_route_cache_config_raft_entry_fenced(
        &self,
        config: MetadataRouteCacheConfig,
        expected_previous_commit_index: u64,
        expected_metadata_control_proof_checksum: &str,
        now_ms: u64,
    ) -> Result<PreparedMetadataRouteCacheConfig, ObjectError> {
        let proof = self.check_expected_metadata_control_proof(
            expected_metadata_control_proof_checksum,
            now_ms,
        )?;
        let mut prepared = self.prepare_metadata_route_cache_config_raft_entry(
            config,
            expected_previous_commit_index,
        )?;
        prepared.metadata_control_proof_format = proof.state_format;
        prepared.metadata_control_proof_checksum =
            expected_metadata_control_proof_checksum.to_string();
        prepared.metadata_control_proof_bytes = proof.state_bytes;
        prepared.metadata_control_proof_now_ms = now_ms;
        prepared.raft_entry_checksum = prepared_metadata_route_cache_config_checksum(&prepared);
        Ok(prepared)
    }

    pub fn apply_prepared_metadata_route_cache_config(
        &mut self,
        prepared: PreparedMetadataRouteCacheConfig,
    ) -> Result<MetadataRouteCacheConfig, ObjectError> {
        let expected_checksum =
            prepared_metadata_route_cache_config_checksum(&PreparedMetadataRouteCacheConfig {
                raft_entry_checksum: String::new(),
                ..prepared.clone()
            });
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(
                "prepared metadata route cache checksum mismatch".to_string(),
            ));
        }
        if prepared.request_checksum
            != metadata_route_cache_config_request_checksum(&prepared.config)
        {
            return Err(ObjectError::Corruption(
                "prepared metadata route cache request checksum mismatch".to_string(),
            ));
        }
        if prepared.prepared_commit_index
            != prepared.expected_previous_commit_index.saturating_add(1)
        {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata route cache commit index must be adjacent".to_string(),
            ));
        }
        if self.last_applied_index() >= prepared.prepared_commit_index
            && self.metadata_route_cache_config() == prepared.config
            && self.export_snapshot()?.snapshot_checksum == prepared.after_state_fingerprint
        {
            return Ok(prepared.config);
        }
        if prepared.expected_previous_commit_index != self.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(format!(
                "prepared metadata route cache expected previous commit {} does not match current {}",
                prepared.expected_previous_commit_index,
                self.last_applied_index()
            )));
        }
        if prepared.before_state_fingerprint != self.export_snapshot()?.snapshot_checksum {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata route cache source state changed".to_string(),
            ));
        }
        let mut projected = self.clone();
        let applied = projected.configure_metadata_route_cache(prepared.config.clone())?;
        if projected.export_snapshot()?.snapshot_checksum != prepared.after_state_fingerprint {
            return Err(ObjectError::Corruption(
                "prepared metadata route cache deterministic apply mismatch".to_string(),
            ));
        }
        *self = projected;
        Ok(applied)
    }

}
