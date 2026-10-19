// SPDX-License-Identifier: Apache-2.0
impl MatrixObjectStore {
    pub fn configure_chunk_trim_flow(
        &mut self,
        rate_bytes_per_sec: u64,
        burst_bytes: u64,
        now_ms: u64,
    ) -> Result<(), ObjectError> {
        self.chunk_service
            .configure_chunk_trim_flow(rate_bytes_per_sec, burst_bytes, now_ms)
    }

    pub fn configure_chunk_trim_flow_ops(
        &mut self,
        rate_ops_per_sec: u64,
        burst_ops: u64,
        now_ms: u64,
    ) -> Result<(), ObjectError> {
        self.chunk_service
            .configure_chunk_trim_flow_ops(rate_ops_per_sec, burst_ops, now_ms)
    }

    pub fn configure_chunk_replica_trim_flow(
        &mut self,
        replica_index: usize,
        rate_bytes_per_sec: u64,
        burst_bytes: u64,
        now_ms: u64,
    ) -> Result<(), ObjectError> {
        self.chunk_service.configure_chunk_replica_trim_flow(
            replica_index,
            rate_bytes_per_sec,
            burst_bytes,
            now_ms,
        )
    }

    pub fn configure_chunk_replica_trim_flow_ops(
        &mut self,
        replica_index: usize,
        rate_ops_per_sec: u64,
        burst_ops: u64,
        now_ms: u64,
    ) -> Result<(), ObjectError> {
        self.chunk_service.configure_chunk_replica_trim_flow_ops(
            replica_index,
            rate_ops_per_sec,
            burst_ops,
            now_ms,
        )
    }

    pub fn prepare_chunk_trim_flow_raft_entry(
        &self,
        rate_bytes_per_sec: u64,
        burst_bytes: u64,
        now_ms: u64,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedChunkTrimFlowConfig, ObjectError> {
        let admitted_route_checksum = self.chunk_service.route_control_proof().state_checksum;
        self.prepare_chunk_trim_flow_raft_entry_fenced(
            rate_bytes_per_sec,
            burst_bytes,
            now_ms,
            expected_previous_commit_index,
            Some(&admitted_route_checksum),
        )
    }

    pub fn prepare_chunk_trim_flow_raft_entry_fenced(
        &self,
        rate_bytes_per_sec: u64,
        burst_bytes: u64,
        now_ms: u64,
        expected_previous_commit_index: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedChunkTrimFlowConfig, ObjectError> {
        self.prepare_chunk_trim_flow_raft_entry_targeted_fenced(
            None,
            rate_bytes_per_sec,
            burst_bytes,
            0,
            0,
            now_ms,
            expected_previous_commit_index,
            expected_route_control_checksum,
        )
    }

    pub fn prepare_chunk_replica_trim_flow_raft_entry_fenced(
        &self,
        replica_index: usize,
        rate_bytes_per_sec: u64,
        burst_bytes: u64,
        now_ms: u64,
        expected_previous_commit_index: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedChunkTrimFlowConfig, ObjectError> {
        self.prepare_chunk_trim_flow_raft_entry_targeted_fenced(
            Some(replica_index),
            rate_bytes_per_sec,
            burst_bytes,
            0,
            0,
            now_ms,
            expected_previous_commit_index,
            expected_route_control_checksum,
        )
    }

    pub fn prepare_chunk_trim_flow_raft_entry_full_fenced(
        &self,
        rate_bytes_per_sec: u64,
        burst_bytes: u64,
        rate_ops_per_sec: u64,
        burst_ops: u64,
        now_ms: u64,
        expected_previous_commit_index: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedChunkTrimFlowConfig, ObjectError> {
        self.prepare_chunk_trim_flow_raft_entry_targeted_fenced(
            None,
            rate_bytes_per_sec,
            burst_bytes,
            rate_ops_per_sec,
            burst_ops,
            now_ms,
            expected_previous_commit_index,
            expected_route_control_checksum,
        )
    }

    pub fn prepare_chunk_replica_trim_flow_raft_entry_full_fenced(
        &self,
        replica_index: usize,
        rate_bytes_per_sec: u64,
        burst_bytes: u64,
        rate_ops_per_sec: u64,
        burst_ops: u64,
        now_ms: u64,
        expected_previous_commit_index: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedChunkTrimFlowConfig, ObjectError> {
        self.prepare_chunk_trim_flow_raft_entry_targeted_fenced(
            Some(replica_index),
            rate_bytes_per_sec,
            burst_bytes,
            rate_ops_per_sec,
            burst_ops,
            now_ms,
            expected_previous_commit_index,
            expected_route_control_checksum,
        )
    }

    fn prepare_chunk_trim_flow_raft_entry_targeted_fenced(
        &self,
        target_replica_index: Option<usize>,
        rate_bytes_per_sec: u64,
        burst_bytes: u64,
        rate_ops_per_sec: u64,
        burst_ops: u64,
        now_ms: u64,
        expected_previous_commit_index: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedChunkTrimFlowConfig, ObjectError> {
        self.chunk_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        if expected_previous_commit_index != self.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared trim flow previous commit fence mismatch".to_string(),
            ));
        }
        let before_page_service_fingerprint =
            page_trim_flow_control_image_fingerprint(&self.chunk_service);
        let mut projected = self.chunk_service.clone();
        if let Some(replica_index) = target_replica_index {
            projected.configure_chunk_replica_trim_flow(
                replica_index,
                rate_bytes_per_sec,
                burst_bytes,
                now_ms,
            )?;
            projected.configure_chunk_replica_trim_flow_ops(
                replica_index,
                rate_ops_per_sec,
                burst_ops,
                now_ms,
            )?;
        } else {
            projected.configure_chunk_trim_flow(rate_bytes_per_sec, burst_bytes, now_ms)?;
            projected.configure_chunk_trim_flow_ops(rate_ops_per_sec, burst_ops, now_ms)?;
        }
        let after_page_service_fingerprint = page_trim_flow_control_image_fingerprint(&projected);
        let prepared_commit_index = expected_previous_commit_index.saturating_add(1);
        let (chunk_trim_flow_prepared_aggregate_checksum, chunk_prepared_replica_count) = projected
            .chunk_trim_flow_prepared_aggregate_checksum(
                target_replica_index,
                rate_bytes_per_sec,
                burst_bytes,
                rate_ops_per_sec,
                burst_ops,
                now_ms,
                expected_previous_commit_index,
                prepared_commit_index,
            )?;
        let route_control_proof = self.chunk_service.route_control_proof();
        let expected_route_control_checksum =
            expected_route_control_checksum.unwrap_or("").to_string();
        let mut prepared = PreparedChunkTrimFlowConfig {
            target_replica_index,
            rate_bytes_per_sec,
            burst_bytes,
            rate_ops_per_sec,
            burst_ops,
            now_ms,
            before_page_service_fingerprint,
            after_page_service_fingerprint,
            chunk_trim_flow_prepared_aggregate_checksum,
            chunk_prepared_replica_count,
            expected_previous_commit_index,
            prepared_commit_index,
            route_control_proof,
            expected_route_control_checksum,
            raft_entry_checksum: String::new(),
        };
        prepared.raft_entry_checksum = prepared_chunk_trim_flow_config_checksum(&prepared);
        Ok(prepared)
    }

    pub fn apply_prepared_chunk_trim_flow(
        &mut self,
        prepared: PreparedChunkTrimFlowConfig,
    ) -> Result<(), ObjectError> {
        let expected_checksum =
            prepared_chunk_trim_flow_config_checksum(&PreparedChunkTrimFlowConfig {
                raft_entry_checksum: String::new(),
                ..prepared.clone()
            });
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(
                "chunk prepared trim flow raft entry checksum mismatch".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared trim flow route fence is required".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum != prepared.route_control_proof.state_checksum {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared trim flow route proof checksum mismatch".to_string(),
            ));
        }
        if prepared.prepared_commit_index
            != prepared.expected_previous_commit_index.saturating_add(1)
        {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared trim flow commit index must be adjacent".to_string(),
            ));
        }
        let mut projected = self.chunk_service.clone();
        if let Some(replica_index) = prepared.target_replica_index {
            projected.configure_chunk_replica_trim_flow(
                replica_index,
                prepared.rate_bytes_per_sec,
                prepared.burst_bytes,
                prepared.now_ms,
            )?;
            projected.configure_chunk_replica_trim_flow_ops(
                replica_index,
                prepared.rate_ops_per_sec,
                prepared.burst_ops,
                prepared.now_ms,
            )?;
        } else {
            projected.configure_chunk_trim_flow(
                prepared.rate_bytes_per_sec,
                prepared.burst_bytes,
                prepared.now_ms,
            )?;
            projected.configure_chunk_trim_flow_ops(
                prepared.rate_ops_per_sec,
                prepared.burst_ops,
                prepared.now_ms,
            )?;
        }
        let (expected_aggregate_checksum, expected_replica_count) = projected
            .chunk_trim_flow_prepared_aggregate_checksum(
                prepared.target_replica_index,
                prepared.rate_bytes_per_sec,
                prepared.burst_bytes,
                prepared.rate_ops_per_sec,
                prepared.burst_ops,
                prepared.now_ms,
                prepared.expected_previous_commit_index,
                prepared.prepared_commit_index,
            )?;
        if prepared.chunk_prepared_replica_count != expected_replica_count {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared trim flow replica-count fence mismatch".to_string(),
            ));
        }
        if prepared.chunk_trim_flow_prepared_aggregate_checksum != expected_aggregate_checksum {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared trim flow aggregate checksum mismatch".to_string(),
            ));
        }
        let current_fingerprint = page_trim_flow_control_image_fingerprint(&self.chunk_service);
        if current_fingerprint == prepared.after_page_service_fingerprint {
            return Ok(());
        }
        if prepared.expected_previous_commit_index != self.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared trim flow previous commit fence mismatch".to_string(),
            ));
        }
        self.chunk_service
            .check_prepared_route_control_proof_cursor(
                &prepared.route_control_proof,
                "chunk prepared trim flow",
            )?;
        if current_fingerprint != prepared.before_page_service_fingerprint {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared trim flow source fingerprint mismatch".to_string(),
            ));
        }
        if let Some(replica_index) = prepared.target_replica_index {
            self.chunk_service.configure_chunk_replica_trim_flow(
                replica_index,
                prepared.rate_bytes_per_sec,
                prepared.burst_bytes,
                prepared.now_ms,
            )?;
            self.chunk_service.configure_chunk_replica_trim_flow_ops(
                replica_index,
                prepared.rate_ops_per_sec,
                prepared.burst_ops,
                prepared.now_ms,
            )?;
        } else {
            self.chunk_service.configure_chunk_trim_flow(
                prepared.rate_bytes_per_sec,
                prepared.burst_bytes,
                prepared.now_ms,
            )?;
            self.chunk_service.configure_chunk_trim_flow_ops(
                prepared.rate_ops_per_sec,
                prepared.burst_ops,
                prepared.now_ms,
            )?;
        }
        if page_trim_flow_control_image_fingerprint(&self.chunk_service)
            != prepared.after_page_service_fingerprint
        {
            return Err(ObjectError::Corruption(
                "chunk prepared trim flow deterministic apply mismatch".to_string(),
            ));
        }
        Ok(())
    }

}
