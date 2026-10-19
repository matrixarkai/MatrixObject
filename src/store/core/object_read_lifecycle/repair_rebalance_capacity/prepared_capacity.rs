impl MatrixObjectStore {
    pub fn prepare_chunk_capacity_raft_entry(
        &self,
        replica_index: usize,
        capacity_bytes: u64,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedChunkCapacityConfig, ObjectError> {
        let admitted_route_checksum = self.chunk_service.route_control_proof().state_checksum;
        self.prepare_chunk_capacity_raft_entry_fenced(
            replica_index,
            capacity_bytes,
            expected_previous_commit_index,
            Some(&admitted_route_checksum),
        )
    }

    pub fn prepare_chunk_capacity_raft_entry_fenced(
        &self,
        replica_index: usize,
        capacity_bytes: u64,
        expected_previous_commit_index: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedChunkCapacityConfig, ObjectError> {
        self.chunk_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        if expected_previous_commit_index != self.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared capacity previous commit fence mismatch".to_string(),
            ));
        }
        let before_page_service_fingerprint =
            page_capacity_control_image_fingerprint(&self.chunk_service);
        let mut projected = self.chunk_service.clone();
        if replica_index == PAGE_CHUNK_CAPACITY_ALL_ELIGIBLE_TARGET {
            projected.configure_all_eligible_chunk_replica_capacity(capacity_bytes)?;
        } else {
            projected.set_chunk_replica_capacity(replica_index, capacity_bytes)?;
        }
        let after_page_service_fingerprint = page_capacity_control_image_fingerprint(&projected);
        let prepared_commit_index = expected_previous_commit_index.saturating_add(1);
        let (chunk_capacity_prepared_aggregate_checksum, chunk_prepared_replica_count) = projected
            .chunk_capacity_prepared_aggregate_checksum(
                replica_index,
                capacity_bytes,
                expected_previous_commit_index,
                prepared_commit_index,
            )?;
        let route_control_proof = self.chunk_service.route_control_proof();
        let expected_route_control_checksum =
            expected_route_control_checksum.unwrap_or("").to_string();
        let mut prepared = PreparedChunkCapacityConfig {
            replica_index,
            capacity_bytes,
            before_page_service_fingerprint,
            after_page_service_fingerprint,
            chunk_capacity_prepared_aggregate_checksum,
            chunk_prepared_replica_count,
            expected_previous_commit_index,
            prepared_commit_index,
            route_control_proof,
            expected_route_control_checksum,
            raft_entry_checksum: String::new(),
        };
        prepared.raft_entry_checksum = prepared_chunk_capacity_config_checksum(&prepared);
        Ok(prepared)
    }

    pub fn apply_prepared_chunk_capacity(
        &mut self,
        prepared: PreparedChunkCapacityConfig,
    ) -> Result<(), ObjectError> {
        let expected_checksum =
            prepared_chunk_capacity_config_checksum(&PreparedChunkCapacityConfig {
                raft_entry_checksum: String::new(),
                ..prepared.clone()
            });
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(
                "chunk prepared capacity raft entry checksum mismatch".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared capacity route fence is required".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum != prepared.route_control_proof.state_checksum {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared capacity route proof checksum mismatch".to_string(),
            ));
        }
        if prepared.prepared_commit_index
            != prepared.expected_previous_commit_index.saturating_add(1)
        {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared capacity commit index must be adjacent".to_string(),
            ));
        }
        let mut projected = self.chunk_service.clone();
        if prepared.replica_index == PAGE_CHUNK_CAPACITY_ALL_ELIGIBLE_TARGET {
            projected.configure_all_eligible_chunk_replica_capacity(prepared.capacity_bytes)?;
        } else {
            projected
                .set_chunk_replica_capacity(prepared.replica_index, prepared.capacity_bytes)?;
        }
        let (expected_aggregate_checksum, expected_replica_count) = projected
            .chunk_capacity_prepared_aggregate_checksum(
                prepared.replica_index,
                prepared.capacity_bytes,
                prepared.expected_previous_commit_index,
                prepared.prepared_commit_index,
            )?;
        if prepared.chunk_prepared_replica_count != expected_replica_count {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared capacity replica-count fence mismatch".to_string(),
            ));
        }
        if prepared.chunk_capacity_prepared_aggregate_checksum != expected_aggregate_checksum {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared capacity aggregate checksum mismatch".to_string(),
            ));
        }
        let current_fingerprint = page_capacity_control_image_fingerprint(&self.chunk_service);
        if current_fingerprint == prepared.after_page_service_fingerprint {
            return Ok(());
        }
        if prepared.expected_previous_commit_index != self.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared capacity previous commit fence mismatch".to_string(),
            ));
        }
        self.chunk_service
            .check_prepared_route_control_proof_cursor(
                &prepared.route_control_proof,
                "chunk prepared capacity",
            )?;
        if prepared.replica_index == PAGE_CHUNK_CAPACITY_ALL_ELIGIBLE_TARGET {
            self.chunk_service
                .configure_all_eligible_chunk_replica_capacity(prepared.capacity_bytes)?;
        } else {
            self.chunk_service
                .set_chunk_replica_capacity(prepared.replica_index, prepared.capacity_bytes)?;
        }
        if page_capacity_control_image_fingerprint(&self.chunk_service)
            != prepared.after_page_service_fingerprint
        {
            return Err(ObjectError::Corruption(
                "chunk prepared capacity deterministic apply mismatch".to_string(),
            ));
        }
        Ok(())
    }

}
