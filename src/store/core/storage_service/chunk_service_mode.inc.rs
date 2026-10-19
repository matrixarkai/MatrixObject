impl MatrixObjectStore {
    pub fn prepare_replica_service_mode_raft_entry(
        &self,
        replica_index: usize,
        accepting_writes: Option<bool>,
        draining: Option<bool>,
        frozen: Option<bool>,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedReplicaServiceModeConfig, ObjectError> {
        let admitted_route_checksum = self.chunk_service.route_control_proof().state_checksum;
        self.prepare_replica_service_mode_raft_entry_fenced(
            replica_index,
            accepting_writes,
            draining,
            frozen,
            expected_previous_commit_index,
            Some(&admitted_route_checksum),
        )
    }

    pub fn prepare_replica_service_mode_raft_entry_fenced(
        &self,
        replica_index: usize,
        accepting_writes: Option<bool>,
        draining: Option<bool>,
        frozen: Option<bool>,
        expected_previous_commit_index: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedReplicaServiceModeConfig, ObjectError> {
        self.chunk_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        if expected_previous_commit_index != self.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared service-mode previous commit fence mismatch".to_string(),
            ));
        }
        let before_page_service_fingerprint =
            page_capacity_control_image_fingerprint(&self.chunk_service);
        let mut projected = self.chunk_service.clone();
        if replica_index == PAGE_REPLICA_SERVICE_MODE_ALL_ELIGIBLE_TARGET {
            projected.configure_all_eligible_chunk_replica_service_mode(
                accepting_writes,
                draining,
                frozen,
            )?;
        } else {
            projected.configure_chunk_replica_service_mode(
                replica_index,
                accepting_writes,
                draining,
                frozen,
            )?;
        }
        let after_page_service_fingerprint = page_capacity_control_image_fingerprint(&projected);
        let prepared_commit_index = expected_previous_commit_index.saturating_add(1);
        let (replica_service_mode_prepared_aggregate_checksum, chunk_prepared_replica_count) =
            projected.replica_service_mode_prepared_aggregate_checksum(
                replica_index,
                accepting_writes,
                draining,
                frozen,
                expected_previous_commit_index,
                prepared_commit_index,
            )?;
        let route_control_proof = self.chunk_service.route_control_proof();
        let expected_route_control_checksum =
            expected_route_control_checksum.unwrap_or("").to_string();
        let mut prepared = PreparedReplicaServiceModeConfig {
            replica_index,
            accepting_writes,
            draining,
            frozen,
            before_page_service_fingerprint,
            after_page_service_fingerprint,
            replica_service_mode_prepared_aggregate_checksum,
            chunk_prepared_replica_count,
            expected_previous_commit_index,
            prepared_commit_index,
            route_control_proof,
            expected_route_control_checksum,
            raft_entry_checksum: String::new(),
        };
        prepared.raft_entry_checksum = prepared_replica_service_mode_config_checksum(&prepared);
        Ok(prepared)
    }

    pub fn apply_prepared_replica_service_mode(
        &mut self,
        prepared: PreparedReplicaServiceModeConfig,
    ) -> Result<(), ObjectError> {
        let expected_checksum =
            prepared_replica_service_mode_config_checksum(&PreparedReplicaServiceModeConfig {
                raft_entry_checksum: String::new(),
                ..prepared.clone()
            });
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(
                "chunk prepared service-mode raft entry checksum mismatch".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared service-mode route fence is required".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum != prepared.route_control_proof.state_checksum {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared service-mode route proof checksum mismatch".to_string(),
            ));
        }
        if prepared.prepared_commit_index
            != prepared.expected_previous_commit_index.saturating_add(1)
        {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared service-mode commit index must be adjacent".to_string(),
            ));
        }
        let mut projected = self.chunk_service.clone();
        if prepared.replica_index == PAGE_REPLICA_SERVICE_MODE_ALL_ELIGIBLE_TARGET {
            projected.configure_all_eligible_chunk_replica_service_mode(
                prepared.accepting_writes,
                prepared.draining,
                prepared.frozen,
            )?;
        } else {
            projected.configure_chunk_replica_service_mode(
                prepared.replica_index,
                prepared.accepting_writes,
                prepared.draining,
                prepared.frozen,
            )?;
        }
        let (expected_aggregate_checksum, expected_replica_count) = projected
            .replica_service_mode_prepared_aggregate_checksum(
                prepared.replica_index,
                prepared.accepting_writes,
                prepared.draining,
                prepared.frozen,
                prepared.expected_previous_commit_index,
                prepared.prepared_commit_index,
            )?;
        if prepared.chunk_prepared_replica_count != expected_replica_count {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared service-mode replica-count fence mismatch".to_string(),
            ));
        }
        if prepared.replica_service_mode_prepared_aggregate_checksum != expected_aggregate_checksum {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared service-mode aggregate checksum mismatch".to_string(),
            ));
        }
        let current_fingerprint = page_capacity_control_image_fingerprint(&self.chunk_service);
        if current_fingerprint == prepared.after_page_service_fingerprint {
            return Ok(());
        }
        if prepared.expected_previous_commit_index != self.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared service-mode previous commit fence mismatch".to_string(),
            ));
        }
        self.chunk_service
            .check_prepared_route_control_proof_cursor(
                &prepared.route_control_proof,
                "chunk prepared service-mode",
            )?;
        if prepared.replica_index == PAGE_REPLICA_SERVICE_MODE_ALL_ELIGIBLE_TARGET {
            self.chunk_service
                .configure_all_eligible_chunk_replica_service_mode(
                    prepared.accepting_writes,
                    prepared.draining,
                    prepared.frozen,
                )?;
        } else {
            self.chunk_service.configure_chunk_replica_service_mode(
                prepared.replica_index,
                prepared.accepting_writes,
                prepared.draining,
                prepared.frozen,
            )?;
        }
        if page_capacity_control_image_fingerprint(&self.chunk_service)
            != prepared.after_page_service_fingerprint
        {
            return Err(ObjectError::Corruption(
                "chunk prepared service-mode deterministic apply mismatch".to_string(),
            ));
        }
        Ok(())
    }

}
