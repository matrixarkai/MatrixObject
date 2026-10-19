// SPDX-License-Identifier: Apache-2.0
impl MatrixObjectStore {
    pub fn configure_chunk_mutation_qos(
        &mut self,
        config: ReplicaMutationQosConfig,
    ) -> Result<ReplicaMutationQosConfig, ObjectError> {
        self.chunk_service.configure_chunk_mutation_qos(config)
    }

    pub fn configure_chunk_replica_mutation_qos(
        &mut self,
        replica_index: usize,
        config: ReplicaMutationQosConfig,
    ) -> Result<ReplicaMutationQosConfig, ObjectError> {
        self.chunk_service
            .configure_chunk_replica_mutation_qos(replica_index, config)
    }

    pub fn prepare_chunk_mutation_qos_raft_entry(
        &self,
        config: ReplicaMutationQosConfig,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedChunkMutationQosConfig, ObjectError> {
        let admitted_route_checksum = self.chunk_service.route_control_proof().state_checksum;
        self.prepare_chunk_mutation_qos_raft_entry_fenced(
            config,
            expected_previous_commit_index,
            Some(&admitted_route_checksum),
        )
    }

    pub fn prepare_chunk_mutation_qos_raft_entry_fenced(
        &self,
        config: ReplicaMutationQosConfig,
        expected_previous_commit_index: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedChunkMutationQosConfig, ObjectError> {
        self.prepare_chunk_mutation_qos_raft_entry_targeted_fenced(
            None,
            config,
            expected_previous_commit_index,
            expected_route_control_checksum,
        )
    }

    pub fn prepare_chunk_replica_mutation_qos_raft_entry_fenced(
        &self,
        replica_index: usize,
        config: ReplicaMutationQosConfig,
        expected_previous_commit_index: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedChunkMutationQosConfig, ObjectError> {
        self.prepare_chunk_mutation_qos_raft_entry_targeted_fenced(
            Some(replica_index),
            config,
            expected_previous_commit_index,
            expected_route_control_checksum,
        )
    }

    fn prepare_chunk_mutation_qos_raft_entry_targeted_fenced(
        &self,
        target_replica_index: Option<usize>,
        config: ReplicaMutationQosConfig,
        expected_previous_commit_index: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedChunkMutationQosConfig, ObjectError> {
        self.chunk_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        if expected_previous_commit_index != self.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared mutation qos previous commit fence mismatch".to_string(),
            ));
        }
        let mut projected = self.chunk_service.clone();
        let prepared_commit_index = expected_previous_commit_index.saturating_add(1);
        let applied_config = if let Some(replica_index) = target_replica_index {
            projected.configure_chunk_replica_mutation_qos_at_commit(
                replica_index,
                config,
                prepared_commit_index,
            )?
        } else {
            projected.configure_chunk_mutation_qos_at_commit(config, prepared_commit_index)?
        };
        let (chunk_mutation_qos_prepared_aggregate_checksum, chunk_prepared_replica_count) =
            projected.chunk_mutation_qos_prepared_aggregate_checksum(
                target_replica_index,
                &applied_config,
                expected_previous_commit_index,
                prepared_commit_index,
            )?;
        let mut prepared = PreparedChunkMutationQosConfig {
            target_replica_index,
            config: applied_config,
            chunk_service: projected,
            chunk_mutation_qos_prepared_aggregate_checksum,
            chunk_prepared_replica_count,
            expected_previous_commit_index,
            prepared_commit_index,
            route_control_proof: self.chunk_service.route_control_proof(),
            expected_route_control_checksum: expected_route_control_checksum
                .unwrap_or_default()
                .to_string(),
            raft_entry_checksum: String::new(),
        };
        prepared.raft_entry_checksum = prepared_chunk_mutation_qos_config_checksum(&prepared);
        Ok(prepared)
    }

    pub fn apply_prepared_chunk_mutation_qos(
        &mut self,
        prepared: PreparedChunkMutationQosConfig,
    ) -> Result<ReplicaMutationQosConfig, ObjectError> {
        let expected_checksum =
            prepared_chunk_mutation_qos_config_checksum(&PreparedChunkMutationQosConfig {
                raft_entry_checksum: String::new(),
                ..prepared.clone()
            });
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(
                "chunk prepared mutation qos raft entry checksum mismatch".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared mutation qos route fence is required".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum != prepared.route_control_proof.state_checksum {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared mutation qos route proof checksum mismatch".to_string(),
            ));
        }
        if prepared.prepared_commit_index
            != prepared.expected_previous_commit_index.saturating_add(1)
        {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared mutation qos commit index must be adjacent".to_string(),
            ));
        }
        let mut projected = self.chunk_service.clone();
        let applied_config = if let Some(replica_index) = prepared.target_replica_index {
            projected.configure_chunk_replica_mutation_qos_at_commit(
                replica_index,
                prepared.config.clone(),
                prepared.prepared_commit_index,
            )?
        } else {
            projected.configure_chunk_mutation_qos_at_commit(
                prepared.config.clone(),
                prepared.prepared_commit_index,
            )?
        };
        if applied_config != prepared.config {
            return Err(ObjectError::Corruption(
                "chunk prepared mutation qos config projection mismatch".to_string(),
            ));
        }
        let (expected_aggregate_checksum, expected_replica_count) = projected
            .chunk_mutation_qos_prepared_aggregate_checksum(
                prepared.target_replica_index,
                &prepared.config,
                prepared.expected_previous_commit_index,
                prepared.prepared_commit_index,
            )?;
        if prepared.chunk_prepared_replica_count != expected_replica_count {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared mutation qos replica-count fence mismatch".to_string(),
            ));
        }
        if prepared.chunk_mutation_qos_prepared_aggregate_checksum != expected_aggregate_checksum {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared mutation qos aggregate checksum mismatch".to_string(),
            ));
        }
        let mut expected_material = Vec::new();
        append_page_service_fingerprint(&mut expected_material, &projected);
        let mut prepared_material = Vec::new();
        append_page_service_fingerprint(&mut prepared_material, &prepared.chunk_service);
        if etag(&prepared_material) != etag(&expected_material) {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared mutation qos projected state changed".to_string(),
            ));
        }
        let mut current_material = Vec::new();
        append_page_service_fingerprint(&mut current_material, &self.chunk_service);
        if etag(&current_material) == etag(&prepared_material) {
            return Ok(prepared.config);
        }
        if prepared.expected_previous_commit_index != self.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared mutation qos previous commit fence mismatch".to_string(),
            ));
        }
        self.chunk_service
            .check_prepared_route_control_proof_cursor(
                &prepared.route_control_proof,
                "chunk prepared mutation qos",
            )?;
        self.chunk_service = prepared.chunk_service;
        Ok(prepared.config)
    }

}
