// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixChunkService {
    pub fn configure_chunk_mutation_qos(
        &mut self,
        config: ReplicaMutationQosConfig,
    ) -> Result<ReplicaMutationQosConfig, ObjectError> {
        let commit_index = self
            .replicas
            .iter()
            .map(|replica| replica.chunk_mutation_control_next_log_index())
            .max()
            .unwrap_or(1);
        self.configure_chunk_mutation_qos_at_commit(config, commit_index)
    }

    pub fn configure_chunk_replica_mutation_qos(
        &mut self,
        replica_index: usize,
        config: ReplicaMutationQosConfig,
    ) -> Result<ReplicaMutationQosConfig, ObjectError> {
        let commit_index = self
            .replicas
            .get(replica_index)
            .map(|replica| replica.chunk_mutation_control_next_log_index())
            .ok_or_else(|| {
                ObjectError::InvalidArgument("replica index out of range".to_string())
            })?;
        self.configure_chunk_replica_mutation_qos_at_commit(replica_index, config, commit_index)
    }

    pub(crate) fn configure_chunk_mutation_qos_at_commit(
        &mut self,
        config: ReplicaMutationQosConfig,
        commit_index: u64,
    ) -> Result<ReplicaMutationQosConfig, ObjectError> {
        validate_chunk_mutation_qos_config(&config)?;
        let quarantined = &self.quarantined_replicas;
        let disabled = &self.disabled_chunk_endpoints;
        for (index, replica) in self.replicas.iter_mut().enumerate() {
            if quarantined.contains(&index) || disabled.contains(&index) {
                continue;
            }
            replica.configure_mutation_qos_at_commit(config.clone(), commit_index)?;
        }
        Ok(config)
    }

    pub(crate) fn configure_chunk_replica_mutation_qos_at_commit(
        &mut self,
        replica_index: usize,
        config: ReplicaMutationQosConfig,
        commit_index: u64,
    ) -> Result<ReplicaMutationQosConfig, ObjectError> {
        validate_chunk_mutation_qos_config(&config)?;
        let replica = self.replicas.get_mut(replica_index).ok_or_else(|| {
            ObjectError::InvalidArgument("replica index out of range".to_string())
        })?;
        replica.configure_mutation_qos_at_commit(config.clone(), commit_index)?;
        Ok(config)
    }

    pub(crate) fn chunk_mutation_qos_prepared_aggregate_checksum(
        &self,
        target_replica_index: Option<usize>,
        config: &ReplicaMutationQosConfig,
        expected_previous_commit_index: u64,
        prepared_commit_index: u64,
    ) -> Result<(String, u64), ObjectError> {
        let mut material = Vec::with_capacity(160 + self.replicas.len().saturating_mul(640));
        append_string(&mut material, "MOS-RUST-BLOCK-CHUNK-MUTATION-QOS-PREPARED1");
        append_u64(&mut material, expected_previous_commit_index);
        append_u64(&mut material, prepared_commit_index);
        append_u64(&mut material, target_replica_index.map_or(0, |_| 1));
        append_u64(
            &mut material,
            target_replica_index.map_or(0, |index| index as u64),
        );
        append_chunk_mutation_qos_config(&mut material, config);
        let quarantined = &self.quarantined_replicas;
        let disabled = &self.disabled_chunk_endpoints;
        let mut replica_count = 0_u64;
        for (index, replica) in self.replicas.iter().enumerate() {
            if let Some(target) = target_replica_index {
                if index != target {
                    continue;
                }
            } else if quarantined.contains(&index) || disabled.contains(&index) {
                continue;
            }
            replica_count = replica_count.saturating_add(1);
            append_u64(&mut material, index as u64);
            append_replica_service_control_proof(&mut material, &replica.control_proof());
        }
        if target_replica_index.is_some() && replica_count == 0 {
            return Err(ObjectError::InvalidArgument(
                "chunk replica mutation-qos target is out of range".to_string(),
            ));
        }
        Ok((etag(&material), replica_count))
    }
}
