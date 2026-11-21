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

}
