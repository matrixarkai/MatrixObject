// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixChunkService {
    pub fn chunk_replica_mutation_qos_configs(&self) -> Vec<ReplicaMutationQosConfig> {
        self.replicas
            .iter()
            .map(MatrixReplicaService::mutation_qos_config)
            .collect()
    }

    pub fn chunk_replica_mutation_qos_states(&self) -> Vec<ReplicaMutationQosState> {
        self.replicas
            .iter()
            .map(MatrixReplicaService::mutation_qos_state)
            .collect()
    }

    pub fn chunk_replica_capacity_bytes(&self) -> Vec<u64> {
        self.replicas
            .iter()
            .map(MatrixReplicaService::capacity_bytes)
            .collect()
    }

    pub fn chunk_replica_capacity_free_bytes(&self) -> Vec<u64> {
        self.replicas
            .iter()
            .map(MatrixReplicaService::capacity_free_bytes)
            .collect()
    }

    pub fn chunk_replica_capacity_rejected_writes(&self) -> Vec<u64> {
        self.replicas
            .iter()
            .map(MatrixReplicaService::capacity_rejected_writes)
            .collect()
    }

    pub fn chunk_replica_physical_bytes(&self) -> Vec<u64> {
        self.replicas
            .iter()
            .map(MatrixReplicaService::physical_bytes)
            .collect()
    }

    pub fn chunk_replica_physical_write_count(&self) -> Vec<u64> {
        self.replicas
            .iter()
            .map(MatrixReplicaService::physical_write_count)
            .collect()
    }

    pub fn chunk_replica_physical_write_bytes(&self) -> Vec<u64> {
        self.replicas
            .iter()
            .map(MatrixReplicaService::physical_write_bytes)
            .collect()
    }

    pub fn chunk_replica_physical_discard_ranges(&self) -> Vec<u64> {
        self.replicas
            .iter()
            .map(MatrixReplicaService::physical_discard_ranges)
            .collect()
    }

    pub fn chunk_replica_physical_discard_bytes(&self) -> Vec<u64> {
        self.replicas
            .iter()
            .map(MatrixReplicaService::physical_discard_bytes)
            .collect()
    }

    pub fn chunk_replica_compressed_chunk_count(&self) -> Vec<u64> {
        self.replicas
            .iter()
            .map(MatrixReplicaService::compressed_chunk_count)
            .collect()
    }

    pub fn chunk_replica_compression_saved_bytes(&self) -> Vec<u64> {
        self.replicas
            .iter()
            .map(MatrixReplicaService::compression_saved_bytes)
            .collect()
    }

    pub fn chunk_replica_ec_reconstructed_reads(&self) -> Vec<u64> {
        self.replicas
            .iter()
            .map(MatrixReplicaService::ec_reconstructed_reads)
            .collect()
    }

    pub fn chunk_replica_ec_reconstruction_failures(&self) -> Vec<u64> {
        self.replicas
            .iter()
            .map(MatrixReplicaService::ec_reconstruction_failures)
            .collect()
    }

    pub fn physical_write_count(&self) -> u64 {
        self.replicas
            .iter()
            .map(MatrixReplicaService::physical_write_count)
            .fold(0u64, u64::saturating_add)
    }

    pub fn physical_write_bytes(&self) -> u64 {
        self.replicas
            .iter()
            .map(MatrixReplicaService::physical_write_bytes)
            .fold(0u64, u64::saturating_add)
    }

    pub fn physical_discard_ranges(&self) -> u64 {
        self.replicas
            .iter()
            .map(MatrixReplicaService::physical_discard_ranges)
            .fold(0u64, u64::saturating_add)
    }

    pub fn physical_discard_bytes(&self) -> u64 {
        self.replicas
            .iter()
            .map(MatrixReplicaService::physical_discard_bytes)
            .fold(0u64, u64::saturating_add)
    }

    pub fn compressed_chunk_count(&self) -> u64 {
        self.replicas
            .iter()
            .map(MatrixReplicaService::compressed_chunk_count)
            .fold(0u64, u64::saturating_add)
    }

    pub fn compression_saved_bytes(&self) -> u64 {
        self.replicas
            .iter()
            .map(MatrixReplicaService::compression_saved_bytes)
            .fold(0u64, u64::saturating_add)
    }

    pub fn ec_reconstructed_reads(&self) -> u64 {
        self.replicas
            .iter()
            .map(MatrixReplicaService::ec_reconstructed_reads)
            .fold(0u64, u64::saturating_add)
    }

    pub fn ec_reconstruction_failures(&self) -> u64 {
        self.replicas
            .iter()
            .map(MatrixReplicaService::ec_reconstruction_failures)
            .fold(0u64, u64::saturating_add)
    }

    pub fn chunk_replica_accepting_writes(&self) -> Vec<bool> {
        self.replicas
            .iter()
            .map(MatrixReplicaService::accepting_writes)
            .collect()
    }

    pub fn draining_chunk_replicas(&self) -> Vec<usize> {
        self.replicas
            .iter()
            .enumerate()
            .filter_map(|(index, replica)| replica.is_draining().then_some(index))
            .collect()
    }

    pub fn chunk_replica_service_mode_rejected_mutations(&self) -> Vec<u64> {
        self.replicas
            .iter()
            .map(MatrixReplicaService::service_mode_rejected_mutations)
            .collect()
    }
}
