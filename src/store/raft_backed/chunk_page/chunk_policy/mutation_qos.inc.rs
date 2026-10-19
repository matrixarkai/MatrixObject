// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_configure_chunk_mutation_qos(
        &mut self,
        config: ReplicaMutationQosConfig,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_configure_chunk_mutation_qos_fenced(config, None)
    }

    pub fn propose_configure_chunk_mutation_qos_fenced(
        &mut self,
        config: ReplicaMutationQosConfig,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.store
            .chunk_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        self.propose(ObjectCommand::ConfigureChunkMutationQos { config })
    }

    pub fn propose_configure_chunk_mutation_qos_linearizable_fenced(
        &mut self,
        config: ReplicaMutationQosConfig,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_storage_control_read_index_committed_at(
            "configure chunk mutation qos",
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        self.propose_configure_chunk_mutation_qos_fenced(config, expected_route_control_checksum)
    }

    pub fn propose_configure_chunk_replica_mutation_qos(
        &mut self,
        replica_index: usize,
        config: ReplicaMutationQosConfig,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_configure_chunk_replica_mutation_qos_fenced(replica_index, config, None)
    }

    pub fn propose_configure_chunk_replica_mutation_qos_fenced(
        &mut self,
        replica_index: usize,
        config: ReplicaMutationQosConfig,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.store
            .chunk_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        self.propose(ObjectCommand::ConfigureChunkReplicaMutationQos {
            replica_index,
            config,
        })
    }

    pub fn propose_configure_chunk_replica_mutation_qos_linearizable_fenced(
        &mut self,
        replica_index: usize,
        config: ReplicaMutationQosConfig,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_storage_control_read_index_committed_at(
            "configure chunk replica mutation qos",
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        self.propose_configure_chunk_replica_mutation_qos_fenced(
            replica_index,
            config,
            expected_route_control_checksum,
        )
    }
}
