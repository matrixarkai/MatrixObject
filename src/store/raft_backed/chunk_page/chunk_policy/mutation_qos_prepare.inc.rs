impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn prepare_configure_chunk_mutation_qos_raft_entry(
        &self,
        config: ReplicaMutationQosConfig,
    ) -> Result<PreparedChunkMutationQosConfig, ObjectError> {
        self.store
            .prepare_chunk_mutation_qos_raft_entry(config, self.store.last_applied_index())
    }

    pub fn prepare_configure_chunk_replica_mutation_qos_raft_entry(
        &self,
        replica_index: usize,
        config: ReplicaMutationQosConfig,
    ) -> Result<PreparedChunkMutationQosConfig, ObjectError> {
        let admitted_route_checksum = self
            .store
            .chunk_service
            .route_control_proof()
            .state_checksum;
        self.store
            .prepare_chunk_replica_mutation_qos_raft_entry_fenced(
                replica_index,
                config,
                self.store.last_applied_index(),
                Some(&admitted_route_checksum),
            )
    }

    pub fn prepare_configure_chunk_mutation_qos_raft_entry_linearizable(
        &self,
        config: ReplicaMutationQosConfig,
    ) -> Result<PreparedChunkMutationQosConfig, ObjectError> {
        self.prepare_configure_chunk_mutation_qos_raft_entry_linearizable_fenced(config, None)
    }

    pub fn prepare_configure_chunk_mutation_qos_raft_entry_linearizable_fenced(
        &self,
        config: ReplicaMutationQosConfig,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedChunkMutationQosConfig, ObjectError> {
        let proof = self.ensure_storage_control_read_index(
            "prepare chunk mutation qos",
            expected_route_control_checksum,
        )?;
        let route_checksum =
            expected_route_control_checksum.unwrap_or(proof.state_checksum.as_str());
        self.store.prepare_chunk_mutation_qos_raft_entry_fenced(
            config,
            self.store.last_applied_index(),
            Some(route_checksum),
        )
    }

    pub fn prepare_configure_chunk_replica_mutation_qos_raft_entry_linearizable_fenced(
        &self,
        replica_index: usize,
        config: ReplicaMutationQosConfig,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedChunkMutationQosConfig, ObjectError> {
        let proof = self.ensure_storage_control_read_index(
            "prepare chunk replica mutation qos",
            expected_route_control_checksum,
        )?;
        let route_checksum =
            expected_route_control_checksum.unwrap_or(proof.state_checksum.as_str());
        self.store
            .prepare_chunk_replica_mutation_qos_raft_entry_fenced(
                replica_index,
                config,
                self.store.last_applied_index(),
                Some(route_checksum),
            )
    }

    pub fn prepare_configure_chunk_mutation_qos_raft_entry_with_read_proof_linearizable_fenced(
        &self,
        config: ReplicaMutationQosConfig,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ProvenStorageRead<PreparedChunkMutationQosConfig>, ObjectError> {
        self.prepare_configure_chunk_mutation_qos_raft_entry_with_read_proof_linearizable_fenced_committed_at(
            config,
            expected_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prepare_configure_chunk_replica_mutation_qos_raft_entry_with_read_proof_linearizable_fenced(
        &self,
        replica_index: usize,
        config: ReplicaMutationQosConfig,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ProvenStorageRead<PreparedChunkMutationQosConfig>, ObjectError> {
        let requested_read_index = self.raft.commit_index();
        let proof = self.ensure_storage_control_read_index_committed_at(
            "prepare chunk replica mutation qos",
            expected_route_control_checksum,
            requested_read_index,
            0,
        )?;
        let route_checksum =
            expected_route_control_checksum.unwrap_or(proof.state_checksum.as_str());
        let prepared = self
            .store
            .prepare_chunk_replica_mutation_qos_raft_entry_fenced(
                replica_index,
                config,
                self.store.last_applied_index(),
                Some(route_checksum),
            )?;
        let proof = self.page_storage_read_proof_committed(requested_read_index, 0);
        Ok(ProvenStorageRead {
            value: prepared,
            proof,
        })
    }

    pub fn prepare_configure_chunk_mutation_qos_raft_entry_with_read_proof_linearizable_fenced_committed_at(
        &self,
        config: ReplicaMutationQosConfig,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenStorageRead<PreparedChunkMutationQosConfig>, ObjectError> {
        let requested_read_index = self
            .raft
            .commit_index()
            .max(min_read_index)
            .max(min_commit_index);
        let proof = self.ensure_storage_control_read_index_committed_at(
            "prepare chunk mutation qos",
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        let route_checksum =
            expected_route_control_checksum.unwrap_or(proof.state_checksum.as_str());
        let prepared = self.store.prepare_chunk_mutation_qos_raft_entry_fenced(
            config,
            self.store.last_applied_index(),
            Some(route_checksum),
        )?;
        let proof = self.page_storage_read_proof_committed(requested_read_index, min_commit_index);
        Ok(ProvenStorageRead {
            value: prepared,
            proof,
        })
    }
}
