// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_configure_chunk_replica_service_mode(
        &mut self,
        replica_index: usize,
        accepting_writes: Option<bool>,
        draining: Option<bool>,
        frozen: Option<bool>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_configure_chunk_replica_service_mode_fenced(
            replica_index,
            accepting_writes,
            draining,
            frozen,
            None,
        )
    }

    pub fn propose_configure_chunk_replica_service_mode_fenced(
        &mut self,
        replica_index: usize,
        accepting_writes: Option<bool>,
        draining: Option<bool>,
        frozen: Option<bool>,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.store
            .chunk_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        self.propose(ObjectCommand::ConfigureChunkReplicaServiceMode {
            replica_index,
            accepting_writes,
            draining,
            frozen,
        })
    }

    pub fn propose_configure_chunk_replica_service_mode_linearizable_fenced(
        &mut self,
        replica_index: usize,
        accepting_writes: Option<bool>,
        draining: Option<bool>,
        frozen: Option<bool>,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_storage_control_read_index_committed_at(
            "configure chunk replica service-mode",
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        self.propose_configure_chunk_replica_service_mode_fenced(
            replica_index,
            accepting_writes,
            draining,
            frozen,
            expected_route_control_checksum,
        )
    }

    pub fn prepare_configure_chunk_replica_service_mode_raft_entry(
        &self,
        replica_index: usize,
        accepting_writes: Option<bool>,
        draining: Option<bool>,
        frozen: Option<bool>,
    ) -> Result<PreparedReplicaServiceModeConfig, ObjectError> {
        self.store.prepare_replica_service_mode_raft_entry(
            replica_index,
            accepting_writes,
            draining,
            frozen,
            self.store.last_applied_index(),
        )
    }

    pub fn prepare_configure_all_eligible_replica_service_mode_raft_entry(
        &self,
        accepting_writes: Option<bool>,
        draining: Option<bool>,
        frozen: Option<bool>,
    ) -> Result<PreparedReplicaServiceModeConfig, ObjectError> {
        self.store.prepare_replica_service_mode_raft_entry(
            PAGE_REPLICA_SERVICE_MODE_ALL_ELIGIBLE_TARGET,
            accepting_writes,
            draining,
            frozen,
            self.store.last_applied_index(),
        )
    }

    pub fn prepare_configure_chunk_replica_service_mode_raft_entry_linearizable(
        &self,
        replica_index: usize,
        accepting_writes: Option<bool>,
        draining: Option<bool>,
        frozen: Option<bool>,
    ) -> Result<PreparedReplicaServiceModeConfig, ObjectError> {
        self.prepare_configure_chunk_replica_service_mode_raft_entry_linearizable_fenced(
            replica_index,
            accepting_writes,
            draining,
            frozen,
            None,
        )
    }

    pub fn prepare_configure_chunk_replica_service_mode_raft_entry_linearizable_fenced(
        &self,
        replica_index: usize,
        accepting_writes: Option<bool>,
        draining: Option<bool>,
        frozen: Option<bool>,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedReplicaServiceModeConfig, ObjectError> {
        let proof = self.ensure_storage_control_read_index(
            "prepare chunk replica service-mode",
            expected_route_control_checksum,
        )?;
        let route_checksum =
            expected_route_control_checksum.unwrap_or(proof.state_checksum.as_str());
        self.store.prepare_replica_service_mode_raft_entry_fenced(
            replica_index,
            accepting_writes,
            draining,
            frozen,
            self.store.last_applied_index(),
            Some(route_checksum),
        )
    }

    pub fn prepare_configure_all_eligible_replica_service_mode_raft_entry_linearizable_fenced(
        &self,
        accepting_writes: Option<bool>,
        draining: Option<bool>,
        frozen: Option<bool>,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedReplicaServiceModeConfig, ObjectError> {
        self.prepare_configure_chunk_replica_service_mode_raft_entry_linearizable_fenced(
            PAGE_REPLICA_SERVICE_MODE_ALL_ELIGIBLE_TARGET,
            accepting_writes,
            draining,
            frozen,
            expected_route_control_checksum,
        )
    }

    pub fn prepare_configure_chunk_replica_service_mode_raft_entry_with_read_proof_linearizable_fenced(
        &self,
        replica_index: usize,
        accepting_writes: Option<bool>,
        draining: Option<bool>,
        frozen: Option<bool>,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ProvenStorageRead<PreparedReplicaServiceModeConfig>, ObjectError> {
        self.prepare_configure_chunk_replica_service_mode_raft_entry_with_read_proof_linearizable_fenced_committed_at(
            replica_index,
            accepting_writes,
            draining,
            frozen,
            expected_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prepare_configure_all_eligible_replica_service_mode_raft_entry_with_read_proof_linearizable_fenced(
        &self,
        accepting_writes: Option<bool>,
        draining: Option<bool>,
        frozen: Option<bool>,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ProvenStorageRead<PreparedReplicaServiceModeConfig>, ObjectError> {
        self.prepare_configure_chunk_replica_service_mode_raft_entry_with_read_proof_linearizable_fenced(
            PAGE_REPLICA_SERVICE_MODE_ALL_ELIGIBLE_TARGET,
            accepting_writes,
            draining,
            frozen,
            expected_route_control_checksum,
        )
    }

    pub fn prepare_configure_chunk_replica_service_mode_raft_entry_with_read_proof_linearizable_fenced_committed_at(
        &self,
        replica_index: usize,
        accepting_writes: Option<bool>,
        draining: Option<bool>,
        frozen: Option<bool>,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenStorageRead<PreparedReplicaServiceModeConfig>, ObjectError> {
        let requested_read_index = self
            .raft
            .commit_index()
            .max(min_read_index)
            .max(min_commit_index);
        let proof = self.ensure_storage_control_read_index_committed_at(
            "prepare chunk replica service-mode",
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        let route_checksum =
            expected_route_control_checksum.unwrap_or(proof.state_checksum.as_str());
        let prepared = self.store.prepare_replica_service_mode_raft_entry_fenced(
            replica_index,
            accepting_writes,
            draining,
            frozen,
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
