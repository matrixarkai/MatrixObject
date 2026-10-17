impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_configure_chunk_replica_capacity(
        &mut self,
        replica_index: usize,
        capacity_bytes: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_configure_chunk_replica_capacity_fenced(replica_index, capacity_bytes, None)
    }

    pub fn propose_configure_chunk_replica_capacity_fenced(
        &mut self,
        replica_index: usize,
        capacity_bytes: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.store
            .page_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        self.propose(ObjectCommand::ConfigureChunkReplicaCapacity {
            replica_index,
            capacity_bytes,
        })
    }

    pub fn propose_configure_chunk_replica_capacity_linearizable_fenced(
        &mut self,
        replica_index: usize,
        capacity_bytes: u64,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_storage_control_read_index_committed_at(
            "configure chunk replica capacity",
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        self.propose_configure_chunk_replica_capacity_fenced(
            replica_index,
            capacity_bytes,
            expected_route_control_checksum,
        )
    }

    pub fn prepare_configure_chunk_replica_capacity_raft_entry(
        &self,
        replica_index: usize,
        capacity_bytes: u64,
    ) -> Result<PreparedChunkCapacityConfig, ObjectError> {
        self.store.prepare_chunk_capacity_raft_entry(
            replica_index,
            capacity_bytes,
            self.store.last_applied_index(),
        )
    }

    pub fn prepare_configure_all_eligible_chunk_capacity_raft_entry(
        &self,
        capacity_bytes: u64,
    ) -> Result<PreparedChunkCapacityConfig, ObjectError> {
        self.store.prepare_chunk_capacity_raft_entry(
            PAGE_CHUNK_CAPACITY_ALL_ELIGIBLE_TARGET,
            capacity_bytes,
            self.store.last_applied_index(),
        )
    }

    fn ensure_storage_control_read_index(
        &self,
        operation: &str,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PageRouteControlProof, ObjectError> {
        self.ensure_storage_control_read_index_committed_at(
            operation,
            expected_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    fn ensure_storage_control_read_index_committed_at(
        &self,
        operation: &str,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<PageRouteControlProof, ObjectError> {
        let required_read_index = self
            .raft
            .commit_index()
            .max(min_read_index)
            .max(min_commit_index);
        if self.raft.commit_index() < min_commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "{} storage control requires raft commit index at least {} but raft is at {}",
                operation,
                min_commit_index,
                self.raft.commit_index()
            )));
        }
        if self.raft.commit_index() < min_read_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "{} storage control requires raft commit index at least {} but raft is at {}",
                operation,
                min_read_index,
                self.raft.commit_index()
            )));
        }
        if self.store.last_applied_index() < required_read_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "{} storage control read index is not satisfied: applied={} required={}",
                operation,
                self.store.last_applied_index(),
                required_read_index
            )));
        }
        let proof = self.store.page_service.route_control_proof();
        if let Some(expected) = expected_route_control_checksum {
            if expected.is_empty() {
                return Err(ObjectError::InvalidArgument(
                    "storage control proof checksum expectation is empty".to_string(),
                ));
            }
            if proof.state_checksum != expected {
                return Err(ObjectError::PreconditionFailed(format!(
                    "{} storage control proof checksum mismatch: expected {} got {}",
                    operation, expected, proof.state_checksum
                )));
            }
        }
        Ok(proof)
    }

    fn require_expected_route_control_checksum<'a>(
        operation: &str,
        expected_route_control_checksum: Option<&'a str>,
    ) -> Result<&'a str, ObjectError> {
        let expected_route_control_checksum = expected_route_control_checksum.ok_or_else(|| {
            ObjectError::PreconditionFailed(format!(
                "{} requires expected route checksum",
                operation
            ))
        })?;
        if expected_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(format!(
                "{} requires expected route checksum",
                operation
            )));
        }
        Ok(expected_route_control_checksum)
    }

    pub fn prepare_configure_chunk_replica_capacity_raft_entry_linearizable(
        &self,
        replica_index: usize,
        capacity_bytes: u64,
    ) -> Result<PreparedChunkCapacityConfig, ObjectError> {
        self.prepare_configure_chunk_replica_capacity_raft_entry_linearizable_fenced(
            replica_index,
            capacity_bytes,
            None,
        )
    }

    pub fn prepare_configure_chunk_replica_capacity_raft_entry_linearizable_fenced(
        &self,
        replica_index: usize,
        capacity_bytes: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedChunkCapacityConfig, ObjectError> {
        let proof = self.ensure_storage_control_read_index(
            "prepare chunk replica capacity",
            expected_route_control_checksum,
        )?;
        let route_checksum =
            expected_route_control_checksum.unwrap_or(proof.state_checksum.as_str());
        self.store.prepare_chunk_capacity_raft_entry_fenced(
            replica_index,
            capacity_bytes,
            self.store.last_applied_index(),
            Some(route_checksum),
        )
    }

    pub fn prepare_configure_all_eligible_chunk_capacity_raft_entry_linearizable_fenced(
        &self,
        capacity_bytes: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedChunkCapacityConfig, ObjectError> {
        self.prepare_configure_chunk_replica_capacity_raft_entry_linearizable_fenced(
            PAGE_CHUNK_CAPACITY_ALL_ELIGIBLE_TARGET,
            capacity_bytes,
            expected_route_control_checksum,
        )
    }

    pub fn prepare_configure_chunk_replica_capacity_raft_entry_with_read_proof_linearizable_fenced(
        &self,
        replica_index: usize,
        capacity_bytes: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ProvenStorageRead<PreparedChunkCapacityConfig>, ObjectError> {
        self.prepare_configure_chunk_replica_capacity_raft_entry_with_read_proof_linearizable_fenced_committed_at(
            replica_index,
            capacity_bytes,
            expected_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prepare_configure_all_eligible_chunk_capacity_raft_entry_with_read_proof_linearizable_fenced(
        &self,
        capacity_bytes: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ProvenStorageRead<PreparedChunkCapacityConfig>, ObjectError> {
        self.prepare_configure_chunk_replica_capacity_raft_entry_with_read_proof_linearizable_fenced(
            PAGE_CHUNK_CAPACITY_ALL_ELIGIBLE_TARGET,
            capacity_bytes,
            expected_route_control_checksum,
        )
    }

    pub fn prepare_configure_chunk_replica_capacity_raft_entry_with_read_proof_linearizable_fenced_committed_at(
        &self,
        replica_index: usize,
        capacity_bytes: u64,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenStorageRead<PreparedChunkCapacityConfig>, ObjectError> {
        let requested_read_index = self
            .raft
            .commit_index()
            .max(min_read_index)
            .max(min_commit_index);
        let proof = self.ensure_storage_control_read_index_committed_at(
            "prepare chunk replica capacity",
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        let route_checksum =
            expected_route_control_checksum.unwrap_or(proof.state_checksum.as_str());
        let prepared = self.store.prepare_chunk_capacity_raft_entry_fenced(
            replica_index,
            capacity_bytes,
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
