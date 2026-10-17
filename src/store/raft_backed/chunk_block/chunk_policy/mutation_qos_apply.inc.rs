impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_prepared_chunk_mutation_qos(
        &mut self,
        prepared: PreparedChunkMutationQosConfig,
    ) -> Result<ObjectMetadata, ObjectError> {
        if self.raft.commit_index() != self.store.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "prepared chunk mutation qos requires raft/store commit alignment".to_string(),
            ));
        }
        self.store
            .clone()
            .apply_command(ObjectCommand::ApplyPreparedChunkMutationQos {
                prepared: prepared.clone(),
            })?;
        let entry = self
            .raft
            .append(ObjectCommand::ApplyPreparedChunkMutationQos { prepared })?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::PreconditionFailed(
                "prepared chunk mutation qos raft index is not contiguous".to_string(),
            ));
        }
        self.store.apply_raft_entry(&entry)
    }

    pub fn propose_prepared_chunk_mutation_qos_linearizable_fenced(
        &mut self,
        prepared: PreparedChunkMutationQosConfig,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_route_control_checksum = expected_route_control_checksum
            .filter(|checksum| !checksum.is_empty())
            .ok_or_else(|| {
                ObjectError::PreconditionFailed(
                    "prepared chunk mutation qos requires expected route control checksum"
                        .to_string(),
                )
            })?;
        self.ensure_storage_control_read_index(
            "propose prepared chunk mutation qos",
            Some(expected_route_control_checksum),
        )?;
        if prepared.expected_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "prepared chunk mutation qos requires prepared route control checksum".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum != expected_route_control_checksum {
            return Err(ObjectError::PreconditionFailed(
                "prepared chunk mutation qos route checksum does not match caller fence"
                    .to_string(),
            ));
        }
        self.propose_prepared_chunk_mutation_qos(prepared)
    }

    pub fn propose_proven_chunk_mutation_qos_fenced(
        &mut self,
        proven_prepared: ProvenStorageRead<PreparedChunkMutationQosConfig>,
        expected_route_control_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_route_control_checksum = expected_route_control_checksum.as_ref();
        if expected_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven chunk mutation qos requires expected route control checksum".to_string(),
            ));
        }
        if proven_prepared.proof.control_checksum != expected_route_control_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven chunk mutation qos route checksum mismatch: expected {} actual {}",
                expected_route_control_checksum, proven_prepared.proof.control_checksum
            )));
        }
        let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
        if expected_raft_entry_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven chunk mutation qos requires expected raft entry checksum".to_string(),
            ));
        }
        proven_prepared
            .value
            .verify_expected_raft_entry_checksum(expected_raft_entry_checksum)?;
        if self.store.last_applied_index() < proven_prepared.proof.storage_applied_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven chunk mutation qos storage proof is not applied: applied={} required={}",
                self.store.last_applied_index(),
                proven_prepared.proof.storage_applied_index
            )));
        }
        self.propose_prepared_chunk_mutation_qos_linearizable_fenced(
            proven_prepared.value,
            Some(expected_route_control_checksum),
        )
    }

    pub fn propose_configure_chunk_mutation_qos_prepared(
        &mut self,
        config: ChunkMutationQosConfig,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_configure_chunk_mutation_qos_raft_entry(config)?;
        self.propose_prepared_chunk_mutation_qos(prepared)
    }

    pub fn propose_configure_chunk_replica_mutation_qos_prepared(
        &mut self,
        replica_index: usize,
        config: ChunkMutationQosConfig,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared =
            self.prepare_configure_chunk_replica_mutation_qos_raft_entry(replica_index, config)?;
        self.propose_prepared_chunk_mutation_qos(prepared)
    }

    pub fn propose_configure_chunk_mutation_qos_prepared_linearizable(
        &mut self,
        config: ChunkMutationQosConfig,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_configure_chunk_mutation_qos_prepared_linearizable_fenced(config, None)
    }

    pub fn propose_configure_chunk_mutation_qos_prepared_linearizable_fenced(
        &mut self,
        config: ChunkMutationQosConfig,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_configure_chunk_mutation_qos_raft_entry_linearizable_fenced(
            config,
            expected_route_control_checksum,
        )?;
        let route_checksum = Self::prepared_or_caller_route_checksum(
            expected_route_control_checksum,
            &prepared.expected_route_control_checksum,
            "configure chunk mutation qos prepared linearizable",
        )?;
        self.propose_prepared_chunk_mutation_qos_linearizable_fenced(
            prepared,
            Some(route_checksum.as_str()),
        )
    }

    pub fn propose_configure_chunk_replica_mutation_qos_prepared_linearizable(
        &mut self,
        replica_index: usize,
        config: ChunkMutationQosConfig,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_configure_chunk_replica_mutation_qos_prepared_linearizable_fenced(
            replica_index,
            config,
            None,
        )
    }

    pub fn propose_configure_chunk_replica_mutation_qos_prepared_linearizable_fenced(
        &mut self,
        replica_index: usize,
        config: ChunkMutationQosConfig,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self
            .prepare_configure_chunk_replica_mutation_qos_raft_entry_linearizable_fenced(
                replica_index,
                config,
                expected_route_control_checksum,
            )?;
        let route_checksum = Self::prepared_or_caller_route_checksum(
            expected_route_control_checksum,
            &prepared.expected_route_control_checksum,
            "configure chunk replica mutation qos prepared linearizable",
        )?;
        self.propose_prepared_chunk_mutation_qos_linearizable_fenced(
            prepared,
            Some(route_checksum.as_str()),
        )
    }
}
