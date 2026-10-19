impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_configure_page_route_qos(
        &mut self,
        config: ChunkRouteQosConfig,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_configure_page_route_qos_fenced(config, None)
    }

    pub fn propose_configure_page_route_qos_fenced(
        &mut self,
        config: ChunkRouteQosConfig,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.store
            .chunk_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        self.propose(ObjectCommand::ConfigurePageRouteQos { config })
    }

    pub fn propose_configure_page_route_qos_linearizable_fenced(
        &mut self,
        config: ChunkRouteQosConfig,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_storage_control_read_index_committed_at(
            "configure block route qos",
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        self.propose_configure_page_route_qos_fenced(config, expected_route_control_checksum)
    }

    pub fn prepare_configure_page_route_qos_raft_entry(
        &self,
        config: ChunkRouteQosConfig,
    ) -> Result<PreparedPageRouteQosConfig, ObjectError> {
        self.store
            .prepare_page_route_qos_raft_entry(config, self.store.last_applied_index())
    }

    pub fn prepare_configure_page_route_qos_raft_entry_linearizable(
        &self,
        config: ChunkRouteQosConfig,
    ) -> Result<PreparedPageRouteQosConfig, ObjectError> {
        self.prepare_configure_page_route_qos_raft_entry_linearizable_fenced(config, None)
    }

    pub fn prepare_configure_page_route_qos_raft_entry_linearizable_fenced(
        &self,
        config: ChunkRouteQosConfig,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedPageRouteQosConfig, ObjectError> {
        let proof = self.ensure_storage_control_read_index(
            "prepare block route qos",
            expected_route_control_checksum,
        )?;
        let route_checksum =
            expected_route_control_checksum.unwrap_or(proof.state_checksum.as_str());
        self.store.prepare_page_route_qos_raft_entry_fenced(
            config,
            self.store.last_applied_index(),
            Some(route_checksum),
        )
    }

    pub fn prepare_configure_page_route_qos_raft_entry_with_read_proof_linearizable_fenced(
        &self,
        config: ChunkRouteQosConfig,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ProvenStorageRead<PreparedPageRouteQosConfig>, ObjectError> {
        let requested_read_index = self.raft.commit_index();
        let prepared = self.prepare_configure_page_route_qos_raft_entry_linearizable_fenced(
            config,
            expected_route_control_checksum,
        )?;
        let proof = self.page_storage_read_proof(requested_read_index);
        Ok(ProvenStorageRead {
            value: prepared,
            proof,
        })
    }

    pub fn propose_prepared_page_route_qos(
        &mut self,
        prepared: PreparedPageRouteQosConfig,
    ) -> Result<ObjectMetadata, ObjectError> {
        if self.raft.commit_index() != self.store.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "prepared page route qos requires raft/store commit alignment".to_string(),
            ));
        }
        self.store
            .clone()
            .apply_command(ObjectCommand::ApplyPreparedPageRouteQos {
                prepared: prepared.clone(),
            })?;
        let entry = self
            .raft
            .append(ObjectCommand::ApplyPreparedPageRouteQos { prepared })?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::PreconditionFailed(
                "prepared page route qos raft index is not contiguous".to_string(),
            ));
        }
        self.store.apply_raft_entry(&entry)
    }

    pub fn propose_prepared_page_route_qos_linearizable_fenced(
        &mut self,
        prepared: PreparedPageRouteQosConfig,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_route_control_checksum = expected_route_control_checksum
            .filter(|checksum| !checksum.is_empty())
            .ok_or_else(|| {
                ObjectError::PreconditionFailed(
                    "prepared page route qos requires expected route control checksum".to_string(),
                )
            })?;
        self.ensure_storage_control_read_index(
            "propose prepared block route qos",
            Some(expected_route_control_checksum),
        )?;
        if prepared.expected_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "prepared page route qos requires prepared route control checksum".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum != expected_route_control_checksum {
            return Err(ObjectError::PreconditionFailed(
                "prepared page route qos route checksum does not match caller fence".to_string(),
            ));
        }
        self.propose_prepared_page_route_qos(prepared)
    }

    pub fn propose_proven_page_route_qos_fenced(
        &mut self,
        proven_prepared: ProvenStorageRead<PreparedPageRouteQosConfig>,
        expected_route_control_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_route_control_checksum = expected_route_control_checksum.as_ref();
        if expected_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven page route qos requires expected route control checksum".to_string(),
            ));
        }
        if proven_prepared.proof.control_checksum != expected_route_control_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven page route qos route checksum mismatch: expected {} actual {}",
                expected_route_control_checksum, proven_prepared.proof.control_checksum
            )));
        }
        let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
        if expected_raft_entry_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven page route qos requires expected raft entry checksum".to_string(),
            ));
        }
        proven_prepared
            .value
            .verify_expected_raft_entry_checksum(expected_raft_entry_checksum)?;
        if self.store.last_applied_index() < proven_prepared.proof.storage_applied_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven page route qos storage proof is not applied: applied={} required={}",
                self.store.last_applied_index(),
                proven_prepared.proof.storage_applied_index
            )));
        }
        self.propose_prepared_page_route_qos_linearizable_fenced(
            proven_prepared.value,
            Some(expected_route_control_checksum),
        )
    }

    pub fn propose_configure_page_route_qos_prepared(
        &mut self,
        config: ChunkRouteQosConfig,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_configure_page_route_qos_raft_entry(config)?;
        self.propose_prepared_page_route_qos(prepared)
    }

    pub fn propose_configure_page_route_qos_prepared_linearizable(
        &mut self,
        config: ChunkRouteQosConfig,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_configure_page_route_qos_prepared_linearizable_fenced(config, None)
    }

    pub fn propose_configure_page_route_qos_prepared_linearizable_fenced(
        &mut self,
        config: ChunkRouteQosConfig,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_configure_page_route_qos_raft_entry_linearizable_fenced(
            config,
            expected_route_control_checksum,
        )?;
        let route_checksum = Self::prepared_or_caller_route_checksum(
            expected_route_control_checksum,
            &prepared.expected_route_control_checksum,
            "configure block route qos prepared linearizable",
        )?;
        self.propose_prepared_page_route_qos_linearizable_fenced(
            prepared,
            Some(route_checksum.as_str()),
        )
    }

}
