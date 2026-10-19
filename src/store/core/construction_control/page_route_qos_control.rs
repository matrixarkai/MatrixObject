impl MatrixObjectStore {
    pub fn configure_page_route_qos(
        &mut self,
        config: ChunkRouteQosConfig,
    ) -> Result<ChunkRouteQosConfig, ObjectError> {
        self.chunk_service.configure_route_qos(config)
    }

    pub fn prepare_page_route_qos_raft_entry(
        &self,
        config: ChunkRouteQosConfig,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedPageRouteQosConfig, ObjectError> {
        let admitted_route_checksum = self.chunk_service.route_control_proof().state_checksum;
        self.prepare_page_route_qos_raft_entry_fenced(
            config,
            expected_previous_commit_index,
            Some(&admitted_route_checksum),
        )
    }

    pub fn prepare_page_route_qos_raft_entry_fenced(
        &self,
        config: ChunkRouteQosConfig,
        expected_previous_commit_index: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedPageRouteQosConfig, ObjectError> {
        self.chunk_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        if expected_previous_commit_index != self.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "page prepared route qos previous commit fence mismatch".to_string(),
            ));
        }
        let before_page_service_fingerprint =
            chunk_route_control_image_fingerprint(&self.chunk_service);
        let mut projected = self.chunk_service.clone();
        let prepared_commit_index = expected_previous_commit_index.saturating_add(1);
        let applied_config =
            projected.configure_route_qos_at_commit(config, prepared_commit_index)?;
        let after_page_service_fingerprint = chunk_route_control_image_fingerprint(&projected);
        let mut prepared = PreparedPageRouteQosConfig {
            config: applied_config,
            before_page_service_fingerprint,
            after_page_service_fingerprint,
            expected_previous_commit_index,
            prepared_commit_index,
            route_control_proof: self.chunk_service.route_control_proof(),
            expected_route_control_checksum: expected_route_control_checksum
                .unwrap_or_default()
                .to_string(),
            raft_entry_checksum: String::new(),
        };
        prepared.raft_entry_checksum = prepared_page_route_qos_config_checksum(&prepared);
        Ok(prepared)
    }

    pub fn apply_prepared_page_route_qos(
        &mut self,
        prepared: PreparedPageRouteQosConfig,
    ) -> Result<ChunkRouteQosConfig, ObjectError> {
        let expected_checksum =
            prepared_page_route_qos_config_checksum(&PreparedPageRouteQosConfig {
                raft_entry_checksum: String::new(),
                ..prepared.clone()
            });
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(
                "page prepared route qos raft entry checksum mismatch".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "page prepared route qos route fence is required".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum != prepared.route_control_proof.state_checksum {
            return Err(ObjectError::PreconditionFailed(
                "page prepared route qos route proof checksum mismatch".to_string(),
            ));
        }
        if prepared.prepared_commit_index
            != prepared.expected_previous_commit_index.saturating_add(1)
        {
            return Err(ObjectError::PreconditionFailed(
                "page prepared route qos commit index must be adjacent".to_string(),
            ));
        }
        let current_fingerprint = chunk_route_control_image_fingerprint(&self.chunk_service);
        if current_fingerprint == prepared.after_page_service_fingerprint {
            return Ok(prepared.config);
        }
        if prepared.expected_previous_commit_index != self.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "page prepared route qos previous commit fence mismatch".to_string(),
            ));
        }
        self.chunk_service
            .check_prepared_route_control_proof_cursor(
                &prepared.route_control_proof,
                "block prepared route qos",
            )?;
        if current_fingerprint != prepared.before_page_service_fingerprint {
            return Err(ObjectError::PreconditionFailed(
                "page prepared route qos source fingerprint mismatch".to_string(),
            ));
        }
        let applied_config = self.chunk_service.configure_route_qos_at_commit(
            prepared.config.clone(),
            prepared.prepared_commit_index,
        )?;
        if applied_config != prepared.config
            || chunk_route_control_image_fingerprint(&self.chunk_service)
                != prepared.after_page_service_fingerprint
        {
            return Err(ObjectError::Corruption(
                "page prepared route qos deterministic apply mismatch".to_string(),
            ));
        }
        Ok(prepared.config)
    }

    pub fn chunk_route_qos_config(&self) -> ChunkRouteQosConfig {
        self.chunk_service.route_qos_config()
    }

    pub fn chunk_route_qos_state(&self) -> ChunkRouteQosState {
        self.chunk_service.route_qos_state()
    }

}
