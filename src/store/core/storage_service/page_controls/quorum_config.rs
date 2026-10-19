impl MatrixObjectStore {
    pub fn configure_page_quorum(
        &mut self,
        write_quorum: usize,
        read_quorum: usize,
    ) -> Result<(), ObjectError> {
        self.chunk_service
            .configure_quorums(write_quorum, read_quorum)
    }

    pub fn prepare_page_quorum_raft_entry(
        &self,
        write_quorum: usize,
        read_quorum: usize,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedPageQuorumConfig, ObjectError> {
        let admitted_route_checksum = self.chunk_service.route_control_proof().state_checksum;
        self.prepare_page_quorum_raft_entry_fenced(
            write_quorum,
            read_quorum,
            expected_previous_commit_index,
            Some(&admitted_route_checksum),
        )
    }

    pub fn prepare_page_quorum_raft_entry_fenced(
        &self,
        write_quorum: usize,
        read_quorum: usize,
        expected_previous_commit_index: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedPageQuorumConfig, ObjectError> {
        self.chunk_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        if expected_previous_commit_index != self.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "block prepared quorum previous commit fence mismatch".to_string(),
            ));
        }
        let before_page_service_fingerprint =
            chunk_route_control_image_fingerprint(&self.chunk_service);
        let mut projected = self.chunk_service.clone();
        projected.configure_quorums(write_quorum, read_quorum)?;
        let after_page_service_fingerprint = chunk_route_control_image_fingerprint(&projected);
        let mut prepared = PreparedPageQuorumConfig {
            write_quorum,
            read_quorum,
            before_page_service_fingerprint,
            after_page_service_fingerprint,
            expected_previous_commit_index,
            prepared_commit_index: expected_previous_commit_index.saturating_add(1),
            route_control_proof: self.chunk_service.route_control_proof(),
            expected_route_control_checksum: expected_route_control_checksum
                .unwrap_or_default()
                .to_string(),
            raft_entry_checksum: String::new(),
        };
        prepared.raft_entry_checksum = prepared_page_quorum_config_checksum(&prepared);
        Ok(prepared)
    }

    pub fn apply_prepared_page_quorum(
        &mut self,
        prepared: PreparedPageQuorumConfig,
    ) -> Result<(), ObjectError> {
        let expected_checksum = prepared_page_quorum_config_checksum(&PreparedPageQuorumConfig {
            raft_entry_checksum: String::new(),
            ..prepared.clone()
        });
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(
                "block prepared quorum raft entry checksum mismatch".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "block prepared quorum route fence is required".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum != prepared.route_control_proof.state_checksum {
            return Err(ObjectError::PreconditionFailed(
                "block prepared quorum route proof checksum mismatch".to_string(),
            ));
        }
        if prepared.prepared_commit_index
            != prepared.expected_previous_commit_index.saturating_add(1)
        {
            return Err(ObjectError::PreconditionFailed(
                "block prepared quorum commit index must be adjacent".to_string(),
            ));
        }
        let current_fingerprint = chunk_route_control_image_fingerprint(&self.chunk_service);
        if current_fingerprint == prepared.after_page_service_fingerprint {
            return Ok(());
        }
        if prepared.expected_previous_commit_index != self.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "block prepared quorum previous commit fence mismatch".to_string(),
            ));
        }
        self.chunk_service
            .check_prepared_route_control_proof_cursor(
                &prepared.route_control_proof,
                "block prepared quorum",
            )?;
        if current_fingerprint != prepared.before_page_service_fingerprint {
            return Err(ObjectError::PreconditionFailed(
                "block prepared quorum source fingerprint mismatch".to_string(),
            ));
        }
        self.chunk_service
            .configure_quorums(prepared.write_quorum, prepared.read_quorum)?;
        if chunk_route_control_image_fingerprint(&self.chunk_service)
            != prepared.after_page_service_fingerprint
        {
            return Err(ObjectError::Corruption(
                "block prepared quorum deterministic apply mismatch".to_string(),
            ));
        }
        Ok(())
    }


}
