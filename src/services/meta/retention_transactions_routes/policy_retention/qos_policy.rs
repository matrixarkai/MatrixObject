// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixMetaService {
    pub fn metadata_qos_config(&self) -> MetadataQosConfig {
        self.qos_config.clone()
    }

    pub fn metadata_qos_state(&self) -> MetadataQosState {
        self.qos_state.clone()
    }

    pub fn configure_qos(
        &mut self,
        config: MetadataQosConfig,
    ) -> Result<MetadataQosConfig, ObjectError> {
        validate_metadata_qos_config(&config)?;
        self.qos_config = config.clone();
        self.qos_state = MetadataQosState::default();
        self.next_commit_index = self.next_commit_index.saturating_add(1);
        Ok(config)
    }

    pub fn prepare_qos_config_raft_entry(
        &self,
        config: MetadataQosConfig,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataQosConfig, ObjectError> {
        if expected_previous_commit_index != self.next_commit_index.saturating_sub(1) {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata qos expected previous commit {} does not match current {}",
                expected_previous_commit_index,
                self.next_commit_index.saturating_sub(1)
            )));
        }
        let before_state_fingerprint = self.metadata_state_fingerprint();
        let mut projected = self.clone();
        let applied_config = projected.configure_qos(config)?;
        let request_checksum = metadata_qos_config_request_checksum(&applied_config);
        let mut prepared = PreparedMetadataQosConfig {
            config: applied_config,
            before_state_fingerprint,
            metadata_control_proof_format: String::new(),
            metadata_control_proof_checksum: String::new(),
            metadata_control_proof_bytes: 0,
            metadata_control_proof_now_ms: 0,
            request_checksum,
            expected_previous_commit_index,
            prepared_commit_index: expected_previous_commit_index.saturating_add(1),
            raft_entry_checksum: String::new(),
        };
        prepared.raft_entry_checksum = prepared_metadata_qos_config_checksum(&prepared);
        Ok(prepared)
    }

    pub fn apply_prepared_qos_config(
        &mut self,
        prepared: PreparedMetadataQosConfig,
    ) -> Result<MetadataQosConfig, ObjectError> {
        let expected_checksum = prepared_metadata_qos_config_checksum(&PreparedMetadataQosConfig {
            raft_entry_checksum: String::new(),
            ..prepared.clone()
        });
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(
                "prepared metadata qos checksum mismatch".to_string(),
            ));
        }
        if prepared.request_checksum != metadata_qos_config_request_checksum(&prepared.config) {
            return Err(ObjectError::Corruption(
                "prepared metadata qos request checksum mismatch".to_string(),
            ));
        }
        if prepared.prepared_commit_index
            != prepared.expected_previous_commit_index.saturating_add(1)
        {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata qos commit index is not adjacent".to_string(),
            ));
        }
        if self.metadata_log_has_applied(prepared.prepared_commit_index)
            && self.qos_config == prepared.config
        {
            return Ok(prepared.config);
        }
        if prepared.expected_previous_commit_index != self.next_commit_index.saturating_sub(1) {
            return Err(ObjectError::PreconditionFailed(format!(
                "prepared metadata qos expected previous commit {} does not match current {}",
                prepared.expected_previous_commit_index,
                self.next_commit_index.saturating_sub(1)
            )));
        }
        if prepared.before_state_fingerprint != self.metadata_state_fingerprint() {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata qos source state changed".to_string(),
            ));
        }
        self.configure_qos(prepared.config)
    }

}
