// SPDX-License-Identifier: Apache-2.0
impl MatrixObjectStore {

    pub fn metadata_qos_config(&self) -> MetadataQosConfig {
        self.meta_service.metadata_qos_config()
    }


    pub fn metadata_qos_state(&self) -> MetadataQosState {
        self.meta_service.metadata_qos_state()
    }


    pub fn configure_metadata_qos(
        &mut self,
        config: MetadataQosConfig,
    ) -> Result<MetadataQosConfig, ObjectError> {
        self.meta_service.configure_qos(config)
    }


    pub fn prepare_metadata_qos_config_raft_entry(
        &self,
        config: MetadataQosConfig,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataQosConfig, ObjectError> {
        self.meta_service
            .prepare_qos_config_raft_entry(config, expected_previous_commit_index)
    }


    pub fn prepare_metadata_qos_config_raft_entry_fenced(
        &self,
        config: MetadataQosConfig,
        expected_previous_commit_index: u64,
        expected_metadata_control_proof_checksum: &str,
        now_ms: u64,
    ) -> Result<PreparedMetadataQosConfig, ObjectError> {
        let proof = self.check_expected_metadata_control_proof(
            expected_metadata_control_proof_checksum,
            now_ms,
        )?;
        let mut prepared =
            self.prepare_metadata_qos_config_raft_entry(config, expected_previous_commit_index)?;
        prepared.metadata_control_proof_format = proof.state_format;
        prepared.metadata_control_proof_checksum =
            expected_metadata_control_proof_checksum.to_string();
        prepared.metadata_control_proof_bytes = proof.state_bytes;
        prepared.metadata_control_proof_now_ms = now_ms;
        prepared.raft_entry_checksum = prepared_metadata_qos_config_checksum(&prepared);
        Ok(prepared)
    }


    pub fn apply_prepared_metadata_qos_config(
        &mut self,
        prepared: PreparedMetadataQosConfig,
    ) -> Result<MetadataQosConfig, ObjectError> {
        self.meta_service.apply_prepared_qos_config(prepared)
    }
}
