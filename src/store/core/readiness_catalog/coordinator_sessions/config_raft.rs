// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn prepare_metadata_coordinator_config_raft_entry(
        &self,
        config: MetadataCoordinatorConfig,
        idempotency_key: &str,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataCoordinatorConfig, ObjectError> {
        self.meta_service.prepare_coordinator_config_raft_entry(
            config,
            idempotency_key,
            expected_previous_commit_index,
        )
    }

    pub fn prepare_metadata_coordinator_config_raft_entry_fenced(
        &self,
        config: MetadataCoordinatorConfig,
        idempotency_key: &str,
        expected_previous_commit_index: u64,
        expected_metadata_control_proof_checksum: &str,
        now_ms: u64,
    ) -> Result<PreparedMetadataCoordinatorConfig, ObjectError> {
        self.check_expected_metadata_control_proof(
            expected_metadata_control_proof_checksum,
            now_ms,
        )?;
        let mut prepared = self.prepare_metadata_coordinator_config_raft_entry(
            config,
            idempotency_key,
            expected_previous_commit_index,
        )?;
        let proof = self.metadata_control_proof_at(now_ms);
        prepared.metadata_control_proof_format = proof.state_format;
        prepared.metadata_control_proof_checksum =
            expected_metadata_control_proof_checksum.to_string();
        prepared.metadata_control_proof_bytes = proof.state_bytes;
        prepared.metadata_control_proof_now_ms = now_ms;
        prepared.raft_entry_checksum =
            prepared_metadata_coordinator_config_checksum(&PreparedMetadataCoordinatorConfig {
                raft_entry_checksum: String::new(),
                ..prepared.clone()
            });
        Ok(prepared)
    }

    pub fn apply_prepared_metadata_coordinator_config(
        &mut self,
        prepared: PreparedMetadataCoordinatorConfig,
    ) -> Result<MetadataCoordinatorConfigReceipt, ObjectError> {
        let receipt = self
            .meta_service
            .apply_prepared_coordinator_config(prepared)?;
        if !receipt.deduped {
            self.invalidate_metadata_route_cache();
        }
        Ok(receipt)
    }

}
