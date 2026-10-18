impl MatrixObjectStore {

    pub fn metadata_admission_policy(&self) -> MetadataAdmissionPolicy {
        self.meta_service.admission_policy()
    }


    pub fn configure_metadata_admission_policy(
        &mut self,
        policy: MetadataAdmissionPolicy,
    ) -> Result<MetadataAdmissionPolicy, ObjectError> {
        self.meta_service.configure_admission_policy(policy)
    }


    pub fn prepare_metadata_admission_policy_raft_entry(
        &self,
        policy: MetadataAdmissionPolicy,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataAdmissionPolicy, ObjectError> {
        self.meta_service
            .prepare_admission_policy_raft_entry(policy, expected_previous_commit_index)
    }


    pub fn prepare_metadata_admission_policy_raft_entry_fenced(
        &self,
        policy: MetadataAdmissionPolicy,
        expected_previous_commit_index: u64,
        expected_metadata_control_proof_checksum: &str,
        now_ms: u64,
    ) -> Result<PreparedMetadataAdmissionPolicy, ObjectError> {
        let proof = self.check_expected_metadata_control_proof(
            expected_metadata_control_proof_checksum,
            now_ms,
        )?;
        let mut prepared = self
            .prepare_metadata_admission_policy_raft_entry(policy, expected_previous_commit_index)?;
        prepared.metadata_control_proof_format = proof.state_format;
        prepared.metadata_control_proof_checksum =
            expected_metadata_control_proof_checksum.to_string();
        prepared.metadata_control_proof_bytes = proof.state_bytes;
        prepared.metadata_control_proof_now_ms = now_ms;
        prepared.raft_entry_checksum = prepared_metadata_admission_policy_checksum(&prepared);
        Ok(prepared)
    }


    pub fn apply_prepared_metadata_admission_policy(
        &mut self,
        prepared: PreparedMetadataAdmissionPolicy,
    ) -> Result<MetadataAdmissionPolicy, ObjectError> {
        self.meta_service.apply_prepared_admission_policy(prepared)
    }
}
