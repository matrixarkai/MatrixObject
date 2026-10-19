// SPDX-License-Identifier: Apache-2.0
impl MatrixMetaService {
    pub fn admission_policy(&self) -> MetadataAdmissionPolicy {
        self.admission_policy.clone()
    }

    pub fn configure_admission_policy(
        &mut self,
        policy: MetadataAdmissionPolicy,
    ) -> Result<MetadataAdmissionPolicy, ObjectError> {
        validate_metadata_admission_policy(&policy)?;
        self.admission_policy = policy.clone();
        self.next_commit_index = self.next_commit_index.saturating_add(1);
        Ok(policy)
    }

    pub fn prepare_admission_policy_raft_entry(
        &self,
        policy: MetadataAdmissionPolicy,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataAdmissionPolicy, ObjectError> {
        if expected_previous_commit_index != self.next_commit_index.saturating_sub(1) {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata admission expected previous commit {} does not match current {}",
                expected_previous_commit_index,
                self.next_commit_index.saturating_sub(1)
            )));
        }
        let before_state_fingerprint = self.metadata_state_fingerprint();
        let mut projected = self.clone();
        let applied_policy = projected.configure_admission_policy(policy)?;
        let request_checksum = metadata_admission_policy_request_checksum(&applied_policy);
        let mut prepared = PreparedMetadataAdmissionPolicy {
            policy: applied_policy,
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
        prepared.raft_entry_checksum = prepared_metadata_admission_policy_checksum(&prepared);
        Ok(prepared)
    }

    pub fn apply_prepared_admission_policy(
        &mut self,
        prepared: PreparedMetadataAdmissionPolicy,
    ) -> Result<MetadataAdmissionPolicy, ObjectError> {
        let expected_checksum =
            prepared_metadata_admission_policy_checksum(&PreparedMetadataAdmissionPolicy {
                raft_entry_checksum: String::new(),
                ..prepared.clone()
            });
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(
                "prepared metadata admission checksum mismatch".to_string(),
            ));
        }
        if prepared.request_checksum != metadata_admission_policy_request_checksum(&prepared.policy)
        {
            return Err(ObjectError::Corruption(
                "prepared metadata admission request checksum mismatch".to_string(),
            ));
        }
        if prepared.prepared_commit_index
            != prepared.expected_previous_commit_index.saturating_add(1)
        {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata admission commit index is not adjacent".to_string(),
            ));
        }
        if self.metadata_log_has_applied(prepared.prepared_commit_index)
            && self.admission_policy == prepared.policy
        {
            return Ok(prepared.policy);
        }
        if prepared.expected_previous_commit_index != self.next_commit_index.saturating_sub(1) {
            return Err(ObjectError::PreconditionFailed(format!(
                "prepared metadata admission expected previous commit {} does not match current {}",
                prepared.expected_previous_commit_index,
                self.next_commit_index.saturating_sub(1)
            )));
        }
        if prepared.before_state_fingerprint != self.metadata_state_fingerprint() {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata admission source state changed".to_string(),
            ));
        }
        self.configure_admission_policy(prepared.policy)
    }

}
