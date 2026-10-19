// SPDX-License-Identifier: Apache-2.0
impl MatrixMetaService {
    pub fn prepare_lease_raft_entry(
        &self,
        operation: &str,
        namespace: &str,
        owner: &str,
        epoch: u64,
        ttl_ms: u64,
        now_ms: u64,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataLease, ObjectError> {
        if expected_previous_commit_index != self.next_commit_index.saturating_sub(1) {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata lease prepare expected previous commit {} but store is at {}",
                expected_previous_commit_index,
                self.next_commit_index.saturating_sub(1)
            )));
        }
        let before_state_fingerprint = self.metadata_state_fingerprint();
        let mut projected = self.clone();
        let (lease, released) = match operation {
            "acquire" => (
                projected.acquire_lease(namespace, owner, ttl_ms, now_ms)?,
                false,
            ),
            "renew" => (
                projected.renew_lease(namespace, owner, epoch, ttl_ms, now_ms)?,
                false,
            ),
            "release" => (projected.release_lease(namespace, owner, epoch)?, true),
            _ => {
                return Err(ObjectError::InvalidArgument(
                    "metadata lease prepare operation must be acquire, renew, or release"
                        .to_string(),
                ));
            }
        };
        let after_state_fingerprint = projected.metadata_state_fingerprint();
        let request_checksum =
            metadata_lease_request_checksum(operation, namespace, owner, epoch, ttl_ms, now_ms);
        let mut prepared = PreparedMetadataLease {
            operation: operation.to_string(),
            namespace: namespace.to_string(),
            owner: owner.to_string(),
            epoch,
            ttl_ms,
            now_ms,
            lease,
            released,
            request_checksum,
            before_state_fingerprint,
            after_state_fingerprint,
            metadata_read_index: 0,
            metadata_raft_term: 0,
            metadata_control_proof_checksum: String::new(),
            expected_previous_commit_index,
            prepared_commit_index: expected_previous_commit_index.saturating_add(1),
            raft_entry_checksum: String::new(),
        };
        prepared.raft_entry_checksum = prepared_metadata_lease_checksum(&prepared);
        Ok(prepared)
    }

    pub fn apply_prepared_lease(
        &mut self,
        prepared: PreparedMetadataLease,
    ) -> Result<MetadataLease, ObjectError> {
        let expected_checksum = prepared_metadata_lease_checksum(&PreparedMetadataLease {
            raft_entry_checksum: String::new(),
            ..prepared.clone()
        });
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata lease raft entry checksum mismatch".to_string(),
            ));
        }
        let expected_request_checksum = metadata_lease_request_checksum(
            &prepared.operation,
            &prepared.namespace,
            &prepared.owner,
            prepared.epoch,
            prepared.ttl_ms,
            prepared.now_ms,
        );
        if prepared.request_checksum != expected_request_checksum {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata lease request checksum mismatch".to_string(),
            ));
        }
        if self.prepared_lease_already_applied(&prepared) {
            return Ok(prepared.lease);
        }
        if prepared.expected_previous_commit_index != self.next_commit_index.saturating_sub(1) {
            return Err(ObjectError::PreconditionFailed(format!(
                "prepared metadata lease previous commit mismatch: expected {} got {}",
                prepared.expected_previous_commit_index,
                self.next_commit_index.saturating_sub(1)
            )));
        }
        if prepared.prepared_commit_index
            != prepared.expected_previous_commit_index.saturating_add(1)
        {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata lease commit index is not adjacent".to_string(),
            ));
        }
        if prepared.before_state_fingerprint != self.metadata_state_fingerprint() {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata lease source fingerprint mismatch".to_string(),
            ));
        }
        let lease = match prepared.operation.as_str() {
            "acquire" => self.acquire_lease(
                &prepared.namespace,
                &prepared.owner,
                prepared.ttl_ms,
                prepared.now_ms,
            )?,
            "renew" => self.renew_lease(
                &prepared.namespace,
                &prepared.owner,
                prepared.epoch,
                prepared.ttl_ms,
                prepared.now_ms,
            )?,
            "release" => {
                if !prepared.released {
                    return Err(ObjectError::PreconditionFailed(
                        "prepared metadata lease release image is not marked released".to_string(),
                    ));
                }
                self.release_lease(&prepared.namespace, &prepared.owner, prepared.epoch)?
            }
            _ => {
                return Err(ObjectError::InvalidArgument(
                    "prepared metadata lease operation must be acquire, renew, or release"
                        .to_string(),
                ));
            }
        };
        if lease != prepared.lease {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata lease image mismatch after apply".to_string(),
            ));
        }
        if self.metadata_state_fingerprint() != prepared.after_state_fingerprint {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata lease target fingerprint mismatch".to_string(),
            ));
        }
        Ok(lease)
    }
}
