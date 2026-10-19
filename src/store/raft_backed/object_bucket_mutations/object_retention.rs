// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_set_object_retention(
        &mut self,
        bucket: &str,
        key: &str,
        retention_until_ms: u64,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::SetObjectRetention {
            bucket: bucket.to_string(),
            key: key.to_string(),
            retention_until_ms,
            update_time_ms,
        })
    }

    pub fn propose_set_object_retention_with_read_proof_linearizable(
        &mut self,
        bucket: &str,
        key: &str,
        retention_until_ms: u64,
        update_time_ms: u64,
    ) -> Result<ProvenMetadataRead<ObjectRetentionMutationReport>, ObjectError> {
        self.propose_set_object_retention_fenced_with_read_proof_linearizable(
            bucket,
            key,
            retention_until_ms,
            update_time_ms,
            "",
        )
    }

    pub fn propose_set_object_retention_fenced_with_read_proof_linearizable(
        &mut self,
        bucket: &str,
        key: &str,
        retention_until_ms: u64,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenMetadataRead<ObjectRetentionMutationReport>, ObjectError> {
        self.propose_set_object_retention_fenced_with_read_proof_linearizable_committed_at(
            bucket,
            key,
            retention_until_ms,
            update_time_ms,
            expected_page_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_set_object_retention_with_read_proof_linearizable_committed_at(
        &mut self,
        bucket: &str,
        key: &str,
        retention_until_ms: u64,
        update_time_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<ObjectRetentionMutationReport>, ObjectError> {
        self.propose_set_object_retention_fenced_with_read_proof_linearizable_committed_at(
            bucket,
            key,
            retention_until_ms,
            update_time_ms,
            "",
            min_read_index,
            min_commit_index,
        )
    }
}
