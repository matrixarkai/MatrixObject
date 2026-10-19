// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_set_object_legal_hold(
        &mut self,
        bucket: &str,
        key: &str,
        enabled: bool,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::SetObjectLegalHold {
            bucket: bucket.to_string(),
            key: key.to_string(),
            enabled,
            update_time_ms,
        })
    }

    pub fn propose_set_object_legal_hold_with_read_proof_linearizable(
        &mut self,
        bucket: &str,
        key: &str,
        enabled: bool,
        update_time_ms: u64,
    ) -> Result<ProvenMetadataRead<ObjectLegalHoldMutationReport>, ObjectError> {
        self.propose_set_object_legal_hold_fenced_with_read_proof_linearizable(
            bucket,
            key,
            enabled,
            update_time_ms,
            "",
        )
    }

    pub fn propose_set_object_legal_hold_fenced_with_read_proof_linearizable(
        &mut self,
        bucket: &str,
        key: &str,
        enabled: bool,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenMetadataRead<ObjectLegalHoldMutationReport>, ObjectError> {
        self.propose_set_object_legal_hold_fenced_with_read_proof_linearizable_committed_at(
            bucket,
            key,
            enabled,
            update_time_ms,
            expected_page_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_set_object_legal_hold_with_read_proof_linearizable_committed_at(
        &mut self,
        bucket: &str,
        key: &str,
        enabled: bool,
        update_time_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<ObjectLegalHoldMutationReport>, ObjectError> {
        self.propose_set_object_legal_hold_fenced_with_read_proof_linearizable_committed_at(
            bucket,
            key,
            enabled,
            update_time_ms,
            "",
            min_read_index,
            min_commit_index,
        )
    }

    pub fn propose_set_object_legal_hold_fenced_with_read_proof_linearizable_committed_at(
        &mut self,
        bucket: &str,
        key: &str,
        enabled: bool,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<ObjectLegalHoldMutationReport>, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_metadata_read_index_committed_at(
            "set object legal hold",
            update_time_ms,
            min_read_index,
            min_commit_index,
        )?;
        self.ensure_page_route_control_ready_at(
            "set object legal hold",
            expected_page_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        let route_control_proof = self.store.chunk_service.route_control_proof();
        let metadata = self.propose_set_object_legal_hold(bucket, key, enabled, update_time_ms)?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "set object legal hold post-commit",
            update_time_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        let report = ObjectLegalHoldMutationReport {
            enabled: metadata.lock.legal_hold,
            route_control_proof,
            expected_route_control_checksum: expected_page_route_control_checksum.to_string(),
            metadata,
        };
        Ok(proven_metadata_read(report, &readiness))
    }
}
