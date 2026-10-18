impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_set_object_retention_fenced_with_read_proof_linearizable_committed_at(
        &mut self,
        bucket: &str,
        key: &str,
        retention_until_ms: u64,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<ObjectRetentionMutationReport>, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_metadata_read_index_committed_at(
            "set object retention",
            update_time_ms,
            min_read_index,
            min_commit_index,
        )?;
        self.ensure_page_route_control_ready_at(
            "set object retention",
            expected_page_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        let route_control_proof = self.store.page_service.route_control_proof();
        let metadata =
            self.propose_set_object_retention(bucket, key, retention_until_ms, update_time_ms)?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "set object retention post-commit",
            update_time_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        let report = ObjectRetentionMutationReport {
            retention_until_ms: metadata.lock.retention_until_ms,
            route_control_proof,
            expected_route_control_checksum: expected_page_route_control_checksum.to_string(),
            metadata,
        };
        Ok(proven_metadata_read(report, &readiness))
    }

    pub fn propose_set_object_tags_with_read_proof_linearizable(
        &mut self,
        bucket: &str,
        key: &str,
        tags: BTreeMap<String, String>,
        update_time_ms: u64,
    ) -> Result<ProvenMetadataRead<ObjectTagsMutationReport>, ObjectError> {
        self.propose_set_object_tags_fenced_with_read_proof_linearizable(
            bucket,
            key,
            tags,
            update_time_ms,
            "",
        )
    }

    pub fn propose_set_object_tags_fenced_with_read_proof_linearizable(
        &mut self,
        bucket: &str,
        key: &str,
        tags: BTreeMap<String, String>,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenMetadataRead<ObjectTagsMutationReport>, ObjectError> {
        self.propose_set_object_tags_fenced_with_read_proof_linearizable_committed_at(
            bucket,
            key,
            tags,
            update_time_ms,
            expected_page_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_set_object_tags_with_read_proof_linearizable_committed_at(
        &mut self,
        bucket: &str,
        key: &str,
        tags: BTreeMap<String, String>,
        update_time_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<ObjectTagsMutationReport>, ObjectError> {
        self.propose_set_object_tags_fenced_with_read_proof_linearizable_committed_at(
            bucket,
            key,
            tags,
            update_time_ms,
            "",
            min_read_index,
            min_commit_index,
        )
    }

    pub fn propose_set_object_tags_fenced_with_read_proof_linearizable_committed_at(
        &mut self,
        bucket: &str,
        key: &str,
        tags: BTreeMap<String, String>,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<ObjectTagsMutationReport>, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_metadata_read_index_committed_at(
            "set object tags",
            update_time_ms,
            min_read_index,
            min_commit_index,
        )?;
        self.ensure_page_route_control_ready_at(
            "set object tags",
            expected_page_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        let route_control_proof = self.store.page_service.route_control_proof();
        let metadata = self.propose_set_object_tags(bucket, key, tags, update_time_ms)?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "set object tags post-commit",
            update_time_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        let report = ObjectTagsMutationReport {
            tags: metadata.tags.clone().into(),
            route_control_proof,
            expected_route_control_checksum: expected_page_route_control_checksum.to_string(),
            metadata,
        };
        Ok(proven_metadata_read(report, &readiness))
    }
}
