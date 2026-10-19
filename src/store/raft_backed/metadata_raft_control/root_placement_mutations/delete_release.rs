impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_delete_with_root_release(
        &mut self,
        bucket: &str,
        key: &str,
        if_match_etag: Option<String>,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::DeleteObjectWithRootRelease {
            bucket: bucket.to_string(),
            key: key.to_string(),
            update_time_ms,
            if_match_etag,
        })
    }

    pub fn propose_delete_with_root_release_fenced(
        &mut self,
        bucket: &str,
        key: &str,
        if_match_etag: Option<String>,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ObjectDeleteReport, ObjectError> {
        self.ensure_page_route_control_ready(
            "object delete",
            expected_page_route_control_checksum.as_ref(),
        )?;
        let route_control_proof = self.store.chunk_service.route_control_proof();
        let request = ObjectDeleteRequest {
            bucket: bucket.to_string(),
            key: key.to_string(),
            if_match_etag: if_match_etag.clone(),
            version: None,
        };
        let metadata = self.propose(ObjectCommand::DeleteObjectWithRootRelease {
            bucket: bucket.to_string(),
            key: key.to_string(),
            update_time_ms,
            if_match_etag,
        })?;
        Ok(ObjectDeleteReport {
            request,
            deleted_bytes: metadata.length,
            metadata,
            read_proof: ObjectReadProof::default(),
            route_control_proof,
            expected_route_control_checksum: expected_page_route_control_checksum
                .as_ref()
                .to_string(),
        })
    }

    pub fn propose_delete_with_root_release_fenced_with_read_proof_linearizable(
        &mut self,
        bucket: &str,
        key: &str,
        if_match_etag: Option<String>,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenMetadataRead<ObjectDeleteReport>, ObjectError> {
        self.propose_delete_with_root_release_fenced_with_read_proof_linearizable_committed_at(
            bucket,
            key,
            if_match_etag,
            update_time_ms,
            expected_page_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_delete_with_root_release_with_route_proof_linearizable_fenced(
        &mut self,
        bucket: &str,
        key: &str,
        if_match_etag: Option<String>,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenRouteFencedMetadataRead<ObjectDeleteReport>, ObjectError> {
        self.propose_delete_with_root_release_with_route_proof_linearizable_fenced_committed_at(
            bucket,
            key,
            if_match_etag,
            update_time_ms,
            expected_page_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_delete_with_root_release_with_route_proof_linearizable_fenced_committed_at(
        &mut self,
        bucket: &str,
        key: &str,
        if_match_etag: Option<String>,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenRouteFencedMetadataRead<ObjectDeleteReport>, ObjectError> {
        let expected_checksum = expected_page_route_control_checksum.as_ref().to_string();
        let proven = self
            .propose_delete_with_root_release_fenced_with_read_proof_linearizable_committed_at(
                bucket,
                key,
                if_match_etag,
                update_time_ms,
                &expected_checksum,
                min_read_index,
                min_commit_index,
            )?;
        Ok(ProvenRouteFencedMetadataRead {
            route_control_proof: proven.value.route_control_proof.clone(),
            expected_route_control_checksum: expected_checksum,
            value: proven.value,
            proof: proven.proof,
        })
    }

    pub fn propose_delete_with_root_release_with_route_proof_from_meta_shard_fenced(
        &mut self,
        bucket: &str,
        key: &str,
        if_match_etag: Option<String>,
        update_time_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenRouteFencedMetadataRead<ObjectDeleteReport>, ObjectError> {
        self.check_object_meta_shard_owner_serving(
            bucket,
            key,
            expected_shard_owner,
            expected_shard_epoch,
            update_time_ms,
        )?;
        self.propose_delete_with_root_release_with_route_proof_linearizable_fenced_committed_at(
            bucket,
            key,
            if_match_etag,
            update_time_ms,
            expected_page_route_control_checksum,
            min_read_index,
            min_commit_index,
        )
    }

    pub fn propose_delete_with_root_release_fenced_with_read_proof_linearizable_committed_at(
        &mut self,
        bucket: &str,
        key: &str,
        if_match_etag: Option<String>,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<ObjectDeleteReport>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "delete object with root release",
            update_time_ms,
            min_read_index,
            min_commit_index,
        )?;
        let mut report = self.propose_delete_with_root_release_fenced(
            bucket,
            key,
            if_match_etag,
            update_time_ms,
            expected_page_route_control_checksum,
        )?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "delete object with root release post-commit",
            update_time_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        report.read_proof = object_read_proof_from_readiness(&readiness);
        Ok(proven_metadata_read(report, &readiness))
    }
}
