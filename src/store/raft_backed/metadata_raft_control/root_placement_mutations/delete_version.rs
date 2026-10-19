impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_delete_object_version(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::DeleteObjectVersion {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version,
            update_time_ms,
        })
    }

    pub fn propose_delete_object_version_fenced(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ObjectDeleteReport, ObjectError> {
        self.ensure_page_route_control_ready(
            "object version delete",
            expected_page_route_control_checksum.as_ref(),
        )?;
        let route_control_proof = self.store.chunk_service.route_control_proof();
        let request = ObjectDeleteRequest {
            bucket: bucket.to_string(),
            key: key.to_string(),
            if_match_etag: None,
            version: Some(version),
        };
        let metadata = self.propose(ObjectCommand::DeleteObjectVersion {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version,
            update_time_ms,
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

    pub fn propose_delete_object_version_fenced_with_read_proof_linearizable(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenMetadataRead<ObjectDeleteReport>, ObjectError> {
        self.propose_delete_object_version_fenced_with_read_proof_linearizable_committed_at(
            bucket,
            key,
            version,
            update_time_ms,
            expected_page_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_delete_object_version_with_route_proof_linearizable_fenced(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenRouteFencedMetadataRead<ObjectDeleteReport>, ObjectError> {
        self.propose_delete_object_version_with_route_proof_linearizable_fenced_committed_at(
            bucket,
            key,
            version,
            update_time_ms,
            expected_page_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_delete_object_version_with_route_proof_linearizable_fenced_committed_at(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenRouteFencedMetadataRead<ObjectDeleteReport>, ObjectError> {
        let expected_checksum = expected_page_route_control_checksum.as_ref().to_string();
        let proven = self
            .propose_delete_object_version_fenced_with_read_proof_linearizable_committed_at(
                bucket,
                key,
                version,
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

    pub fn propose_delete_object_version_fenced_with_read_proof_linearizable_committed_at(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<ObjectDeleteReport>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "delete object version",
            update_time_ms,
            min_read_index,
            min_commit_index,
        )?;
        let mut report = self.propose_delete_object_version_fenced(
            bucket,
            key,
            version,
            update_time_ms,
            expected_page_route_control_checksum,
        )?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "delete object version post-commit",
            update_time_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        report.read_proof = object_read_proof_from_readiness(&readiness);
        Ok(proven_metadata_read(report, &readiness))
    }
}
