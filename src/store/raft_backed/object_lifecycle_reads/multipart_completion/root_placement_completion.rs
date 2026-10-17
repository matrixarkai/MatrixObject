impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_complete_multipart_upload_with_root_placement(
        &mut self,
        upload_id: &str,
        expected_size: Option<u64>,
        expected_etag: Option<String>,
        expected_checksum: Option<String>,
        replica_count: usize,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_complete_multipart_upload_with_root_placement_fenced(
            upload_id,
            expected_size,
            expected_etag,
            expected_checksum,
            replica_count,
            update_time_ms,
            "",
        )
    }

    pub fn propose_complete_multipart_upload_with_root_placement_fenced(
        &mut self,
        upload_id: &str,
        expected_size: Option<u64>,
        expected_etag: Option<String>,
        expected_checksum: Option<String>,
        replica_count: usize,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_page_route_control_ready(
            "multipart complete with root placement",
            expected_page_route_control_checksum.as_ref(),
        )?;
        self.propose(ObjectCommand::CompleteMultipartUploadWithRootPlacement {
            upload_id: upload_id.to_string(),
            expected_size,
            expected_etag,
            expected_checksum,
            replica_count,
            update_time_ms,
        })
    }

    pub fn propose_complete_multipart_upload_with_root_placement_fenced_with_read_proof_linearizable(
        &mut self,
        upload_id: &str,
        expected_size: Option<u64>,
        expected_etag: Option<String>,
        expected_checksum: Option<String>,
        replica_count: usize,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenMetadataRead<MultipartCompleteReport>, ObjectError> {
        self.propose_complete_multipart_upload_with_root_placement_fenced_with_read_proof_linearizable_committed_at(
            upload_id,
            expected_size,
            expected_etag,
            expected_checksum,
            replica_count,
            update_time_ms,
            expected_page_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_complete_multipart_upload_with_root_placement_fenced_with_read_proof_linearizable_committed_at(
        &mut self,
        upload_id: &str,
        expected_size: Option<u64>,
        expected_etag: Option<String>,
        expected_checksum: Option<String>,
        replica_count: usize,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<MultipartCompleteReport>, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_metadata_read_index_committed_at(
            "multipart complete with root placement",
            update_time_ms,
            min_read_index,
            min_commit_index,
        )?;
        let route_control_proof = self.store.page_service.route_control_proof();
        let metadata = self.propose_complete_multipart_upload_with_root_placement_fenced(
            upload_id,
            expected_size,
            expected_etag,
            expected_checksum,
            replica_count,
            update_time_ms,
            expected_page_route_control_checksum,
        )?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "multipart complete with root placement post-commit",
            update_time_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        let mut report = MultipartCompleteReport {
            metadata,
            route_control_proof,
            expected_route_control_checksum: expected_page_route_control_checksum.to_string(),
            multipart_complete_proof_format: String::new(),
            multipart_complete_proof_checksum: String::new(),
            expected_multipart_complete_proof_checksum: String::new(),
        };
        stamp_multipart_complete_proof(
            "ProposeCompleteMultipartUploadWithRootPlacement",
            &mut report,
        );
        Ok(proven_metadata_read(report, &readiness))
    }

    pub fn propose_complete_multipart_upload_with_root_placement_with_route_proof_linearizable_fenced(
        &mut self,
        upload_id: &str,
        expected_size: Option<u64>,
        expected_etag: Option<String>,
        expected_checksum: Option<String>,
        replica_count: usize,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenRouteFencedMetadataRead<MultipartCompleteReport>, ObjectError> {
        self.propose_complete_multipart_upload_with_root_placement_with_route_proof_linearizable_fenced_committed_at(
            upload_id,
            expected_size,
            expected_etag,
            expected_checksum,
            replica_count,
            update_time_ms,
            expected_page_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_complete_multipart_upload_with_root_placement_with_route_proof_linearizable_fenced_committed_at(
        &mut self,
        upload_id: &str,
        expected_size: Option<u64>,
        expected_etag: Option<String>,
        expected_checksum: Option<String>,
        replica_count: usize,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenRouteFencedMetadataRead<MultipartCompleteReport>, ObjectError> {
        let expected_checksum_string = expected_page_route_control_checksum.as_ref().to_string();
        let proven = self
            .propose_complete_multipart_upload_with_root_placement_fenced_with_read_proof_linearizable_committed_at(
                upload_id,
                expected_size,
                expected_etag,
                expected_checksum,
                replica_count,
                update_time_ms,
                &expected_checksum_string,
                min_read_index,
                min_commit_index,
            )?;
        Ok(ProvenRouteFencedMetadataRead {
            route_control_proof: proven.value.route_control_proof.clone(),
            expected_route_control_checksum: expected_checksum_string,
            value: proven.value,
            proof: proven.proof,
        })
    }

    pub fn propose_complete_multipart_upload_with_root_placement_with_route_proof_from_meta_shard_fenced(
        &mut self,
        upload_id: &str,
        expected_size: Option<u64>,
        expected_etag: Option<String>,
        expected_checksum: Option<String>,
        replica_count: usize,
        update_time_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenRouteFencedMetadataRead<MultipartCompleteReport>, ObjectError> {
        self.check_multipart_meta_shard_owner_serving(
            upload_id,
            expected_shard_owner,
            expected_shard_epoch,
            update_time_ms,
        )?;
        self.propose_complete_multipart_upload_with_root_placement_with_route_proof_linearizable_fenced_committed_at(
            upload_id,
            expected_size,
            expected_etag,
            expected_checksum,
            replica_count,
            update_time_ms,
            expected_page_route_control_checksum,
            min_read_index,
            min_commit_index,
        )
    }
}
