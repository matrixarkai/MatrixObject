// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_complete_multipart_upload_with_route_report(
        &mut self,
        upload_id: &str,
        expected_size: Option<u64>,
        expected_etag: Option<String>,
        update_time_ms: u64,
    ) -> Result<MultipartCompleteReport, ObjectError> {
        let expected_route_control_checksum = self
            .store
            .chunk_service
            .route_control_proof()
            .state_checksum;
        self.propose_complete_multipart_upload_with_route_report_linearizable_fenced(
            upload_id,
            expected_size,
            expected_etag,
            update_time_ms,
            &expected_route_control_checksum,
        )
    }

    pub fn propose_complete_multipart_upload_with_route_report_linearizable_fenced(
        &mut self,
        upload_id: &str,
        expected_size: Option<u64>,
        expected_etag: Option<String>,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<MultipartCompleteReport, ObjectError> {
        self.propose_complete_multipart_upload_with_checksum_route_report_linearizable_fenced(
            upload_id,
            expected_size,
            expected_etag,
            None,
            update_time_ms,
            expected_page_route_control_checksum,
        )
    }

    pub fn propose_complete_multipart_upload_with_checksum_route_report_linearizable_fenced(
        &mut self,
        upload_id: &str,
        expected_size: Option<u64>,
        expected_etag: Option<String>,
        expected_checksum: Option<String>,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<MultipartCompleteReport, ObjectError> {
        self.store.multipart_upload(upload_id)?;
        self.ensure_page_route_control_ready(
            "multipart complete",
            expected_page_route_control_checksum.as_ref(),
        )?;
        let route_control_proof = self.store.chunk_service.route_control_proof();
        let command = ObjectCommand::CompleteMultipartUpload {
            upload_id: upload_id.to_string(),
            expected_size,
            expected_etag,
            expected_checksum,
            update_time_ms,
        };
        let entry = self.raft.append(command)?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::PreconditionFailed(
                "multipart complete raft index is not contiguous".to_string(),
            ));
        }
        let metadata = self.store.apply_raft_entry(&entry)?;
        let mut report = MultipartCompleteReport {
            metadata,
            route_control_proof,
            expected_route_control_checksum: expected_page_route_control_checksum
                .as_ref()
                .to_string(),
            multipart_complete_proof_format: String::new(),
            multipart_complete_proof_checksum: String::new(),
            expected_multipart_complete_proof_checksum: String::new(),
        };
        stamp_multipart_complete_proof("ProposeCompleteMultipartUpload", &mut report);
        Ok(report)
    }

    pub fn propose_complete_multipart_upload_with_checksum_route_report_and_read_proof_linearizable_fenced(
        &mut self,
        upload_id: &str,
        expected_size: Option<u64>,
        expected_etag: Option<String>,
        expected_checksum: Option<String>,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenMetadataRead<MultipartCompleteReport>, ObjectError> {
        self.propose_complete_multipart_upload_with_checksum_route_report_and_read_proof_linearizable_fenced_committed_at(
            upload_id,
            expected_size,
            expected_etag,
            expected_checksum,
            update_time_ms,
            expected_page_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_complete_multipart_upload_with_checksum_route_proof_linearizable_fenced(
        &mut self,
        upload_id: &str,
        expected_size: Option<u64>,
        expected_etag: Option<String>,
        expected_checksum: Option<String>,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenRouteFencedMetadataRead<MultipartCompleteReport>, ObjectError> {
        self.propose_complete_multipart_upload_with_checksum_route_proof_linearizable_fenced_committed_at(
            upload_id,
            expected_size,
            expected_etag,
            expected_checksum,
            update_time_ms,
            expected_page_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_complete_multipart_upload_with_checksum_route_proof_linearizable_fenced_committed_at(
        &mut self,
        upload_id: &str,
        expected_size: Option<u64>,
        expected_etag: Option<String>,
        expected_checksum: Option<String>,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenRouteFencedMetadataRead<MultipartCompleteReport>, ObjectError> {
        let expected_checksum_string = expected_page_route_control_checksum.as_ref().to_string();
        let proven = self
            .propose_complete_multipart_upload_with_checksum_route_report_and_read_proof_linearizable_fenced_committed_at(
                upload_id,
                expected_size,
                expected_etag,
                expected_checksum,
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

    pub fn propose_complete_multipart_upload_with_checksum_route_report_and_read_proof_linearizable_fenced_committed_at(
        &mut self,
        upload_id: &str,
        expected_size: Option<u64>,
        expected_etag: Option<String>,
        expected_checksum: Option<String>,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<MultipartCompleteReport>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "multipart complete",
            update_time_ms,
            min_read_index,
            min_commit_index,
        )?;
        let report = self
            .propose_complete_multipart_upload_with_checksum_route_report_linearizable_fenced(
                upload_id,
                expected_size,
                expected_etag,
                expected_checksum,
                update_time_ms,
                expected_page_route_control_checksum,
            )?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "multipart complete post-commit",
            update_time_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        Ok(proven_metadata_read(report, &readiness))
    }
}
