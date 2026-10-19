// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_abort_multipart_upload_with_route_report(
        &mut self,
        upload_id: &str,
        update_time_ms: u64,
    ) -> Result<MultipartAbortReport, ObjectError> {
        let expected_route_control_checksum = self
            .store
            .chunk_service
            .route_control_proof()
            .state_checksum;
        self.propose_abort_multipart_upload_with_route_report_linearizable_fenced(
            upload_id,
            update_time_ms,
            &expected_route_control_checksum,
        )
    }

    pub fn propose_abort_multipart_upload_with_route_report_linearizable_fenced(
        &mut self,
        upload_id: &str,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<MultipartAbortReport, ObjectError> {
        let upload = self.store.multipart_upload(upload_id)?;
        self.ensure_page_route_control_ready(
            "multipart abort",
            expected_page_route_control_checksum.as_ref(),
        )?;
        let route_control_proof = self.store.chunk_service.route_control_proof();
        let command = ObjectCommand::AbortMultipartUpload {
            upload_id: upload_id.to_string(),
            update_time_ms,
        };
        let entry = self.raft.append(command)?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::PreconditionFailed(
                "multipart abort raft index is not contiguous".to_string(),
            ));
        }
        self.store.apply_raft_entry(&entry)?;
        let mut report = MultipartAbortReport {
            upload,
            route_control_proof,
            expected_route_control_checksum: expected_page_route_control_checksum
                .as_ref()
                .to_string(),
            multipart_abort_proof_format: String::new(),
            multipart_abort_proof_checksum: String::new(),
            expected_multipart_abort_proof_checksum: String::new(),
        };
        stamp_multipart_abort_proof("ProposeAbortMultipartUpload", &mut report);
        Ok(report)
    }

    pub fn propose_abort_multipart_upload_idempotent_with_route_report_linearizable_fenced(
        &mut self,
        upload_id: &str,
        update_time_ms: u64,
        idempotency_key: &str,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<MultipartAbortReport, ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        let request_checksum = multipart_abort_request_checksum(upload_id)?;
        self.ensure_page_route_control_ready(
            "multipart abort idempotent",
            expected_page_route_control_checksum.as_ref(),
        )?;
        let route_control_proof = self.store.chunk_service.route_control_proof();
        if let Some(existing) = self.store.multipart_abort_receipts.get(idempotency_key) {
            if existing.upload_id == upload_id && existing.request_checksum == request_checksum {
                let mut report = MultipartAbortReport {
                    upload: existing.upload.clone(),
                    route_control_proof,
                    expected_route_control_checksum: expected_page_route_control_checksum
                        .as_ref()
                        .to_string(),
                    multipart_abort_proof_format: String::new(),
                    multipart_abort_proof_checksum: String::new(),
                    expected_multipart_abort_proof_checksum: String::new(),
                };
                stamp_multipart_abort_proof("ProposeAbortMultipartUploadIdempotent", &mut report);
                return Ok(report);
            }
            return Err(ObjectError::PreconditionFailed(
                "multipart abort idempotency key reused with different request".to_string(),
            ));
        }
        let command = ObjectCommand::AbortMultipartUploadIdempotent {
            upload_id: upload_id.to_string(),
            update_time_ms,
            idempotency_key: idempotency_key.to_string(),
        };
        let entry = self.raft.append(command)?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::PreconditionFailed(
                "multipart abort idempotent raft index is not contiguous".to_string(),
            ));
        }
        self.store.apply_raft_entry(&entry)?;
        let receipt = self
            .store
            .multipart_abort_receipts
            .get(idempotency_key)
            .ok_or_else(|| {
                ObjectError::Corruption("multipart abort receipt missing".to_string())
            })?;
        let mut report = MultipartAbortReport {
            upload: receipt.upload.clone(),
            route_control_proof,
            expected_route_control_checksum: expected_page_route_control_checksum
                .as_ref()
                .to_string(),
            multipart_abort_proof_format: String::new(),
            multipart_abort_proof_checksum: String::new(),
            expected_multipart_abort_proof_checksum: String::new(),
        };
        stamp_multipart_abort_proof("ProposeAbortMultipartUploadIdempotent", &mut report);
        Ok(report)
    }

    pub fn propose_abort_multipart_upload_idempotent_from_meta_shard(
        &mut self,
        upload_id: &str,
        update_time_ms: u64,
        idempotency_key: &str,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<MultipartAbortReport, ObjectError> {
        self.check_multipart_meta_shard_owner_serving(
            upload_id,
            expected_shard_owner,
            expected_shard_epoch,
            update_time_ms,
        )?;
        self.store.ensure_metadata_mutation_fence(
            "multipart abort idempotent from meta shard",
            min_read_index,
            0,
        )?;
        self.propose_abort_multipart_upload_idempotent_with_route_report_linearizable_fenced(
            upload_id,
            update_time_ms,
            idempotency_key,
            expected_page_route_control_checksum,
        )
    }

    pub fn propose_abort_multipart_upload_with_route_report_linearizable_fenced_with_abort_proof(
        &mut self,
        upload_id: &str,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_multipart_abort_proof_checksum: Option<&str>,
    ) -> Result<MultipartAbortReport, ObjectError> {
        let mut report = self
            .propose_abort_multipart_upload_with_route_report_linearizable_fenced(
                upload_id,
                update_time_ms,
                expected_page_route_control_checksum,
            )?;
        report.expected_multipart_abort_proof_checksum = expected_multipart_abort_proof_checksum
            .unwrap_or_default()
            .to_string();
        check_expected_multipart_abort_proof_checksum(
            "ProposeAbortMultipartUpload",
            &report.multipart_abort_proof_checksum,
            expected_multipart_abort_proof_checksum,
        )?;
        Ok(report)
    }

    pub fn propose_abort_multipart_upload_with_route_report_and_read_proof_linearizable_fenced(
        &mut self,
        upload_id: &str,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenMetadataRead<MultipartAbortReport>, ObjectError> {
        self.propose_abort_multipart_upload_with_route_report_and_read_proof_linearizable_fenced_committed_at(
            upload_id,
            update_time_ms,
            expected_page_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_abort_multipart_upload_with_route_report_and_read_proof_linearizable_fenced_committed_at(
        &mut self,
        upload_id: &str,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<MultipartAbortReport>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "multipart abort",
            update_time_ms,
            min_read_index,
            min_commit_index,
        )?;
        let report = self.propose_abort_multipart_upload_with_route_report_linearizable_fenced(
            upload_id,
            update_time_ms,
            expected_page_route_control_checksum,
        )?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "multipart abort post-commit",
            update_time_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        Ok(proven_metadata_read(report, &readiness))
    }

    pub fn propose_abort_multipart_upload_from_meta_shard(
        &mut self,
        upload_id: &str,
        update_time_ms: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<MultipartAbortReport, ObjectError> {
        self.check_multipart_meta_shard_owner_serving(
            upload_id,
            expected_shard_owner,
            expected_shard_epoch,
            update_time_ms,
        )?;
        self.store.ensure_metadata_mutation_fence(
            "multipart abort from meta shard",
            min_read_index,
            0,
        )?;
        self.propose_abort_multipart_upload_with_route_report_linearizable_fenced(
            upload_id,
            update_time_ms,
            expected_page_route_control_checksum,
        )
    }

    pub fn propose_abort_multipart_upload_with_read_proof_from_meta_shard(
        &mut self,
        upload_id: &str,
        update_time_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenMetadataRead<MultipartAbortReport>, ObjectError> {
        self.check_multipart_meta_shard_owner_serving(
            upload_id,
            expected_shard_owner,
            expected_shard_epoch,
            update_time_ms,
        )?;
        self.propose_abort_multipart_upload_with_route_report_and_read_proof_linearizable_fenced_committed_at(
            upload_id,
            update_time_ms,
            expected_page_route_control_checksum,
            min_read_index,
            min_commit_index,
        )
    }
}
