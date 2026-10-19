impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_complete_multipart_upload(
        &mut self,
        upload_id: &str,
        expected_size: Option<u64>,
        expected_etag: Option<String>,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_complete_multipart_upload_with_expected_checksum(
            upload_id,
            expected_size,
            expected_etag,
            None,
            update_time_ms,
        )
    }

    pub fn propose_complete_multipart_upload_with_expected_checksum(
        &mut self,
        upload_id: &str,
        expected_size: Option<u64>,
        expected_etag: Option<String>,
        expected_checksum: Option<String>,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::CompleteMultipartUpload {
            upload_id: upload_id.to_string(),
            expected_size,
            expected_etag,
            expected_checksum,
            update_time_ms,
        })
    }

    pub fn propose_complete_multipart_upload_idempotent(
        &mut self,
        upload_id: &str,
        expected_size: Option<u64>,
        expected_etag: Option<String>,
        expected_checksum: Option<String>,
        update_time_ms: u64,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        let request_checksum = multipart_complete_request_checksum(
            upload_id,
            expected_size,
            expected_etag.as_deref(),
            expected_checksum.as_deref(),
        )?;
        if let Some(existing) = self.store.multipart_complete_receipts.get(idempotency_key) {
            if existing.upload_id == upload_id && existing.request_checksum == request_checksum {
                return Ok(existing.metadata.clone());
            }
            return Err(ObjectError::PreconditionFailed(
                "multipart complete idempotency key reused with different request".to_string(),
            ));
        }
        self.propose(ObjectCommand::CompleteMultipartUploadIdempotent {
            upload_id: upload_id.to_string(),
            expected_size,
            expected_etag,
            expected_checksum,
            update_time_ms,
            idempotency_key: idempotency_key.to_string(),
        })
    }

    pub fn propose_complete_multipart_upload_idempotent_with_route_report_linearizable_fenced(
        &mut self,
        upload_id: &str,
        expected_size: Option<u64>,
        expected_etag: Option<String>,
        expected_checksum: Option<String>,
        update_time_ms: u64,
        idempotency_key: &str,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<MultipartCompleteReport, ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        let request_checksum = multipart_complete_request_checksum(
            upload_id,
            expected_size,
            expected_etag.as_deref(),
            expected_checksum.as_deref(),
        )?;
        self.ensure_page_route_control_ready(
            "multipart complete idempotent",
            expected_page_route_control_checksum.as_ref(),
        )?;
        let route_control_proof = self.store.chunk_service.route_control_proof();
        if let Some(existing) = self.store.multipart_complete_receipts.get(idempotency_key) {
            if existing.upload_id == upload_id && existing.request_checksum == request_checksum {
                let mut report = MultipartCompleteReport {
                    metadata: existing.metadata.clone(),
                    route_control_proof,
                    expected_route_control_checksum: expected_page_route_control_checksum
                        .as_ref()
                        .to_string(),
                    multipart_complete_proof_format: String::new(),
                    multipart_complete_proof_checksum: String::new(),
                    expected_multipart_complete_proof_checksum: String::new(),
                };
                stamp_multipart_complete_proof(
                    "ProposeCompleteMultipartUploadIdempotent",
                    &mut report,
                );
                return Ok(report);
            }
            return Err(ObjectError::PreconditionFailed(
                "multipart complete idempotency key reused with different request".to_string(),
            ));
        }
        let command = ObjectCommand::CompleteMultipartUploadIdempotent {
            upload_id: upload_id.to_string(),
            expected_size,
            expected_etag,
            expected_checksum,
            update_time_ms,
            idempotency_key: idempotency_key.to_string(),
        };
        let entry = self.raft.append(command)?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::PreconditionFailed(
                "multipart complete idempotent raft index is not contiguous".to_string(),
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
        stamp_multipart_complete_proof("ProposeCompleteMultipartUploadIdempotent", &mut report);
        Ok(report)
    }

    pub fn propose_complete_multipart_upload_idempotent_from_meta_shard(
        &mut self,
        upload_id: &str,
        expected_size: Option<u64>,
        expected_etag: Option<String>,
        expected_checksum: Option<String>,
        update_time_ms: u64,
        idempotency_key: &str,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<MultipartCompleteReport, ObjectError> {
        self.check_multipart_meta_shard_owner_serving(
            upload_id,
            expected_shard_owner,
            expected_shard_epoch,
            update_time_ms,
        )?;
        self.store.ensure_metadata_mutation_fence(
            "multipart complete idempotent from meta shard",
            min_read_index,
            0,
        )?;
        self.propose_complete_multipart_upload_idempotent_with_route_report_linearizable_fenced(
            upload_id,
            expected_size,
            expected_etag,
            expected_checksum,
            update_time_ms,
            idempotency_key,
            expected_page_route_control_checksum,
        )
    }
}
