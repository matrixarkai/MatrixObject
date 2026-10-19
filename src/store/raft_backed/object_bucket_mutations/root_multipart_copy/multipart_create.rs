// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_create_multipart_upload(
        &mut self,
        upload_id: impl Into<String>,
        bucket: &str,
        key: &str,
        content_type: impl Into<String>,
        options: PutOptions,
        create_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::CreateMultipartUpload {
            upload_id: upload_id.into(),
            bucket: bucket.to_string(),
            key: key.to_string(),
            content_type: content_type.into(),
            options,
            create_time_ms,
        })
    }

    pub fn propose_create_multipart_upload_idempotent(
        &mut self,
        upload_id: impl Into<String>,
        bucket: &str,
        key: &str,
        content_type: impl Into<String>,
        options: PutOptions,
        create_time_ms: u64,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        self.propose(ObjectCommand::CreateMultipartUploadIdempotent {
            upload_id: upload_id.into(),
            bucket: bucket.to_string(),
            key: key.to_string(),
            content_type: content_type.into(),
            options,
            create_time_ms,
            idempotency_key: idempotency_key.to_string(),
        })
    }

    pub fn propose_create_multipart_upload_with_route_report(
        &mut self,
        upload_id: impl Into<String>,
        bucket: &str,
        key: &str,
        content_type: impl Into<String>,
        options: PutOptions,
        create_time_ms: u64,
    ) -> Result<MultipartCreateReport, ObjectError> {
        let expected_route_control_checksum = self
            .store
            .chunk_service
            .route_control_proof()
            .state_checksum;
        self.propose_create_multipart_upload_with_route_report_linearizable_fenced(
            upload_id,
            bucket,
            key,
            content_type,
            options,
            create_time_ms,
            &expected_route_control_checksum,
        )
    }

    pub fn propose_create_multipart_upload_idempotent_with_route_report_linearizable_fenced(
        &mut self,
        upload_id: impl Into<String>,
        bucket: &str,
        key: &str,
        content_type: impl Into<String>,
        options: PutOptions,
        create_time_ms: u64,
        idempotency_key: &str,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<MultipartCreateReport, ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        self.ensure_page_route_control_ready(
            "multipart create idempotent",
            expected_page_route_control_checksum.as_ref(),
        )?;
        let route_control_proof = self.store.chunk_service.route_control_proof();
        let upload_id = upload_id.into();
        let content_type = content_type.into();
        if let Ok(existing) = self.store.multipart_upload(&upload_id) {
            if existing.bucket == bucket
                && existing.key == key
                && existing.content_type == content_type
                && existing.options == options
                && existing.create_idempotency_key.as_deref() == Some(idempotency_key)
            {
                let mut report = MultipartCreateReport {
                    upload: existing,
                    route_control_proof,
                    expected_route_control_checksum: expected_page_route_control_checksum
                        .as_ref()
                        .to_string(),
                    multipart_create_proof_format: String::new(),
                    multipart_create_proof_checksum: String::new(),
                    expected_multipart_create_proof_checksum: String::new(),
                };
                stamp_multipart_create_proof("ProposeCreateMultipartUploadIdempotent", &mut report);
                return Ok(report);
            }
            return Err(ObjectError::PreconditionFailed(
                "multipart create idempotency key reused with different upload intent".to_string(),
            ));
        }
        let command = ObjectCommand::CreateMultipartUploadIdempotent {
            upload_id: upload_id.clone(),
            bucket: bucket.to_string(),
            key: key.to_string(),
            content_type,
            options,
            create_time_ms,
            idempotency_key: idempotency_key.to_string(),
        };
        let entry = self.raft.append(command)?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::PreconditionFailed(
                "multipart create idempotent raft index is not contiguous".to_string(),
            ));
        }
        self.store.apply_raft_entry(&entry)?;
        let mut report = MultipartCreateReport {
            upload: self.store.multipart_upload(&upload_id)?,
            route_control_proof,
            expected_route_control_checksum: expected_page_route_control_checksum
                .as_ref()
                .to_string(),
            multipart_create_proof_format: String::new(),
            multipart_create_proof_checksum: String::new(),
            expected_multipart_create_proof_checksum: String::new(),
        };
        stamp_multipart_create_proof("ProposeCreateMultipartUploadIdempotent", &mut report);
        Ok(report)
    }

}
