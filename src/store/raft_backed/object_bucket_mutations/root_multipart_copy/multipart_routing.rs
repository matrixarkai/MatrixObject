// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_create_multipart_upload_idempotent_from_meta_shard(
        &mut self,
        upload_id: impl Into<String>,
        bucket: &str,
        key: &str,
        content_type: impl Into<String>,
        options: PutOptions,
        create_time_ms: u64,
        idempotency_key: &str,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<MultipartCreateReport, ObjectError> {
        self.check_object_meta_shard_owner_serving(
            bucket,
            key,
            expected_shard_owner,
            expected_shard_epoch,
            create_time_ms,
        )?;
        self.store.ensure_metadata_mutation_fence(
            "multipart create idempotent from meta shard",
            min_read_index.max(options.min_read_index),
            options.min_commit_index,
        )?;
        self.propose_create_multipart_upload_idempotent_with_route_report_linearizable_fenced(
            upload_id,
            bucket,
            key,
            content_type,
            options,
            create_time_ms,
            idempotency_key,
            expected_page_route_control_checksum,
        )
    }

    pub fn propose_create_multipart_upload_with_route_report_linearizable_fenced(
        &mut self,
        upload_id: impl Into<String>,
        bucket: &str,
        key: &str,
        content_type: impl Into<String>,
        options: PutOptions,
        create_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<MultipartCreateReport, ObjectError> {
        self.ensure_page_route_control_ready(
            "multipart create",
            expected_page_route_control_checksum.as_ref(),
        )?;
        let route_control_proof = self.store.chunk_service.route_control_proof();
        let upload_id = upload_id.into();
        let command = ObjectCommand::CreateMultipartUpload {
            upload_id: upload_id.clone(),
            bucket: bucket.to_string(),
            key: key.to_string(),
            content_type: content_type.into(),
            options,
            create_time_ms,
        };
        let entry = self.raft.append(command)?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::PreconditionFailed(
                "multipart create raft index is not contiguous".to_string(),
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
        stamp_multipart_create_proof("ProposeCreateMultipartUpload", &mut report);
        Ok(report)
    }

    pub fn propose_create_multipart_upload_with_route_report_linearizable_fenced_with_create_proof(
        &mut self,
        upload_id: impl Into<String>,
        bucket: &str,
        key: &str,
        content_type: impl Into<String>,
        options: PutOptions,
        create_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_multipart_create_proof_checksum: Option<&str>,
    ) -> Result<MultipartCreateReport, ObjectError> {
        let mut report = self
            .propose_create_multipart_upload_with_route_report_linearizable_fenced(
                upload_id,
                bucket,
                key,
                content_type,
                options,
                create_time_ms,
                expected_page_route_control_checksum,
            )?;
        report.expected_multipart_create_proof_checksum = expected_multipart_create_proof_checksum
            .unwrap_or_default()
            .to_string();
        check_expected_multipart_create_proof_checksum(
            "ProposeCreateMultipartUpload",
            &report.multipart_create_proof_checksum,
            expected_multipart_create_proof_checksum,
        )?;
        Ok(report)
    }

    pub fn propose_create_multipart_upload_with_route_report_and_read_proof_linearizable_fenced(
        &mut self,
        upload_id: impl Into<String>,
        bucket: &str,
        key: &str,
        content_type: impl Into<String>,
        options: PutOptions,
        create_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenMetadataRead<MultipartCreateReport>, ObjectError> {
        self.propose_create_multipart_upload_with_route_report_and_read_proof_linearizable_fenced_committed_at(
            upload_id,
            bucket,
            key,
            content_type,
            options.clone(),
            create_time_ms,
            expected_page_route_control_checksum,
            self.raft.commit_index().max(options.min_read_index),
            options.min_commit_index,
        )
    }

    pub fn propose_create_multipart_upload_with_route_report_and_read_proof_linearizable_fenced_committed_at(
        &mut self,
        upload_id: impl Into<String>,
        bucket: &str,
        key: &str,
        content_type: impl Into<String>,
        options: PutOptions,
        create_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<MultipartCreateReport>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "multipart create",
            create_time_ms,
            min_read_index,
            min_commit_index,
        )?;
        let report = self.propose_create_multipart_upload_with_route_report_linearizable_fenced(
            upload_id,
            bucket,
            key,
            content_type,
            options,
            create_time_ms,
            expected_page_route_control_checksum,
        )?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "multipart create post-commit",
            create_time_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        Ok(proven_metadata_read(report, &readiness))
    }

    pub fn propose_create_multipart_upload_from_meta_shard(
        &mut self,
        upload_id: impl Into<String>,
        bucket: &str,
        key: &str,
        content_type: impl Into<String>,
        options: PutOptions,
        create_time_ms: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<MultipartCreateReport, ObjectError> {
        self.check_object_meta_shard_owner_serving(
            bucket,
            key,
            expected_shard_owner,
            expected_shard_epoch,
            create_time_ms,
        )?;
        self.store.ensure_metadata_mutation_fence(
            "multipart create from meta shard",
            min_read_index.max(options.min_read_index),
            options.min_commit_index,
        )?;
        self.propose_create_multipart_upload_with_route_report_linearizable_fenced(
            upload_id,
            bucket,
            key,
            content_type,
            options,
            create_time_ms,
            expected_page_route_control_checksum,
        )
    }

    pub fn propose_create_multipart_upload_with_read_proof_from_meta_shard(
        &mut self,
        upload_id: impl Into<String>,
        bucket: &str,
        key: &str,
        content_type: impl Into<String>,
        options: PutOptions,
        create_time_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenMetadataRead<MultipartCreateReport>, ObjectError> {
        self.check_object_meta_shard_owner_serving(
            bucket,
            key,
            expected_shard_owner,
            expected_shard_epoch,
            create_time_ms,
        )?;
        self.propose_create_multipart_upload_with_route_report_and_read_proof_linearizable_fenced_committed_at(
            upload_id,
            bucket,
            key,
            content_type,
            options,
            create_time_ms,
            expected_page_route_control_checksum,
            min_read_index,
            min_commit_index,
        )
    }

}
