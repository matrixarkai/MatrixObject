// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_upload_part(
        &mut self,
        upload_id: &str,
        part_number: u32,
        data: Vec<u8>,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::UploadPart {
            upload_id: upload_id.to_string(),
            part_number,
            data,
            update_time_ms,
        })
    }

    pub fn propose_upload_part_idempotent(
        &mut self,
        upload_id: &str,
        part_number: u32,
        data: Vec<u8>,
        update_time_ms: u64,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        self.propose(ObjectCommand::UploadPartIdempotent {
            upload_id: upload_id.to_string(),
            part_number,
            data,
            update_time_ms,
            idempotency_key: idempotency_key.to_string(),
        })
    }

    pub fn propose_upload_part_with_route_report(
        &mut self,
        upload_id: &str,
        part_number: u32,
        data: Vec<u8>,
        update_time_ms: u64,
    ) -> Result<MultipartPartReport, ObjectError> {
        let expected_route_control_checksum = self
            .store
            .chunk_service
            .route_control_proof()
            .state_checksum;
        self.propose_upload_part_with_route_report_linearizable_fenced(
            upload_id,
            part_number,
            data,
            update_time_ms,
            &expected_route_control_checksum,
        )
    }

    pub fn propose_upload_part_with_route_report_linearizable_fenced(
        &mut self,
        upload_id: &str,
        part_number: u32,
        data: Vec<u8>,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<MultipartPartReport, ObjectError> {
        self.store.multipart_upload(upload_id)?;
        self.ensure_page_route_control_ready(
            "multipart upload part",
            expected_page_route_control_checksum.as_ref(),
        )?;
        let route_control_proof = self.store.chunk_service.route_control_proof();
        let command = ObjectCommand::UploadPart {
            upload_id: upload_id.to_string(),
            part_number,
            data,
            update_time_ms,
        };
        let entry = self.raft.append(command)?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::PreconditionFailed(
                "multipart upload part raft index is not contiguous".to_string(),
            ));
        }
        self.store.apply_raft_entry(&entry)?;
        let upload = self.store.multipart_upload(upload_id)?;
        let part = upload
            .parts
            .get(&part_number)
            .cloned()
            .ok_or_else(|| ObjectError::NotFound("multipart part missing".to_string()))?;
        let mut report = MultipartPartReport {
            part,
            received_bytes: upload.total_bytes,
            route_control_proof,
            expected_route_control_checksum: expected_page_route_control_checksum
                .as_ref()
                .to_string(),
            multipart_part_proof_format: String::new(),
            multipart_part_proof_checksum: String::new(),
            expected_multipart_part_proof_checksum: String::new(),
        };
        stamp_multipart_part_proof("ProposeUploadPart", &mut report);
        Ok(report)
    }

    pub fn propose_upload_part_idempotent_with_route_report_linearizable_fenced(
        &mut self,
        upload_id: &str,
        part_number: u32,
        data: Vec<u8>,
        update_time_ms: u64,
        idempotency_key: &str,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<MultipartPartReport, ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        let existing_upload = self.store.multipart_upload(upload_id)?;
        self.ensure_page_route_control_ready(
            "multipart upload part idempotent",
            expected_page_route_control_checksum.as_ref(),
        )?;
        let route_control_proof = self.store.chunk_service.route_control_proof();
        if let Some(existing) = existing_upload.parts.get(&part_number) {
            let same_payload = existing_upload
                .part_payloads
                .get(&part_number)
                .map_or(false, |payload| payload == &data);
            if existing.idempotency_key.as_deref() != Some(idempotency_key) {
                return Err(ObjectError::PreconditionFailed(
                    "multipart part idempotency key reused with different part intent".to_string(),
                ));
            }
            if !same_payload {
                return Err(ObjectError::PreconditionFailed(
                    "multipart part idempotent retry payload mismatch".to_string(),
                ));
            }
            let mut report = MultipartPartReport {
                part: existing.clone(),
                received_bytes: existing_upload.total_bytes,
                route_control_proof,
                expected_route_control_checksum: expected_page_route_control_checksum
                    .as_ref()
                    .to_string(),
                multipart_part_proof_format: String::new(),
                multipart_part_proof_checksum: String::new(),
                expected_multipart_part_proof_checksum: String::new(),
            };
            stamp_multipart_part_proof("ProposeUploadPartIdempotent", &mut report);
            return Ok(report);
        }
        if existing_upload
            .parts
            .values()
            .any(|part| part.idempotency_key.as_deref() == Some(idempotency_key))
        {
            return Err(ObjectError::PreconditionFailed(
                "multipart part idempotency key reused for a different part number".to_string(),
            ));
        }
        let command = ObjectCommand::UploadPartIdempotent {
            upload_id: upload_id.to_string(),
            part_number,
            data,
            update_time_ms,
            idempotency_key: idempotency_key.to_string(),
        };
        let entry = self.raft.append(command)?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::PreconditionFailed(
                "multipart upload part idempotent raft index is not contiguous".to_string(),
            ));
        }
        self.store.apply_raft_entry(&entry)?;
        let upload = self.store.multipart_upload(upload_id)?;
        let part = upload
            .parts
            .get(&part_number)
            .cloned()
            .ok_or_else(|| ObjectError::NotFound("multipart part missing".to_string()))?;
        let mut report = MultipartPartReport {
            part,
            received_bytes: upload.total_bytes,
            route_control_proof,
            expected_route_control_checksum: expected_page_route_control_checksum
                .as_ref()
                .to_string(),
            multipart_part_proof_format: String::new(),
            multipart_part_proof_checksum: String::new(),
            expected_multipart_part_proof_checksum: String::new(),
        };
        stamp_multipart_part_proof("ProposeUploadPartIdempotent", &mut report);
        Ok(report)
    }

    pub fn propose_upload_part_idempotent_from_meta_shard(
        &mut self,
        upload_id: &str,
        part_number: u32,
        data: Vec<u8>,
        update_time_ms: u64,
        idempotency_key: &str,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<MultipartPartReport, ObjectError> {
        self.check_multipart_meta_shard_owner_serving(
            upload_id,
            expected_shard_owner,
            expected_shard_epoch,
            update_time_ms,
        )?;
        self.store.ensure_metadata_mutation_fence(
            "multipart upload part idempotent from meta shard",
            min_read_index,
            0,
        )?;
        self.propose_upload_part_idempotent_with_route_report_linearizable_fenced(
            upload_id,
            part_number,
            data,
            update_time_ms,
            idempotency_key,
            expected_page_route_control_checksum,
        )
    }

    pub fn propose_upload_part_with_route_report_linearizable_fenced_with_part_proof(
        &mut self,
        upload_id: &str,
        part_number: u32,
        data: Vec<u8>,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_multipart_part_proof_checksum: Option<&str>,
    ) -> Result<MultipartPartReport, ObjectError> {
        let mut report = self.propose_upload_part_with_route_report_linearizable_fenced(
            upload_id,
            part_number,
            data,
            update_time_ms,
            expected_page_route_control_checksum,
        )?;
        report.expected_multipart_part_proof_checksum = expected_multipart_part_proof_checksum
            .unwrap_or_default()
            .to_string();
        check_expected_multipart_part_proof_checksum(
            "ProposeUploadPart",
            &report.multipart_part_proof_checksum,
            expected_multipart_part_proof_checksum,
        )?;
        Ok(report)
    }

    pub fn propose_upload_part_with_route_report_and_read_proof_linearizable_fenced(
        &mut self,
        upload_id: &str,
        part_number: u32,
        data: Vec<u8>,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenMetadataRead<MultipartPartReport>, ObjectError> {
        self.propose_upload_part_with_route_report_and_read_proof_linearizable_fenced_committed_at(
            upload_id,
            part_number,
            data,
            update_time_ms,
            expected_page_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_upload_part_with_route_report_and_read_proof_linearizable_fenced_committed_at(
        &mut self,
        upload_id: &str,
        part_number: u32,
        data: Vec<u8>,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<MultipartPartReport>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "multipart upload part",
            update_time_ms,
            min_read_index,
            min_commit_index,
        )?;
        let report = self.propose_upload_part_with_route_report_linearizable_fenced(
            upload_id,
            part_number,
            data,
            update_time_ms,
            expected_page_route_control_checksum,
        )?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "multipart upload part post-commit",
            update_time_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        Ok(proven_metadata_read(report, &readiness))
    }

    pub fn propose_upload_part_from_meta_shard(
        &mut self,
        upload_id: &str,
        part_number: u32,
        data: Vec<u8>,
        update_time_ms: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<MultipartPartReport, ObjectError> {
        self.check_multipart_meta_shard_owner_serving(
            upload_id,
            expected_shard_owner,
            expected_shard_epoch,
            update_time_ms,
        )?;
        self.store.ensure_metadata_mutation_fence(
            "multipart upload part from meta shard",
            min_read_index,
            0,
        )?;
        self.propose_upload_part_with_route_report_linearizable_fenced(
            upload_id,
            part_number,
            data,
            update_time_ms,
            expected_page_route_control_checksum,
        )
    }

    pub fn propose_upload_part_with_read_proof_from_meta_shard(
        &mut self,
        upload_id: &str,
        part_number: u32,
        data: Vec<u8>,
        update_time_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenMetadataRead<MultipartPartReport>, ObjectError> {
        self.check_multipart_meta_shard_owner_serving(
            upload_id,
            expected_shard_owner,
            expected_shard_epoch,
            update_time_ms,
        )?;
        self.propose_upload_part_with_route_report_and_read_proof_linearizable_fenced_committed_at(
            upload_id,
            part_number,
            data,
            update_time_ms,
            expected_page_route_control_checksum,
            min_read_index,
            min_commit_index,
        )
    }

}
