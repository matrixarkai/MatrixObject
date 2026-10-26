// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn complete_multipart_upload(
        &mut self,
        upload_id: &str,
        expected_size: Option<u64>,
        expected_etag: Option<String>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.complete_multipart_upload_with_expected_checksum(
            upload_id,
            expected_size,
            expected_etag,
            None,
        )
    }

    pub fn complete_multipart_upload_with_expected_checksum(
        &mut self,
        upload_id: &str,
        expected_size: Option<u64>,
        expected_etag: Option<String>,
        expected_checksum: Option<String>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let update_time_ms = now_millis();
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let command = ObjectCommand::CompleteMultipartUpload {
            upload_id: upload_id.to_string(),
            expected_size,
            expected_etag: expected_etag.clone(),
            expected_checksum: expected_checksum.clone(),
            update_time_ms,
        };
        let metadata = self.apply_command(command)?;
        if let Err(err) = self.append_multipart_complete_journal_record(
            upload_id,
            expected_size,
            expected_etag.as_deref(),
            expected_checksum.as_deref(),
            None,
            update_time_ms,
        ) {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(metadata)
    }

    pub fn complete_multipart_upload_with_placement(
        &mut self,
        upload_id: &str,
        expected_size: Option<u64>,
        expected_etag: Option<String>,
        expected_checksum: Option<String>,
        replica_count: usize,
    ) -> Result<ObjectMetadata, ObjectError> {
        let update_time_ms = now_millis();
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let command = ObjectCommand::CompleteMultipartUploadWithRootPlacement {
            upload_id: upload_id.to_string(),
            expected_size,
            expected_etag: expected_etag.clone(),
            expected_checksum: expected_checksum.clone(),
            replica_count,
            update_time_ms,
        };
        let metadata = self.apply_command(command)?;
        if let Err(err) = self.append_multipart_complete_journal_record(
            upload_id,
            expected_size,
            expected_etag.as_deref(),
            expected_checksum.as_deref(),
            Some(replica_count),
            update_time_ms,
        ) {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(metadata)
    }

    pub fn complete_multipart_upload_with_route_report(
        &mut self,
        upload_id: &str,
        expected_size: Option<u64>,
        expected_etag: Option<String>,
    ) -> Result<MultipartCompleteReport, ObjectError> {
        self.complete_multipart_upload_with_route_report_fenced(
            upload_id,
            expected_size,
            expected_etag,
            None,
        )
    }

    pub fn complete_multipart_upload_with_route_report_fenced(
        &mut self,
        upload_id: &str,
        expected_size: Option<u64>,
        expected_etag: Option<String>,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<MultipartCompleteReport, ObjectError> {
        self.complete_multipart_upload_with_checksum_route_report_fenced(
            upload_id,
            expected_size,
            expected_etag,
            None,
            expected_route_control_checksum,
        )
    }

    pub fn complete_multipart_upload_with_checksum_route_report_fenced(
        &mut self,
        upload_id: &str,
        expected_size: Option<u64>,
        expected_etag: Option<String>,
        expected_checksum: Option<String>,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<MultipartCompleteReport, ObjectError> {
        self.multipart_upload(upload_id)?;
        self.chunk_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        let route_control_proof = self.chunk_service.route_control_proof();
        let metadata = self.complete_multipart_upload_with_expected_checksum(
            upload_id,
            expected_size,
            expected_etag,
            expected_checksum,
        )?;
        let mut report = MultipartCompleteReport {
            metadata,
            route_control_proof,
            expected_route_control_checksum: expected_route_control_checksum
                .unwrap_or_default()
                .to_string(),
            multipart_complete_proof_format: String::new(),
            multipart_complete_proof_checksum: String::new(),
            expected_multipart_complete_proof_checksum: String::new(),
        };
        stamp_multipart_complete_proof("CompleteMultipartUpload", &mut report);
        Ok(report)
    }

    pub fn complete_multipart_upload_with_checksum_route_report_fenced_with_complete_proof(
        &mut self,
        upload_id: &str,
        expected_size: Option<u64>,
        expected_etag: Option<String>,
        expected_checksum: Option<String>,
        expected_route_control_checksum: Option<&str>,
        expected_multipart_complete_proof_checksum: Option<&str>,
    ) -> Result<MultipartCompleteReport, ObjectError> {
        let mut report = self.complete_multipart_upload_with_checksum_route_report_fenced(
            upload_id,
            expected_size,
            expected_etag,
            expected_checksum,
            expected_route_control_checksum,
        )?;
        report.expected_multipart_complete_proof_checksum =
            expected_multipart_complete_proof_checksum
                .unwrap_or_default()
                .to_string();
        check_expected_multipart_complete_proof_checksum(
            "CompleteMultipartUpload",
            &report.multipart_complete_proof_checksum,
            expected_multipart_complete_proof_checksum,
        )?;
        Ok(report)
    }

}
