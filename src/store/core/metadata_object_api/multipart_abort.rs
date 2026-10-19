// SPDX-License-Identifier: Apache-2.0
impl MatrixObjectStore {
    pub fn abort_multipart_upload(
        &mut self,
        upload_id: &str,
    ) -> Result<MultipartUpload, ObjectError> {
        let upload = self.multipart_upload(upload_id)?;
        let update_time_ms = now_millis();
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let command = ObjectCommand::AbortMultipartUpload {
            upload_id: upload_id.to_string(),
            update_time_ms,
        };
        self.apply_command(command)?;
        if let Err(err) = self.append_multipart_abort_journal_record(upload_id, update_time_ms) {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(upload)
    }

    pub fn abort_multipart_upload_with_route_report(
        &mut self,
        upload_id: &str,
    ) -> Result<MultipartAbortReport, ObjectError> {
        self.abort_multipart_upload_with_route_report_fenced(upload_id, None)
    }

    pub fn abort_multipart_upload_with_route_report_fenced(
        &mut self,
        upload_id: &str,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<MultipartAbortReport, ObjectError> {
        self.multipart_upload(upload_id)?;
        self.chunk_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        let route_control_proof = self.chunk_service.route_control_proof();
        let upload = self.abort_multipart_upload(upload_id)?;
        let mut report = MultipartAbortReport {
            upload,
            route_control_proof,
            expected_route_control_checksum: expected_route_control_checksum
                .unwrap_or_default()
                .to_string(),
            multipart_abort_proof_format: String::new(),
            multipart_abort_proof_checksum: String::new(),
            expected_multipart_abort_proof_checksum: String::new(),
        };
        stamp_multipart_abort_proof("AbortMultipartUpload", &mut report);
        Ok(report)
    }

    pub fn abort_multipart_upload_with_route_report_fenced_with_abort_proof(
        &mut self,
        upload_id: &str,
        expected_route_control_checksum: Option<&str>,
        expected_multipart_abort_proof_checksum: Option<&str>,
    ) -> Result<MultipartAbortReport, ObjectError> {
        let mut report = self.abort_multipart_upload_with_route_report_fenced(
            upload_id,
            expected_route_control_checksum,
        )?;
        report.expected_multipart_abort_proof_checksum = expected_multipart_abort_proof_checksum
            .unwrap_or_default()
            .to_string();
        check_expected_multipart_abort_proof_checksum(
            "AbortMultipartUpload",
            &report.multipart_abort_proof_checksum,
            expected_multipart_abort_proof_checksum,
        )?;
        Ok(report)
    }

}
