impl MatrixObjectStore {
    pub fn create_multipart_upload(
        &mut self,
        bucket: &str,
        key: &str,
        content_type: impl Into<String>,
        options: PutOptions,
    ) -> Result<MultipartUpload, ObjectError> {
        let upload_id = self.next_upload_id();
        let create_time_ms = now_millis();
        let content_type = content_type.into();
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let command = ObjectCommand::CreateMultipartUpload {
            upload_id: upload_id.clone(),
            bucket: bucket.to_string(),
            key: key.to_string(),
            content_type: content_type.clone(),
            options: options.clone(),
            create_time_ms,
        };
        self.apply_command(command)?;
        if let Err(err) = self.append_multipart_create_journal_record(
            &upload_id,
            bucket,
            key,
            &content_type,
            &options,
            create_time_ms,
        ) {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        self.multipart_upload(upload_id.as_str())
    }

    pub fn create_multipart_upload_with_route_report(
        &mut self,
        bucket: &str,
        key: &str,
        content_type: impl Into<String>,
        options: PutOptions,
    ) -> Result<MultipartCreateReport, ObjectError> {
        self.create_multipart_upload_with_route_report_fenced(
            bucket,
            key,
            content_type,
            options,
            None,
        )
    }

    pub fn create_multipart_upload_with_route_report_fenced(
        &mut self,
        bucket: &str,
        key: &str,
        content_type: impl Into<String>,
        options: PutOptions,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<MultipartCreateReport, ObjectError> {
        self.page_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        let route_control_proof = self.page_service.route_control_proof();
        let upload = self.create_multipart_upload(bucket, key, content_type, options)?;
        let mut report = MultipartCreateReport {
            upload,
            route_control_proof,
            expected_route_control_checksum: expected_route_control_checksum
                .unwrap_or_default()
                .to_string(),
            multipart_create_proof_format: String::new(),
            multipart_create_proof_checksum: String::new(),
            expected_multipart_create_proof_checksum: String::new(),
        };
        stamp_multipart_create_proof("CreateMultipartUpload", &mut report);
        Ok(report)
    }

    pub fn create_multipart_upload_with_route_report_fenced_with_create_proof(
        &mut self,
        bucket: &str,
        key: &str,
        content_type: impl Into<String>,
        options: PutOptions,
        expected_route_control_checksum: Option<&str>,
        expected_multipart_create_proof_checksum: Option<&str>,
    ) -> Result<MultipartCreateReport, ObjectError> {
        let mut report = self.create_multipart_upload_with_route_report_fenced(
            bucket,
            key,
            content_type,
            options,
            expected_route_control_checksum,
        )?;
        report.expected_multipart_create_proof_checksum = expected_multipart_create_proof_checksum
            .unwrap_or_default()
            .to_string();
        check_expected_multipart_create_proof_checksum(
            "CreateMultipartUpload",
            &report.multipart_create_proof_checksum,
            expected_multipart_create_proof_checksum,
        )?;
        Ok(report)
    }

}
