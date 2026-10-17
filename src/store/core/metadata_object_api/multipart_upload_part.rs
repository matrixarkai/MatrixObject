impl MatrixObjectStore {
    pub fn upload_part(
        &mut self,
        upload_id: &str,
        part_number: u32,
        data: Vec<u8>,
    ) -> Result<MultipartPart, ObjectError> {
        let update_time_ms = now_millis();
        if self.persistent_journal_path.is_empty() {
            self.apply_command(ObjectCommand::UploadPart {
                upload_id: upload_id.to_string(),
                part_number,
                data,
                update_time_ms,
            })?;
            return self
                .multipart_upload(upload_id)?
                .parts
                .get(&part_number)
                .cloned()
                .ok_or_else(|| ObjectError::NotFound("multipart part missing".to_string()));
        }
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let command = ObjectCommand::UploadPart {
            upload_id: upload_id.to_string(),
            part_number,
            data: data.clone(),
            update_time_ms,
        };
        self.apply_command(command)?;
        if let Err(err) = self.append_multipart_upload_part_journal_record(
            upload_id,
            part_number,
            &data,
            update_time_ms,
        ) {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        self.multipart_upload(upload_id)?
            .parts
            .get(&part_number)
            .cloned()
            .ok_or_else(|| ObjectError::NotFound("multipart part missing".to_string()))
    }

    pub fn upload_part_with_route_report(
        &mut self,
        upload_id: &str,
        part_number: u32,
        data: Vec<u8>,
    ) -> Result<MultipartPartReport, ObjectError> {
        self.upload_part_with_route_report_fenced(upload_id, part_number, data, None)
    }

    pub fn upload_part_with_route_report_fenced(
        &mut self,
        upload_id: &str,
        part_number: u32,
        data: Vec<u8>,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<MultipartPartReport, ObjectError> {
        self.multipart_upload(upload_id)?;
        self.page_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        let route_control_proof = self.page_service.route_control_proof();
        let part = self.upload_part(upload_id, part_number, data)?;
        let received_bytes = self.multipart_upload(upload_id)?.total_bytes;
        let mut report = MultipartPartReport {
            part,
            received_bytes,
            route_control_proof,
            expected_route_control_checksum: expected_route_control_checksum
                .unwrap_or_default()
                .to_string(),
            multipart_part_proof_format: String::new(),
            multipart_part_proof_checksum: String::new(),
            expected_multipart_part_proof_checksum: String::new(),
        };
        stamp_multipart_part_proof("UploadPart", &mut report);
        Ok(report)
    }

    pub fn upload_part_with_route_report_fenced_with_part_proof(
        &mut self,
        upload_id: &str,
        part_number: u32,
        data: Vec<u8>,
        expected_route_control_checksum: Option<&str>,
        expected_multipart_part_proof_checksum: Option<&str>,
    ) -> Result<MultipartPartReport, ObjectError> {
        let mut report = self.upload_part_with_route_report_fenced(
            upload_id,
            part_number,
            data,
            expected_route_control_checksum,
        )?;
        report.expected_multipart_part_proof_checksum = expected_multipart_part_proof_checksum
            .unwrap_or_default()
            .to_string();
        check_expected_multipart_part_proof_checksum(
            "UploadPart",
            &report.multipart_part_proof_checksum,
            expected_multipart_part_proof_checksum,
        )?;
        Ok(report)
    }

}
