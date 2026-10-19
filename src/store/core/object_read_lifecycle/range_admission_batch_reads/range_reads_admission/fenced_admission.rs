// SPDX-License-Identifier: Apache-2.0
impl MatrixObjectStore {
    pub fn can_accept_object_range_read_fenced(
        &self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectRangeAdmission, ObjectError> {
        self.chunk_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        let mut report =
            self.can_accept_object_range_read(bucket, key, offset, length, max_payload_bytes)?;
        report.expected_route_control_checksum = expected_route_control_checksum
            .unwrap_or_default()
            .to_string();
        stamp_object_range_admission_proof("CanReadObjectRange", &mut report);
        Ok(report)
    }

    pub fn can_accept_object_range_read_bounded_fenced(
        &self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectRangeAdmission, ObjectError> {
        self.chunk_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        let mut report = self.can_accept_object_range_read_bounded(
            bucket,
            key,
            offset,
            length,
            max_payload_bytes,
            max_chunk_read_count,
        )?;
        report.expected_route_control_checksum = expected_route_control_checksum
            .unwrap_or_default()
            .to_string();
        stamp_object_range_admission_proof("CanReadObjectRange", &mut report);
        Ok(report)
    }

    pub fn can_accept_object_range_read_bounded_storage_fenced(
        &self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        expected_route_control_checksum: Option<&str>,
        expected_storage_admission_proof_checksum: Option<&str>,
    ) -> Result<ObjectRangeAdmission, ObjectError> {
        let report = self.can_accept_object_range_read_bounded_fenced(
            bucket,
            key,
            offset,
            length,
            max_payload_bytes,
            max_chunk_read_count,
            expected_route_control_checksum,
        )?;
        check_expected_object_storage_admission_proof_checksum(
            "object range read admission",
            &report,
            expected_storage_admission_proof_checksum,
        )?;
        Ok(report)
    }

}
