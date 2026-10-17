impl MatrixObjectStore {
    pub fn batch_get_objects_conditional(
        &mut self,
        requests: Vec<ConditionalObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
    ) -> Result<ConditionalObjectBatchReadReport, ObjectError> {
        self.batch_get_objects_conditional_internal(requests, max_items, max_payload_bytes, None)
    }

    pub fn batch_get_objects_conditional_fenced(
        &mut self,
        requests: Vec<ConditionalObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ConditionalObjectBatchReadReport, ObjectError> {
        self.page_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        let mut report = self.batch_get_objects_conditional_internal(
            requests,
            max_items,
            max_payload_bytes,
            None,
        )?;
        report.expected_route_control_checksum = expected_route_control_checksum
            .unwrap_or_default()
            .to_string();
        Ok(report)
    }

    pub fn batch_get_objects_conditional_with_metadata_read(
        &mut self,
        requests: Vec<ConditionalObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        options: MetadataReadOptions,
    ) -> Result<ConditionalObjectBatchReadReport, ObjectError> {
        self.batch_get_objects_conditional_internal(
            requests,
            max_items,
            max_payload_bytes,
            Some(options),
        )
    }

    pub fn batch_get_objects_conditional_with_read_admission(
        &self,
        requests: Vec<ConditionalObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
    ) -> Result<AdmittedConditionalObjectBatchReadReport, ObjectError> {
        self.batch_get_objects_conditional_with_bounded_read_admission(
            requests,
            max_items,
            max_payload_bytes,
            0,
        )
    }

    pub fn batch_get_objects_conditional_with_bounded_read_admission(
        &self,
        requests: Vec<ConditionalObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
    ) -> Result<AdmittedConditionalObjectBatchReadReport, ObjectError> {
        self.batch_get_objects_conditional_with_read_admission_internal(
            requests,
            max_items,
            max_payload_bytes,
            max_chunk_read_count,
            None,
        )
    }

    pub fn batch_get_objects_conditional_with_read_admission_and_metadata_read(
        &self,
        requests: Vec<ConditionalObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        options: MetadataReadOptions,
    ) -> Result<AdmittedConditionalObjectBatchReadReport, ObjectError> {
        self.batch_get_objects_conditional_with_bounded_read_admission_and_metadata_read(
            requests,
            max_items,
            max_payload_bytes,
            0,
            options,
        )
    }

    pub fn batch_get_objects_conditional_with_bounded_read_admission_and_metadata_read(
        &self,
        requests: Vec<ConditionalObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        options: MetadataReadOptions,
    ) -> Result<AdmittedConditionalObjectBatchReadReport, ObjectError> {
        self.batch_get_objects_conditional_with_read_admission_internal(
            requests,
            max_items,
            max_payload_bytes,
            max_chunk_read_count,
            Some(options),
        )
    }

    pub fn batch_get_object_versions_conditional(
        &self,
        requests: Vec<VersionedConditionalObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
    ) -> Result<VersionedConditionalObjectBatchReadReport, ObjectError> {
        self.batch_get_object_versions_conditional_internal(
            requests,
            max_items,
            max_payload_bytes,
            None,
        )
    }

    pub fn batch_get_object_versions_conditional_with_metadata_read(
        &self,
        requests: Vec<VersionedConditionalObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        options: MetadataReadOptions,
    ) -> Result<VersionedConditionalObjectBatchReadReport, ObjectError> {
        self.batch_get_object_versions_conditional_internal(
            requests,
            max_items,
            max_payload_bytes,
            Some(options),
        )
    }

    pub fn batch_get_object_versions_conditional_with_read_admission(
        &self,
        requests: Vec<VersionedConditionalObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
    ) -> Result<AdmittedVersionedConditionalObjectBatchReadReport, ObjectError> {
        self.batch_get_object_versions_conditional_with_bounded_read_admission(
            requests,
            max_items,
            max_payload_bytes,
            0,
        )
    }

    pub fn batch_get_object_versions_conditional_with_bounded_read_admission(
        &self,
        requests: Vec<VersionedConditionalObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
    ) -> Result<AdmittedVersionedConditionalObjectBatchReadReport, ObjectError> {
        self.batch_get_object_versions_conditional_with_read_admission_internal(
            requests,
            max_items,
            max_payload_bytes,
            max_chunk_read_count,
            None,
        )
    }

    pub fn batch_get_object_versions_conditional_with_read_admission_and_metadata_read(
        &self,
        requests: Vec<VersionedConditionalObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        options: MetadataReadOptions,
    ) -> Result<AdmittedVersionedConditionalObjectBatchReadReport, ObjectError> {
        self.batch_get_object_versions_conditional_with_bounded_read_admission_and_metadata_read(
            requests,
            max_items,
            max_payload_bytes,
            0,
            options,
        )
    }

    pub fn batch_get_object_versions_conditional_with_bounded_read_admission_and_metadata_read(
        &self,
        requests: Vec<VersionedConditionalObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        options: MetadataReadOptions,
    ) -> Result<AdmittedVersionedConditionalObjectBatchReadReport, ObjectError> {
        self.batch_get_object_versions_conditional_with_read_admission_internal(
            requests,
            max_items,
            max_payload_bytes,
            max_chunk_read_count,
            Some(options),
        )
    }
}
