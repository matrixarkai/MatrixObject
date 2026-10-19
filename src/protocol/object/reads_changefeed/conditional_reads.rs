#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConditionalObjectReadItem {
    pub request: ConditionalObjectReadRequest,
    pub object: Option<ConditionalObjectData>,
    pub stats: ChunkReadStats,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
}

impl ConditionalObjectReadItem {
    pub fn to_control_fields(
        &self,
        field_prefix: &str,
        payload_offset: &mut u64,
    ) -> BTreeMap<String, String> {
        let not_modified = self
            .object
            .as_ref()
            .is_some_and(|object| object.not_modified);
        let mut fields = batch_read_item_status_fields(
            self.error_code.as_deref(),
            self.error_message.as_deref(),
            self.object.is_some(),
            field_prefix,
        );
        fields.insert(
            format!("{field_prefix}not_modified"),
            not_modified.to_string(),
        );
        fields.extend(
            self.request
                .to_control_fields(&format!("{field_prefix}request.")),
        );
        fields.extend(self.stats.to_control_fields(field_prefix));
        if let Some(object) = &self.object {
            let payload_length = object
                .data
                .as_ref()
                .map(|data| data.len() as u64)
                .unwrap_or(0);
            append_payload_slice_fields(payload_length, payload_offset, field_prefix, &mut fields);
            fields.extend(object.to_control_fields(&format!("{field_prefix}info.")));
        }
        fields
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ConditionalObjectBatchReadReport {
    pub items: Vec<ConditionalObjectReadItem>,
    pub requested_objects: u64,
    pub successful_objects: u64,
    pub not_modified_objects: u64,
    pub failed_objects: u64,
    pub payload_bytes: u64,
    pub read_stats: ChunkReadStats,
    pub read_proof: ObjectReadProof,
    pub route_control_proof: ChunkRouteControlProof,
    pub expected_route_control_checksum: String,
    pub object_batch_read_proof_format: String,
    pub object_batch_read_proof_checksum: String,
    pub expected_object_batch_read_proof_checksum: String,
}

impl ConditionalObjectBatchReadReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = batch_read_report_base_fields(
            self.items.len(),
            self.requested_objects,
            self.successful_objects,
            self.not_modified_objects,
            0,
            self.failed_objects,
            self.payload_bytes,
            field_prefix,
        );
        append_route_fenced_read_control_fields(
            &self.read_proof,
            &self.route_control_proof,
            &self.expected_route_control_checksum,
            field_prefix,
            &mut fields,
        );
        fields.extend(
            self.read_stats
                .to_control_fields(&format!("{field_prefix}read_stats.")),
        );
        append_object_batch_read_proof_control_fields(
            &self.object_batch_read_proof_format,
            &self.object_batch_read_proof_checksum,
            &self.expected_object_batch_read_proof_checksum,
            field_prefix,
            &mut fields,
        );
        let mut payload_offset = 0;
        for (index, item) in self.items.iter().enumerate() {
            fields.extend(item.to_control_fields(
                &format!("{field_prefix}result.{index}."),
                &mut payload_offset,
            ));
        }
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdmittedConditionalObjectReadItem {
    pub request: ConditionalObjectReadRequest,
    pub object: Option<AdmittedConditionalObjectData>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
}

impl AdmittedConditionalObjectReadItem {
    pub fn to_control_fields(
        &self,
        field_prefix: &str,
        payload_offset: &mut u64,
    ) -> BTreeMap<String, String> {
        let not_modified = self
            .object
            .as_ref()
            .and_then(|object| object.object.as_ref())
            .is_some_and(|object| object.not_modified);
        let mut fields = batch_read_item_status_fields(
            self.error_code.as_deref(),
            self.error_message.as_deref(),
            self.object.is_some(),
            field_prefix,
        );
        fields.insert(
            format!("{field_prefix}not_modified"),
            not_modified.to_string(),
        );
        fields.extend(
            self.request
                .to_control_fields(&format!("{field_prefix}request.")),
        );
        if let Some(object) = &self.object {
            let payload_length = object
                .object
                .as_ref()
                .and_then(|object| object.data.as_ref())
                .map(|data| data.len() as u64)
                .unwrap_or(0);
            append_payload_slice_fields(payload_length, payload_offset, field_prefix, &mut fields);
            fields.extend(object.to_control_fields(field_prefix));
        }
        fields
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AdmittedConditionalObjectBatchReadReport {
    pub items: Vec<AdmittedConditionalObjectReadItem>,
    pub requested_objects: u64,
    pub successful_objects: u64,
    pub not_modified_objects: u64,
    pub admission_rejected_objects: u64,
    pub failed_objects: u64,
    pub payload_bytes: u64,
    pub max_payload_bytes: u64,
    pub reserved_chunk_read_count: u64,
    pub max_chunk_read_count: u64,
    pub budget_limited: bool,
    pub read_proof: ObjectReadProof,
    pub route_control_proof: ChunkRouteControlProof,
    pub expected_route_control_checksum: String,
    pub storage_admission_proof_format: String,
    pub storage_admission_proof_checksum: String,
    pub expected_storage_admission_proof_checksum: String,
}

impl AdmittedConditionalObjectBatchReadReport {
    pub fn verify_storage_admission_proof(&self) -> Result<(), ObjectError> {
        if self.storage_admission_proof_format != STORAGE_ADMISSION_PROOF_FORMAT {
            return Err(ObjectError::PreconditionFailed(format!(
                "conditional admitted batch object read storage admission proof format mismatch: expected {} got {}",
                STORAGE_ADMISSION_PROOF_FORMAT, self.storage_admission_proof_format
            )));
        }
        let actual = admitted_conditional_object_batch_read_proof_checksum(
            "ConditionalBatchReadObjectsWithAdmission",
            self,
        );
        if self.storage_admission_proof_checksum != actual {
            return Err(ObjectError::PreconditionFailed(format!(
                "conditional admitted batch object read storage admission proof checksum mismatch: expected {} got {}",
                actual, self.storage_admission_proof_checksum
            )));
        }
        Ok(())
    }

    pub fn verify_expected_storage_admission_proof_checksum(
        &self,
        expected_storage_admission_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        self.verify_storage_admission_proof()?;
        check_expected_admitted_conditional_object_batch_read_proof_checksum(
            "conditional admitted batch object read",
            &self.storage_admission_proof_checksum,
            Some(expected_storage_admission_proof_checksum.as_ref()),
        )
    }

    pub fn require_storage_admission_proof_checksum(
        self,
        expected_storage_admission_proof_checksum: impl AsRef<str>,
    ) -> Result<Self, ObjectError> {
        self.verify_expected_storage_admission_proof_checksum(
            expected_storage_admission_proof_checksum,
        )?;
        Ok(self)
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = admitted_batch_read_report_base_fields(
            self.items.len(),
            self.requested_objects,
            self.successful_objects,
            self.not_modified_objects,
            self.admission_rejected_objects,
            self.failed_objects,
            self.payload_bytes,
            self.max_payload_bytes,
            self.reserved_chunk_read_count,
            self.max_chunk_read_count,
            self.budget_limited,
            &self.storage_admission_proof_format,
            &self.storage_admission_proof_checksum,
            &self.expected_storage_admission_proof_checksum,
            field_prefix,
        );
        append_route_fenced_read_control_fields(
            &self.read_proof,
            &self.route_control_proof,
            &self.expected_route_control_checksum,
            field_prefix,
            &mut fields,
        );
        let mut payload_offset = 0;
        for (index, item) in self.items.iter().enumerate() {
            fields.extend(item.to_control_fields(
                &format!("{field_prefix}result.{index}."),
                &mut payload_offset,
            ));
        }
        fields
    }
}

