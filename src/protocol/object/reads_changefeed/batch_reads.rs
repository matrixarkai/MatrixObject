// SPDX-License-Identifier: Apache-2.0
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ObjectChangefeedCompactionReport {
    pub compacted_records: u64,
    pub compacted_bytes: u64,
    pub retained_records: u64,
    pub retained_bytes: u64,
    pub safe_floor_sequence: u64,
    pub cursor_count: u64,
    pub limited: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectReadRequest {
    pub bucket: String,
    pub key: String,
    pub range: Option<DataRange>,
    pub version: Option<u64>,
}

impl ObjectReadRequest {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(format!("{field_prefix}bucket"), self.bucket.clone());
        fields.insert(format!("{field_prefix}key"), self.key.clone());
        if let Some(range) = &self.range {
            fields.insert(format!("{field_prefix}offset"), range.offset.to_string());
            fields.insert(format!("{field_prefix}length"), range.length.to_string());
        }
        if let Some(version) = self.version {
            fields.insert(format!("{field_prefix}version"), version.to_string());
        }
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConditionalObjectReadRequest {
    pub bucket: String,
    pub key: String,
    pub range: Option<DataRange>,
    pub preconditions: ObjectReadPreconditions,
}

impl ConditionalObjectReadRequest {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(format!("{field_prefix}bucket"), self.bucket.clone());
        fields.insert(format!("{field_prefix}key"), self.key.clone());
        append_range_control_fields(&self.range, field_prefix, &mut fields);
        append_precondition_control_fields(&self.preconditions, field_prefix, &mut fields);
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VersionedConditionalObjectReadRequest {
    pub bucket: String,
    pub key: String,
    pub version: u64,
    pub range: Option<DataRange>,
    pub preconditions: ObjectReadPreconditions,
}

impl VersionedConditionalObjectReadRequest {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(format!("{field_prefix}bucket"), self.bucket.clone());
        fields.insert(format!("{field_prefix}key"), self.key.clone());
        fields.insert(format!("{field_prefix}version"), self.version.to_string());
        append_range_control_fields(&self.range, field_prefix, &mut fields);
        append_precondition_control_fields(&self.preconditions, field_prefix, &mut fields);
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectReadItem {
    pub request: ObjectReadRequest,
    pub object: Option<ObjectData>,
    pub stats: ChunkReadStats,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
}

impl ObjectReadItem {
    pub fn to_control_fields(
        &self,
        field_prefix: &str,
        payload_offset: &mut u64,
    ) -> BTreeMap<String, String> {
        let mut fields = batch_read_item_status_fields(
            self.error_code.as_deref(),
            self.error_message.as_deref(),
            self.object.is_some(),
            field_prefix,
        );
        fields.extend(
            self.request
                .to_control_fields(&format!("{field_prefix}request.")),
        );
        fields.extend(self.stats.to_control_fields(field_prefix));
        if let Some(object) = &self.object {
            append_payload_slice_fields(
                object.data.len() as u64,
                payload_offset,
                field_prefix,
                &mut fields,
            );
            fields.extend(object.to_control_fields(&format!("{field_prefix}info.")));
        }
        fields
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ObjectBatchReadReport {
    pub items: Vec<ObjectReadItem>,
    pub requested_objects: u64,
    pub successful_objects: u64,
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

impl ObjectBatchReadReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = batch_read_report_base_fields(
            self.items.len(),
            self.requested_objects,
            self.successful_objects,
            0,
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
pub struct AdmittedObjectReadItem {
    pub request: ObjectReadRequest,
    pub object: Option<AdmittedObjectData>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
}

impl AdmittedObjectReadItem {
    pub fn to_control_fields(
        &self,
        field_prefix: &str,
        payload_offset: &mut u64,
    ) -> BTreeMap<String, String> {
        let mut fields = batch_read_item_status_fields(
            self.error_code.as_deref(),
            self.error_message.as_deref(),
            self.object.is_some(),
            field_prefix,
        );
        fields.extend(
            self.request
                .to_control_fields(&format!("{field_prefix}request.")),
        );
        if let Some(object) = &self.object {
            let payload_length = object
                .object
                .as_ref()
                .map(|object| object.data.len() as u64)
                .unwrap_or(0);
            append_payload_slice_fields(payload_length, payload_offset, field_prefix, &mut fields);
            fields.extend(object.to_control_fields(field_prefix));
        }
        fields
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AdmittedObjectBatchReadReport {
    pub items: Vec<AdmittedObjectReadItem>,
    pub requested_objects: u64,
    pub successful_objects: u64,
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

impl AdmittedObjectBatchReadReport {
    pub fn verify_storage_admission_proof(&self) -> Result<(), ObjectError> {
        if self.storage_admission_proof_format != STORAGE_ADMISSION_PROOF_FORMAT {
            return Err(ObjectError::PreconditionFailed(format!(
                "admitted batch object read storage admission proof format mismatch: expected {} got {}",
                STORAGE_ADMISSION_PROOF_FORMAT, self.storage_admission_proof_format
            )));
        }
        let actual =
            admitted_object_batch_read_proof_checksum("BatchReadObjectsWithAdmission", self);
        if self.storage_admission_proof_checksum != actual {
            return Err(ObjectError::PreconditionFailed(format!(
                "admitted batch object read storage admission proof checksum mismatch: expected {} got {}",
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
        check_expected_admitted_object_batch_read_proof_checksum(
            "admitted batch object read",
            self,
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
            0,
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

