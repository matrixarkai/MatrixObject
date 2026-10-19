#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectReadRepairReport {
    pub object: ObjectData,
    pub requested_offset: u64,
    pub requested_length: u64,
    pub payload_checksum: String,
    pub stats: ChunkReadStats,
    pub repaired_replicas: u64,
    pub failed_repairs: u64,
    pub degraded_chunks: u64,
    pub route_control_proof: ChunkRouteControlProof,
    pub expected_route_control_checksum: String,
}

impl ObjectReadRepairReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .object
            .to_control_fields(&format!("{field_prefix}object."));
        for (name, value) in [
            ("requested_offset", self.requested_offset),
            ("requested_length", self.requested_length),
            ("repaired_replicas", self.repaired_replicas),
            ("failed_repairs", self.failed_repairs),
            ("degraded_chunks", self.degraded_chunks),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(
            format!("{field_prefix}payload_checksum"),
            self.payload_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}expected_route_control_checksum"),
            self.expected_route_control_checksum.clone(),
        );
        fields.extend(
            self.stats
                .to_control_fields(&format!("{field_prefix}read_stats.")),
        );
        fields.extend(
            self.route_control_proof
                .to_control_fields(&format!("{field_prefix}route_control.")),
        );
        fields
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ObjectReadPreconditions {
    pub if_match_etags: Vec<String>,
    pub if_none_match_etags: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConditionalObjectMetadata {
    pub metadata: ObjectMetadata,
    pub not_modified: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConditionalObjectData {
    pub metadata: ObjectMetadata,
    pub data: Option<Vec<u8>>,
    pub not_modified: bool,
}

impl ConditionalObjectData {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self.metadata.to_control_fields(field_prefix);
        fields.insert(
            format!("{field_prefix}not_modified"),
            self.not_modified.to_string(),
        );
        fields.insert(
            format!("{field_prefix}payload_present"),
            self.data.is_some().to_string(),
        );
        if let Some(data) = &self.data {
            fields.insert(
                format!("{field_prefix}payload_bytes"),
                data.len().to_string(),
            );
            fields.insert(format!("{field_prefix}payload_checksum"), etag(data));
        } else {
            fields.insert(format!("{field_prefix}payload_bytes"), "0".to_string());
        }
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdmittedConditionalObjectData {
    pub object: Option<ConditionalObjectData>,
    pub admission: ObjectRangeAdmission,
}

impl AdmittedConditionalObjectData {
    pub fn verify_object_read_storage_admission_proof(&self) -> Result<(), ObjectError> {
        verify_object_storage_admission_proof_for_operations(
            "conditional admitted object read",
            &self.admission,
            &["CanReadObjectRange", "CanReadObject"],
        )
    }

    pub fn verify_expected_object_read_storage_admission_proof_checksum(
        &self,
        expected_storage_admission_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        verify_expected_object_storage_admission_proof_for_operations(
            "conditional admitted object read",
            &self.admission,
            &["CanReadObjectRange", "CanReadObject"],
            expected_storage_admission_proof_checksum,
        )
    }

    pub fn require_object_read_storage_admission_proof_checksum(
        self,
        expected_storage_admission_proof_checksum: impl AsRef<str>,
    ) -> Result<Self, ObjectError> {
        self.verify_expected_object_read_storage_admission_proof_checksum(
            expected_storage_admission_proof_checksum,
        )?;
        Ok(self)
    }

    pub fn verify_storage_admission_proof_for_operation(
        &self,
        operation: &str,
    ) -> Result<(), ObjectError> {
        if self.admission.storage_admission_proof_format != STORAGE_ADMISSION_PROOF_FORMAT {
            return Err(ObjectError::PreconditionFailed(format!(
                "{} storage admission proof format mismatch: expected {} got {}",
                operation,
                STORAGE_ADMISSION_PROOF_FORMAT,
                self.admission.storage_admission_proof_format
            )));
        }
        let actual = object_range_admission_proof_checksum(operation, &self.admission);
        if self.admission.storage_admission_proof_checksum != actual {
            return Err(ObjectError::PreconditionFailed(format!(
                "{} storage admission proof checksum mismatch: expected {} got {}",
                operation, actual, self.admission.storage_admission_proof_checksum
            )));
        }
        Ok(())
    }

    pub fn verify_expected_storage_admission_proof_checksum_for_operation(
        &self,
        operation: &str,
        expected_storage_admission_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        self.verify_storage_admission_proof_for_operation(operation)?;
        check_expected_object_storage_admission_proof_checksum(
            operation,
            &self.admission,
            Some(expected_storage_admission_proof_checksum.as_ref()),
        )
    }

    pub fn require_storage_admission_proof_checksum_for_operation(
        self,
        operation: &str,
        expected_storage_admission_proof_checksum: impl AsRef<str>,
    ) -> Result<Self, ObjectError> {
        self.verify_expected_storage_admission_proof_checksum_for_operation(
            operation,
            expected_storage_admission_proof_checksum,
        )?;
        Ok(self)
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}object_present"),
            self.object.is_some().to_string(),
        );
        if let Some(object) = &self.object {
            fields.extend(object.to_control_fields(&format!("{field_prefix}object.")));
        }
        fields.extend(
            self.admission
                .to_control_fields(&format!("{field_prefix}admission.")),
        );
        fields
    }
}
