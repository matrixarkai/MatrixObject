#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectData {
    pub metadata: ObjectMetadata,
    pub data: Vec<u8>,
}

impl ObjectData {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self.metadata.to_control_fields(field_prefix);
        fields.insert(
            format!("{field_prefix}payload_bytes"),
            self.data.len().to_string(),
        );
        fields.insert(format!("{field_prefix}payload_checksum"), etag(&self.data));
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdmittedObjectData {
    pub object: Option<ObjectData>,
    pub admission: ObjectRangeAdmission,
}

impl AdmittedObjectData {
    pub fn verify_object_read_storage_admission_proof(&self) -> Result<(), ObjectError> {
        verify_object_storage_admission_proof_for_operations(
            "admitted object read",
            &self.admission,
            &["CanReadObjectRange", "CanReadObject"],
        )
    }

    pub fn verify_expected_object_read_storage_admission_proof_checksum(
        &self,
        expected_storage_admission_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        verify_expected_object_storage_admission_proof_for_operations(
            "admitted object read",
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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ObjectReadProof {
    pub requested_read_index: u64,
    pub consensus_read_index: u64,
    pub metadata_commit_index: u64,
    pub read_fence_read_index_lag: u64,
    pub read_fence_applied_lag: u64,
}

impl ObjectReadProof {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}requested_read_index"),
            self.requested_read_index.to_string(),
        );
        fields.insert(
            format!("{field_prefix}consensus_read_index"),
            self.consensus_read_index.to_string(),
        );
        fields.insert(
            format!("{field_prefix}metadata_commit_index"),
            self.metadata_commit_index.to_string(),
        );
        fields.insert(
            format!("{field_prefix}read_fence_read_index_lag"),
            self.read_fence_read_index_lag.to_string(),
        );
        fields.insert(
            format!("{field_prefix}read_fence_applied_lag"),
            self.read_fence_applied_lag.to_string(),
        );
        fields
    }
}

