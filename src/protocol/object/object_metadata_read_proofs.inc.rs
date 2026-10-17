#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProvenMetadataRead<T> {
    pub value: T,
    pub proof: ObjectReadProof,
}

impl<T> ProvenMetadataRead<T> {
    pub fn proof_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        self.proof.to_control_fields(field_prefix)
    }
}

impl ProvenMetadataRead<AdmittedObjectData> {
    pub fn verify_object_read_storage_admission_proof(&self) -> Result<(), ObjectError> {
        self.value.verify_object_read_storage_admission_proof()
    }

    pub fn verify_expected_object_read_storage_admission_proof_checksum(
        &self,
        expected_storage_admission_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        self.value
            .verify_expected_object_read_storage_admission_proof_checksum(
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
        self.value
            .verify_storage_admission_proof_for_operation(operation)
    }

    pub fn verify_expected_storage_admission_proof_checksum_for_operation(
        &self,
        operation: &str,
        expected_storage_admission_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        self.value
            .verify_expected_storage_admission_proof_checksum_for_operation(
                operation,
                expected_storage_admission_proof_checksum,
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
}

impl ProvenMetadataRead<AdmittedConditionalObjectData> {
    pub fn verify_object_read_storage_admission_proof(&self) -> Result<(), ObjectError> {
        self.value.verify_object_read_storage_admission_proof()
    }

    pub fn verify_expected_object_read_storage_admission_proof_checksum(
        &self,
        expected_storage_admission_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        self.value
            .verify_expected_object_read_storage_admission_proof_checksum(
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
        self.value
            .verify_storage_admission_proof_for_operation(operation)
    }

    pub fn verify_expected_storage_admission_proof_checksum_for_operation(
        &self,
        operation: &str,
        expected_storage_admission_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        self.value
            .verify_expected_storage_admission_proof_checksum_for_operation(
                operation,
                expected_storage_admission_proof_checksum,
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
}
