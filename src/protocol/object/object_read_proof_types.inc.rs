#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ObjectCheckpointFlushReport {
    pub metadata_commit_index_before: u64,
    pub metadata_commit_index_after: u64,
    pub metadata_change_checkpoint_lag_before: u64,
    pub metadata_change_checkpoint_lag_after: u64,
    pub object_manifest_checkpoint_index_before: u64,
    pub object_manifest_checkpoint_index_after: u64,
    pub object_metadata_checkpoint_index_before: u64,
    pub object_metadata_checkpoint_index_after: u64,
    pub object_manifest_checkpoint_lag_before: u64,
    pub object_manifest_checkpoint_lag_after: u64,
    pub object_metadata_checkpoint_lag_before: u64,
    pub object_metadata_checkpoint_lag_after: u64,
    pub read_proof: ObjectReadProof,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProvenObjectMetadata {
    pub metadata: ObjectMetadata,
    pub proof: ObjectReadProof,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProvenConditionalObjectMetadata {
    pub metadata: ConditionalObjectMetadata,
    pub proof: ObjectReadProof,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProvenObjectData {
    pub object: ObjectData,
    pub proof: ObjectReadProof,
}

impl ProvenObjectData {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self.object.to_control_fields(field_prefix);
        fields.extend(self.proof.to_control_fields(field_prefix));
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProvenConditionalObjectData {
    pub object: ConditionalObjectData,
    pub proof: ObjectReadProof,
}

impl ProvenConditionalObjectData {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self.object.to_control_fields(field_prefix);
        fields.extend(self.proof.to_control_fields(field_prefix));
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProvenObjectMetadataBatchRead {
    pub batch: ObjectMetadataBatchRead,
    pub proof: ObjectReadProof,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProvenConditionalObjectMetadataBatchRead {
    pub batch: ConditionalObjectMetadataBatchRead,
    pub proof: ObjectReadProof,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProvenObjectListPage {
    pub page: ObjectListPage,
    pub proof: ObjectReadProof,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProvenObjectVersionPage {
    pub page: ObjectVersionPage,
    pub proof: ObjectReadProof,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProvenBucketObjectVersionPage {
    pub page: BucketObjectVersionPage,
    pub proof: ObjectReadProof,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProvenObjectTagPage {
    pub page: ObjectTagPage,
    pub proof: ObjectReadProof,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProvenMultipartUploadPage {
    pub page: MultipartUploadPage,
    pub proof: ObjectReadProof,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProvenMultipartPartPage {
    pub page: MultipartPartPage,
    pub proof: ObjectReadProof,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProvenObjectBatchReadReport {
    pub report: ObjectBatchReadReport,
    pub proof: ObjectReadProof,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProvenAdmittedObjectBatchReadReport {
    pub report: AdmittedObjectBatchReadReport,
    pub proof: ObjectReadProof,
}

impl ProvenAdmittedObjectBatchReadReport {
    pub fn verify_storage_admission_proof(&self) -> Result<(), ObjectError> {
        self.report.verify_storage_admission_proof()
    }

    pub fn verify_expected_storage_admission_proof_checksum(
        &self,
        expected_storage_admission_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        self.report
            .verify_expected_storage_admission_proof_checksum(
                expected_storage_admission_proof_checksum,
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
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProvenAdmittedConditionalObjectBatchReadReport {
    pub report: AdmittedConditionalObjectBatchReadReport,
    pub proof: ObjectReadProof,
}

impl ProvenAdmittedConditionalObjectBatchReadReport {
    pub fn verify_storage_admission_proof(&self) -> Result<(), ObjectError> {
        self.report.verify_storage_admission_proof()
    }

    pub fn verify_expected_storage_admission_proof_checksum(
        &self,
        expected_storage_admission_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        self.report
            .verify_expected_storage_admission_proof_checksum(
                expected_storage_admission_proof_checksum,
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
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProvenVersionedConditionalObjectBatchReadReport {
    pub report: VersionedConditionalObjectBatchReadReport,
    pub proof: ObjectReadProof,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProvenAdmittedVersionedConditionalObjectBatchReadReport {
    pub report: AdmittedVersionedConditionalObjectBatchReadReport,
    pub proof: ObjectReadProof,
}

impl ProvenAdmittedVersionedConditionalObjectBatchReadReport {
    pub fn verify_storage_admission_proof(&self) -> Result<(), ObjectError> {
        self.report.verify_storage_admission_proof()
    }

    pub fn verify_expected_storage_admission_proof_checksum(
        &self,
        expected_storage_admission_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        self.report
            .verify_expected_storage_admission_proof_checksum(
                expected_storage_admission_proof_checksum,
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
}
