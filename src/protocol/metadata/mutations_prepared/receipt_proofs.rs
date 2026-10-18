#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataReadCondition {
    pub key: String,
    pub expected_version: u64,
    pub require_present: bool,
    pub require_deleted: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetadataReadSetReport {
    pub checked_records: u64,
    pub matched_records: u64,
    pub missing_records: u64,
    pub deleted_records: u64,
    pub max_version: u64,
    pub records: Vec<MetadataRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataMutation {
    pub key: SharedText,
    pub value: String,
    pub expected_version: u64,
    pub delete: bool,
}

impl MetadataMutation {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(format!("{field_prefix}key"), self.key.to_string());
        fields.insert(format!("{field_prefix}value"), self.value.clone().into());
        fields.insert(
            format!("{field_prefix}expected_version"),
            self.expected_version.to_string().into(),
        );
        fields.insert(format!("{field_prefix}delete"), self.delete.to_string().into());
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataBatchReceipt {
    pub idempotency_key: String,
    pub request_checksum: String,
    pub owner_fenced: bool,
    pub expected_owner: String,
    pub expected_epoch: u64,
    pub lease_owner: Option<String>,
    pub lease_epoch: Option<u64>,
    pub now_ms: u64,
    pub records: Vec<MetadataRecord>,
    pub first_commit_index: u64,
    pub last_commit_index: u64,
    pub deduped: bool,
}

impl MetadataBatchReceipt {
    pub fn proof_format(&self) -> &'static str {
        "MXOS-META-BATCH-RECEIPT-PROOF1"
    }

    pub fn proof_checksum(&self) -> String {
        metadata_batch_receipt_proof_checksum(self)
    }

    pub fn verify_expected_proof_checksum(
        &self,
        expected_metadata_batch_receipt_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        let expected_metadata_batch_receipt_proof_checksum =
            expected_metadata_batch_receipt_proof_checksum.as_ref();
        if expected_metadata_batch_receipt_proof_checksum.is_empty() {
            return Ok(());
        }
        let actual = self.proof_checksum();
        if actual != expected_metadata_batch_receipt_proof_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata batch receipt proof checksum mismatch: expected {} got {}",
                expected_metadata_batch_receipt_proof_checksum, actual
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetadataBatchReceiptPage {
    pub receipts: Vec<MetadataBatchReceipt>,
    pub marker: Option<String>,
    pub next_marker: Option<String>,
    pub owner_fenced: Option<bool>,
    pub expected_owner: Option<String>,
    pub expected_epoch: Option<u64>,
    pub lease_owner: Option<String>,
    pub record_key_prefix: Option<String>,
    pub receipt_count: u64,
    pub total_receipt_count: u64,
    pub scanned_count: u64,
    pub matched_count: u64,
    pub skipped_owner_fence_count: u64,
    pub skipped_expected_owner_count: u64,
    pub skipped_expected_epoch_count: u64,
    pub skipped_lease_owner_count: u64,
    pub skipped_record_prefix_count: u64,
    pub payload_bytes: u64,
    pub max_payload_bytes: u64,
    pub budget_limited: bool,
    pub truncated: bool,
}

impl MetadataBatchReceiptPage {
    pub fn proof_format(&self) -> &'static str {
        "MXOS-META-BATCH-RECEIPT-PAGE-PROOF1"
    }

    pub fn proof_checksum(&self) -> String {
        metadata_batch_receipt_page_proof_checksum(self)
    }

    pub fn verify_expected_proof_checksum(
        &self,
        expected_metadata_batch_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        let expected_metadata_batch_receipt_page_proof_checksum =
            expected_metadata_batch_receipt_page_proof_checksum.as_ref();
        if expected_metadata_batch_receipt_page_proof_checksum.is_empty() {
            return Ok(());
        }
        let actual = self.proof_checksum();
        if actual != expected_metadata_batch_receipt_page_proof_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata batch receipt page proof checksum mismatch: expected {} got {}",
                expected_metadata_batch_receipt_page_proof_checksum, actual
            )));
        }
        Ok(())
    }
}

impl ProvenReceiptRead<MetadataBatchReceiptPage> {
    pub fn verify_expected_batch_receipt_page_proof_checksum(
        &self,
        expected_metadata_batch_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        self.value
            .verify_expected_proof_checksum(expected_metadata_batch_receipt_page_proof_checksum)
    }

    pub fn require_batch_receipt_page_proof_checksum(
        self,
        expected_metadata_batch_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<Self, ObjectError> {
        self.verify_expected_batch_receipt_page_proof_checksum(
            expected_metadata_batch_receipt_page_proof_checksum,
        )?;
        Ok(self)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataMutationReceipt {
    pub audit_key: String,
    pub receipt_family: String,
    pub idempotency_key: String,
    pub method: String,
    pub request_checksum: String,
    pub commit_index: u64,
    pub update_time_ms: u64,
    pub owner_fenced: bool,
    pub expected_owner: String,
    pub expected_epoch: u64,
    pub lease_owner: Option<String>,
    pub record_keys: Vec<String>,
}

impl MetadataMutationReceipt {
    pub fn proof_format(&self) -> &'static str {
        "MXOS-META-MUTATION-RECEIPT-PROOF1"
    }

    pub fn proof_checksum(&self) -> String {
        metadata_mutation_receipt_proof_checksum(self)
    }

    pub fn verify_expected_proof_checksum(
        &self,
        expected_metadata_mutation_receipt_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        let expected_metadata_mutation_receipt_proof_checksum =
            expected_metadata_mutation_receipt_proof_checksum.as_ref();
        if expected_metadata_mutation_receipt_proof_checksum.is_empty() {
            return Ok(());
        }
        let actual = self.proof_checksum();
        if actual != expected_metadata_mutation_receipt_proof_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata mutation receipt proof checksum mismatch: expected {} got {}",
                expected_metadata_mutation_receipt_proof_checksum, actual
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetadataMutationReceiptPage {
    pub receipts: Vec<MetadataMutationReceipt>,
    pub marker: Option<String>,
    pub next_marker: Option<String>,
    pub method: Option<String>,
    pub request_checksum: Option<String>,
    pub owner_fenced: Option<bool>,
    pub record_key_prefix: Option<String>,
    pub receipt_count: u64,
    pub total_receipt_count: u64,
    pub scanned_count: u64,
    pub matched_count: u64,
    pub skipped_method_count: u64,
    pub skipped_checksum_count: u64,
    pub skipped_owner_fence_count: u64,
    pub skipped_record_prefix_count: u64,
    pub payload_bytes: u64,
    pub max_payload_bytes: u64,
    pub budget_limited: bool,
    pub truncated: bool,
}

impl MetadataMutationReceiptPage {
    pub fn proof_format(&self) -> &'static str {
        "MXOS-META-MUTATION-RECEIPT-PAGE-PROOF1"
    }

    pub fn proof_checksum(&self) -> String {
        metadata_mutation_receipt_page_proof_checksum(self)
    }

    pub fn verify_expected_proof_checksum(
        &self,
        expected_metadata_mutation_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        let expected_metadata_mutation_receipt_page_proof_checksum =
            expected_metadata_mutation_receipt_page_proof_checksum.as_ref();
        if expected_metadata_mutation_receipt_page_proof_checksum.is_empty() {
            return Ok(());
        }
        let actual = self.proof_checksum();
        if actual != expected_metadata_mutation_receipt_page_proof_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata mutation receipt page proof checksum mismatch: expected {} got {}",
                expected_metadata_mutation_receipt_page_proof_checksum, actual
            )));
        }
        Ok(())
    }
}

impl ProvenReceiptRead<MetadataMutationReceiptPage> {
    pub fn verify_expected_mutation_receipt_page_proof_checksum(
        &self,
        expected_metadata_mutation_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        self.value
            .verify_expected_proof_checksum(expected_metadata_mutation_receipt_page_proof_checksum)
    }

    pub fn require_mutation_receipt_page_proof_checksum(
        self,
        expected_metadata_mutation_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<Self, ObjectError> {
        self.verify_expected_mutation_receipt_page_proof_checksum(
            expected_metadata_mutation_receipt_page_proof_checksum,
        )?;
        Ok(self)
    }
}

