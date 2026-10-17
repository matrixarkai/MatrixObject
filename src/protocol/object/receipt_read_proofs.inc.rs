#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReceiptReadProof {
    pub requested_receipt_read_index: u64,
    pub receipt_applied_index: u64,
    pub receipt_safe_read_fence: u64,
    pub receipt_read_index_lag: u64,
    pub receipt_applied_lag: u64,
    pub receipt_read_fence_covered: bool,
    pub receipt_read_proof_checksum: String,
}

impl ReceiptReadProof {
    pub fn verify_expected_checksum(
        &self,
        expected_receipt_read_proof_checksum: &str,
    ) -> Result<(), ObjectError> {
        if expected_receipt_read_proof_checksum.is_empty()
            || self.receipt_read_proof_checksum == expected_receipt_read_proof_checksum
        {
            return Ok(());
        }
        Err(ObjectError::PreconditionFailed(format!(
            "receipt read proof checksum mismatch: expected {} got {}",
            expected_receipt_read_proof_checksum, self.receipt_read_proof_checksum
        )))
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StorageReceiptPageProof {
    pub storage_receipt_page_proof_format: String,
    pub storage_receipt_page_proof_checksum: String,
}

impl StorageReceiptPageProof {
    pub fn verify_expected_checksum(
        &self,
        expected_storage_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        let expected_storage_receipt_page_proof_checksum =
            expected_storage_receipt_page_proof_checksum.as_ref();
        if expected_storage_receipt_page_proof_checksum.is_empty()
            || self.storage_receipt_page_proof_checksum
                == expected_storage_receipt_page_proof_checksum
        {
            return Ok(());
        }
        Err(ObjectError::PreconditionFailed(format!(
            "storage receipt page proof checksum mismatch: expected {} got {}",
            expected_storage_receipt_page_proof_checksum, self.storage_receipt_page_proof_checksum
        )))
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}storage_receipt_page_proof_format"),
            self.storage_receipt_page_proof_format.clone(),
        );
        fields.insert(
            format!("{field_prefix}storage_receipt_page_proof_checksum"),
            self.storage_receipt_page_proof_checksum.clone(),
        );
        fields
    }
}

pub trait StorageReceiptPageProofSource {
    fn storage_receipt_page_kind(&self) -> &'static str;
    fn storage_receipt_page_receipt_count(&self) -> u64;
    fn storage_receipt_page_total_receipt_count(&self) -> u64;
    fn storage_receipt_page_scanned_count(&self) -> u64;
    fn storage_receipt_page_limit(&self) -> u64;
    fn storage_receipt_page_truncated(&self) -> bool;
    fn storage_receipt_page_next_marker(&self) -> Option<&str>;
    fn storage_receipt_page_slab_filter(&self) -> Option<&SlabId> {
        None
    }
    fn storage_receipt_page_chunk_filter(&self) -> Option<u64> {
        None
    }
    fn storage_receipt_page_chunk_id_filter(&self) -> Option<&ChunkId> {
        None
    }
    fn storage_receipt_page_chunk_size(&self) -> Option<u64> {
        None
    }
    fn storage_receipt_page_skipped_slab_count(&self) -> u64 {
        0
    }
    fn storage_receipt_page_skipped_chunk_count(&self) -> u64 {
        0
    }
    fn storage_receipt_page_typed_proof_checksum(&self) -> String;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProvenReceiptRead<T> {
    pub value: T,
    pub proof: ReceiptReadProof,
    pub storage_page_proof: Option<StorageReceiptPageProof>,
}

impl<T> ProvenReceiptRead<T> {
    pub fn require_receipt_read_proof_checksum(
        self,
        expected_receipt_read_proof_checksum: &str,
    ) -> Result<Self, ObjectError> {
        self.proof
            .verify_expected_checksum(expected_receipt_read_proof_checksum)?;
        Ok(self)
    }
}

impl<T> ProvenReceiptRead<T>
where
    T: StorageReceiptPageProofSource,
{
    pub fn require_storage_receipt_page_proof_checksum(
        self,
        expected_storage_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<Self, ObjectError> {
        self.storage_page_proof
            .as_ref()
            .ok_or_else(|| {
                ObjectError::Corruption(
                    "storage receipt page proof is missing from proven receipt read".to_string(),
                )
            })?
            .verify_expected_checksum(expected_storage_receipt_page_proof_checksum)?;
        Ok(self)
    }
}

impl ProvenReceiptRead<MetadataTransactionReceiptPage> {
    pub fn verify_expected_transaction_receipt_page_proof_checksum(
        &self,
        expected_metadata_transaction_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        self.value.verify_expected_proof_checksum(
            expected_metadata_transaction_receipt_page_proof_checksum,
        )
    }

    pub fn require_transaction_receipt_page_proof_checksum(
        self,
        expected_metadata_transaction_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<Self, ObjectError> {
        self.verify_expected_transaction_receipt_page_proof_checksum(
            expected_metadata_transaction_receipt_page_proof_checksum,
        )?;
        Ok(self)
    }
}

