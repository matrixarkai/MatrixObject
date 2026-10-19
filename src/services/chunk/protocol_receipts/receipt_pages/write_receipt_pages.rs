#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReplicaWriteReceiptPage {
    pub receipts: Vec<ReplicaWriteReceipt>,
    pub receipt_count: u64,
    pub total_receipt_count: u64,
    pub scanned_count: u64,
    pub limit: u64,
    pub truncated: bool,
    pub next_marker: Option<String>,
    pub page_id: Option<PageId>,
    pub skipped_chunk_count: u64,
}

impl ReplicaWriteReceiptPage {
    pub fn proof_format(&self) -> &'static str {
        "MXOS-CHUNK-WRITE-RECEIPT-PAGE-PROOF1"
    }

    pub fn proof_checksum(&self) -> String {
        chunk_write_receipt_page_proof_checksum(self)
    }

    pub fn verify_expected_proof_checksum(
        &self,
        expected_chunk_write_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        verify_storage_receipt_page_invariants(
            "chunk write",
            self.receipt_count,
            self.total_receipt_count,
            self.scanned_count,
            self.limit,
            self.truncated,
            &self.next_marker,
            self.receipts.len(),
            self.skipped_chunk_count,
        )?;
        if let Some(page_id) = &self.page_id {
            if self
                .receipts
                .iter()
                .any(|receipt| &receipt.page_id != page_id)
            {
                return Err(ObjectError::Corruption(
                    "chunk write receipt page contains receipt outside chunk filter".to_string(),
                ));
            }
        }
        let expected_chunk_write_receipt_page_proof_checksum =
            expected_chunk_write_receipt_page_proof_checksum.as_ref();
        if expected_chunk_write_receipt_page_proof_checksum.is_empty() {
            return Ok(());
        }
        let actual = self.proof_checksum();
        if actual != expected_chunk_write_receipt_page_proof_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "chunk write receipt page proof checksum mismatch: expected {} got {}",
                expected_chunk_write_receipt_page_proof_checksum, actual
            )));
        }
        Ok(())
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = chunk_receipt_page_common_control_fields(
            self.receipt_count,
            self.total_receipt_count,
            self.scanned_count,
            self.limit,
            self.truncated,
            &self.next_marker,
            &self.page_id,
            self.skipped_chunk_count,
            field_prefix,
        );
        fields.insert(
            format!("{field_prefix}chunk_write_receipt_page_proof_format"),
            self.proof_format().to_string(),
        );
        fields.insert(
            format!("{field_prefix}chunk_write_receipt_page_proof_checksum"),
            self.proof_checksum(),
        );
        for (index, receipt) in self.receipts.iter().enumerate() {
            fields.extend(receipt.to_control_fields(&format!("{field_prefix}receipt.{index}.")));
        }
        fields
    }
}

impl ProvenReceiptRead<ReplicaWriteReceiptPage> {
    pub fn verify_expected_chunk_write_receipt_page_proof_checksum(
        &self,
        expected_chunk_write_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        self.value
            .verify_expected_proof_checksum(expected_chunk_write_receipt_page_proof_checksum)
    }

    pub fn require_chunk_write_receipt_page_proof_checksum(
        self,
        expected_chunk_write_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<Self, ObjectError> {
        self.verify_expected_chunk_write_receipt_page_proof_checksum(
            expected_chunk_write_receipt_page_proof_checksum,
        )?;
        Ok(self)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReplicaBatchWriteReceiptPage {
    pub receipts: Vec<ReplicaBatchWriteReceipt>,
    pub receipt_count: u64,
    pub total_receipt_count: u64,
    pub scanned_count: u64,
    pub limit: u64,
    pub truncated: bool,
    pub next_marker: Option<String>,
    pub page_id: Option<PageId>,
    pub skipped_chunk_count: u64,
}

impl ReplicaBatchWriteReceiptPage {
    pub fn proof_format(&self) -> &'static str {
        "MXOS-CHUNK-WRITE-BATCH-RECEIPT-PAGE-PROOF1"
    }

    pub fn proof_checksum(&self) -> String {
        chunk_batch_write_receipt_page_proof_checksum(self)
    }

    pub fn verify_expected_proof_checksum(
        &self,
        expected_chunk_batch_write_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        verify_storage_receipt_page_invariants(
            "chunk batch write",
            self.receipt_count,
            self.total_receipt_count,
            self.scanned_count,
            self.limit,
            self.truncated,
            &self.next_marker,
            self.receipts.len(),
            self.skipped_chunk_count,
        )?;
        if let Some(page_id) = &self.page_id {
            if self
                .receipts
                .iter()
                .any(|receipt| &receipt.page_id != page_id)
            {
                return Err(ObjectError::Corruption(
                    "chunk batch write receipt page contains receipt outside chunk filter"
                        .to_string(),
                ));
            }
        }
        let expected_chunk_batch_write_receipt_page_proof_checksum =
            expected_chunk_batch_write_receipt_page_proof_checksum.as_ref();
        if expected_chunk_batch_write_receipt_page_proof_checksum.is_empty() {
            return Ok(());
        }
        let actual = self.proof_checksum();
        if actual != expected_chunk_batch_write_receipt_page_proof_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "chunk batch write receipt page proof checksum mismatch: expected {} got {}",
                expected_chunk_batch_write_receipt_page_proof_checksum, actual
            )));
        }
        Ok(())
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = chunk_receipt_page_common_control_fields(
            self.receipt_count,
            self.total_receipt_count,
            self.scanned_count,
            self.limit,
            self.truncated,
            &self.next_marker,
            &self.page_id,
            self.skipped_chunk_count,
            field_prefix,
        );
        fields.insert(
            format!("{field_prefix}chunk_batch_write_receipt_page_proof_format"),
            self.proof_format().to_string(),
        );
        fields.insert(
            format!("{field_prefix}chunk_batch_write_receipt_page_proof_checksum"),
            self.proof_checksum(),
        );
        for (index, receipt) in self.receipts.iter().enumerate() {
            fields.extend(receipt.to_control_fields(&format!("{field_prefix}receipt.{index}.")));
        }
        fields
    }
}

impl ProvenReceiptRead<ReplicaBatchWriteReceiptPage> {
    pub fn verify_expected_chunk_batch_write_receipt_page_proof_checksum(
        &self,
        expected_chunk_batch_write_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        self.value
            .verify_expected_proof_checksum(expected_chunk_batch_write_receipt_page_proof_checksum)
    }

    pub fn require_chunk_batch_write_receipt_page_proof_checksum(
        self,
        expected_chunk_batch_write_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<Self, ObjectError> {
        self.verify_expected_chunk_batch_write_receipt_page_proof_checksum(
            expected_chunk_batch_write_receipt_page_proof_checksum,
        )?;
        Ok(self)
    }
}
