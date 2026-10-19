#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReplicaRepairReceiptPage {
    pub receipts: Vec<ReplicaRepairReceipt>,
    pub receipt_count: u64,
    pub total_receipt_count: u64,
    pub scanned_count: u64,
    pub limit: u64,
    pub truncated: bool,
    pub next_marker: Option<String>,
    pub page_id: Option<PageId>,
    pub skipped_chunk_count: u64,
}

impl ReplicaRepairReceiptPage {
    pub fn proof_format(&self) -> &'static str {
        "MXOS-CHUNK-REPAIR-RECEIPT-PAGE-PROOF1"
    }

    pub fn proof_checksum(&self) -> String {
        chunk_repair_receipt_page_proof_checksum(self)
    }

    pub fn verify_expected_proof_checksum(
        &self,
        expected_chunk_repair_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        verify_storage_receipt_page_invariants(
            "chunk repair",
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
                    "chunk repair receipt page contains receipt outside chunk filter".to_string(),
                ));
            }
        }
        let expected_chunk_repair_receipt_page_proof_checksum =
            expected_chunk_repair_receipt_page_proof_checksum.as_ref();
        if expected_chunk_repair_receipt_page_proof_checksum.is_empty() {
            return Ok(());
        }
        let actual = self.proof_checksum();
        if actual != expected_chunk_repair_receipt_page_proof_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "chunk repair receipt page proof checksum mismatch: expected {} got {}",
                expected_chunk_repair_receipt_page_proof_checksum, actual
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
            format!("{field_prefix}chunk_repair_receipt_page_proof_format"),
            self.proof_format().to_string(),
        );
        fields.insert(
            format!("{field_prefix}chunk_repair_receipt_page_proof_checksum"),
            self.proof_checksum(),
        );
        for (index, receipt) in self.receipts.iter().enumerate() {
            fields.extend(receipt.to_control_fields(&format!("{field_prefix}receipt.{index}.")));
        }
        fields
    }
}

impl ProvenReceiptRead<ReplicaRepairReceiptPage> {
    pub fn verify_expected_chunk_repair_receipt_page_proof_checksum(
        &self,
        expected_chunk_repair_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        self.value
            .verify_expected_proof_checksum(expected_chunk_repair_receipt_page_proof_checksum)
    }

    pub fn require_chunk_repair_receipt_page_proof_checksum(
        self,
        expected_chunk_repair_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<Self, ObjectError> {
        self.verify_expected_chunk_repair_receipt_page_proof_checksum(
            expected_chunk_repair_receipt_page_proof_checksum,
        )?;
        Ok(self)
    }
}

macro_rules! impl_chunk_storage_receipt_page_source {
    ($ty:ty, $kind:expr) => {
        impl StorageReceiptPageProofSource for $ty {
            fn storage_receipt_page_kind(&self) -> &'static str {
                $kind
            }

            fn storage_receipt_page_receipt_count(&self) -> u64 {
                self.receipt_count
            }

            fn storage_receipt_page_total_receipt_count(&self) -> u64 {
                self.total_receipt_count
            }

            fn storage_receipt_page_scanned_count(&self) -> u64 {
                self.scanned_count
            }

            fn storage_receipt_page_limit(&self) -> u64 {
                self.limit
            }

            fn storage_receipt_page_truncated(&self) -> bool {
                self.truncated
            }

            fn storage_receipt_page_next_marker(&self) -> Option<&str> {
                self.next_marker.as_deref()
            }

            fn storage_receipt_page_chunk_id_filter(&self) -> Option<&PageId> {
                self.page_id.as_ref()
            }

            fn storage_receipt_page_skipped_chunk_count(&self) -> u64 {
                self.skipped_chunk_count
            }

            fn storage_receipt_page_typed_proof_checksum(&self) -> String {
                self.proof_checksum()
            }
        }
    };
}

impl_chunk_storage_receipt_page_source!(ReplicaWriteReceiptPage, "chunk.write");
impl_chunk_storage_receipt_page_source!(ReplicaBatchWriteReceiptPage, "chunk.batch_write");
impl_chunk_storage_receipt_page_source!(ReplicaDiscardReceiptPage, "chunk.discard");
impl_chunk_storage_receipt_page_source!(ReplicaBatchDiscardReceiptPage, "chunk.batch_discard");
impl_chunk_storage_receipt_page_source!(ReplicaRepairReceiptPage, "chunk.repair");

fn chunk_receipt_page_common_control_fields(
    receipt_count: u64,
    total_receipt_count: u64,
    scanned_count: u64,
    limit: u64,
    truncated: bool,
    next_marker: &Option<String>,
    page_id: &Option<PageId>,
    skipped_chunk_count: u64,
    field_prefix: &str,
) -> BTreeMap<String, String> {
    let mut fields = BTreeMap::new();
    fields.insert(
        format!("{field_prefix}receipt_count"),
        receipt_count.to_string(),
    );
    fields.insert(
        format!("{field_prefix}total_receipt_count"),
        total_receipt_count.to_string(),
    );
    fields.insert(
        format!("{field_prefix}scanned_count"),
        scanned_count.to_string(),
    );
    fields.insert(format!("{field_prefix}limit"), limit.to_string());
    fields.insert(format!("{field_prefix}truncated"), truncated.to_string());
    fields.insert(
        format!("{field_prefix}next_marker"),
        next_marker.clone().unwrap_or_default(),
    );
    fields.insert(
        format!("{field_prefix}chunk_filter_present"),
        page_id.is_some().to_string(),
    );
    if let Some(page_id) = page_id {
        fields.extend(page_id.to_control_fields(&format!("{field_prefix}chunk_filter.")));
    }
    fields.insert(
        format!("{field_prefix}skipped_chunk_count"),
        skipped_chunk_count.to_string(),
    );
    fields
}
