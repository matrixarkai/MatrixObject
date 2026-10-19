#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PageRangeRepairReceiptPage {
    pub receipts: Vec<ChunkRangeRepairReceipt>,
    pub receipt_count: u64,
    pub total_receipt_count: u64,
    pub scanned_count: u64,
    pub limit: u64,
    pub truncated: bool,
    pub next_marker: Option<String>,
    pub chunk_id: Option<ChunkId>,
    pub skipped_chunk_count: u64,
}

impl PageRangeRepairReceiptPage {
    pub fn proof_format(&self) -> &'static str {
        "MXOS-BLOCK-RANGE-REPAIR-RECEIPT-PAGE-PROOF1"
    }

    pub fn proof_checksum(&self) -> String {
        page_range_repair_receipt_page_proof_checksum(self)
    }

    pub fn verify_expected_proof_checksum(
        &self,
        expected_page_range_repair_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        verify_storage_receipt_page_invariants(
            "block range repair",
            self.receipt_count,
            self.total_receipt_count,
            self.scanned_count,
            self.limit,
            self.truncated,
            &self.next_marker,
            self.receipts.len(),
            self.skipped_chunk_count,
        )?;
        if let Some(chunk_id) = &self.chunk_id {
            if self
                .receipts
                .iter()
                .any(|receipt| &receipt.chunk_id != chunk_id)
            {
                return Err(ObjectError::Corruption(
                    "block range repair receipt page contains receipt outside segment filter"
                        .to_string(),
                ));
            }
        }
        let expected_page_range_repair_receipt_page_proof_checksum =
            expected_page_range_repair_receipt_page_proof_checksum.as_ref();
        if expected_page_range_repair_receipt_page_proof_checksum.is_empty() {
            return Ok(());
        }
        let actual = self.proof_checksum();
        if actual != expected_page_range_repair_receipt_page_proof_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "block range repair receipt page proof checksum mismatch: expected {} got {}",
                expected_page_range_repair_receipt_page_proof_checksum, actual
            )));
        }
        Ok(())
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = page_receipt_page_common_control_fields(
            self.receipt_count,
            self.total_receipt_count,
            self.scanned_count,
            self.limit,
            self.truncated,
            &self.next_marker,
            self.chunk_id.as_ref(),
            self.skipped_chunk_count,
            field_prefix,
        );
        fields.insert(
            format!("{field_prefix}block_range_repair_receipt_page_proof_format"),
            self.proof_format().to_string(),
        );
        fields.insert(
            format!("{field_prefix}block_range_repair_receipt_page_proof_checksum"),
            self.proof_checksum(),
        );
        for (index, receipt) in self.receipts.iter().enumerate() {
            fields.extend(receipt.to_control_fields(&format!("{field_prefix}receipt.{index}.")));
        }
        fields
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PageAuditRepairReceiptPage {
    pub receipts: Vec<ChunkAuditRepairReceipt>,
    pub receipt_count: u64,
    pub total_receipt_count: u64,
    pub scanned_count: u64,
    pub limit: u64,
    pub truncated: bool,
    pub next_marker: Option<String>,
    pub after_chunk_id: Option<PageId>,
    pub skipped_after_chunk_count: u64,
}

impl PageAuditRepairReceiptPage {
    pub fn proof_format(&self) -> &'static str {
        "MXOS-BLOCK-AUDIT-REPAIR-RECEIPT-PAGE-PROOF1"
    }

    pub fn proof_checksum(&self) -> String {
        page_audit_repair_receipt_page_proof_checksum(self)
    }

    pub fn verify_expected_proof_checksum(
        &self,
        expected_page_audit_repair_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        verify_storage_receipt_page_invariants(
            "block audit repair",
            self.receipt_count,
            self.total_receipt_count,
            self.scanned_count,
            self.limit,
            self.truncated,
            &self.next_marker,
            self.receipts.len(),
            self.skipped_after_chunk_count,
        )?;
        if let Some(after_chunk_id) = &self.after_chunk_id {
            if self
                .receipts
                .iter()
                .any(|receipt| receipt.after_chunk_id.as_ref() != Some(after_chunk_id))
            {
                return Err(ObjectError::Corruption(
                    "block audit repair receipt page contains receipt outside after-chunk filter"
                        .to_string(),
                ));
            }
        }
        let expected_page_audit_repair_receipt_page_proof_checksum =
            expected_page_audit_repair_receipt_page_proof_checksum.as_ref();
        if expected_page_audit_repair_receipt_page_proof_checksum.is_empty() {
            return Ok(());
        }
        let actual = self.proof_checksum();
        if actual != expected_page_audit_repair_receipt_page_proof_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "block audit repair receipt page proof checksum mismatch: expected {} got {}",
                expected_page_audit_repair_receipt_page_proof_checksum, actual
            )));
        }
        Ok(())
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = page_audit_repair_receipt_page_common_control_fields(
            self.receipt_count,
            self.total_receipt_count,
            self.scanned_count,
            self.limit,
            self.truncated,
            &self.next_marker,
            &self.after_chunk_id,
            self.skipped_after_chunk_count,
            field_prefix,
        );
        fields.insert(
            format!("{field_prefix}block_audit_repair_receipt_page_proof_format"),
            self.proof_format().to_string(),
        );
        fields.insert(
            format!("{field_prefix}block_audit_repair_receipt_page_proof_checksum"),
            self.proof_checksum(),
        );
        for (index, receipt) in self.receipts.iter().enumerate() {
            fields.extend(receipt.to_control_fields(&format!("{field_prefix}receipt.{index}.")));
        }
        fields
    }
}

