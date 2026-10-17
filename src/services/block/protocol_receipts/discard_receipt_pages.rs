#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PageDiscardReceiptPage {
    pub receipts: Vec<PageDiscardReceipt>,
    pub receipt_count: u64,
    pub total_receipt_count: u64,
    pub scanned_count: u64,
    pub limit: u64,
    pub truncated: bool,
    pub next_marker: Option<String>,
    pub slab_id: Option<SlabId>,
    pub skipped_slab_count: u64,
    pub chunk_index: Option<u64>,
    pub chunk_size: Option<u64>,
    pub skipped_chunk_count: u64,
}

impl PageDiscardReceiptPage {
    pub fn proof_format(&self) -> &'static str {
        "MXOS-BLOCK-DISCARD-RECEIPT-PAGE-PROOF1"
    }

    pub fn proof_checksum(&self) -> String {
        page_discard_receipt_page_proof_checksum(self)
    }

    pub fn verify_expected_proof_checksum(
        &self,
        expected_page_discard_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        verify_storage_receipt_page_invariants(
            "block discard",
            self.receipt_count,
            self.total_receipt_count,
            self.scanned_count,
            self.limit,
            self.truncated,
            &self.next_marker,
            self.receipts.len(),
            self.skipped_slab_count,
        )?;
        if let Some(slab_id) = &self.slab_id {
            if self
                .receipts
                .iter()
                .any(|receipt| &receipt.slab_id != slab_id)
            {
                return Err(ObjectError::Corruption(
                    "block discard receipt page contains receipt outside segment filter"
                        .to_string(),
                ));
            }
        }
        if let Some(chunk_index) = self.chunk_index {
            let chunk_size = self.chunk_size.ok_or_else(|| {
                ObjectError::Corruption(
                    "block discard receipt page chunk filter is missing chunk size".to_string(),
                )
            })?;
            if self.receipts.iter().any(|receipt| {
                !page_range_overlaps_chunk(receipt.offset, receipt.length, chunk_index, chunk_size)
            }) {
                return Err(ObjectError::Corruption(
                    "block discard receipt page contains receipt outside chunk filter".to_string(),
                ));
            }
        }
        let expected_page_discard_receipt_page_proof_checksum =
            expected_page_discard_receipt_page_proof_checksum.as_ref();
        if expected_page_discard_receipt_page_proof_checksum.is_empty() {
            return Ok(());
        }
        let actual = self.proof_checksum();
        if actual != expected_page_discard_receipt_page_proof_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "block discard receipt page proof checksum mismatch: expected {} got {}",
                expected_page_discard_receipt_page_proof_checksum, actual
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
            self.slab_id.as_ref(),
            self.skipped_slab_count,
            field_prefix,
        );
        insert_page_chunk_filter_control_fields(
            &mut fields,
            field_prefix,
            self.chunk_index,
            self.chunk_size,
            self.skipped_chunk_count,
        );
        fields.insert(
            format!("{field_prefix}block_discard_receipt_page_proof_format"),
            self.proof_format().to_string(),
        );
        fields.insert(
            format!("{field_prefix}block_discard_receipt_page_proof_checksum"),
            self.proof_checksum(),
        );
        for (index, receipt) in self.receipts.iter().enumerate() {
            fields.extend(receipt.to_control_fields(&format!("{field_prefix}receipt.{index}.")));
        }
        fields
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PageBatchDiscardReceiptPage {
    pub receipts: Vec<PageBatchDiscardReceipt>,
    pub receipt_count: u64,
    pub total_receipt_count: u64,
    pub scanned_count: u64,
    pub limit: u64,
    pub truncated: bool,
    pub next_marker: Option<String>,
}

impl PageBatchDiscardReceiptPage {
    pub fn proof_format(&self) -> &'static str {
        "MXOS-BLOCK-DISCARD-BATCH-RECEIPT-PAGE-PROOF1"
    }

    pub fn proof_checksum(&self) -> String {
        page_batch_discard_receipt_page_proof_checksum(self)
    }

    pub fn verify_expected_proof_checksum(
        &self,
        expected_page_batch_discard_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        verify_storage_receipt_page_invariants(
            "block batch discard",
            self.receipt_count,
            self.total_receipt_count,
            self.scanned_count,
            self.limit,
            self.truncated,
            &self.next_marker,
            self.receipts.len(),
            0,
        )?;
        let expected_page_batch_discard_receipt_page_proof_checksum =
            expected_page_batch_discard_receipt_page_proof_checksum.as_ref();
        if expected_page_batch_discard_receipt_page_proof_checksum.is_empty() {
            return Ok(());
        }
        let actual = self.proof_checksum();
        if actual != expected_page_batch_discard_receipt_page_proof_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "block batch discard receipt page proof checksum mismatch: expected {} got {}",
                expected_page_batch_discard_receipt_page_proof_checksum, actual
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
            None,
            0,
            field_prefix,
        );
        fields.insert(
            format!("{field_prefix}block_batch_discard_receipt_page_proof_format"),
            self.proof_format().to_string(),
        );
        fields.insert(
            format!("{field_prefix}block_batch_discard_receipt_page_proof_checksum"),
            self.proof_checksum(),
        );
        for (index, receipt) in self.receipts.iter().enumerate() {
            fields.extend(receipt.to_control_fields(&format!("{field_prefix}receipt.{index}.")));
        }
        fields
    }
}

