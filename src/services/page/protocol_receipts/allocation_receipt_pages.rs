#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PageAllocationReceiptPage {
    pub receipts: Vec<ChunkAllocationReceipt>,
    pub receipt_count: u64,
    pub total_receipt_count: u64,
    pub scanned_count: u64,
    pub limit: u64,
    pub truncated: bool,
    pub next_marker: Option<String>,
    pub chunk_id: Option<ChunkId>,
    pub skipped_slab_count: u64,
    pub page_index: Option<u64>,
    pub page_size: Option<u64>,
    pub skipped_chunk_count: u64,
}

impl PageAllocationReceiptPage {
    pub fn proof_format(&self) -> &'static str {
        if self.receipts.iter().all(|receipt| receipt.released) && !self.receipts.is_empty() {
            "MXOS-BLOCK-ALLOCATION-RELEASE-RECEIPT-PAGE-PROOF1"
        } else {
            "MXOS-BLOCK-ALLOCATION-RECEIPT-PAGE-PROOF1"
        }
    }

    pub fn proof_checksum(&self) -> String {
        page_allocation_receipt_page_proof_checksum(self)
    }

    pub fn verify_expected_proof_checksum(
        &self,
        expected_page_allocation_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        verify_storage_receipt_page_invariants(
            "block allocation",
            self.receipt_count,
            self.total_receipt_count,
            self.scanned_count,
            self.limit,
            self.truncated,
            &self.next_marker,
            self.receipts.len(),
            self.skipped_slab_count,
        )?;
        if let Some(chunk_id) = &self.chunk_id {
            if self
                .receipts
                .iter()
                .any(|receipt| &receipt.allocation.chunk_id != chunk_id)
            {
                return Err(ObjectError::Corruption(
                    "block allocation receipt page contains receipt outside segment filter"
                        .to_string(),
                ));
            }
        }
        if let Some(page_index) = self.page_index {
            let page_size = self.page_size.ok_or_else(|| {
                ObjectError::Corruption(
                    "block allocation receipt page chunk filter is missing chunk size".to_string(),
                )
            })?;
            if self.receipts.iter().any(|receipt| {
                !allocation_overlaps_chunk(&receipt.allocation, page_index, page_size)
            }) {
                return Err(ObjectError::Corruption(
                    "block allocation receipt page contains receipt outside chunk filter"
                        .to_string(),
                ));
            }
        }
        let expected_page_allocation_receipt_page_proof_checksum =
            expected_page_allocation_receipt_page_proof_checksum.as_ref();
        if expected_page_allocation_receipt_page_proof_checksum.is_empty() {
            return Ok(());
        }
        let actual = self.proof_checksum();
        if actual != expected_page_allocation_receipt_page_proof_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "block allocation receipt page proof checksum mismatch: expected {} got {}",
                expected_page_allocation_receipt_page_proof_checksum, actual
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
            self.skipped_slab_count,
            field_prefix,
        );
        insert_page_chunk_filter_control_fields(
            &mut fields,
            field_prefix,
            self.page_index,
            self.page_size,
            self.skipped_chunk_count,
        );
        fields.insert(
            format!("{field_prefix}block_allocation_receipt_page_proof_format"),
            self.proof_format().to_string(),
        );
        fields.insert(
            format!("{field_prefix}block_allocation_receipt_page_proof_checksum"),
            self.proof_checksum(),
        );
        for (index, receipt) in self.receipts.iter().enumerate() {
            fields.extend(receipt.to_control_fields(&format!("{field_prefix}receipt.{index}.")));
        }
        fields
    }
}

pub type PageReleaseAllocationReceiptPage = PageAllocationReceiptPage;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PageBatchAllocationReceiptPage {
    pub receipts: Vec<ChunkBatchAllocationReceipt>,
    pub receipt_count: u64,
    pub total_receipt_count: u64,
    pub scanned_count: u64,
    pub limit: u64,
    pub truncated: bool,
    pub next_marker: Option<String>,
    pub chunk_id: Option<ChunkId>,
    pub skipped_slab_count: u64,
    pub page_index: Option<u64>,
    pub page_size: Option<u64>,
    pub skipped_chunk_count: u64,
}

impl PageBatchAllocationReceiptPage {
    pub fn proof_format(&self) -> &'static str {
        "MXOS-BLOCK-ALLOCATION-BATCH-RECEIPT-PAGE-PROOF1"
    }

    pub fn proof_checksum(&self) -> String {
        page_batch_allocation_receipt_page_proof_checksum(self)
    }

    pub fn verify_expected_proof_checksum(
        &self,
        expected_page_batch_allocation_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        verify_storage_receipt_page_invariants(
            "block batch allocation",
            self.receipt_count,
            self.total_receipt_count,
            self.scanned_count,
            self.limit,
            self.truncated,
            &self.next_marker,
            self.receipts.len(),
            self.skipped_slab_count,
        )?;
        if let Some(chunk_id) = &self.chunk_id {
            if self.receipts.iter().any(|receipt| {
                receipt
                    .allocations
                    .iter()
                    .any(|allocation| &allocation.chunk_id != chunk_id)
            }) {
                return Err(ObjectError::Corruption(
                    "block batch allocation receipt page contains receipt outside segment filter"
                        .to_string(),
                ));
            }
        }
        if let Some(page_index) = self.page_index {
            let page_size = self.page_size.ok_or_else(|| {
                ObjectError::Corruption(
                    "block batch allocation receipt page chunk filter is missing chunk size"
                        .to_string(),
                )
            })?;
            if self.receipts.iter().any(|receipt| {
                !receipt.allocations.iter().any(|allocation| {
                    allocation_overlaps_chunk(allocation, page_index, page_size)
                })
            }) {
                return Err(ObjectError::Corruption(
                    "block batch allocation receipt page contains receipt outside chunk filter"
                        .to_string(),
                ));
            }
        }
        let expected_page_batch_allocation_receipt_page_proof_checksum =
            expected_page_batch_allocation_receipt_page_proof_checksum.as_ref();
        if expected_page_batch_allocation_receipt_page_proof_checksum.is_empty() {
            return Ok(());
        }
        let actual = self.proof_checksum();
        if actual != expected_page_batch_allocation_receipt_page_proof_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "block batch allocation receipt page proof checksum mismatch: expected {} got {}",
                expected_page_batch_allocation_receipt_page_proof_checksum, actual
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
            self.skipped_slab_count,
            field_prefix,
        );
        insert_page_chunk_filter_control_fields(
            &mut fields,
            field_prefix,
            self.page_index,
            self.page_size,
            self.skipped_chunk_count,
        );
        fields.insert(
            format!("{field_prefix}block_batch_allocation_receipt_page_proof_format"),
            self.proof_format().to_string(),
        );
        fields.insert(
            format!("{field_prefix}block_batch_allocation_receipt_page_proof_checksum"),
            self.proof_checksum(),
        );
        for (index, receipt) in self.receipts.iter().enumerate() {
            fields.extend(receipt.to_control_fields(&format!("{field_prefix}receipt.{index}.")));
        }
        fields
    }
}

