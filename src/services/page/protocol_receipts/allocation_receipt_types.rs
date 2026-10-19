#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkAllocationReceipt {
    pub allocation: ChunkAllocation,
    pub idempotency_key: String,
    pub released: bool,
    pub deduped: bool,
}

impl ChunkAllocationReceipt {
    pub fn proof_format(&self) -> &'static str {
        if self.released {
            "MXOS-BLOCK-ALLOCATION-RELEASE-RECEIPT-PROOF1"
        } else {
            "MXOS-BLOCK-ALLOCATION-RECEIPT-PROOF1"
        }
    }

    pub fn proof_checksum(&self) -> String {
        chunk_allocation_receipt_proof_checksum(self)
    }

    pub fn verify_expected_proof_checksum(
        &self,
        expected_page_allocation_receipt_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        let expected_page_allocation_receipt_proof_checksum =
            expected_page_allocation_receipt_proof_checksum.as_ref();
        if expected_page_allocation_receipt_proof_checksum.is_empty() {
            return Ok(());
        }
        let actual = self.proof_checksum();
        if actual != expected_page_allocation_receipt_proof_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "block allocation receipt proof checksum mismatch: expected {} got {}",
                expected_page_allocation_receipt_proof_checksum, actual
            )));
        }
        Ok(())
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self.allocation.to_control_fields(field_prefix);
        fields.extend(
            self.allocation
                .to_control_fields(&format!("{field_prefix}allocation.")),
        );
        fields.insert(
            format!("{field_prefix}idempotency_key"),
            self.idempotency_key.clone(),
        );
        fields.insert(format!("{field_prefix}released"), self.released.to_string());
        fields.insert(format!("{field_prefix}deduped"), self.deduped.to_string());
        fields.insert(
            format!("{field_prefix}block_allocation_receipt_proof_format"),
            self.proof_format().to_string(),
        );
        fields.insert(
            format!("{field_prefix}block_allocation_receipt_proof_checksum"),
            self.proof_checksum(),
        );
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkBatchAllocationReceipt {
    pub allocations: Vec<ChunkAllocation>,
    pub idempotency_key: String,
    pub deduped: bool,
}

impl ChunkBatchAllocationReceipt {
    pub fn proof_format(&self) -> &'static str {
        "MXOS-BLOCK-ALLOCATION-BATCH-RECEIPT-PROOF1"
    }

    pub fn proof_checksum(&self) -> String {
        page_batch_allocation_receipt_proof_checksum(self)
    }

    pub fn verify_expected_proof_checksum(
        &self,
        expected_page_batch_allocation_receipt_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        let expected_page_batch_allocation_receipt_proof_checksum =
            expected_page_batch_allocation_receipt_proof_checksum.as_ref();
        if expected_page_batch_allocation_receipt_proof_checksum.is_empty() {
            return Ok(());
        }
        let actual = self.proof_checksum();
        if actual != expected_page_batch_allocation_receipt_proof_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "block batch allocation receipt proof checksum mismatch: expected {} got {}",
                expected_page_batch_allocation_receipt_proof_checksum, actual
            )));
        }
        Ok(())
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}allocation_count"),
            self.allocations.len().to_string(),
        );
        fields.insert(
            format!("{field_prefix}idempotency_key"),
            self.idempotency_key.clone(),
        );
        fields.insert(format!("{field_prefix}deduped"), self.deduped.to_string());
        fields.insert(
            format!("{field_prefix}block_batch_allocation_receipt_proof_format"),
            self.proof_format().to_string(),
        );
        fields.insert(
            format!("{field_prefix}block_batch_allocation_receipt_proof_checksum"),
            self.proof_checksum(),
        );
        for (index, allocation) in self.allocations.iter().enumerate() {
            fields.extend(
                allocation.to_control_fields(&format!("{field_prefix}allocation.{index}.")),
            );
        }
        fields
    }
}

