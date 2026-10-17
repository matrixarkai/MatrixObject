#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PageStorageMutationReport {
    pub stats: PageBatchStats,
    pub route_control_proof: PageRouteControlProof,
    pub expected_route_control_checksum: String,
    pub route_control_fenced: bool,
    pub route_control_fence_matched: bool,
    pub chunk_control_proof: PageChunkControlProof,
    pub expected_chunk_control_proof_checksum: String,
    pub chunk_control_fenced: bool,
    pub chunk_control_fence_matched: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PageIdempotentAllocationReport {
    pub receipt: PageBatchAllocationReceipt,
    pub mutation: PageStorageMutationReport,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PageIdempotentAllocationReleaseReport {
    pub receipt: PageAllocationReceipt,
    pub mutation: PageStorageMutationReport,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PageIdempotentWriteReport {
    pub receipt: PageWriteReceipt,
    pub mutation: PageStorageMutationReport,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PageIdempotentBatchWriteReport {
    pub receipt: PageBatchWriteReceipt,
    pub mutation: PageStorageMutationReport,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PageIdempotentDiscardReport {
    pub receipt: PageDiscardReceipt,
    pub mutation: PageStorageMutationReport,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PageIdempotentBatchDiscardReport {
    pub receipt: PageBatchDiscardReceipt,
    pub mutation: PageStorageMutationReport,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PageRangeRepairReceipt {
    pub slab_id: SlabId,
    pub offset: u64,
    pub length: u64,
    pub max_repair_chunks: u64,
    pub start_chunk_index: u64,
    pub request_checksum: String,
    pub idempotency_key: String,
    pub report: BoundedRepairReport,
    pub deduped: bool,
}

impl PageRangeRepairReceipt {
    pub fn proof_format(&self) -> &'static str {
        "MXOS-BLOCK-RANGE-REPAIR-RECEIPT1"
    }

    pub fn proof_checksum(&self) -> String {
        page_range_repair_receipt_proof_checksum(self)
    }

    pub fn verify_expected_proof_checksum(
        &self,
        expected_page_range_repair_receipt_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        let expected_page_range_repair_receipt_proof_checksum =
            expected_page_range_repair_receipt_proof_checksum.as_ref();
        if expected_page_range_repair_receipt_proof_checksum.is_empty() {
            return Ok(());
        }
        let actual = self.proof_checksum();
        if actual != expected_page_range_repair_receipt_proof_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "block range repair receipt proof checksum mismatch: expected {} got {}",
                expected_page_range_repair_receipt_proof_checksum, actual
            )));
        }
        Ok(())
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .slab_id
            .to_control_fields(&format!("{field_prefix}segment."));
        for (name, value) in [
            ("volume_id", self.slab_id.volume_id),
            ("segment_id", self.slab_id.id),
            ("offset", self.offset),
            ("length", self.length),
            ("max_repair_chunks", self.max_repair_chunks),
            ("start_chunk_index", self.start_chunk_index),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(
            format!("{field_prefix}request_checksum"),
            self.request_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}idempotency_key"),
            self.idempotency_key.clone(),
        );
        fields.extend(
            self.report
                .to_control_fields(&format!("{field_prefix}report.")),
        );
        fields.insert(format!("{field_prefix}deduped"), self.deduped.to_string());
        fields.insert(
            format!("{field_prefix}block_range_repair_receipt_proof_format"),
            self.proof_format().to_string(),
        );
        fields.insert(
            format!("{field_prefix}block_range_repair_receipt_proof_checksum"),
            self.proof_checksum(),
        );
        fields
    }
}

