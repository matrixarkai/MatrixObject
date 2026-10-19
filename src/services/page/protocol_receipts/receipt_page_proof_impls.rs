macro_rules! impl_page_receipt_page_proof_guard {
    ($ty:ty, $verify_method:ident, $require_method:ident) => {
        impl ProvenReceiptRead<$ty> {
            pub fn $verify_method(
                &self,
                expected_receipt_page_proof_checksum: impl AsRef<str>,
            ) -> Result<(), ObjectError> {
                self.value
                    .verify_expected_proof_checksum(expected_receipt_page_proof_checksum)
            }

            pub fn $require_method(
                self,
                expected_receipt_page_proof_checksum: impl AsRef<str>,
            ) -> Result<Self, ObjectError> {
                self.$verify_method(expected_receipt_page_proof_checksum)?;
                Ok(self)
            }
        }
    };
}

impl_page_receipt_page_proof_guard!(
    PageAllocationReceiptPage,
    verify_expected_page_allocation_receipt_page_proof_checksum,
    require_page_allocation_receipt_page_proof_checksum
);
impl_page_receipt_page_proof_guard!(
    PageBatchAllocationReceiptPage,
    verify_expected_page_batch_allocation_receipt_page_proof_checksum,
    require_page_batch_allocation_receipt_page_proof_checksum
);
impl_page_receipt_page_proof_guard!(
    PageWriteReceiptPage,
    verify_expected_page_write_receipt_page_proof_checksum,
    require_page_write_receipt_page_proof_checksum
);
impl_page_receipt_page_proof_guard!(
    PageBatchWriteReceiptPage,
    verify_expected_page_batch_write_receipt_page_proof_checksum,
    require_page_batch_write_receipt_page_proof_checksum
);
impl_page_receipt_page_proof_guard!(
    PageDiscardReceiptPage,
    verify_expected_page_discard_receipt_page_proof_checksum,
    require_page_discard_receipt_page_proof_checksum
);
impl_page_receipt_page_proof_guard!(
    PageBatchDiscardReceiptPage,
    verify_expected_page_batch_discard_receipt_page_proof_checksum,
    require_page_batch_discard_receipt_page_proof_checksum
);
impl_page_receipt_page_proof_guard!(
    PageRangeRepairReceiptPage,
    verify_expected_page_range_repair_receipt_page_proof_checksum,
    require_page_range_repair_receipt_page_proof_checksum
);
impl_page_receipt_page_proof_guard!(
    PageAuditRepairReceiptPage,
    verify_expected_page_audit_repair_receipt_page_proof_checksum,
    require_page_audit_repair_receipt_page_proof_checksum
);

macro_rules! impl_page_storage_receipt_page_source {
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

            fn storage_receipt_page_typed_proof_checksum(&self) -> String {
                self.proof_checksum()
            }
        }
    };
}

macro_rules! impl_filtered_page_storage_receipt_page_source {
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

            fn storage_receipt_page_slab_filter(&self) -> Option<&ChunkId> {
                self.chunk_id.as_ref()
            }

            fn storage_receipt_page_chunk_filter(&self) -> Option<u64> {
                self.page_index
            }

            fn storage_receipt_page_chunk_size(&self) -> Option<u64> {
                self.page_size
            }

            fn storage_receipt_page_skipped_chunk_count(&self) -> u64 {
                self.skipped_chunk_count
            }

            fn storage_receipt_page_skipped_page_count(&self) -> u64 {
                self.skipped_page_count
            }

            fn storage_receipt_page_typed_proof_checksum(&self) -> String {
                self.proof_checksum()
            }
        }
    };
}

impl_filtered_page_storage_receipt_page_source!(PageAllocationReceiptPage, "block.allocation");
impl_filtered_page_storage_receipt_page_source!(
    PageBatchAllocationReceiptPage,
    "block.batch_allocation"
);
impl_filtered_page_storage_receipt_page_source!(PageWriteReceiptPage, "block.write");
impl_page_storage_receipt_page_source!(PageBatchWriteReceiptPage, "block.batch_write");
impl_filtered_page_storage_receipt_page_source!(PageDiscardReceiptPage, "block.discard");
impl_page_storage_receipt_page_source!(PageBatchDiscardReceiptPage, "block.batch_discard");
impl_page_storage_receipt_page_source!(PageRangeRepairReceiptPage, "block.range_repair");
impl_page_storage_receipt_page_source!(PageAuditRepairReceiptPage, "block.audit_repair");

