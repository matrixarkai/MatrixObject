impl MatrixPageService {
    pub fn receipt_retention(&self) -> usize {
        self.receipt_retention
    }

    pub fn configure_receipt_retention(&mut self, retention: usize) -> Result<u64, ObjectError> {
        validate_storage_receipt_retention(retention)?;
        self.receipt_retention = retention;
        Ok(self.trim_receipts_to_retention())
    }

    pub(crate) fn trim_receipts_to_retention(&mut self) -> u64 {
        trim_btree_map_to_retention(
            &mut self.allocation_request_receipts,
            self.receipt_retention,
        )
        .saturating_add(trim_btree_map_to_retention(
            &mut self.batch_allocation_request_receipts,
            self.receipt_retention,
        ))
        .saturating_add(trim_btree_map_to_retention(
            &mut self.release_allocation_request_receipts,
            self.receipt_retention,
        ))
        .saturating_add(trim_btree_map_to_retention(
            &mut self.write_request_receipts,
            self.receipt_retention,
        ))
        .saturating_add(trim_btree_map_to_retention(
            &mut self.batch_write_request_receipts,
            self.receipt_retention,
        ))
        .saturating_add(trim_btree_map_to_retention(
            &mut self.discard_request_receipts,
            self.receipt_retention,
        ))
        .saturating_add(trim_btree_map_to_retention(
            &mut self.batch_discard_request_receipts,
            self.receipt_retention,
        ))
        .saturating_add(trim_btree_map_to_retention(
            &mut self.repair_request_receipts,
            self.receipt_retention,
        ))
        .saturating_add(trim_btree_map_to_retention(
            &mut self.range_repair_request_receipts,
            self.receipt_retention,
        ))
        .saturating_add(trim_btree_map_to_retention(
            &mut self.audit_repair_request_receipts,
            self.receipt_retention,
        ))
    }
}
