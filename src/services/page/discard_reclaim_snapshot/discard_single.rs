impl MatrixPageService {
    pub fn discard_slab_range_idempotent(
        &mut self,
        slab_id: &SlabId,
        offset: u64,
        length: u64,
        idempotency_key: &str,
    ) -> Result<PageDiscardReceipt, ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        if length == 0 {
            return Err(ObjectError::InvalidArgument(
                "block idempotent discard length must be positive".to_string(),
            ));
        }
        let request_checksum = page_discard_request_checksum(slab_id, offset, length);
        if let Some(existing) = self.discard_request_receipts.get(idempotency_key) {
            if existing.slab_id != *slab_id
                || existing.offset != offset
                || existing.length != length
                || existing.request_checksum != request_checksum
            {
                return Err(ObjectError::PreconditionFailed(
                    "block discard idempotency key reused with different request".to_string(),
                ));
            }
            let mut receipt = existing.clone();
            receipt.deduped = true;
            return Ok(receipt);
        }
        self.discard_slab_range_with_chunk_receipts(
            slab_id,
            offset,
            length,
            Some(idempotency_key),
        )?;
        let receipt = PageDiscardReceipt {
            slab_id: slab_id.clone(),
            offset,
            length,
            request_checksum,
            idempotency_key: idempotency_key.to_string(),
            deduped: false,
        };
        self.discard_request_receipts
            .insert(idempotency_key.to_string(), receipt.clone());
        self.trim_receipts_to_retention();
        Ok(receipt)
    }

    pub fn discard_slab_range_idempotent_fenced(
        &mut self,
        slab_id: &SlabId,
        offset: u64,
        length: u64,
        idempotency_key: &str,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PageDiscardReceipt, ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        if self.discard_request_receipts.contains_key(idempotency_key) {
            return self.discard_slab_range_idempotent(
                slab_id,
                offset,
                length,
                idempotency_key,
            );
        }
        self.check_expected_route_control_checksum(expected_route_control_checksum)?;
        self.discard_slab_range_idempotent(slab_id, offset, length, idempotency_key)
    }

    pub fn discard_slab_range_idempotent_fenced_report(
        &mut self,
        slab_id: &SlabId,
        offset: u64,
        length: u64,
        idempotency_key: &str,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PageIdempotentDiscardReport, ObjectError> {
        let receipt = self.discard_slab_range_idempotent_fenced(
            slab_id,
            offset,
            length,
            idempotency_key,
            expected_route_control_checksum,
        )?;
        let stats = PageBatchStats {
            input_ranges: if receipt.length == 0 { 0 } else { 1 },
            mapped_chunk_ranges: self.mapped_chunk_count(receipt.offset, receipt.length)?,
            payload_bytes: receipt.length,
            ..PageBatchStats::default()
        };
        Ok(PageIdempotentDiscardReport {
            receipt,
            mutation: self.storage_mutation_report(stats, expected_route_control_checksum, None),
        })
    }
}
