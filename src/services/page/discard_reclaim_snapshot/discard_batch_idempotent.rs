impl MatrixPageService {
    pub fn discard_slab_ranges_idempotent(
        &mut self,
        requests: Vec<PageRangeRequest>,
        idempotency_key: &str,
    ) -> Result<(PageBatchStats, PageBatchDiscardReceipt), ObjectError> {
        self.discard_slab_ranges_idempotent_borrowed(&requests, idempotency_key)
    }

    pub fn discard_slab_ranges_idempotent_borrowed(
        &mut self,
        requests: &[PageRangeRequest],
        idempotency_key: &str,
    ) -> Result<(PageBatchStats, PageBatchDiscardReceipt), ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        if requests.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "block idempotent batch discard must contain at least one range".to_string(),
            ));
        }
        for request in requests {
            if request.length == 0 {
                return Err(ObjectError::InvalidArgument(
                    "block idempotent batch discard length must be positive".to_string(),
                ));
            }
        }
        let mut _requested_bytes = 0u64;
        for request in requests {
            validate_pipeline_range("block batch discard", request.offset, request.length)?;
            _requested_bytes = _requested_bytes
                .checked_add(request.length)
                .ok_or_else(|| {
                    ObjectError::InvalidArgument(
                        "block idempotent batch discard byte count overflows".to_string(),
                    )
                })?;
        }
        let request_checksum = page_batch_discard_request_checksum(requests);
        if let Some(existing) = self.batch_discard_request_receipts.get(idempotency_key) {
            if existing.request_checksum != request_checksum {
                return Err(ObjectError::PreconditionFailed(
                    "block batch discard idempotency key reused with different request".to_string(),
                ));
            }
            let mut receipt = existing.clone();
            receipt.deduped = true;
            return Ok((receipt.stats.clone(), receipt));
        }
        let stats =
            self.discard_slab_ranges_with_chunk_receipts(requests, Some(idempotency_key))?;
        let receipt = PageBatchDiscardReceipt {
            request_checksum,
            idempotency_key: idempotency_key.to_string(),
            stats: stats.clone(),
            deduped: false,
        };
        self.batch_discard_request_receipts
            .insert(idempotency_key.to_string(), receipt.clone());
        self.trim_receipts_to_retention();
        Ok((stats, receipt))
    }

    pub fn discard_slab_ranges_idempotent_bounded_from(
        &mut self,
        requests: Vec<PageRangeRequest>,
        max_discard_bytes: u64,
        discard_start_range_index: u64,
        idempotency_key: &str,
    ) -> Result<
        (
            PageBatchStats,
            PageBatchDiscardReceipt,
            PageRangeAdmission,
        ),
        ObjectError,
    > {
        validate_idempotency_key(idempotency_key)?;
        let admission = self.can_accept_slab_discards_from_borrowed(
            &requests,
            max_discard_bytes,
            discard_start_range_index,
        )?;
        let selected = select_admitted_page_discard_requests(requests, &admission)?;
        let (stats, receipt) = self.discard_slab_ranges_idempotent(selected, idempotency_key)?;
        if stats.payload_bytes != admission.selected_discard_bytes {
            return Err(ObjectError::Corruption(
                "block idempotent bounded discard stats do not match admission".to_string(),
            ));
        }
        Ok((stats, receipt, admission))
    }

    pub fn discard_slab_ranges_idempotent_bounded(
        &mut self,
        requests: Vec<PageRangeRequest>,
        max_discard_bytes: u64,
        idempotency_key: &str,
    ) -> Result<
        (
            PageBatchStats,
            PageBatchDiscardReceipt,
            PageRangeAdmission,
        ),
        ObjectError,
    > {
        self.discard_slab_ranges_idempotent_bounded_from(
            requests,
            max_discard_bytes,
            0,
            idempotency_key,
        )
    }

    pub fn discard_slab_ranges_idempotent_fenced(
        &mut self,
        requests: Vec<PageRangeRequest>,
        idempotency_key: &str,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<(PageBatchStats, PageBatchDiscardReceipt), ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        if self
            .batch_discard_request_receipts
            .contains_key(idempotency_key)
        {
            return self.discard_slab_ranges_idempotent(requests, idempotency_key);
        }
        self.check_expected_route_control_checksum(expected_route_control_checksum)?;
        self.discard_slab_ranges_idempotent(requests, idempotency_key)
    }

    pub fn discard_slab_ranges_idempotent_fenced_report(
        &mut self,
        requests: Vec<PageRangeRequest>,
        idempotency_key: &str,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PageIdempotentBatchDiscardReport, ObjectError> {
        let (stats, receipt) = self.discard_slab_ranges_idempotent_fenced(
            requests,
            idempotency_key,
            expected_route_control_checksum,
        )?;
        Ok(PageIdempotentBatchDiscardReport {
            receipt,
            mutation: self.storage_mutation_report(stats, expected_route_control_checksum, None),
        })
    }

    pub fn discard_slab_ranges_idempotent_strict_fenced_report(
        &mut self,
        requests: Vec<PageRangeRequest>,
        idempotency_key: &str,
        expected_route_control_checksum: Option<&str>,
        expected_page_discard_batch_proof_checksum: impl AsRef<str>,
    ) -> Result<PageIdempotentBatchDiscardReport, ObjectError> {
        let report = self.discard_slab_ranges_idempotent_fenced_report(
            requests,
            idempotency_key,
            expected_route_control_checksum,
        )?;
        report
            .receipt
            .verify_expected_proof_checksum(expected_page_discard_batch_proof_checksum)?;
        Ok(report)
    }
}
