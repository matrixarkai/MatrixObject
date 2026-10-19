impl MatrixReplicaService {
    pub fn reclaim_chunks_with_limits_idempotent(
        &mut self,
        max_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
        min_discard_ratio_ppm: u64,
        max_discard_ranges: usize,
        max_trim_bytes: u64,
        idempotency_key: &str,
    ) -> Result<(ReplicaPartialReclaimReport, ReplicaReclaimReceipt), ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        let request_checksum = chunk_reclaim_request_checksum(
            max_chunks,
            max_rewrite_bytes,
            min_discarded_bytes,
            min_discard_ratio_ppm,
            max_discard_ranges,
            max_trim_bytes,
        );
        if let Some(existing) = self.reclaim_request_receipts.get(idempotency_key) {
            if existing.request_checksum != request_checksum {
                return Err(ObjectError::PreconditionFailed(
                    "chunk reclaim idempotency key reused with different request".to_string(),
                ));
            }
            let mut receipt = existing.clone();
            receipt.deduped = true;
            return Ok((receipt.report.clone(), receipt));
        }
        let report = self.reclaim_chunks_with_limits(
            max_chunks,
            max_rewrite_bytes,
            min_discarded_bytes,
            min_discard_ratio_ppm,
            max_discard_ranges,
            max_trim_bytes,
        );
        let receipt = ReplicaReclaimReceipt {
            request_checksum,
            idempotency_key: idempotency_key.to_string(),
            max_chunks: max_chunks as u64,
            max_rewrite_bytes,
            min_discarded_bytes,
            min_discard_ratio_ppm,
            max_discard_ranges: max_discard_ranges as u64,
            max_trim_bytes,
            report: report.clone(),
            deduped: false,
        };
        std::sync::Arc::make_mut(&mut self.reclaim_request_receipts)
            .insert(idempotency_key.to_string(), receipt.clone());
        self.trim_receipts_to_retention();
        Ok((report, receipt))
    }
}
