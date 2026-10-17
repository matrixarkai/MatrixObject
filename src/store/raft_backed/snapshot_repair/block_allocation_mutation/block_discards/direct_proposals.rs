impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_discard_page_range_idempotent(
        &mut self,
        slab_id: SlabId,
        offset: u64,
        length: u64,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::DiscardPageRangeIdempotent {
            slab_id,
            offset,
            length,
            idempotency_key: idempotency_key.to_string(),
        })
    }

    pub fn propose_discard_page_range_idempotent_fenced(
        &mut self,
        slab_id: SlabId,
        offset: u64,
        length: u64,
        idempotency_key: &str,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        if self
            .store
            .page_service
            .discard_request_receipts
            .contains_key(idempotency_key)
        {
            return self.propose_discard_page_range_idempotent(
                slab_id,
                offset,
                length,
                idempotency_key,
            );
        }
        self.store
            .page_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        self.propose_discard_page_range_idempotent(slab_id, offset, length, idempotency_key)
    }

    pub fn propose_discard_page_range_idempotent_fenced_report(
        &mut self,
        slab_id: SlabId,
        offset: u64,
        length: u64,
        idempotency_key: &str,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<(ObjectMetadata, PageIdempotentDiscardReport), ObjectError> {
        let metadata = self.propose_discard_page_range_idempotent_fenced(
            slab_id,
            offset,
            length,
            idempotency_key,
            expected_route_control_checksum,
        )?;
        let receipt = self
            .store
            .page_service
            .discard_request_receipts
            .get(idempotency_key)
            .cloned()
            .ok_or_else(|| {
                ObjectError::Corruption(
                    "raft idempotent block discard receipt missing after apply".to_string(),
                )
            })?;
        let stats = PageBatchStats {
            input_ranges: if receipt.length == 0 { 0 } else { 1 },
            mapped_chunk_ranges: self
                .store
                .page_service
                .mapped_chunk_count(receipt.offset, receipt.length)?,
            payload_bytes: receipt.length,
            ..PageBatchStats::default()
        };
        let report = PageIdempotentDiscardReport {
            receipt,
            mutation: self.store.page_service.storage_mutation_report(
                stats,
                expected_route_control_checksum,
                None,
            ),
        };
        Ok((metadata, report))
    }

    pub fn propose_discard_page_ranges_idempotent(
        &mut self,
        requests: Vec<PageRangeRequest>,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::DiscardPageRangesIdempotent {
            requests,
            idempotency_key: idempotency_key.to_string(),
        })
    }

    pub fn propose_discard_page_ranges_idempotent_fenced(
        &mut self,
        requests: Vec<PageRangeRequest>,
        idempotency_key: &str,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        if self
            .store
            .page_service
            .batch_discard_request_receipts
            .contains_key(idempotency_key)
        {
            return self.propose_discard_page_ranges_idempotent(requests, idempotency_key);
        }
        self.store
            .page_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        self.propose_discard_page_ranges_idempotent(requests, idempotency_key)
    }

    pub fn propose_discard_page_ranges_idempotent_fenced_report(
        &mut self,
        requests: Vec<PageRangeRequest>,
        idempotency_key: &str,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<(ObjectMetadata, PageIdempotentBatchDiscardReport), ObjectError> {
        let metadata = self.propose_discard_page_ranges_idempotent_fenced(
            requests,
            idempotency_key,
            expected_route_control_checksum,
        )?;
        let receipt = self
            .store
            .page_service
            .batch_discard_request_receipts
            .get(idempotency_key)
            .cloned()
            .ok_or_else(|| {
                ObjectError::Corruption(
                    "raft idempotent block batch discard receipt missing after apply".to_string(),
                )
            })?;
        let report = PageIdempotentBatchDiscardReport {
            mutation: self.store.page_service.storage_mutation_report(
                receipt.stats.clone(),
                expected_route_control_checksum,
                None,
            ),
            receipt,
        };
        Ok((metadata, report))
    }

    pub fn propose_discard_page_ranges_idempotent_strict_fenced_report(
        &mut self,
        requests: Vec<PageRangeRequest>,
        idempotency_key: &str,
        expected_route_control_checksum: Option<&str>,
        expected_page_discard_batch_proof_checksum: impl AsRef<str>,
    ) -> Result<(ObjectMetadata, PageIdempotentBatchDiscardReport), ObjectError> {
        let (metadata, report) = self.propose_discard_page_ranges_idempotent_fenced_report(
            requests,
            idempotency_key,
            expected_route_control_checksum,
        )?;
        report
            .receipt
            .verify_expected_proof_checksum(expected_page_discard_batch_proof_checksum)?;
        Ok((metadata, report))
    }

}
