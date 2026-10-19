impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_release_page_allocation_idempotent(
        &mut self,
        allocation_id: &str,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_release_page_allocation_idempotent_fenced(
            allocation_id,
            idempotency_key,
            None,
        )
    }

    pub fn propose_release_page_allocation_idempotent_fenced(
        &mut self,
        allocation_id: &str,
        idempotency_key: &str,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        if !self
            .store
            .chunk_service
            .release_allocation_request_receipts
            .contains_key(idempotency_key)
        {
            self.store
                .chunk_service
                .check_expected_route_control_checksum(expected_route_control_checksum)?;
        }
        self.propose(ObjectCommand::ReleasePageAllocationIdempotent {
            allocation_id: allocation_id.to_string(),
            idempotency_key: idempotency_key.to_string(),
        })
    }

    pub fn propose_release_page_allocation_idempotent_fenced_report(
        &mut self,
        allocation_id: &str,
        idempotency_key: &str,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<(ObjectMetadata, ChunkIdempotentAllocationReleaseReport), ObjectError> {
        let deduped = self
            .store
            .chunk_service
            .release_allocation_request_receipts
            .contains_key(idempotency_key);
        let metadata = self.propose_release_page_allocation_idempotent_fenced(
            allocation_id,
            idempotency_key,
            expected_route_control_checksum,
        )?;
        let allocation = self
            .store
            .chunk_service
            .release_allocation_request_receipts
            .get(idempotency_key)
            .cloned()
            .ok_or_else(|| {
                ObjectError::Corruption(
                    "raft idempotent block allocation release receipt missing after apply"
                        .to_string(),
                )
            })?;
        let receipt = ChunkAllocationReceipt {
            allocation,
            idempotency_key: idempotency_key.to_string(),
            released: true,
            deduped,
        };
        let stats = ChunkBatchStats {
            input_ranges: if receipt.allocation.length == 0 { 0 } else { 1 },
            mapped_chunk_ranges: self
                .store
                .chunk_service
                .mapped_chunk_count(receipt.allocation.offset, receipt.allocation.length)?,
            payload_bytes: receipt.allocation.length,
            ..ChunkBatchStats::default()
        };
        let report = ChunkIdempotentAllocationReleaseReport {
            receipt,
            mutation: self.store.chunk_service.storage_mutation_report(
                stats,
                expected_route_control_checksum,
                None,
            ),
        };
        Ok((metadata, report))
    }

    pub fn propose_release_page_allocation_idempotent_strict_fenced_report(
        &mut self,
        allocation_id: &str,
        idempotency_key: &str,
        expected_route_control_checksum: Option<&str>,
        expected_page_allocation_release_receipt_proof_checksum: impl AsRef<str>,
    ) -> Result<(ObjectMetadata, ChunkIdempotentAllocationReleaseReport), ObjectError> {
        let (metadata, report) = self.propose_release_page_allocation_idempotent_fenced_report(
            allocation_id,
            idempotency_key,
            expected_route_control_checksum,
        )?;
        report.receipt.verify_expected_proof_checksum(
            expected_page_allocation_release_receipt_proof_checksum,
        )?;
        Ok((metadata, report))
    }

    pub fn propose_release_page_allocation_idempotent_linearizable_fenced(
        &mut self,
        allocation_id: &str,
        idempotency_key: &str,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_storage_control_read_index_committed_at(
            "release block allocation",
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        self.propose_release_page_allocation_idempotent_fenced(
            allocation_id,
            idempotency_key,
            expected_route_control_checksum,
        )
    }

    pub fn propose_release_page_allocation_idempotent_linearizable_fenced_report(
        &mut self,
        allocation_id: &str,
        idempotency_key: &str,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<(ObjectMetadata, ChunkIdempotentAllocationReleaseReport), ObjectError> {
        self.ensure_storage_control_read_index_committed_at(
            "release block allocation",
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        self.propose_release_page_allocation_idempotent_fenced_report(
            allocation_id,
            idempotency_key,
            expected_route_control_checksum,
        )
    }

    pub fn propose_allocate_page_ranges_idempotent(
        &mut self,
        lengths: Vec<u64>,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_allocate_page_ranges_idempotent_fenced(lengths, now_ms, idempotency_key, None)
    }

    pub fn propose_allocate_page_ranges_idempotent_fenced(
        &mut self,
        lengths: Vec<u64>,
        now_ms: u64,
        idempotency_key: &str,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        if !self
            .store
            .chunk_service
            .batch_allocation_request_receipts
            .contains_key(idempotency_key)
        {
            self.store
                .chunk_service
                .check_expected_route_control_checksum(expected_route_control_checksum)?;
        }
        self.propose(ObjectCommand::AllocatePageRangesIdempotent {
            lengths,
            now_ms,
            idempotency_key: idempotency_key.to_string(),
        })
    }

    pub fn propose_allocate_page_ranges_idempotent_fenced_report(
        &mut self,
        lengths: Vec<u64>,
        now_ms: u64,
        idempotency_key: &str,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<(ObjectMetadata, ChunkIdempotentAllocationReport), ObjectError> {
        let deduped = self
            .store
            .chunk_service
            .batch_allocation_request_receipts
            .contains_key(idempotency_key);
        let metadata = self.propose_allocate_page_ranges_idempotent_fenced(
            lengths,
            now_ms,
            idempotency_key,
            expected_route_control_checksum,
        )?;
        let receipt = self
            .store
            .chunk_service
            .batch_allocation_request_receipts
            .get(idempotency_key)
            .cloned()
            .map(|allocations| ChunkBatchAllocationReceipt {
                allocations,
                idempotency_key: idempotency_key.to_string(),
                deduped,
            })
            .ok_or_else(|| {
                ObjectError::Corruption(
                    "raft idempotent block batch allocation receipt missing after apply"
                        .to_string(),
                )
            })?;
        let payload_bytes = receipt
            .allocations
            .iter()
            .map(|allocation| allocation.length)
            .sum();
        let stats = ChunkBatchStats {
            input_ranges: receipt
                .allocations
                .iter()
                .filter(|allocation| allocation.length > 0)
                .count() as u64,
            mapped_chunk_ranges: receipt
                .allocations
                .iter()
                .map(|allocation| {
                    self.store
                        .chunk_service
                        .mapped_chunk_count(allocation.offset, allocation.length)
                })
                .try_fold(0u64, |total, count| {
                    count.map(|count| total.saturating_add(count))
                })?,
            payload_bytes,
            ..ChunkBatchStats::default()
        };
        let report = ChunkIdempotentAllocationReport {
            receipt,
            mutation: self.store.chunk_service.storage_mutation_report(
                stats,
                expected_route_control_checksum,
                None,
            ),
        };
        Ok((metadata, report))
    }

    pub fn propose_allocate_page_ranges_idempotent_strict_fenced_report(
        &mut self,
        lengths: Vec<u64>,
        now_ms: u64,
        idempotency_key: &str,
        expected_route_control_checksum: Option<&str>,
        expected_page_batch_allocation_receipt_proof_checksum: impl AsRef<str>,
    ) -> Result<(ObjectMetadata, ChunkIdempotentAllocationReport), ObjectError> {
        let (metadata, report) = self.propose_allocate_page_ranges_idempotent_fenced_report(
            lengths,
            now_ms,
            idempotency_key,
            expected_route_control_checksum,
        )?;
        report.receipt.verify_expected_proof_checksum(
            expected_page_batch_allocation_receipt_proof_checksum,
        )?;
        Ok((metadata, report))
    }

    pub fn propose_allocate_page_ranges_idempotent_linearizable_fenced(
        &mut self,
        lengths: Vec<u64>,
        now_ms: u64,
        idempotency_key: &str,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_storage_control_read_index_committed_at(
            "allocate block ranges",
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        self.propose_allocate_page_ranges_idempotent_fenced(
            lengths,
            now_ms,
            idempotency_key,
            expected_route_control_checksum,
        )
    }

    pub fn propose_allocate_page_ranges_idempotent_linearizable_fenced_report(
        &mut self,
        lengths: Vec<u64>,
        now_ms: u64,
        idempotency_key: &str,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<(ObjectMetadata, ChunkIdempotentAllocationReport), ObjectError> {
        self.ensure_storage_control_read_index_committed_at(
            "allocate block ranges",
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        self.propose_allocate_page_ranges_idempotent_fenced_report(
            lengths,
            now_ms,
            idempotency_key,
            expected_route_control_checksum,
        )
    }
}
