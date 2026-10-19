// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_write_page_ranges_idempotent(
        &mut self,
        writes: Vec<ChunkRangeWrite>,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::WritePageRangesIdempotent {
            writes,
            idempotency_key: idempotency_key.to_string(),
        })
    }

    pub fn propose_write_page_ranges_idempotent_fenced(
        &mut self,
        writes: Vec<ChunkRangeWrite>,
        idempotency_key: &str,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        if self
            .store
            .chunk_service
            .batch_write_request_receipts
            .contains_key(idempotency_key)
        {
            return self.propose_write_page_ranges_idempotent(writes, idempotency_key);
        }
        self.store
            .chunk_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        self.propose_write_page_ranges_idempotent(writes, idempotency_key)
    }

    pub fn propose_write_page_ranges_idempotent_fenced_report(
        &mut self,
        writes: Vec<ChunkRangeWrite>,
        idempotency_key: &str,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<(ObjectMetadata, ChunkIdempotentBatchWriteReport), ObjectError> {
        let metadata = self.propose_write_page_ranges_idempotent_fenced(
            writes,
            idempotency_key,
            expected_route_control_checksum,
        )?;
        let receipt = self
            .store
            .chunk_service
            .batch_write_request_receipts
            .get(idempotency_key)
            .cloned()
            .ok_or_else(|| {
                ObjectError::Corruption(
                    "raft idempotent page batch write receipt missing after apply".to_string(),
                )
            })?;
        let report = ChunkIdempotentBatchWriteReport {
            mutation: self.store.chunk_service.storage_mutation_report(
                receipt.stats.clone(),
                expected_route_control_checksum,
                None,
            ),
            receipt,
        };
        Ok((metadata, report))
    }

    pub fn propose_write_page_ranges_idempotent_strict_fenced_report(
        &mut self,
        writes: Vec<ChunkRangeWrite>,
        idempotency_key: &str,
        expected_route_control_checksum: Option<&str>,
        expected_page_write_batch_proof_checksum: impl AsRef<str>,
    ) -> Result<(ObjectMetadata, ChunkIdempotentBatchWriteReport), ObjectError> {
        let (metadata, report) = self.propose_write_page_ranges_idempotent_fenced_report(
            writes,
            idempotency_key,
            expected_route_control_checksum,
        )?;
        report
            .receipt
            .verify_expected_proof_checksum(expected_page_write_batch_proof_checksum)?;
        Ok((metadata, report))
    }
}
