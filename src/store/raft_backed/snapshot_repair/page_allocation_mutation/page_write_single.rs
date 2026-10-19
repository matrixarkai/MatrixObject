impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_write_page_range_idempotent(
        &mut self,
        chunk_id: ChunkId,
        offset: u64,
        data: Vec<u8>,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::WritePageRangeIdempotent {
            chunk_id,
            offset,
            data,
            idempotency_key: idempotency_key.to_string(),
        })
    }

    pub fn propose_write_page_range_idempotent_fenced(
        &mut self,
        chunk_id: ChunkId,
        offset: u64,
        data: Vec<u8>,
        idempotency_key: &str,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        if self
            .store
            .chunk_service
            .write_request_receipts
            .contains_key(idempotency_key)
        {
            return self.propose_write_page_range_idempotent(
                chunk_id,
                offset,
                data,
                idempotency_key,
            );
        }
        self.store
            .chunk_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        self.propose_write_page_range_idempotent(chunk_id, offset, data, idempotency_key)
    }

    pub fn propose_write_page_range_idempotent_fenced_report(
        &mut self,
        chunk_id: ChunkId,
        offset: u64,
        data: Vec<u8>,
        idempotency_key: &str,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<(ObjectMetadata, ChunkIdempotentWriteReport), ObjectError> {
        let metadata = self.propose_write_page_range_idempotent_fenced(
            chunk_id.clone(),
            offset,
            data,
            idempotency_key,
            expected_route_control_checksum,
        )?;
        let receipt = self
            .store
            .chunk_service
            .write_request_receipts
            .get(idempotency_key)
            .cloned()
            .ok_or_else(|| {
                ObjectError::Corruption(
                    "raft idempotent page write receipt missing after apply".to_string(),
                )
            })?;
        let stats = ChunkBatchStats {
            input_ranges: if receipt.length == 0 { 0 } else { 1 },
            mapped_chunk_ranges: self
                .store
                .chunk_service
                .mapped_chunk_count(receipt.offset, receipt.length)?,
            payload_bytes: receipt.length,
            ..ChunkBatchStats::default()
        };
        let report = ChunkIdempotentWriteReport {
            receipt,
            mutation: self.store.chunk_service.storage_mutation_report(
                stats,
                expected_route_control_checksum,
                None,
            ),
        };
        Ok((metadata, report))
    }
}
