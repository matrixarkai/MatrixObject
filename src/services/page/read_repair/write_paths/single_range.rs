// SPDX-License-Identifier: Apache-2.0
impl MatrixChunkService {
    pub fn write_chunk_range(
        &mut self,
        chunk_id: &ChunkId,
        offset: u64,
        data: &[u8],
    ) -> Result<(), ObjectError> {
        self.write_chunk_range_with_chunk_receipts(chunk_id, offset, data, None)
    }

    pub fn write_chunk_range_storage_fenced(
        &mut self,
        chunk_id: &ChunkId,
        offset: u64,
        data: &[u8],
        expected_route_control_checksum: Option<&str>,
        expected_chunk_control_proof_checksum: Option<&str>,
    ) -> Result<(), ObjectError> {
        self.check_expected_route_control_checksum(expected_route_control_checksum)?;
        self.check_expected_chunk_control_proof_checksum(expected_chunk_control_proof_checksum)?;
        self.write_chunk_range(chunk_id, offset, data)
    }

    pub fn write_chunk_range_storage_fenced_report(
        &mut self,
        chunk_id: &ChunkId,
        offset: u64,
        data: &[u8],
        expected_route_control_checksum: Option<&str>,
        expected_chunk_control_proof_checksum: Option<&str>,
    ) -> Result<ChunkStorageMutationReport, ObjectError> {
        self.write_chunk_range_storage_fenced(
            chunk_id,
            offset,
            data,
            expected_route_control_checksum,
            expected_chunk_control_proof_checksum,
        )?;
        let stats = ChunkBatchStats {
            input_ranges: if data.is_empty() { 0 } else { 1 },
            mapped_chunk_ranges: self.mapped_chunk_count(offset, data.len() as u64)?,
            payload_bytes: data.len() as u64,
            ..ChunkBatchStats::default()
        };
        Ok(self.storage_mutation_report(
            stats,
            expected_route_control_checksum,
            expected_chunk_control_proof_checksum,
        ))
    }

    fn write_chunk_range_with_chunk_receipts(
        &mut self,
        chunk_id: &ChunkId,
        offset: u64,
        data: &[u8],
        idempotency_key: Option<&str>,
    ) -> Result<(), ObjectError> {
        if self.is_chunk_sealed(chunk_id) {
            return Err(ObjectError::PreconditionFailed(
                "page chunk is sealed".to_string(),
            ));
        }
        self.admit_page_route_qos(0, 0, 1, data.len() as u64, 0, 0, now_millis())?;
        let locations = self.map_range(chunk_id, offset, data.len() as u64)?;
        let projected_lengths = Self::projected_chunk_lengths_for_locations(&locations)?;
        let accepted_replicas = self.admissible_write_replicas(&projected_lengths)?;
        let mut cursor = 0usize;
        for loc in locations {
            let loc_length = usize::try_from(loc.length).map_err(|_| {
                ObjectError::InvalidArgument("page write chunk length exceeds memory size".to_string())
            })?;
            let next = cursor.checked_add(loc_length).ok_or_else(|| {
                ObjectError::InvalidArgument("page write chunk cursor overflows".to_string())
            })?;
            if next > data.len() {
                return Err(ObjectError::Corruption(
                    "page write chunk mapping exceeds payload".to_string(),
                ));
            }
            let slice = &data[cursor..next];
            let mut ok = 0usize;
            let mut first_error: Option<ObjectError> = None;
            for (index, replica) in self.replicas.iter_mut().enumerate() {
                if !accepted_replicas.contains(&index) {
                    continue;
                }
                let write_result = if let Some(base_key) = idempotency_key {
                    let page_key = replica_receipt_key(
                        "sw",
                        base_key,
                        &loc.page_id,
                        index,
                        &(loc.page_offset, slice),
                    );
                    replica
                        .write_chunk_idempotent(
                            loc.page_id.clone(),
                            loc.page_offset,
                            slice,
                            &page_key,
                        )
                        .map(|_| ())
                } else {
                    replica.write_chunk(loc.page_id.clone(), loc.page_offset, slice)
                };
                match write_result {
                    Ok(()) => ok += 1,
                    Err(err) if first_error.is_none() => first_error = Some(err),
                    Err(_) => {}
                }
            }
            if ok < self.write_quorum {
                return Err(first_error.unwrap_or_else(|| {
                    ObjectError::Corruption("page write quorum was not met".to_string())
                }));
            }
            if ok < accepted_replicas.len() {
                self.cumulative_quorum_write_degraded_chunks = self
                    .cumulative_quorum_write_degraded_chunks
                    .saturating_add(1);
            }
            cursor = next;
        }
        Ok(())
    }

    pub fn write_chunk_range_idempotent(
        &mut self,
        chunk_id: &ChunkId,
        offset: u64,
        data: &[u8],
        idempotency_key: &str,
    ) -> Result<ChunkWriteReceipt, ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        let length = data.len() as u64;
        if length == 0 {
            return Err(ObjectError::InvalidArgument(
                "page idempotent write length must be positive".to_string(),
            ));
        }
        let request_checksum = page_write_request_checksum(chunk_id, offset, data);
        let payload_checksum = etag(data);
        if let Some(existing) = self.write_request_receipts.get(idempotency_key) {
            if existing.chunk_id != *chunk_id
                || existing.offset != offset
                || existing.length != length
                || existing.request_checksum != request_checksum
                || existing.payload_checksum != payload_checksum
            {
                return Err(ObjectError::PreconditionFailed(
                    "page write idempotency key reused with different request".to_string(),
                ));
            }
            let mut receipt = existing.clone();
            receipt.deduped = true;
            return Ok(receipt);
        }
        self.write_chunk_range_with_chunk_receipts(
            chunk_id,
            offset,
            data,
            Some(idempotency_key),
        )?;
        let receipt = ChunkWriteReceipt {
            chunk_id: chunk_id.clone(),
            offset,
            length,
            request_checksum,
            payload_checksum,
            idempotency_key: idempotency_key.to_string(),
            deduped: false,
        };
        self.write_request_receipts
            .insert(idempotency_key.to_string(), receipt.clone());
        self.trim_receipts_to_retention();
        Ok(receipt)
    }

    pub fn write_chunk_range_idempotent_fenced(
        &mut self,
        chunk_id: &ChunkId,
        offset: u64,
        data: &[u8],
        idempotency_key: &str,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ChunkWriteReceipt, ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        if self.write_request_receipts.contains_key(idempotency_key) {
            return self.write_chunk_range_idempotent(chunk_id, offset, data, idempotency_key);
        }
        self.check_expected_route_control_checksum(expected_route_control_checksum)?;
        self.write_chunk_range_idempotent(chunk_id, offset, data, idempotency_key)
    }

    pub fn write_chunk_range_idempotent_fenced_report(
        &mut self,
        chunk_id: &ChunkId,
        offset: u64,
        data: &[u8],
        idempotency_key: &str,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ChunkIdempotentWriteReport, ObjectError> {
        let receipt = self.write_chunk_range_idempotent_fenced(
            chunk_id,
            offset,
            data,
            idempotency_key,
            expected_route_control_checksum,
        )?;
        let stats = ChunkBatchStats {
            input_ranges: if receipt.length == 0 { 0 } else { 1 },
            mapped_chunk_ranges: self.mapped_chunk_count(receipt.offset, receipt.length)?,
            payload_bytes: receipt.length,
            ..ChunkBatchStats::default()
        };
        Ok(ChunkIdempotentWriteReport {
            receipt,
            mutation: self.storage_mutation_report(stats, expected_route_control_checksum, None),
        })
    }
}
