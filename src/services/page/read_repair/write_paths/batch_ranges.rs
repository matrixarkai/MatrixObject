impl MatrixChunkService {
    pub fn write_slab_ranges(
        &mut self,
        writes: Vec<ChunkRangeWrite>,
    ) -> Result<ChunkBatchStats, ObjectError> {
        self.write_slab_ranges_with_chunk_receipts(&writes, None)
    }

    pub fn write_slab_ranges_storage_fenced(
        &mut self,
        writes: Vec<ChunkRangeWrite>,
        expected_route_control_checksum: Option<&str>,
        expected_chunk_control_proof_checksum: Option<&str>,
    ) -> Result<ChunkBatchStats, ObjectError> {
        self.check_expected_route_control_checksum(expected_route_control_checksum)?;
        self.check_expected_chunk_control_proof_checksum(expected_chunk_control_proof_checksum)?;
        self.write_slab_ranges(writes)
    }

    pub fn write_slab_ranges_storage_fenced_report(
        &mut self,
        writes: Vec<ChunkRangeWrite>,
        expected_route_control_checksum: Option<&str>,
        expected_chunk_control_proof_checksum: Option<&str>,
    ) -> Result<ChunkStorageMutationReport, ObjectError> {
        let stats = self.write_slab_ranges_storage_fenced(
            writes,
            expected_route_control_checksum,
            expected_chunk_control_proof_checksum,
        )?;
        Ok(self.storage_mutation_report(
            stats,
            expected_route_control_checksum,
            expected_chunk_control_proof_checksum,
        ))
    }

    fn write_slab_ranges_with_chunk_receipts(
        &mut self,
        writes: &[ChunkRangeWrite],
        idempotency_key: Option<&str>,
    ) -> Result<ChunkBatchStats, ObjectError> {
        let mut stats = ChunkBatchStats::default();
        let mut grouped: BTreeMap<(SlabId, PageId), Vec<(u64, &[u8])>> = BTreeMap::new();
        for write in writes {
            if self.is_slab_sealed(&write.slab_id) {
                return Err(ObjectError::PreconditionFailed(
                    "block segment is sealed".to_string(),
                ));
            }
            validate_pipeline_range("block batch write", write.offset, write.data.len() as u64)?;
            stats.input_ranges = stats.input_ranges.saturating_add(1);
            stats.payload_bytes = stats
                .payload_bytes
                .checked_add(write.data.len() as u64)
                .ok_or_else(|| {
                    ObjectError::InvalidArgument(
                        "block batch write payload byte count overflows".to_string(),
                    )
                })?;
            let mapped_chunk_ranges =
                self.mapped_chunk_count(write.offset, write.data.len() as u64)?;
            stats.mapped_chunk_ranges = stats
                .mapped_chunk_ranges
                .checked_add(mapped_chunk_ranges)
                .ok_or_else(|| {
                    ObjectError::InvalidArgument(
                        "block batch write chunk fanout overflows".to_string(),
                    )
                })?;
            let locations =
                self.map_range(&write.slab_id, write.offset, write.data.len() as u64)?;
            let mut cursor = 0usize;
            for loc in locations {
                let loc_length = usize::try_from(loc.length).map_err(|_| {
                    ObjectError::InvalidArgument(
                        "block batch write chunk length exceeds memory size".to_string(),
                    )
                })?;
                let next = cursor.checked_add(loc_length).ok_or_else(|| {
                    ObjectError::InvalidArgument(
                        "block batch write chunk cursor overflows".to_string(),
                    )
                })?;
                if next > write.data.len() {
                    return Err(ObjectError::Corruption(
                        "block batch write chunk mapping exceeds payload".to_string(),
                    ));
                }
                grouped
                    .entry((write.slab_id.clone(), loc.page_id))
                    .or_default()
                    .push((loc.page_offset, &write.data[cursor..next]));
                cursor = next;
            }
        }
        self.admit_page_route_qos(
            0,
            0,
            stats.input_ranges,
            stats.payload_bytes,
            0,
            0,
            now_millis(),
        )?;
        let projected_lengths = Self::projected_chunk_lengths_for_grouped_write_views(&grouped)?;
        let accepted_replicas = self.admissible_write_replicas(&projected_lengths)?;
        for ((_, page_id), chunk_writes) in grouped {
            let mut ok = 0usize;
            let mut first_error: Option<ObjectError> = None;
            let mut logical_coalesced_ranges = 0u64;
            let mut physical_write_count = 0u64;
            let mut physical_write_bytes = 0u64;
            for (index, replica) in self.replicas.iter_mut().enumerate() {
                if !accepted_replicas.contains(&index) {
                    continue;
                }
                let write_result = if let Some(base_key) = idempotency_key {
                    let page_key =
                        replica_receipt_key("bw", base_key, &page_id, index, &chunk_writes);
                    replica
                        .write_chunk_ranges_idempotent_borrowed(
                            page_id.clone(),
                            &chunk_writes,
                            &page_key,
                        )
                        .map(|(stats, _)| stats)
                } else {
                    replica.write_chunk_ranges_borrowed(page_id.clone(), &chunk_writes)
                };
                match write_result {
                    Ok(replica_stats) => {
                        ok += 1;
                        if logical_coalesced_ranges == 0 {
                            logical_coalesced_ranges = replica_stats.coalesced_ranges;
                        }
                        physical_write_count =
                            physical_write_count.saturating_add(replica_stats.physical_write_count);
                        physical_write_bytes =
                            physical_write_bytes.saturating_add(replica_stats.physical_write_bytes);
                    }
                    Err(err) if first_error.is_none() => first_error = Some(err),
                    Err(_) => {}
                }
            }
            if ok < self.write_quorum {
                return Err(first_error.unwrap_or_else(|| {
                    ObjectError::Corruption("block write quorum was not met".to_string())
                }));
            }
            if ok < accepted_replicas.len() {
                self.cumulative_quorum_write_degraded_chunks = self
                    .cumulative_quorum_write_degraded_chunks
                    .saturating_add(1);
            }
            stats.coalesced_chunk_ranges = stats
                .coalesced_chunk_ranges
                .saturating_add(logical_coalesced_ranges);
            stats.physical_chunk_write_count = stats
                .physical_chunk_write_count
                .saturating_add(physical_write_count);
            stats.physical_chunk_write_bytes = stats
                .physical_chunk_write_bytes
                .saturating_add(physical_write_bytes);
        }
        Ok(stats)
    }

    pub fn write_slab_ranges_idempotent(
        &mut self,
        writes: Vec<ChunkRangeWrite>,
        idempotency_key: &str,
    ) -> Result<(ChunkBatchStats, ChunkBatchWriteReceipt), ObjectError> {
        self.write_slab_ranges_idempotent_borrowed(&writes, idempotency_key)
    }

    pub fn write_slab_ranges_idempotent_borrowed(
        &mut self,
        writes: &[ChunkRangeWrite],
        idempotency_key: &str,
    ) -> Result<(ChunkBatchStats, ChunkBatchWriteReceipt), ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        if writes.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "block idempotent batch write must contain at least one range".to_string(),
            ));
        }
        for write in writes {
            if write.data.is_empty() {
                return Err(ObjectError::InvalidArgument(
                    "block idempotent batch write length must be positive".to_string(),
                ));
            }
        }
        let mut _payload_bytes = 0u64;
        for write in writes {
            write
                .offset
                .checked_add(write.data.len() as u64)
                .ok_or_else(|| {
                    ObjectError::InvalidArgument(
                        "block idempotent batch write range overflows".to_string(),
                    )
                })?;
            _payload_bytes = _payload_bytes
                .checked_add(write.data.len() as u64)
                .ok_or_else(|| {
                    ObjectError::InvalidArgument(
                        "block idempotent batch write payload byte count overflows".to_string(),
                    )
                })?;
        }
        let request_checksum = page_batch_write_request_checksum(writes);
        let payload_checksum = page_batch_write_payload_checksum(writes);
        if let Some(existing) = self.batch_write_request_receipts.get(idempotency_key) {
            if existing.request_checksum != request_checksum
                || existing.payload_checksum != payload_checksum
            {
                return Err(ObjectError::PreconditionFailed(
                    "block batch write idempotency key reused with different request".to_string(),
                ));
            }
            let mut receipt = existing.clone();
            receipt.deduped = true;
            return Ok((receipt.stats.clone(), receipt));
        }
        let stats = self.write_slab_ranges_with_chunk_receipts(writes, Some(idempotency_key))?;
        let receipt = ChunkBatchWriteReceipt {
            request_checksum,
            payload_checksum,
            idempotency_key: idempotency_key.to_string(),
            stats: stats.clone(),
            deduped: false,
        };
        self.batch_write_request_receipts
            .insert(idempotency_key.to_string(), receipt.clone());
        self.trim_receipts_to_retention();
        Ok((stats, receipt))
    }

    pub fn write_slab_ranges_idempotent_fenced(
        &mut self,
        writes: Vec<ChunkRangeWrite>,
        idempotency_key: &str,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<(ChunkBatchStats, ChunkBatchWriteReceipt), ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        if self
            .batch_write_request_receipts
            .contains_key(idempotency_key)
        {
            return self.write_slab_ranges_idempotent(writes, idempotency_key);
        }
        self.check_expected_route_control_checksum(expected_route_control_checksum)?;
        self.write_slab_ranges_idempotent(writes, idempotency_key)
    }

    pub fn write_slab_ranges_idempotent_fenced_report(
        &mut self,
        writes: Vec<ChunkRangeWrite>,
        idempotency_key: &str,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ChunkIdempotentBatchWriteReport, ObjectError> {
        let (stats, receipt) = self.write_slab_ranges_idempotent_fenced(
            writes,
            idempotency_key,
            expected_route_control_checksum,
        )?;
        Ok(ChunkIdempotentBatchWriteReport {
            receipt,
            mutation: self.storage_mutation_report(stats, expected_route_control_checksum, None),
        })
    }

    pub fn write_slab_ranges_idempotent_strict_fenced_report(
        &mut self,
        writes: Vec<ChunkRangeWrite>,
        idempotency_key: &str,
        expected_route_control_checksum: Option<&str>,
        expected_page_write_batch_proof_checksum: impl AsRef<str>,
    ) -> Result<ChunkIdempotentBatchWriteReport, ObjectError> {
        let report = self.write_slab_ranges_idempotent_fenced_report(
            writes,
            idempotency_key,
            expected_route_control_checksum,
        )?;
        report
            .receipt
            .verify_expected_proof_checksum(expected_page_write_batch_proof_checksum)?;
        Ok(report)
    }
}
