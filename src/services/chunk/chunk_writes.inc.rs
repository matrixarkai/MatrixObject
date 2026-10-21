// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixReplicaService {
    pub fn write_chunk(
        &mut self,
        page_id: PageId,
        offset: u64,
        data: &[u8],
    ) -> Result<(), ObjectError> {
        self.ensure_writable()?;
        let end = validate_pipeline_range("chunk write", offset, data.len() as u64)?;
        let projected_len = self
            .pages
            .get(&page_id)
            .map_or(end, |state| state.data.len() as u64)
            .max(end);
        self.ensure_capacity_for_chunk_len(&page_id, projected_len)?;
        self.admit_mutation_qos(
            if data.is_empty() { 0 } else { 1 },
            data.len() as u64,
            0,
            0,
            now_millis(),
        )?;
        let cache_chunk_id = page_id.clone();
        // `make_mut` copies this one chunk's state if a snapshot still names
        // it, so the copy is per chunk WRITTEN rather than per chunk held.
        let chunk = std::sync::Arc::make_mut(
            self.pages
                .entry(page_id)
                .or_insert_with(|| std::sync::Arc::new(PageState::empty())),
        );
        let end_index = usize::try_from(end).map_err(|_| {
            ObjectError::InvalidArgument("chunk write range exceeds memory size".to_string())
        })?;
        let start_index = usize::try_from(offset).map_err(|_| {
            ObjectError::InvalidArgument("chunk write offset exceeds memory size".to_string())
        })?;
        if end_index > chunk.data.len() {
            let payload = std::sync::Arc::make_mut(&mut chunk.data);
            // reserve_exact, not resize's doubling: a chunk filled by many small
            // writes otherwise holds ~1.9x the bytes it stores (measured 163 MB
            // of capacity for 87 MB of data at three replicas).
            payload.reserve_exact(end_index.saturating_sub(payload.len()));
            payload.resize(end_index, 0);
        }
        std::sync::Arc::make_mut(&mut chunk.data)[start_index..end_index].copy_from_slice(data);
        chunk.logical_length = chunk.logical_length.max(end);
        if !chunk.discarded_ranges.is_empty() {
            // These bytes are real again, so the discard over them no longer
            // holds: without this a read of the new data is refused for
            // intersecting a discarded range.
            chunk.discarded_ranges =
                subtract_range(&chunk.discarded_ranges, offset, data.len() as u64);
        }
        chunk.write_sequence = chunk.write_sequence.saturating_add(1);
        chunk.checksum_fnv1a64 = fnv1a64(&chunk.data);
        if self.keep_ec_sidecar {
            refresh_chunk_ec_sidecars(chunk);
        } else {
            clear_chunk_ec_sidecars(chunk);
        }
        self.invalidate_chunk_cache(&cache_chunk_id, offset, data.len() as u64);
        self.physical_write_count = self.physical_write_count.saturating_add(1);
        self.physical_write_bytes = self.physical_write_bytes.saturating_add(data.len() as u64);
        self.mark_metadata_mutation();
        Ok(())
    }

    pub fn write_chunk_fenced(
        &mut self,
        page_id: PageId,
        offset: u64,
        data: &[u8],
        expected_control_checksum: Option<&str>,
    ) -> Result<(), ObjectError> {
        self.check_expected_control_checksum(expected_control_checksum)?;
        self.write_chunk(page_id, offset, data)
    }

    pub fn write_chunk_idempotent(
        &mut self,
        page_id: PageId,
        offset: u64,
        data: &[u8],
        idempotency_key: &str,
    ) -> Result<ReplicaWriteReceipt, ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        let length = data.len() as u64;
        if length == 0 {
            return Err(ObjectError::InvalidArgument(
                "chunk idempotent write length must be positive".to_string(),
            ));
        }
        let payload_checksum = etag(data);
        let request_checksum =
            chunk_write_receipt_request_checksum(&page_id, offset, length, &payload_checksum);
        if let Some(existing) = self.write_request_receipts.get(idempotency_key) {
            if existing.page_id != page_id
                || existing.offset != offset
                || existing.length != length
                || existing.request_checksum != request_checksum
                || existing.payload_checksum != payload_checksum
            {
                return Err(ObjectError::PreconditionFailed(
                    "chunk write idempotency key reused with different request".to_string(),
                ));
            }
            let mut receipt = existing.clone();
            receipt.deduped = true;
            return Ok(receipt);
        }
        self.write_chunk(page_id.clone(), offset, data)?;
        let receipt = ReplicaWriteReceipt {
            page_id,
            offset,
            length,
            request_checksum,
            payload_checksum,
            idempotency_key: idempotency_key.to_string(),
            deduped: false,
        };
        std::sync::Arc::make_mut(&mut self.write_request_receipts)
            .insert(idempotency_key.to_string(), receipt.clone());
        self.trim_receipts_to_retention();
        Ok(receipt)
    }

    pub fn write_chunk_ranges(
        &mut self,
        page_id: PageId,
        writes: Vec<(u64, Vec<u8>)>,
    ) -> Result<ReplicaWriteBatchStats, ObjectError> {
        self.write_chunk_ranges_internal(page_id, writes, true)
    }

    pub fn write_chunk_ranges_borrowed(
        &mut self,
        page_id: PageId,
        writes: &[(u64, &[u8])],
    ) -> Result<ReplicaWriteBatchStats, ObjectError> {
        self.write_chunk_ranges_borrowed_internal(page_id, writes, true)
    }

    /// The same write, leaving no receipt behind.
    ///
    /// # WHEN THIS IS THE RIGHT ENTRY
    ///
    /// `write_chunk_ranges_borrowed` records a receipt keyed `direct-batch-write-<request
    /// checksum>`. Deduplication looks receipts up by the CALLER's idempotency key, so that
    /// one can never be matched -- and the caller-keyed entry
    /// `write_chunk_ranges_idempotent_borrowed` already suppresses it and records its own.
    ///
    /// So a caller with no idempotency key gets a receipt that is written and cannot be
    /// consulted. `write_chunk`, the single-range entry the replicated path uses, records
    /// nothing for exactly that reason; this is the batch shape of the same decision.
    ///
    /// What it DOES give up, named rather than implied: the receipt map is encoded into a
    /// replica snapshot and its length goes into the readiness fingerprint and the control
    /// proof, and `list_chunk_batch_write_receipts` shows these to an operator. A write
    /// through here is absent from all of that -- which is where a replicated single-range
    /// write already is.
    pub fn write_chunk_ranges_borrowed_without_receipt(
        &mut self,
        page_id: PageId,
        writes: &[(u64, &[u8])],
    ) -> Result<ReplicaWriteBatchStats, ObjectError> {
        self.write_chunk_ranges_borrowed_internal(page_id, writes, false)
    }

    pub fn write_chunk_ranges_fenced(
        &mut self,
        page_id: PageId,
        writes: Vec<(u64, Vec<u8>)>,
        expected_control_checksum: Option<&str>,
    ) -> Result<ReplicaWriteBatchStats, ObjectError> {
        self.check_expected_control_checksum(expected_control_checksum)?;
        self.write_chunk_ranges(page_id, writes)
    }

    fn write_chunk_ranges_internal(
        &mut self,
        page_id: PageId,
        writes: Vec<(u64, Vec<u8>)>,
        record_internal_receipt: bool,
    ) -> Result<ReplicaWriteBatchStats, ObjectError> {
        let mut borrowed = Vec::with_capacity(writes.len());
        for (offset, data) in &writes {
            borrowed.push((*offset, data.as_slice()));
        }
        self.write_chunk_ranges_borrowed_internal(page_id, &borrowed, record_internal_receipt)
    }

    fn write_chunk_ranges_borrowed_internal(
        &mut self,
        page_id: PageId,
        writes: &[(u64, &[u8])],
        record_internal_receipt: bool,
    ) -> Result<ReplicaWriteBatchStats, ObjectError> {
        self.ensure_writable()?;
        let mut selected = Vec::with_capacity(writes.len());
        let mut stats = ReplicaWriteBatchStats::default();
        let mut selected_strictly_ordered = true;
        let mut last_selected_offset: Option<u64> = None;
        for (offset, data) in writes.iter().copied() {
            if data.is_empty() {
                continue;
            }
            validate_pipeline_range("chunk write", offset, data.len() as u64)?;
            stats.input_ranges = stats.input_ranges.saturating_add(1);
            stats.input_bytes = stats
                .input_bytes
                .checked_add(data.len() as u64)
                .ok_or_else(|| {
                    ObjectError::InvalidArgument(
                        "chunk batch write byte count overflows".to_string(),
                    )
                })?;
            if let Some(last_offset) = last_selected_offset {
                if offset <= last_offset {
                    selected_strictly_ordered = false;
                }
            }
            last_selected_offset = Some(offset);
            selected.push((offset, data));
        }
        if selected.is_empty() {
            return Ok(stats);
        }
        let request_checksum = chunk_batch_write_request_checksum_borrowed(&page_id, &selected);
        let payload_checksum = chunk_batch_write_payload_checksum_borrowed(&selected);
        if !selected_strictly_ordered {
            selected.sort_by_key(|(offset, _)| *offset);
        }
        let mut coalesced: Vec<(u64, std::borrow::Cow<'_, [u8]>)> =
            Vec::with_capacity(selected.len());
        for (offset, data) in selected {
            if let Some((merged_offset, merged_data)) = coalesced.last_mut() {
                let merged_end = merged_offset.saturating_add(merged_data.len() as u64);
                if offset <= merged_end {
                    let relative = (offset - *merged_offset) as usize;
                    let needed = relative.saturating_add(data.len());
                    let merged_data = merged_data.to_mut();
                    if needed > merged_data.len() {
                        merged_data.reserve_exact(needed.saturating_sub(merged_data.len()));
                        merged_data.resize(needed, 0);
                    }
                    merged_data[relative..relative + data.len()].copy_from_slice(data);
                    continue;
                }
            }
            coalesced.push((offset, std::borrow::Cow::Borrowed(data)));
        }
        let projected_len = coalesced.iter().try_fold(
            self.pages
                .get(&page_id)
                .map_or(0u64, |state| state.data.len() as u64),
            |acc, (offset, data)| -> Result<u64, ObjectError> {
                Ok(acc.max(validate_pipeline_range(
                    "chunk write",
                    *offset,
                    data.len() as u64,
                )?))
            },
        )?;
        self.ensure_capacity_for_chunk_len(&page_id, projected_len)?;
        let qos_write_ops = coalesced.len() as u64;
        let qos_write_bytes = coalesced.iter().try_fold(0u64, |acc, (_, data)| {
            acc.checked_add(data.len() as u64).ok_or_else(|| {
                ObjectError::InvalidArgument(
                    "chunk batch write coalesced byte count overflows".to_string(),
                )
            })
        })?;
        self.admit_mutation_qos(qos_write_ops, qos_write_bytes, 0, 0, now_millis())?;
        let coalesced_write_sequence_delta = coalesced.len() as u64;
        let mut invalidations = Vec::with_capacity(coalesced.len());
        let cache_chunk_id = page_id.clone();
        {
            let chunk = std::sync::Arc::make_mut(
                self.pages
                    .entry(page_id.clone())
                    .or_insert_with(|| std::sync::Arc::new(PageState::empty())),
            );
            let projected_len_index = usize::try_from(projected_len).map_err(|_| {
                ObjectError::InvalidArgument(
                    "chunk batch write range exceeds memory size".to_string(),
                )
            })?;
            if projected_len_index > chunk.data.len() {
                let payload = std::sync::Arc::make_mut(&mut chunk.data);
                payload.reserve_exact(projected_len_index.saturating_sub(payload.len()));
                payload.resize(projected_len_index, 0);
            }
            for (offset, data) in coalesced {
                let end = validate_pipeline_range("chunk write", offset, data.len() as u64)?;
                let end_index = usize::try_from(end).map_err(|_| {
                    ObjectError::InvalidArgument(
                        "chunk write range exceeds memory size".to_string(),
                    )
                })?;
                let start_index = usize::try_from(offset).map_err(|_| {
                    ObjectError::InvalidArgument(
                        "chunk write offset exceeds memory size".to_string(),
                    )
                })?;
                std::sync::Arc::make_mut(&mut chunk.data)[start_index..end_index].copy_from_slice(&data);
                chunk.logical_length = chunk.logical_length.max(end);
                invalidations.push((offset, data.len() as u64));
                self.physical_write_count = self.physical_write_count.saturating_add(1);
                self.physical_write_bytes =
                    self.physical_write_bytes.saturating_add(data.len() as u64);
                stats.coalesced_ranges = stats.coalesced_ranges.saturating_add(1);
                stats.coalesced_bytes = stats.coalesced_bytes.saturating_add(data.len() as u64);
                stats.physical_write_count = stats.physical_write_count.saturating_add(1);
                stats.physical_write_bytes =
                    stats.physical_write_bytes.saturating_add(data.len() as u64);
            }
            chunk.write_sequence = chunk
                .write_sequence
                .saturating_add(coalesced_write_sequence_delta);
            chunk.checksum_fnv1a64 = fnv1a64(&chunk.data);
            if self.keep_ec_sidecar {
            refresh_chunk_ec_sidecars(chunk);
        } else {
            clear_chunk_ec_sidecars(chunk);
        }
        }
        for (offset, length) in invalidations {
            self.invalidate_chunk_cache(&cache_chunk_id, offset, length);
        }
        self.mark_metadata_mutation();
        if record_internal_receipt {
            // `format!` sizes its buffer at twice the literal's length and
            // then grows by doubling, so these 43 characters landed in a
            // 76-byte allocation. A receipt map holds up to
            // `receipt_retention` of these keys -- 100,000 by default -- and
            // the slack is resident for as long as the receipt is, which is
            // why the audit counts capacity. Built to its length instead.
            const PREFIX: &str = "direct-batch-write-";
            let mut idempotency_key =
                String::with_capacity(PREFIX.len() + request_checksum.len());
            idempotency_key.push_str(PREFIX);
            idempotency_key.push_str(&request_checksum);
            let receipt = ReplicaBatchWriteReceipt {
                page_id,
                request_checksum,
                payload_checksum,
                idempotency_key: idempotency_key.clone(),
                stats: stats.clone(),
                deduped: false,
            };
            std::sync::Arc::make_mut(&mut self.batch_write_request_receipts)
                .insert(idempotency_key, receipt);
            self.trim_receipts_to_retention();
        }
        Ok(stats)
    }

    pub fn write_chunk_ranges_idempotent(
        &mut self,
        page_id: PageId,
        writes: Vec<(u64, Vec<u8>)>,
        idempotency_key: &str,
    ) -> Result<(ReplicaWriteBatchStats, ReplicaBatchWriteReceipt), ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        let mut selected = Vec::with_capacity(writes.len());
        for (offset, data) in writes {
            if !data.is_empty() {
                selected.push((offset, data));
            }
        }
        if selected.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "chunk idempotent batch write must contain at least one range".to_string(),
            ));
        }
        let mut _requested_bytes = 0u64;
        for (offset, data) in &selected {
            validate_pipeline_range("chunk write", *offset, data.len() as u64)?;
            _requested_bytes =
                _requested_bytes
                    .checked_add(data.len() as u64)
                    .ok_or_else(|| {
                        ObjectError::InvalidArgument(
                            "chunk idempotent batch write byte count overflows".to_string(),
                        )
                    })?;
        }
        let request_checksum = chunk_batch_write_request_checksum(&page_id, &selected);
        let payload_checksum = chunk_batch_write_payload_checksum(&selected);
        if let Some(existing) = self.batch_write_request_receipts.get(idempotency_key) {
            if existing.page_id != page_id
                || existing.request_checksum != request_checksum
                || existing.payload_checksum != payload_checksum
            {
                return Err(ObjectError::PreconditionFailed(
                    "chunk batch write idempotency key reused with different request".to_string(),
                ));
            }
            let mut receipt = existing.clone();
            receipt.deduped = true;
            return Ok((receipt.stats.clone(), receipt));
        }
        let stats = self.write_chunk_ranges_internal(page_id.clone(), selected, false)?;
        let receipt = ReplicaBatchWriteReceipt {
            page_id,
            request_checksum,
            payload_checksum,
            idempotency_key: idempotency_key.to_string(),
            stats: stats.clone(),
            deduped: false,
        };
        std::sync::Arc::make_mut(&mut self.batch_write_request_receipts)
            .insert(idempotency_key.to_string(), receipt.clone());
        self.trim_receipts_to_retention();
        Ok((stats, receipt))
    }

    pub fn write_chunk_ranges_idempotent_borrowed(
        &mut self,
        page_id: PageId,
        writes: &[(u64, &[u8])],
        idempotency_key: &str,
    ) -> Result<(ReplicaWriteBatchStats, ReplicaBatchWriteReceipt), ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        let mut selected = Vec::with_capacity(writes.len());
        for (offset, data) in writes.iter().copied() {
            if !data.is_empty() {
                selected.push((offset, data));
            }
        }
        if selected.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "chunk idempotent batch write must contain at least one range".to_string(),
            ));
        }
        let mut _requested_bytes = 0u64;
        for (offset, data) in &selected {
            validate_pipeline_range("chunk write", *offset, data.len() as u64)?;
            _requested_bytes =
                _requested_bytes
                    .checked_add(data.len() as u64)
                    .ok_or_else(|| {
                        ObjectError::InvalidArgument(
                            "chunk idempotent batch write byte count overflows".to_string(),
                        )
                    })?;
        }
        let request_checksum = chunk_batch_write_request_checksum_borrowed(&page_id, &selected);
        let payload_checksum = chunk_batch_write_payload_checksum_borrowed(&selected);
        if let Some(existing) = self.batch_write_request_receipts.get(idempotency_key) {
            if existing.page_id != page_id
                || existing.request_checksum != request_checksum
                || existing.payload_checksum != payload_checksum
            {
                return Err(ObjectError::PreconditionFailed(
                    "chunk batch write idempotency key reused with different request".to_string(),
                ));
            }
            let mut receipt = existing.clone();
            receipt.deduped = true;
            return Ok((receipt.stats.clone(), receipt));
        }
        let stats = self.write_chunk_ranges_borrowed_internal(page_id.clone(), &selected, false)?;
        let receipt = ReplicaBatchWriteReceipt {
            page_id,
            request_checksum,
            payload_checksum,
            idempotency_key: idempotency_key.to_string(),
            stats: stats.clone(),
            deduped: false,
        };
        std::sync::Arc::make_mut(&mut self.batch_write_request_receipts)
            .insert(idempotency_key.to_string(), receipt.clone());
        self.trim_receipts_to_retention();
        Ok((stats, receipt))
    }

}
