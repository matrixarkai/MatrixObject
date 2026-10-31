// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
fn merge_discard_ranges_in_place(existing: &mut Vec<DataRange>, additions: &[DataRange]) {
    let mut needs_full_merge = false;
    for range in additions.iter().filter(|range| range.length > 0) {
        let Some(last) = existing.last_mut() else {
            existing.push(*range);
            continue;
        };
        let Some(last_end) = last.offset.checked_add(last.length) else {
            existing.push(*range);
            needs_full_merge = true;
            continue;
        };
        let Some(range_end) = range.offset.checked_add(range.length) else {
            existing.push(*range);
            needs_full_merge = true;
            continue;
        };
        if range.offset > last_end {
            existing.push(*range);
            continue;
        }
        if range.offset >= last.offset {
            if range_end > last_end {
                last.length = range_end.saturating_sub(last.offset);
            }
            continue;
        }
        existing.push(*range);
        needs_full_merge = true;
    }
    if needs_full_merge {
        *existing = merge_ranges(std::mem::take(existing));
    }
}

impl MatrixReplicaService {
    pub fn discard_chunk(
        &mut self,
        page_id: &PageId,
        offset: u64,
        length: u64,
    ) -> Result<(), ObjectError> {
        self.ensure_writable()?;
        if length == 0 {
            return Ok(());
        }
        let end = validate_pipeline_range("chunk discard", offset, length)?;
        let logical_length = self
            .pages
            .get(page_id)
            .ok_or_else(|| ObjectError::NotFound("chunk is missing".to_string()))?
            .logical_length;
        if end > logical_length {
            return Err(ObjectError::InvalidArgument(
                "chunk discard exceeds logical length".to_string(),
            ));
        }
        self.admit_mutation_qos(0, 0, 1, length, now_millis())?;
        // NOT AN APPEND, so the batch undo cannot be a length here.
        self.materialise_page_undo(page_id);
        let chunk = self
            .pages
            .get_mut(page_id)
            .ok_or_else(|| ObjectError::NotFound("chunk is missing".to_string()))?;
        let chunk = std::sync::Arc::make_mut(chunk);
        merge_discard_ranges_in_place(
            &mut chunk.discarded_ranges,
            &[DataRange { offset, length }],
        );
        chunk.write_sequence = chunk.write_sequence.saturating_add(1);
        self.invalidate_chunk_cache(page_id, offset, length);
        self.physical_discard_ranges = self.physical_discard_ranges.saturating_add(1);
        self.physical_discard_bytes = self.physical_discard_bytes.saturating_add(length);
        self.mark_metadata_mutation();
        Ok(())
    }

    pub fn discard_chunk_fenced(
        &mut self,
        page_id: &PageId,
        offset: u64,
        length: u64,
        expected_control_checksum: Option<&str>,
    ) -> Result<(), ObjectError> {
        self.check_expected_control_checksum(expected_control_checksum)?;
        self.discard_chunk(page_id, offset, length)
    }

    pub fn can_accept_chunk_discard(
        &self,
        page_id: &PageId,
        ranges: &[DataRange],
    ) -> Result<ReplicaDiscardAdmission, ObjectError> {
        self.can_accept_chunk_discards_borrowed(page_id, ranges, 0)
    }

    pub fn can_accept_chunk_discards(
        &self,
        page_id: &PageId,
        ranges: Vec<DataRange>,
        max_discard_bytes: u64,
    ) -> Result<ReplicaDiscardAdmission, ObjectError> {
        self.can_accept_chunk_discards_from_borrowed(page_id, &ranges, max_discard_bytes, 0)
    }

    pub fn can_accept_chunk_discards_borrowed(
        &self,
        page_id: &PageId,
        ranges: &[DataRange],
        max_discard_bytes: u64,
    ) -> Result<ReplicaDiscardAdmission, ObjectError> {
        self.can_accept_chunk_discards_from_borrowed(page_id, ranges, max_discard_bytes, 0)
    }

    pub fn can_accept_chunk_discards_from(
        &self,
        page_id: &PageId,
        ranges: Vec<DataRange>,
        max_discard_bytes: u64,
        discard_start_range_index: u64,
    ) -> Result<ReplicaDiscardAdmission, ObjectError> {
        self.can_accept_chunk_discards_from_borrowed(
            page_id,
            &ranges,
            max_discard_bytes,
            discard_start_range_index,
        )
    }

    pub fn can_accept_chunk_discards_from_borrowed(
        &self,
        page_id: &PageId,
        ranges: &[DataRange],
        max_discard_bytes: u64,
        discard_start_range_index: u64,
    ) -> Result<ReplicaDiscardAdmission, ObjectError> {
        let mut admission = ReplicaDiscardAdmission {
            max_discard_bytes,
            discard_start_range_index,
            ..ReplicaDiscardAdmission::default()
        };
        let mut selected = Vec::with_capacity(ranges.len());
        for range in ranges {
            if range.length == 0 {
                continue;
            }
            validate_pipeline_data_range("chunk discard", range)?;
            admission.discard_count = admission.discard_count.saturating_add(1);
            admission.discard_bytes = admission.discard_bytes.saturating_add(range.length);
            selected.push(range.clone());
        }
        if discard_start_range_index > selected.len() as u64 {
            return Err(ObjectError::InvalidArgument(format!(
                "chunk batch discard start range index {} exceeds {} non-empty ranges",
                discard_start_range_index,
                selected.len()
            )));
        }
        let mut admitted = Vec::with_capacity(
            selected
                .len()
                .saturating_sub(discard_start_range_index as usize),
        );
        let mut budget_limited = false;
        let mut budget_exhausted = false;
        admission.next_discard_range_index = discard_start_range_index;
        for (index, range) in selected.iter().cloned().enumerate() {
            if (index as u64) < discard_start_range_index {
                continue;
            }
            if budget_exhausted {
                admission.skipped_discard_bytes =
                    admission.skipped_discard_bytes.saturating_add(range.length);
                admission.skipped_discard_range_count =
                    admission.skipped_discard_range_count.saturating_add(1);
                continue;
            }
            if max_discard_bytes > 0
                && admission
                    .selected_discard_bytes
                    .saturating_add(range.length)
                    > max_discard_bytes
            {
                budget_limited = true;
                budget_exhausted = true;
                admission.skipped_discard_bytes =
                    admission.skipped_discard_bytes.saturating_add(range.length);
                admission.skipped_discard_range_count =
                    admission.skipped_discard_range_count.saturating_add(1);
                continue;
            }
            admission.selected_discard_bytes = admission
                .selected_discard_bytes
                .saturating_add(range.length);
            admission.next_discard_range_index = (index as u64).saturating_add(1);
            admitted.push(range);
        }
        admission.discard_has_more = admission.next_discard_range_index < admission.discard_count;
        admission.budget_limited = budget_limited || admission.discard_has_more;
        let merged = merge_ranges(admitted);
        admission.coalesced_ranges = merged.len() as u64;
        admission.service_mode_limited = self.writable_admission_error().is_some();
        admission.qos_limited = self.mutation_qos_would_limit(
            0,
            0,
            admission.coalesced_ranges,
            admission.selected_discard_bytes,
            now_millis(),
        );
        let Some(chunk) = self.pages.get(page_id) else {
            admission.missing_chunk = true;
            admission.can_accept = false;
            return Ok(admission);
        };
        admission.range_invalid = merged.iter().any(|range| {
            range.length > 0
                && range
                    .offset
                    .checked_add(range.length)
                    .map_or(true, |end| end > chunk.logical_length)
        });
        admission.can_accept = !admission.service_mode_limited
            && !admission.budget_limited
            && !admission.qos_limited
            && !admission.missing_chunk
            && !admission.range_invalid;
        Ok(admission)
    }

    pub fn discard_chunk_idempotent(
        &mut self,
        page_id: &PageId,
        offset: u64,
        length: u64,
        idempotency_key: &str,
    ) -> Result<ReplicaDiscardReceipt, ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        if length == 0 {
            return Err(ObjectError::InvalidArgument(
                "chunk idempotent discard length must be positive".to_string(),
            ));
        }
        let request_checksum = chunk_discard_request_checksum(page_id, offset, length);
        if let Some(existing) = self.discard_request_receipts.get(idempotency_key) {
            if existing.page_id != *page_id
                || existing.offset != offset
                || existing.length != length
                || existing.request_checksum != request_checksum
            {
                return Err(ObjectError::PreconditionFailed(
                    "chunk discard idempotency key reused with different request".to_string(),
                ));
            }
            let mut receipt = existing.clone();
            receipt.deduped = true;
            return Ok(receipt);
        }
        self.discard_chunk(page_id, offset, length)?;
        let receipt = ReplicaDiscardReceipt {
            page_id: page_id.clone(),
            offset,
            length,
            request_checksum,
            idempotency_key: idempotency_key.to_string(),
            deduped: false,
        };
        std::sync::Arc::make_mut(&mut self.discard_request_receipts)
            .insert(idempotency_key.to_string(), receipt.clone());
        self.trim_receipts_to_retention();
        Ok(receipt)
    }

    pub fn discard_chunk_ranges(
        &mut self,
        page_id: &PageId,
        ranges: Vec<DataRange>,
    ) -> Result<ReplicaDiscardBatchStats, ObjectError> {
        self.discard_chunk_ranges_borrowed_internal(page_id, &ranges, true)
    }

    pub fn discard_chunk_ranges_borrowed(
        &mut self,
        page_id: &PageId,
        ranges: &[DataRange],
    ) -> Result<ReplicaDiscardBatchStats, ObjectError> {
        self.discard_chunk_ranges_borrowed_internal(page_id, ranges, true)
    }

    pub fn discard_chunk_ranges_fenced(
        &mut self,
        page_id: &PageId,
        ranges: Vec<DataRange>,
        expected_control_checksum: Option<&str>,
    ) -> Result<ReplicaDiscardBatchStats, ObjectError> {
        self.check_expected_control_checksum(expected_control_checksum)?;
        self.discard_chunk_ranges(page_id, ranges)
    }

    fn discard_chunk_ranges_borrowed_internal(
        &mut self,
        page_id: &PageId,
        ranges: &[DataRange],
        record_internal_receipt: bool,
    ) -> Result<ReplicaDiscardBatchStats, ObjectError> {
        self.ensure_writable()?;
        let logical_length = self
            .pages
            .get(page_id)
            .ok_or_else(|| ObjectError::NotFound("chunk is missing".to_string()))?
            .logical_length;
        let mut stats = ReplicaDiscardBatchStats::default();
        for range in ranges {
            if range.length == 0 {
                continue;
            }
            let end = validate_pipeline_data_range("chunk discard", range)?;
            if end > logical_length {
                return Err(ObjectError::InvalidArgument(
                    "chunk discard exceeds logical length".to_string(),
                ));
            }
            stats.input_ranges = stats.input_ranges.saturating_add(1);
            stats.input_bytes = stats.input_bytes.checked_add(range.length).ok_or_else(|| {
                ObjectError::InvalidArgument("chunk batch discard byte count overflows".to_string())
            })?;
        }
        if stats.input_ranges == 0 {
            return Ok(stats);
        }
        let request_checksum = chunk_batch_discard_nonempty_request_checksum(page_id, ranges);
        let merged = merge_range_slice(ranges);
        self.admit_mutation_qos(0, 0, merged.len() as u64, stats.input_bytes, now_millis())?;
        // NOT AN APPEND, so the batch undo cannot be a length here.
        self.materialise_page_undo(page_id);
        let chunk = self
            .pages
            .get_mut(page_id)
            .ok_or_else(|| ObjectError::NotFound("chunk is missing".to_string()))?;
        let chunk = std::sync::Arc::make_mut(chunk);
        merge_discard_ranges_in_place(&mut chunk.discarded_ranges, &merged);
        chunk.write_sequence = chunk.write_sequence.saturating_add(1);
        let _ = chunk;
        for range in merged {
            stats.coalesced_ranges = stats.coalesced_ranges.saturating_add(1);
            stats.coalesced_bytes = stats.coalesced_bytes.saturating_add(range.length);
            stats.physical_discard_ranges = stats.physical_discard_ranges.saturating_add(1);
            stats.physical_discard_bytes =
                stats.physical_discard_bytes.saturating_add(range.length);
            let invalidated = self.invalidate_chunk_cache(page_id, range.offset, range.length);
            stats.invalidated_cache_entries = stats
                .invalidated_cache_entries
                .saturating_add(invalidated.invalidated_entries);
        }
        self.physical_discard_ranges = self
            .physical_discard_ranges
            .saturating_add(stats.physical_discard_ranges);
        self.physical_discard_bytes = self
            .physical_discard_bytes
            .saturating_add(stats.physical_discard_bytes);
        self.mark_metadata_mutation();
        if record_internal_receipt {
            // `format!` sizes its buffer at twice the literal's length and
            // then grows by doubling, so these 45 characters landed in a
            // 84-byte allocation. A receipt map holds up to
            // `receipt_retention` of these keys -- 100,000 by default -- and
            // the slack is resident for as long as the receipt is, which is
            // why the audit counts capacity. Built to its length instead.
            const PREFIX: &str = "direct-batch-discard-";
            let mut idempotency_key =
                String::with_capacity(PREFIX.len() + request_checksum.len());
            idempotency_key.push_str(PREFIX);
            idempotency_key.push_str(&request_checksum);
            let receipt = ReplicaBatchDiscardReceipt {
                page_id: page_id.clone(),
                request_checksum,
                idempotency_key: idempotency_key.clone(),
                stats: stats.clone(),
                deduped: false,
            };
            std::sync::Arc::make_mut(&mut self.batch_discard_request_receipts)
                .insert(idempotency_key, receipt);
            self.trim_receipts_to_retention();
        }
        Ok(stats)
    }

    pub fn discard_chunk_ranges_idempotent(
        &mut self,
        page_id: &PageId,
        ranges: Vec<DataRange>,
        idempotency_key: &str,
    ) -> Result<(ReplicaDiscardBatchStats, ReplicaBatchDiscardReceipt), ObjectError> {
        self.discard_chunk_ranges_idempotent_borrowed(page_id, &ranges, idempotency_key)
    }

    pub fn discard_chunk_ranges_idempotent_borrowed(
        &mut self,
        page_id: &PageId,
        ranges: &[DataRange],
        idempotency_key: &str,
    ) -> Result<(ReplicaDiscardBatchStats, ReplicaBatchDiscardReceipt), ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        if ranges.iter().all(|range| range.length == 0) {
            return Err(ObjectError::InvalidArgument(
                "chunk idempotent batch discard must contain at least one range".to_string(),
            ));
        }
        let mut _requested_bytes = 0u64;
        for range in ranges {
            if range.length == 0 {
                continue;
            }
            validate_pipeline_data_range("chunk discard", range)?;
            _requested_bytes = _requested_bytes.checked_add(range.length).ok_or_else(|| {
                ObjectError::InvalidArgument(
                    "chunk idempotent batch discard byte count overflows".to_string(),
                )
            })?;
        }
        let request_checksum = chunk_batch_discard_nonempty_request_checksum(page_id, ranges);
        if let Some(existing) = self.batch_discard_request_receipts.get(idempotency_key) {
            if existing.page_id != *page_id || existing.request_checksum != request_checksum {
                return Err(ObjectError::PreconditionFailed(
                    "chunk batch discard idempotency key reused with different request".to_string(),
                ));
            }
            let mut receipt = existing.clone();
            receipt.deduped = true;
            return Ok((receipt.stats.clone(), receipt));
        }
        let stats = self.discard_chunk_ranges_borrowed_internal(page_id, ranges, false)?;
        let receipt = ReplicaBatchDiscardReceipt {
            page_id: page_id.clone(),
            request_checksum,
            idempotency_key: idempotency_key.to_string(),
            stats: stats.clone(),
            deduped: false,
        };
        std::sync::Arc::make_mut(&mut self.batch_discard_request_receipts)
            .insert(idempotency_key.to_string(), receipt.clone());
        self.trim_receipts_to_retention();
        Ok((stats, receipt))
    }
}
