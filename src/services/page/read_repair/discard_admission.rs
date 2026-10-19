impl MatrixChunkService {
    pub fn can_accept_slab_discard(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
        length: u64,
        max_discard_bytes: u64,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        validate_pipeline_range("block discard", offset, length)?;
        let locations = self.map_range(chunk_id, offset, length)?;
        let discard_budget_limited = max_discard_bytes > 0 && length > max_discard_bytes;
        let mut discard_ranges: BTreeMap<PageId, Vec<DataRange>> = BTreeMap::new();
        for loc in &locations {
            discard_ranges
                .entry(loc.page_id.clone())
                .or_default()
                .push(DataRange {
                    offset: loc.page_offset,
                    length: loc.length,
                });
        }
        let mut ready_replica_count = 0u64;
        let mut writable_replica_count = 0u64;
        let mut first_error: Option<ObjectError> = None;
        if !discard_budget_limited {
            for (index, replica) in self.replicas.iter().enumerate() {
                if !self.replica_writable(index) {
                    continue;
                }
                writable_replica_count = writable_replica_count.saturating_add(1);
                match Self::replica_accepts_discard_ranges(replica, &discard_ranges) {
                    Ok(()) => ready_replica_count = ready_replica_count.saturating_add(1),
                    Err(err) if first_error.is_none() => first_error = Some(err),
                    Err(_) => {}
                }
            }
        }
        let can_accept = !discard_budget_limited && ready_replica_count >= self.write_quorum as u64;
        let partial_status = if can_accept {
            "ok".to_string()
        } else if discard_budget_limited {
            "discard range exceeds max_discard_bytes".to_string()
        } else {
            first_error
                .map(|err| err.to_string())
                .unwrap_or_else(|| "block discard quorum was not met".to_string())
        };
        let admission_reason = if can_accept {
            "discard_ready"
        } else if discard_budget_limited {
            "discard_budget_limited"
        } else {
            "write_quorum_unavailable"
        };
        let mut admission = ChunkRangeAdmission {
            can_accept,
            input_ranges: if length == 0 { 0 } else { 1 },
            mapped_chunk_ranges: locations.len() as u64,
            payload_bytes: length,
            max_discard_bytes,
            budget_limited: discard_budget_limited,
            ready_replica_count,
            required_quorum: self.write_quorum as u64,
            writable_replica_count,
            selected_discard_bytes: if discard_budget_limited { 0 } else { length },
            skipped_discard_bytes: if discard_budget_limited { length } else { 0 },
            partial_status,
            admission_reason: admission_reason.to_string(),
            write_quorum_limited: !discard_budget_limited
                && ready_replica_count < self.write_quorum as u64,
            discard_budget_limited,
            ..ChunkRangeAdmission::default()
        };
        stamp_page_range_admission_proof("CanDiscardBlock", &mut admission);
        Ok(admission)
    }

    pub fn can_accept_slab_discard_fenced(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
        length: u64,
        max_discard_bytes: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.check_expected_route_control_checksum(expected_route_control_checksum)?;
        let mut admission =
            self.can_accept_slab_discard(chunk_id, offset, length, max_discard_bytes)?;
        self.stamp_route_control_admission(&mut admission, expected_route_control_checksum);
        stamp_page_range_admission_proof("CanDiscardBlock", &mut admission);
        Ok(admission)
    }

    pub fn can_accept_slab_discards(
        &self,
        requests: Vec<ChunkRangeRequest>,
        max_discard_bytes: u64,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.can_accept_slab_discards_borrowed(&requests, max_discard_bytes)
    }

    pub fn can_accept_slab_discards_borrowed(
        &self,
        requests: &[ChunkRangeRequest],
        max_discard_bytes: u64,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.can_accept_slab_discards_from_borrowed(requests, max_discard_bytes, 0)
    }

    pub fn can_accept_slab_discards_from(
        &self,
        requests: Vec<ChunkRangeRequest>,
        max_discard_bytes: u64,
        discard_start_range_index: u64,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.can_accept_slab_discards_from_borrowed(
            &requests,
            max_discard_bytes,
            discard_start_range_index,
        )
    }

    pub fn can_accept_slab_discards_from_borrowed(
        &self,
        requests: &[ChunkRangeRequest],
        max_discard_bytes: u64,
        discard_start_range_index: u64,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        let mut admission = ChunkRangeAdmission {
            input_ranges: requests.iter().filter(|request| request.length > 0).count() as u64,
            max_discard_bytes,
            discard_start_range_index,
            next_discard_range_index: discard_start_range_index.min(requests.len() as u64),
            required_quorum: self.write_quorum as u64,
            ..ChunkRangeAdmission::default()
        };
        let mut discard_ranges: BTreeMap<PageId, Vec<DataRange>> = BTreeMap::new();
        let mut selected_discard_bytes = 0u64;
        let mut skipped_discard_bytes = 0u64;
        let mut skipped_discard_range_count = 0u64;
        let mut budget_limited = false;
        let mut budget_exhausted = false;
        for (request_index, request) in requests.iter().enumerate() {
            validate_pipeline_range("block batch discard", request.offset, request.length)?;
            admission.payload_bytes = admission
                .payload_bytes
                .checked_add(request.length)
                .ok_or_else(|| {
                    ObjectError::InvalidArgument(
                        "block batch discard payload byte count overflows".to_string(),
                    )
                })?;
            if (request_index as u64) < discard_start_range_index {
                continue;
            }
            if budget_exhausted {
                skipped_discard_bytes = skipped_discard_bytes.saturating_add(request.length);
                skipped_discard_range_count = skipped_discard_range_count.saturating_add(1);
                continue;
            }
            if max_discard_bytes > 0
                && selected_discard_bytes.saturating_add(request.length) > max_discard_bytes
            {
                budget_limited = true;
                budget_exhausted = true;
                skipped_discard_bytes = skipped_discard_bytes.saturating_add(request.length);
                skipped_discard_range_count = skipped_discard_range_count.saturating_add(1);
                continue;
            }
            selected_discard_bytes = selected_discard_bytes.saturating_add(request.length);
            admission.next_discard_range_index = (request_index as u64).saturating_add(1);
            for loc in self.map_range(&request.chunk_id, request.offset, request.length)? {
                admission.mapped_chunk_ranges = admission.mapped_chunk_ranges.saturating_add(1);
                discard_ranges
                    .entry(loc.page_id)
                    .or_default()
                    .push(DataRange {
                        offset: loc.page_offset,
                        length: loc.length,
                    });
            }
        }

        let mut merged_discard_ranges: BTreeMap<PageId, Vec<DataRange>> = BTreeMap::new();
        for (page_id, ranges) in discard_ranges {
            let merged = merge_ranges(ranges);
            admission.coalesced_chunk_ranges = admission
                .coalesced_chunk_ranges
                .saturating_add(merged.len() as u64);
            merged_discard_ranges.insert(page_id, merged);
        }

        let mut ready_replica_count = 0u64;
        let mut writable_replica_count = 0u64;
        let mut first_error: Option<ObjectError> = None;
        for (index, replica) in self.replicas.iter().enumerate() {
            if !self.replica_writable(index) {
                continue;
            }
            writable_replica_count = writable_replica_count.saturating_add(1);
            match Self::replica_accepts_discard_ranges(replica, &merged_discard_ranges) {
                Ok(()) => ready_replica_count = ready_replica_count.saturating_add(1),
                Err(err) if first_error.is_none() => first_error = Some(err),
                Err(_) => {}
            }
        }

        admission.ready_replica_count = ready_replica_count;
        admission.writable_replica_count = writable_replica_count;
        admission.budget_limited = budget_limited;
        admission.discard_budget_limited = budget_limited;
        admission.skipped_discard_range_count = skipped_discard_range_count;
        admission.discard_has_more = budget_limited;
        admission.can_accept =
            selected_discard_bytes > 0 && ready_replica_count >= self.write_quorum as u64;
        admission.selected_discard_bytes = if admission.can_accept {
            selected_discard_bytes
        } else {
            0
        };
        admission.skipped_discard_bytes =
            skipped_discard_bytes.saturating_add(if admission.can_accept {
                0
            } else {
                selected_discard_bytes
            });
        admission.partial_status = if admission.can_accept {
            "ok".to_string()
        } else if budget_limited && selected_discard_bytes == 0 {
            "block batch discard selected no ranges within max_discard_bytes".to_string()
        } else {
            first_error
                .map(|err| err.to_string())
                .unwrap_or_else(|| "block batch discard quorum was not met".to_string())
        };
        admission.write_quorum_limited = !admission.can_accept;
        admission.admission_reason = if admission.can_accept {
            "discard_ready".to_string()
        } else if budget_limited && selected_discard_bytes == 0 {
            "discard_budget_limited".to_string()
        } else {
            "write_quorum_unavailable".to_string()
        };
        stamp_page_range_admission_proof("CanDiscardBlockBatch", &mut admission);
        Ok(admission)
    }

    pub fn can_accept_chunk_discards_fenced(
        &self,
        requests: Vec<ChunkRangeRequest>,
        max_discard_bytes: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.check_expected_route_control_checksum(expected_route_control_checksum)?;
        let mut admission = self.can_accept_slab_discards_borrowed(&requests, max_discard_bytes)?;
        self.stamp_route_control_admission(&mut admission, expected_route_control_checksum);
        stamp_page_range_admission_proof("CanDiscardBlockBatch", &mut admission);
        Ok(admission)
    }

    pub fn can_accept_chunk_discards_from_fenced(
        &self,
        requests: Vec<ChunkRangeRequest>,
        max_discard_bytes: u64,
        discard_start_range_index: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.can_accept_chunk_discards_from_fenced_borrowed(
            &requests,
            max_discard_bytes,
            discard_start_range_index,
            expected_route_control_checksum,
        )
    }

    pub fn can_accept_chunk_discards_from_fenced_borrowed(
        &self,
        requests: &[ChunkRangeRequest],
        max_discard_bytes: u64,
        discard_start_range_index: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.check_expected_route_control_checksum(expected_route_control_checksum)?;
        let mut admission = self.can_accept_slab_discards_from_borrowed(
            requests,
            max_discard_bytes,
            discard_start_range_index,
        )?;
        self.stamp_route_control_admission(&mut admission, expected_route_control_checksum);
        stamp_page_range_admission_proof("CanDiscardBlockBatch", &mut admission);
        Ok(admission)
    }

}
