// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixChunkService {
    /// Compatibility spelling; use [`Self::can_accept_page_write`].
    #[deprecated(note = "use can_accept_page_write")]
    pub fn can_accept_slab_write(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
        length: u64,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.can_accept_page_write(chunk_id, offset, length, max_write_bytes, max_chunk_write_count)
    }

    pub fn can_accept_page_write(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
        length: u64,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        let mapped_chunk_ranges = self.mapped_chunk_count(offset, length)?;
        let byte_budget_limited = max_write_bytes > 0 && length > max_write_bytes;
        let chunk_budget_limited =
            max_chunk_write_count > 0 && mapped_chunk_ranges > max_chunk_write_count;
        let mut ready_replica_count = 0u64;
        let mut writable_replica_count = 0u64;
        let mut first_error: Option<ObjectError> = None;
        if !byte_budget_limited && !chunk_budget_limited {
            let locations = self.map_range(chunk_id, offset, length)?;
            let projected_lengths = Self::projected_chunk_lengths_for_locations(&locations)?;
            for (index, replica) in self.replicas.iter().enumerate() {
                if !self.replica_writable(index) {
                    continue;
                }
                writable_replica_count = writable_replica_count.saturating_add(1);
                match Self::replica_accepts_projected_chunk_lengths(replica, &projected_lengths) {
                    Ok(()) => ready_replica_count = ready_replica_count.saturating_add(1),
                    Err(err) if first_error.is_none() => first_error = Some(err),
                    Err(_) => {}
                }
            }
        }
        let budget_limited = byte_budget_limited || chunk_budget_limited;
        let can_accept = !budget_limited && ready_replica_count >= self.write_quorum as u64;
        let partial_status = if can_accept {
            "ok".to_string()
        } else if byte_budget_limited {
            "write payload exceeds max_write_bytes".to_string()
        } else if chunk_budget_limited {
            "write chunk fanout exceeds max_chunk_write_count".to_string()
        } else {
            first_error
                .map(|err| err.to_string())
                .unwrap_or_else(|| "block write quorum was not met".to_string())
        };
        let admission_reason = if can_accept {
            "write_ready"
        } else if budget_limited {
            "write_budget_limited"
        } else {
            "write_quorum_unavailable"
        };
        let mut admission = ChunkRangeAdmission {
            can_accept,
            input_ranges: if length == 0 { 0 } else { 1 },
            mapped_chunk_ranges,
            payload_bytes: length,
            max_write_bytes,
            max_chunk_write_count,
            budget_limited,
            ready_replica_count,
            required_quorum: self.write_quorum as u64,
            writable_replica_count,
            partial_status,
            admission_reason: admission_reason.to_string(),
            write_quorum_limited: !budget_limited && ready_replica_count < self.write_quorum as u64,
            ..ChunkRangeAdmission::default()
        };
        stamp_page_range_admission_proof("CanWriteBlock", &mut admission);
        Ok(admission)
    }

    /// Compatibility spelling; use [`Self::can_accept_page_write_fenced`].
    #[deprecated(note = "use can_accept_page_write_fenced")]
    pub fn can_accept_slab_write_fenced(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
        length: u64,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.can_accept_page_write_fenced(
            chunk_id,
            offset,
            length,
            max_write_bytes,
            max_chunk_write_count,
            expected_route_control_checksum,
        )
    }

    pub fn can_accept_page_write_fenced(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
        length: u64,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.check_expected_route_control_checksum(expected_route_control_checksum)?;
        let mut admission = self.can_accept_page_write(
            chunk_id,
            offset,
            length,
            max_write_bytes,
            max_chunk_write_count,
        )?;
        self.stamp_route_control_admission(&mut admission, expected_route_control_checksum);
        stamp_page_range_admission_proof("CanWriteBlock", &mut admission);
        Ok(admission)
    }

    /// Compatibility spelling; use [`Self::can_accept_page_writes`].
    #[deprecated(note = "use can_accept_page_writes")]
    pub fn can_accept_slab_writes(
        &self,
        writes: Vec<ChunkRangeWrite>,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.can_accept_page_writes(writes, max_write_bytes, max_chunk_write_count)
    }

    pub fn can_accept_page_writes(
        &self,
        writes: Vec<ChunkRangeWrite>,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.can_accept_page_writes_borrowed(&writes, max_write_bytes, max_chunk_write_count)
    }

    /// Compatibility spelling; use [`Self::can_accept_page_writes_borrowed`].
    #[deprecated(note = "use can_accept_page_writes_borrowed")]
    pub fn can_accept_slab_writes_borrowed(
        &self,
        writes: &[ChunkRangeWrite],
        max_write_bytes: u64,
        max_chunk_write_count: u64,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.can_accept_page_writes_borrowed(writes, max_write_bytes, max_chunk_write_count)
    }

    pub fn can_accept_page_writes_borrowed(
        &self,
        writes: &[ChunkRangeWrite],
        max_write_bytes: u64,
        max_chunk_write_count: u64,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.can_accept_page_writes_from_borrowed(
            writes,
            max_write_bytes,
            max_chunk_write_count,
            0,
        )
    }

    /// Compatibility spelling; use [`Self::can_accept_page_writes_from`].
    #[deprecated(note = "use can_accept_page_writes_from")]
    pub fn can_accept_slab_writes_from(
        &self,
        writes: Vec<ChunkRangeWrite>,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
        write_start_range_index: u64,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.can_accept_page_writes_from(
            writes,
            max_write_bytes,
            max_chunk_write_count,
            write_start_range_index,
        )
    }

    pub fn can_accept_page_writes_from(
        &self,
        writes: Vec<ChunkRangeWrite>,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
        write_start_range_index: u64,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.can_accept_page_writes_from_borrowed(
            &writes,
            max_write_bytes,
            max_chunk_write_count,
            write_start_range_index,
        )
    }

    /// Compatibility spelling; use [`Self::can_accept_page_writes_from_borrowed`].
    #[deprecated(note = "use can_accept_page_writes_from_borrowed")]
    pub fn can_accept_slab_writes_from_borrowed(
        &self,
        writes: &[ChunkRangeWrite],
        max_write_bytes: u64,
        max_chunk_write_count: u64,
        write_start_range_index: u64,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.can_accept_page_writes_from_borrowed(
            writes,
            max_write_bytes,
            max_chunk_write_count,
            write_start_range_index,
        )
    }

    pub fn can_accept_page_writes_from_borrowed(
        &self,
        writes: &[ChunkRangeWrite],
        max_write_bytes: u64,
        max_chunk_write_count: u64,
        write_start_range_index: u64,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        let mut admission = ChunkRangeAdmission {
            input_ranges: writes.iter().filter(|write| !write.data.is_empty()).count() as u64,
            max_write_bytes,
            max_chunk_write_count,
            required_quorum: self.write_quorum as u64,
            write_start_range_index,
            next_write_range_index: write_start_range_index,
            ..ChunkRangeAdmission::default()
        };
        if write_start_range_index > admission.input_ranges {
            return Err(ObjectError::InvalidArgument(format!(
                "page batch write start range index {} exceeds {} non-empty ranges",
                write_start_range_index, admission.input_ranges
            )));
        }
        let mut grouped: BTreeMap<(ChunkId, PageId), Vec<(u64, &[u8])>> = BTreeMap::new();
        let mut selected_writes = Vec::with_capacity(admission.input_ranges as usize);
        let mut selected_mapped_chunk_ranges = 0u64;
        let mut non_empty_index = 0u64;
        let mut write_byte_budget_limited = false;
        let mut write_chunk_budget_limited = false;
        for write in writes.iter() {
            if write.data.is_empty() {
                continue;
            }
            let current_range_index = non_empty_index;
            non_empty_index = non_empty_index.saturating_add(1);
            if current_range_index < write_start_range_index {
                continue;
            }
            validate_pipeline_range("block batch write", write.offset, write.data.len() as u64)?;
            let write_payload_bytes = write.data.len() as u64;
            let mapped_chunk_ranges = self.mapped_chunk_count(write.offset, write_payload_bytes)?;
            let next_payload_bytes = admission
                .selected_write_payload_bytes
                .checked_add(write_payload_bytes)
                .ok_or_else(|| {
                    ObjectError::InvalidArgument(
                        "page batch write selected payload byte count overflows".to_string(),
                    )
                })?;
            let next_mapped_chunk_ranges = selected_mapped_chunk_ranges
                .checked_add(mapped_chunk_ranges)
                .ok_or_else(|| {
                    ObjectError::InvalidArgument(
                        "page batch write selected chunk fanout overflows".to_string(),
                    )
                })?;
            write_byte_budget_limited = max_write_bytes > 0 && next_payload_bytes > max_write_bytes;
            write_chunk_budget_limited =
                max_chunk_write_count > 0 && next_mapped_chunk_ranges > max_chunk_write_count;
            if write_byte_budget_limited || write_chunk_budget_limited {
                break;
            }
            admission.selected_write_payload_bytes = next_payload_bytes;
            selected_mapped_chunk_ranges = next_mapped_chunk_ranges;
            admission.selected_write_range_count =
                admission.selected_write_range_count.saturating_add(1);
            admission.next_write_range_index = current_range_index.saturating_add(1);
            selected_writes.push(write);
        }
        admission.write_has_more = admission.next_write_range_index < admission.input_ranges;
        if admission.write_has_more {
            admission.budget_limited = true;
            admission.partial_status = if write_byte_budget_limited {
                "block batch write admitted prefix within max_write_bytes".to_string()
            } else {
                "block batch write admitted prefix within max_chunk_write_count".to_string()
            };
            admission.admission_reason = "write_budget_limited".to_string();
        }
        for write in selected_writes {
            admission.payload_bytes = admission
                .payload_bytes
                .checked_add(write.data.len() as u64)
                .ok_or_else(|| {
                    ObjectError::InvalidArgument(
                        "page batch write payload byte count overflows".to_string(),
                    )
                })?;
            let mapped_chunk_ranges =
                self.mapped_chunk_count(write.offset, write.data.len() as u64)?;
            admission.mapped_chunk_ranges = admission
                .mapped_chunk_ranges
                .checked_add(mapped_chunk_ranges)
                .ok_or_else(|| {
                    ObjectError::InvalidArgument(
                        "page batch write chunk fanout overflows".to_string(),
                    )
                })?;
            let locations =
                self.map_range(&write.chunk_id, write.offset, write.data.len() as u64)?;
            let mut cursor = 0usize;
            for loc in locations {
                let next = cursor + loc.length as usize;
                grouped
                    .entry((write.chunk_id.clone(), loc.page_id))
                    .or_default()
                    .push((loc.page_offset, &write.data[cursor..next]));
                cursor = next;
            }
        }

        let coalesced_chunk_writes = grouped.len() as u64;
        admission.coalesced_chunk_ranges = coalesced_chunk_writes;
        if max_chunk_write_count > 0 && coalesced_chunk_writes > max_chunk_write_count {
            admission.budget_limited = true;
            admission.partial_status =
                "block batch write chunk fanout exceeds max_chunk_write_count".to_string();
            admission.admission_reason = "write_budget_limited".to_string();
            stamp_page_range_admission_proof("CanWriteBlockBatch", &mut admission);
            return Ok(admission);
        }

        if admission.selected_write_range_count == 0 && admission.input_ranges > 0 {
            admission.budget_limited = true;
            admission.partial_status = if write_byte_budget_limited {
                "block batch write payload exceeds max_write_bytes".to_string()
            } else if write_chunk_budget_limited {
                "block batch write chunk fanout exceeds max_chunk_write_count".to_string()
            } else {
                "block batch write selected no ranges".to_string()
            };
            admission.admission_reason = "write_budget_limited".to_string();
            stamp_page_range_admission_proof("CanWriteBlockBatch", &mut admission);
            return Ok(admission);
        }

        let projected_lengths = Self::projected_chunk_lengths_for_grouped_write_views(&grouped)?;
        let mut ready_replica_count = 0u64;
        let mut writable_replica_count = 0u64;
        let mut first_error: Option<ObjectError> = None;
        for (index, replica) in self.replicas.iter().enumerate() {
            if !self.replica_writable(index) {
                continue;
            }
            writable_replica_count = writable_replica_count.saturating_add(1);
            match Self::replica_accepts_projected_chunk_lengths(replica, &projected_lengths) {
                Ok(()) => ready_replica_count = ready_replica_count.saturating_add(1),
                Err(err) if first_error.is_none() => first_error = Some(err),
                Err(_) => {}
            }
        }

        admission.ready_replica_count = ready_replica_count;
        admission.writable_replica_count = writable_replica_count;
        admission.can_accept =
            ready_replica_count >= self.write_quorum as u64 && !admission.budget_limited;
        admission.partial_status = if admission.can_accept {
            "ok".to_string()
        } else if admission.budget_limited && !admission.partial_status.is_empty() {
            admission.partial_status
        } else {
            first_error
                .map(|err| err.to_string())
                .unwrap_or_else(|| "block batch write quorum was not met".to_string())
        };
        admission.write_quorum_limited = !admission.can_accept && !admission.budget_limited;
        admission.admission_reason = if admission.can_accept {
            "write_ready".to_string()
        } else if admission.budget_limited {
            "write_budget_limited".to_string()
        } else {
            "write_quorum_unavailable".to_string()
        };
        stamp_page_range_admission_proof("CanWriteBlockBatch", &mut admission);
        Ok(admission)
    }

    pub fn can_accept_chunk_writes_fenced(
        &self,
        writes: Vec<ChunkRangeWrite>,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.can_accept_chunk_writes_fenced_from(
            writes,
            max_write_bytes,
            max_chunk_write_count,
            0,
            expected_route_control_checksum,
        )
    }

    pub fn can_accept_chunk_writes_fenced_from(
        &self,
        writes: Vec<ChunkRangeWrite>,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
        write_start_range_index: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.can_accept_chunk_writes_fenced_from_borrowed(
            &writes,
            max_write_bytes,
            max_chunk_write_count,
            write_start_range_index,
            expected_route_control_checksum,
        )
    }

    pub fn can_accept_chunk_writes_fenced_from_borrowed(
        &self,
        writes: &[ChunkRangeWrite],
        max_write_bytes: u64,
        max_chunk_write_count: u64,
        write_start_range_index: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.check_expected_route_control_checksum(expected_route_control_checksum)?;
        let mut admission = self.can_accept_page_writes_from_borrowed(
            writes,
            max_write_bytes,
            max_chunk_write_count,
            write_start_range_index,
        )?;
        self.stamp_route_control_admission(&mut admission, expected_route_control_checksum);
        stamp_page_range_admission_proof("CanWriteBlockBatch", &mut admission);
        Ok(admission)
    }

}
