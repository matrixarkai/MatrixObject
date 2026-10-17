impl MatrixChunkService {
    fn ensure_writable(&mut self) -> Result<(), ObjectError> {
        if !self.accepting_writes {
            self.service_mode_rejected_mutations =
                self.service_mode_rejected_mutations.saturating_add(1);
            return Err(ObjectError::PreconditionFailed(
                "chunk service is not accepting writes".to_string(),
            ));
        }
        if self.draining {
            self.service_mode_rejected_mutations =
                self.service_mode_rejected_mutations.saturating_add(1);
            return Err(ObjectError::PreconditionFailed(
                "chunk service is draining".to_string(),
            ));
        }
        if self.frozen {
            self.service_mode_rejected_mutations =
                self.service_mode_rejected_mutations.saturating_add(1);
            return Err(ObjectError::PreconditionFailed(
                "chunk service is frozen for maintenance".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn writable_admission_error(&self) -> Option<ObjectError> {
        if !self.accepting_writes {
            return Some(ObjectError::PreconditionFailed(
                "chunk service is not accepting writes".to_string(),
            ));
        }
        if self.draining {
            return Some(ObjectError::PreconditionFailed(
                "chunk service is draining".to_string(),
            ));
        }
        if self.frozen {
            return Some(ObjectError::PreconditionFailed(
                "chunk service is frozen for maintenance".to_string(),
            ));
        }
        None
    }

    pub(crate) fn projected_physical_bytes_for_chunk_len(
        &self,
        chunk_id: &ChunkId,
        new_len: u64,
    ) -> Result<u64, ObjectError> {
        let current_len = self
            .chunks
            .get(chunk_id)
            .map_or(0u64, |state| state.data.len() as u64);
        self.physical_bytes()
            .checked_sub(current_len)
            .and_then(|base| base.checked_add(new_len))
            .ok_or_else(|| {
                ObjectError::InvalidArgument(
                    "chunk service capacity accounting overflows".to_string(),
                )
            })
    }

    fn ensure_capacity_for_chunk_len(
        &mut self,
        chunk_id: &ChunkId,
        new_len: u64,
    ) -> Result<(), ObjectError> {
        let projected = self.projected_physical_bytes_for_chunk_len(chunk_id, new_len)?;
        if self.capacity_bytes > 0 && projected > self.capacity_bytes {
            self.capacity_rejected_writes = self.capacity_rejected_writes.saturating_add(1);
            return Err(ObjectError::PreconditionFailed(
                "chunk service capacity exceeded".to_string(),
            ));
        }
        Ok(())
    }

    pub fn can_accept_chunk_write(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
        length: u64,
    ) -> Result<ChunkWriteAdmission, ObjectError> {
        let end = validate_pipeline_range("chunk write", offset, length)?;
        let projected_len = self
            .chunks
            .get(chunk_id)
            .map_or(end, |state| state.data.len() as u64)
            .max(end);
        let projected_physical_bytes =
            self.projected_physical_bytes_for_chunk_len(chunk_id, projected_len)?;
        let service_mode_limited = self.writable_admission_error().is_some();
        let capacity_limited =
            self.capacity_bytes > 0 && projected_physical_bytes > self.capacity_bytes;
        let write_count = if length == 0 { 0 } else { 1 };
        let qos_limited = self.mutation_qos_would_limit(write_count, length, 0, 0, now_millis());
        Ok(ChunkWriteAdmission {
            can_accept: !service_mode_limited && !capacity_limited && !qos_limited,
            write_count,
            coalesced_ranges: write_count,
            payload_bytes: length,
            max_write_bytes: 0,
            selected_write_range_count: write_count,
            selected_write_payload_bytes: length,
            write_start_range_index: 0,
            next_write_range_index: write_count,
            write_has_more: false,
            min_read_index: 0,
            min_commit_index: 0,
            projected_physical_bytes,
            capacity_bytes: self.capacity_bytes,
            capacity_free_bytes: self.capacity_free_bytes(),
            service_mode_limited,
            capacity_limited,
            budget_limited: false,
            qos_limited,
        })
    }

    pub fn can_accept_chunk_writes(
        &self,
        chunk_id: &ChunkId,
        writes: Vec<(u64, Vec<u8>)>,
        max_write_bytes: u64,
    ) -> Result<ChunkWriteAdmission, ObjectError> {
        self.can_accept_chunk_writes_from(chunk_id, writes, max_write_bytes, 0)
    }

    pub fn can_accept_chunk_writes_from(
        &self,
        chunk_id: &ChunkId,
        writes: Vec<(u64, Vec<u8>)>,
        max_write_bytes: u64,
        write_start_range_index: u64,
    ) -> Result<ChunkWriteAdmission, ObjectError> {
        let mut borrowed = Vec::with_capacity(writes.len());
        for (offset, data) in &writes {
            borrowed.push((*offset, data.as_slice()));
        }
        self.can_accept_chunk_writes_from_borrowed(
            chunk_id,
            &borrowed,
            max_write_bytes,
            write_start_range_index,
        )
    }

    pub fn can_accept_chunk_writes_borrowed(
        &self,
        chunk_id: &ChunkId,
        writes: &[(u64, &[u8])],
        max_write_bytes: u64,
    ) -> Result<ChunkWriteAdmission, ObjectError> {
        self.can_accept_chunk_writes_from_borrowed(chunk_id, writes, max_write_bytes, 0)
    }

    pub fn can_accept_chunk_writes_from_borrowed(
        &self,
        chunk_id: &ChunkId,
        writes: &[(u64, &[u8])],
        max_write_bytes: u64,
        write_start_range_index: u64,
    ) -> Result<ChunkWriteAdmission, ObjectError> {
        let requested_start = write_start_range_index as usize;
        let mut admitted = Vec::with_capacity(writes.len().saturating_sub(requested_start));
        let mut input_ranges = 0u64;
        let mut input_bytes = 0u64;
        let mut selected_write_range_count = 0u64;
        let mut selected_write_payload_bytes = 0u64;
        let mut next_write_range_index = write_start_range_index;
        let mut budget_limited = false;
        let mut admitted_strictly_ordered = true;
        let mut last_admitted_offset: Option<u64> = None;
        for (offset, data) in writes.iter().copied() {
            if data.is_empty() {
                continue;
            }
            validate_pipeline_range("chunk write", offset, data.len() as u64)?;
            input_ranges = input_ranges.saturating_add(1);
            input_bytes = input_bytes.checked_add(data.len() as u64).ok_or_else(|| {
                ObjectError::InvalidArgument("chunk batch write byte count overflows".to_string())
            })?;
            let current_range_index = input_ranges.saturating_sub(1);
            if current_range_index < write_start_range_index || budget_limited {
                continue;
            }
            let next_payload_bytes = selected_write_payload_bytes
                .checked_add(data.len() as u64)
                .ok_or_else(|| {
                    ObjectError::InvalidArgument(
                        "chunk batch write selected byte count overflows".to_string(),
                    )
                })?;
            if max_write_bytes > 0 && next_payload_bytes > max_write_bytes {
                budget_limited = true;
                continue;
            }
            selected_write_payload_bytes = next_payload_bytes;
            selected_write_range_count = selected_write_range_count.saturating_add(1);
            next_write_range_index = current_range_index.saturating_add(1);
            if let Some(last_offset) = last_admitted_offset {
                if offset <= last_offset {
                    admitted_strictly_ordered = false;
                }
            }
            last_admitted_offset = Some(offset);
            admitted.push((offset, data));
        }
        if write_start_range_index > input_ranges {
            return Err(ObjectError::InvalidArgument(format!(
                "chunk batch write start range index {} exceeds {} non-empty ranges",
                write_start_range_index, input_ranges
            )));
        }
        let write_has_more = next_write_range_index < input_ranges;
        budget_limited |= write_has_more;
        if !admitted_strictly_ordered {
            admitted.sort_by_key(|(offset, _)| *offset);
        }
        let mut coalesced: Vec<(u64, std::borrow::Cow<'_, [u8]>)> =
            Vec::with_capacity(admitted.len());
        for (offset, data) in admitted {
            if let Some((merged_offset, merged_data)) = coalesced.last_mut() {
                let merged_end = merged_offset.saturating_add(merged_data.len() as u64);
                if offset <= merged_end {
                    let relative = (offset - *merged_offset) as usize;
                    let needed = relative.saturating_add(data.len());
                    let merged_data = merged_data.to_mut();
                    if needed > merged_data.len() {
                        merged_data.resize(needed, 0);
                    }
                    merged_data[relative..relative + data.len()].copy_from_slice(data);
                    continue;
                }
            }
            coalesced.push((offset, std::borrow::Cow::Borrowed(data)));
        }
        let projected_len = coalesced.iter().try_fold(
            self.chunks
                .get(chunk_id)
                .map_or(0u64, |state| state.data.len() as u64),
            |acc, (offset, data)| -> Result<u64, ObjectError> {
                Ok(acc.max(validate_pipeline_range(
                    "chunk write",
                    *offset,
                    data.len() as u64,
                )?))
            },
        )?;
        let projected_physical_bytes =
            self.projected_physical_bytes_for_chunk_len(chunk_id, projected_len)?;
        let service_mode_limited = self.writable_admission_error().is_some();
        let capacity_limited =
            self.capacity_bytes > 0 && projected_physical_bytes > self.capacity_bytes;
        let coalesced_ranges = coalesced.len() as u64;
        let coalesced_bytes = coalesced.iter().try_fold(0u64, |acc, (_, data)| {
            acc.checked_add(data.len() as u64).ok_or_else(|| {
                ObjectError::InvalidArgument(
                    "chunk batch write coalesced byte count overflows".to_string(),
                )
            })
        })?;
        let qos_limited =
            self.mutation_qos_would_limit(coalesced_ranges, coalesced_bytes, 0, 0, now_millis());
        Ok(ChunkWriteAdmission {
            can_accept: !service_mode_limited
                && !capacity_limited
                && !budget_limited
                && !qos_limited,
            write_count: input_ranges,
            coalesced_ranges,
            payload_bytes: input_bytes,
            max_write_bytes,
            selected_write_range_count,
            selected_write_payload_bytes,
            write_start_range_index,
            next_write_range_index,
            write_has_more,
            min_read_index: 0,
            min_commit_index: 0,
            projected_physical_bytes,
            capacity_bytes: self.capacity_bytes,
            capacity_free_bytes: self.capacity_free_bytes(),
            service_mode_limited,
            capacity_limited,
            budget_limited,
            qos_limited,
        })
    }

}
