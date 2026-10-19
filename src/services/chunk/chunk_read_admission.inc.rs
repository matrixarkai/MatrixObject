impl MatrixReplicaService {
    pub fn can_accept_chunk_reads(
        &self,
        chunk_id: &PageId,
        ranges: Vec<DataRange>,
        max_payload_bytes: u64,
    ) -> Result<ChunkReadAdmission, ObjectError> {
        let mut admission = ChunkReadAdmission {
            max_payload_bytes,
            ..ChunkReadAdmission::default()
        };
        let mut selected = Vec::with_capacity(ranges.len());
        for range in ranges {
            if range.length == 0 {
                continue;
            }
            validate_pipeline_data_range("chunk read", &range)?;
            admission.stats.input_ranges = admission.stats.input_ranges.saturating_add(1);
            admission.stats.input_bytes = admission
                .stats
                .input_bytes
                .checked_add(range.length)
                .ok_or_else(|| {
                    ObjectError::InvalidArgument(
                        "chunk read admission byte count overflows".to_string(),
                    )
                })?;
            admission.stats.payload_bytes = admission
                .stats
                .payload_bytes
                .checked_add(range.length)
                .ok_or_else(|| {
                    ObjectError::InvalidArgument(
                        "chunk read admission payload byte count overflows".to_string(),
                    )
                })?;
            selected.push(range);
        }
        admission.stats.budget_limited =
            max_payload_bytes > 0 && admission.stats.payload_bytes > max_payload_bytes;
        let Some(chunk) = self.chunks.get(chunk_id) else {
            admission.missing_chunk = true;
            admission.partial_status = "chunk is missing".to_string();
            return Ok(admission);
        };
        let read_intersects_discarded_range = |range: &DataRange, range_end: u64| {
            for discarded in &chunk.discarded_ranges {
                if discarded.length == 0 {
                    continue;
                }
                if discarded.offset >= range_end {
                    break;
                }
                if discarded.offset.saturating_add(discarded.length) <= range.offset {
                    continue;
                }
                if ranges_intersect(
                    range.offset,
                    range.length,
                    discarded.offset,
                    discarded.length,
                ) {
                    return true;
                }
            }
            false
        };
        if selected.len() == 1 {
            let range = selected.pop().ok_or_else(|| {
                ObjectError::Corruption("chunk read admission missing selected range".to_string())
            })?;
            admission.stats.coalesced_ranges = 1;
            admission.stats.coalesced_bytes = range.length;
            let range_end = validate_pipeline_data_range("chunk read", &range)?;
            if range_end > chunk.logical_length {
                admission.range_invalid = true;
            } else if read_intersects_discarded_range(&range, range_end) {
                admission.discarded_range_conflict = true;
            } else {
                let cache_hit = self
                    .read_cache
                    .borrow()
                    .get(&(chunk_id.clone(), range.offset, range.length))
                    .is_some_and(|entry| {
                        entry.result.write_sequence == chunk.write_sequence
                            && entry.result.checksum == chunk.checksum
                            && etag_matches(&entry.result.data, &entry.range_checksum)
                    });
                if cache_hit {
                    admission.stats.cache_hits = 1;
                } else {
                    admission.stats.physical_reads = 1;
                }
            }
            admission.can_accept = !admission.stats.budget_limited
                && !admission.range_invalid
                && !admission.discarded_range_conflict;
            admission.partial_status = if admission.can_accept {
                "ok".to_string()
            } else if admission.stats.budget_limited {
                "chunk read batch payload budget exceeded".to_string()
            } else if admission.range_invalid {
                "chunk read exceeds logical length".to_string()
            } else {
                "chunk read intersects discarded range".to_string()
            };
            return Ok(admission);
        }
        let merged = merge_ranges(selected);
        let read_cache = self.read_cache.borrow();
        for range in &merged {
            admission.stats.coalesced_ranges = admission.stats.coalesced_ranges.saturating_add(1);
            admission.stats.coalesced_bytes =
                admission.stats.coalesced_bytes.saturating_add(range.length);
            let range_end = validate_pipeline_data_range("chunk read", range)?;
            if range_end > chunk.logical_length {
                admission.range_invalid = true;
                continue;
            }
            if read_intersects_discarded_range(range, range_end) {
                admission.discarded_range_conflict = true;
                continue;
            }
            let cache_hit = read_cache
                .get(&(chunk_id.clone(), range.offset, range.length))
                .is_some_and(|entry| {
                    entry.result.write_sequence == chunk.write_sequence
                        && entry.result.checksum == chunk.checksum
                        && etag_matches(&entry.result.data, &entry.range_checksum)
                });
            if cache_hit {
                admission.stats.cache_hits = admission.stats.cache_hits.saturating_add(1);
            } else {
                admission.stats.physical_reads = admission.stats.physical_reads.saturating_add(1);
            }
        }
        admission.can_accept = !admission.stats.budget_limited
            && !admission.missing_chunk
            && !admission.range_invalid
            && !admission.discarded_range_conflict;
        admission.partial_status = if admission.can_accept {
            "ok".to_string()
        } else if admission.stats.budget_limited {
            "chunk read batch payload budget exceeded".to_string()
        } else if admission.range_invalid {
            "chunk read exceeds logical length".to_string()
        } else if admission.discarded_range_conflict {
            "chunk read intersects discarded range".to_string()
        } else {
            "chunk is missing".to_string()
        };
        Ok(admission)
    }
}
