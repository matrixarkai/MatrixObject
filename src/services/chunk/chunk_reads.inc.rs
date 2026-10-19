impl MatrixReplicaService {
    pub fn read_chunk(
        &self,
        chunk_id: &PageId,
        offset: u64,
        length: u64,
    ) -> Result<ChunkReadResult, ObjectError> {
        self.read_chunk_bounded(chunk_id, offset, length, 0)
    }

    pub fn read_chunk_bounded(
        &self,
        chunk_id: &PageId,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
    ) -> Result<ChunkReadResult, ObjectError> {
        if max_payload_bytes > 0 && length > max_payload_bytes {
            return Err(ObjectError::PreconditionFailed(
                "chunk read payload budget exceeded".to_string(),
            ));
        }
        self.read_chunk_with_cache_status(chunk_id, offset, length)
            .map(|(read, _)| read)
    }

    pub fn read_chunk_ranges(
        &self,
        chunk_id: &PageId,
        ranges: Vec<DataRange>,
        max_payload_bytes: u64,
    ) -> Result<(Vec<ChunkRangeReadResult>, ChunkReadBatchStats), ObjectError> {
        let mut stats = ChunkReadBatchStats::default();
        let mut selected: Vec<DataRange> = Vec::with_capacity(ranges.len());
        let mut selected_in_merge_order = true;
        for range in ranges {
            if range.length == 0 {
                continue;
            }
            validate_pipeline_data_range("chunk read", &range)?;
            stats.input_ranges = stats.input_ranges.saturating_add(1);
            stats.input_bytes = stats.input_bytes.checked_add(range.length).ok_or_else(|| {
                ObjectError::InvalidArgument("chunk batch read byte count overflows".to_string())
            })?;
            let next_payload = stats
                .payload_bytes
                .checked_add(range.length)
                .ok_or_else(|| {
                    ObjectError::InvalidArgument(
                        "chunk batch read payload byte count overflows".to_string(),
                    )
                })?;
            if max_payload_bytes > 0 && next_payload > max_payload_bytes {
                stats.budget_limited = true;
                if selected.is_empty() {
                    return Err(ObjectError::PreconditionFailed(
                        "chunk read batch payload budget exceeded".to_string(),
                    ));
                }
                break;
            }
            stats.payload_bytes = next_payload;
            if let Some(previous) = selected.last() {
                if previous.offset > range.offset {
                    selected_in_merge_order = false;
                }
            }
            selected.push(range);
        }
        if selected.is_empty() {
            return Ok((Vec::new(), stats));
        }
        if selected.len() == 1 {
            let range = selected.pop().ok_or_else(|| {
                ObjectError::Corruption("chunk batch read missing selected range".to_string())
            })?;
            let (read, cache_hit) =
                self.read_chunk_with_cache_status(chunk_id, range.offset, range.length)?;
            stats.coalesced_ranges = 1;
            stats.coalesced_bytes = range.length;
            if cache_hit {
                stats.cache_hits = 1;
            } else {
                stats.physical_reads = 1;
            }
            return Ok((
                vec![ChunkRangeReadResult {
                    range,
                    data: read.data,
                    cache_hit,
                }],
                stats,
            ));
        }
        struct MergedRead {
            offset: u64,
            length: u64,
            ref_count: usize,
            data: Option<Vec<u8>>,
            cache_hit: bool,
        }
        let mut merged_ranges: Vec<DataRange> = Vec::with_capacity(selected.len());
        let mut range_to_merged = vec![0usize; selected.len()];
        let mut append_selected_range = |selected_index: usize| -> Result<(), ObjectError> {
            let range = &selected[selected_index];
            let range_end = range.end()?;
            if let Some(last) = merged_ranges.last_mut() {
                let last_end = last.end()?;
                if range.offset <= last_end {
                    if range_end > last_end {
                        last.length = range_end - last.offset;
                    }
                    range_to_merged[selected_index] = merged_ranges.len() - 1;
                    return Ok(());
                }
            }
            merged_ranges.push(*range);
            range_to_merged[selected_index] = merged_ranges.len() - 1;
            Ok(())
        };
        if selected_in_merge_order {
            for selected_index in 0..selected.len() {
                append_selected_range(selected_index)?;
            }
        } else {
            let mut ordered_indexes: Vec<usize> = (0..selected.len()).collect();
            ordered_indexes.sort_by_key(|index| selected[*index].offset);
            for selected_index in ordered_indexes {
                append_selected_range(selected_index)?;
            }
        }

        let mut merged_reads = Vec::with_capacity(merged_ranges.len());
        for merged in merged_ranges {
            let (read, cache_hit) =
                self.read_chunk_with_cache_status(chunk_id, merged.offset, merged.length)?;
            stats.coalesced_ranges = stats.coalesced_ranges.saturating_add(1);
            stats.coalesced_bytes = stats.coalesced_bytes.saturating_add(merged.length);
            if cache_hit {
                stats.cache_hits = stats.cache_hits.saturating_add(1);
            } else {
                stats.physical_reads = stats.physical_reads.saturating_add(1);
            }
            merged_reads.push(MergedRead {
                offset: merged.offset,
                length: merged.length,
                ref_count: 0,
                data: Some(read.data),
                cache_hit,
            });
        }
        for merged_index in &range_to_merged {
            let Some(merged) = merged_reads.get_mut(*merged_index) else {
                return Err(ObjectError::Corruption(
                    "chunk batch read merged range lookup failed".to_string(),
                ));
            };
            merged.ref_count = merged.ref_count.saturating_add(1);
        }
        let mut out = Vec::with_capacity(selected.len());
        for (selected_index, range) in selected.into_iter().enumerate() {
            let range_end = validate_pipeline_data_range("chunk read", &range)?;
            let merged_index = range_to_merged
                .get(selected_index)
                .copied()
                .ok_or_else(|| {
                    ObjectError::Corruption(
                        "chunk batch read missing coalesced payload".to_string(),
                    )
                })?;
            let merged = merged_reads.get_mut(merged_index).ok_or_else(|| {
                ObjectError::Corruption("chunk batch read merged range lookup failed".to_string())
            })?;
            let merged_end = merged.offset.checked_add(merged.length).ok_or_else(|| {
                ObjectError::InvalidArgument("chunk batch merged range overflows".to_string())
            })?;
            if merged.offset > range.offset || merged_end < range_end {
                return Err(ObjectError::Corruption(
                    "chunk batch read missing coalesced payload".to_string(),
                ));
            }
            let start = usize::try_from(range.offset - merged.offset).map_err(|_| {
                ObjectError::InvalidArgument(
                    "chunk batch read offset exceeds memory size".to_string(),
                )
            })?;
            let length = usize::try_from(range.length).map_err(|_| {
                ObjectError::InvalidArgument(
                    "chunk batch read length exceeds memory size".to_string(),
                )
            })?;
            let end = start.checked_add(length).ok_or_else(|| {
                ObjectError::InvalidArgument(
                    "chunk batch read payload range overflows".to_string(),
                )
            })?;
            let payload_len = merged
                .data
                .as_ref()
                .ok_or_else(|| {
                    ObjectError::Corruption(
                        "chunk batch read coalesced payload was already consumed".to_string(),
                    )
                })?
                .len();
            if end > payload_len {
                return Err(ObjectError::Corruption(
                    "chunk batch read coalesced payload is shorter than range".to_string(),
                ));
            }
            let exact_single_use =
                range.offset == merged.offset && range.length == merged.length && merged.ref_count == 1;
            let data = if exact_single_use {
                merged.data.take().ok_or_else(|| {
                    ObjectError::Corruption(
                        "chunk batch read coalesced payload was already consumed".to_string(),
                    )
                })?
            } else {
                let payload = merged.data.as_ref().ok_or_else(|| {
                    ObjectError::Corruption(
                        "chunk batch read coalesced payload was already consumed".to_string(),
                    )
                })?;
                payload[start..end].to_vec()
            };
            out.push(ChunkRangeReadResult {
                range,
                data,
                cache_hit: merged.cache_hit,
            });
        }
        Ok((out, stats))
    }
}
