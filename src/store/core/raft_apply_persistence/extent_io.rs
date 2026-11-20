// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    fn allocate_extents_owned(&mut self, data: Vec<u8>) -> Result<Vec<ObjectExtent>, ObjectError> {
        let max_extent = self.options.max_extent_bytes.max(1) as usize;
        if data.is_empty() || data.len() > max_extent {
            return self.allocate_extents(&data);
        }
        let page_len = data.len();
        if let Some((chunk_id, offset)) = self.take_reusable_stale_range(page_len as u64) {
            self.chunk_service
                .write_chunk_range(&chunk_id, offset, &data)?;
            self.payloads.insert(
                chunk_id.clone(),
                offset,
                StoredPayload {
                    len: page_len as u64,
                    etag_fnv1a64: fnv1a64(&data),
                },
            );
            self.physical_written_bytes =
                saturating_add(self.physical_written_bytes, page_len as u64);
            return Ok(vec![ObjectExtent::new(chunk_id, offset, page_len as u64)?]);
        }
        let chunk_id = self.ensure_writable_chunk(page_len as u64)?;
        let chunk = self
            .chunks
            .get_mut(&chunk_id)
            .map(std::sync::Arc::make_mut)
            .ok_or_else(|| ObjectError::NotFound("allocated chunk missing".to_string()))?;
        let offset = chunk.next_offset;
        chunk.next_offset = chunk
            .next_offset
            .checked_add(page_len as u64)
            .ok_or_else(|| ObjectError::InvalidArgument("chunk offset overflow".to_string()))?;
        chunk.logical_bytes = chunk.next_offset;
        chunk.live_bytes = saturating_add(chunk.live_bytes, page_len as u64);
        self.chunk_service
            .write_chunk_range(&chunk_id, offset, &data)?;
        self.payloads.insert(
            chunk_id.clone(),
            offset,
            StoredPayload {
                len: page_len as u64,
                etag_fnv1a64: fnv1a64(&data),
            },
        );
        self.physical_written_bytes =
            saturating_add(self.physical_written_bytes, page_len as u64);
        Ok(vec![ObjectExtent::new(chunk_id, offset, page_len as u64)?])
    }

    fn allocate_extents(&mut self, data: &[u8]) -> Result<Vec<ObjectExtent>, ObjectError> {
        let max_extent = self.options.max_extent_bytes.max(1) as usize;
        let estimated_extents = data
            .len()
            .saturating_add(max_extent.saturating_sub(1))
            / max_extent;
        let mut extents = Vec::with_capacity(estimated_extents);
        let mut cursor = 0usize;
        while cursor < data.len() {
            let remaining = data.len() - cursor;
            let page_len = remaining.min(max_extent);
            let chunk_id = self.ensure_writable_chunk(page_len as u64)?;
            let chunk = self
                .chunks
                .get_mut(&chunk_id)
                .map(std::sync::Arc::make_mut)
                .ok_or_else(|| ObjectError::NotFound("allocated chunk missing".to_string()))?;
            let offset = chunk.next_offset;
            chunk.next_offset = chunk
                .next_offset
                .checked_add(page_len as u64)
                .ok_or_else(|| {
                    ObjectError::InvalidArgument("chunk offset overflow".to_string())
                })?;
            chunk.logical_bytes = chunk.next_offset;
            chunk.live_bytes = saturating_add(chunk.live_bytes, page_len as u64);
            self.chunk_service.write_chunk_range(
                &chunk_id,
                offset,
                &data[cursor..cursor + page_len],
            )?;
            self.payloads.insert(
                chunk_id.clone(),
                offset,
                StoredPayload {
                    len: page_len as u64,
                    etag_fnv1a64: fnv1a64(&data[cursor..cursor + page_len]),
                },
            );
            self.physical_written_bytes =
                saturating_add(self.physical_written_bytes, page_len as u64);
            extents.push(ObjectExtent::new(chunk_id, offset, page_len as u64)?);
            cursor += page_len;
        }
        Ok(extents)
    }

    /// Extends the object's last extent with the first extent an append just allocated, when the
    /// two are one contiguous byte range of one chunk, and returns the extended extent.
    ///
    /// `allocate_extents` places each piece at the first chunk with room, at its write cursor, so
    /// one writer appending to one object lands every append right after the one before: without
    /// this the object's extent list grows by one entry per append. Merging keeps it at one entry
    /// per `max_extent_bytes` of appended data, which is also what `allocate_extents` would have
    /// produced for the same bytes written at once.
    ///
    /// What the merge keeps true, because the snapshot validator checks it: the payload entry at
    /// the last extent's offset becomes the whole merged range (its length, and its digest resumed
    /// over the appended bytes, which is exactly the digest of the concatenation), and the entry
    /// the allocation inserted for the merged piece is removed, so there is one entry per extent
    /// and none left behind for `mark_extents_stale` to miss. The merged length never exceeds
    /// `max_extent_bytes`, so no extent is longer than the allocator could have made it.
    ///
    /// Nothing is merged when the bucket keeps versions (the superseded metadata still names the
    /// old, shorter extent and its payload entry), or when either extent has no payload entry of
    /// the length it names.
    fn merge_first_appended_extent_into_tail(
        &mut self,
        tail: Option<&ObjectExtent>,
        appended: &mut Vec<ObjectExtent>,
        data: &[u8],
        bucket_keeps_versions: bool,
    ) -> Result<Option<ObjectExtent>, ObjectError> {
        let (Some(tail), Some(first)) = (tail, appended.first().cloned()) else {
            return Ok(None);
        };
        if bucket_keeps_versions
            || tail.chunk_id != first.chunk_id
            || tail.end() != first.offset()
            || tail.length().saturating_add(first.length()) > self.options.max_extent_bytes
        {
            return Ok(None);
        }
        let Some(tail_payload) = self.payloads.get(&tail.chunk_id, tail.offset()).cloned() else {
            return Ok(None);
        };
        if tail_payload.len != tail.length() {
            return Ok(None);
        }
        let merged = ObjectExtent::new(
            tail.chunk_id.clone(),
            tail.offset(),
            tail.length() + first.length(),
        )?;
        appended.remove(0);
        self.payloads.remove(&first.chunk_id, first.offset());
        self.payloads.insert(
            tail.chunk_id.clone(),
            tail.offset(),
            StoredPayload {
                len: merged.length(),
                etag_fnv1a64: fnv1a64_resumed(tail_payload.etag_fnv1a64, &data[..first.length() as usize]),
            },
        );
        Ok(Some(merged))
    }

    /// Takes the first freed range that fits, so an overwrite reuses the space
    /// its previous version left instead of growing the chunk. Returns `None`
    /// unless the store was asked for this.
    fn take_reusable_stale_range(&mut self, length: u64) -> Option<(ChunkId, u64)> {
        if !self.options.reuse_stale_chunk_space || length == 0 {
            return None;
        }
        let (chunk_id, index) = self.chunks.iter().find_map(|(chunk_id, info)| {
            info.stale_ranges
                .iter()
                .position(|range| range.length >= length)
                .map(|index| (chunk_id.clone(), index))
        })?;
        let info = std::sync::Arc::make_mut(self.chunks.get_mut(&chunk_id)?);
        let range = *info.stale_ranges.get(index)?;
        if range.length == length {
            info.stale_ranges.remove(index);
        } else {
            info.stale_ranges[index] = DataRange {
                offset: range.offset.saturating_add(length),
                length: range.length.saturating_sub(length),
            };
        }
        info.stale_bytes = info.stale_bytes.saturating_sub(length);
        info.live_bytes = saturating_add(info.live_bytes, length);
        Some((chunk_id, range.offset))
    }

    fn ensure_writable_chunk(&mut self, length: u64) -> Result<ChunkId, ObjectError> {
        if length > self.options.chunk_size {
            return Err(ObjectError::InvalidArgument(
                "extent length exceeds chunk_size".to_string(),
            ));
        }
        if let Some((id, _)) = self.chunks.iter().find(|(chunk_id, info)| {
            // NOT THE CHUNK BEING DRAINED. Allocating into it is how a compaction came to
            // rewrite its own source and then declare the result stale.
            self.compacting_chunk.as_ref() != Some(*chunk_id)
                && info.next_offset + length <= self.options.chunk_size
        }) {
            return Ok(id.clone());
        }
        let id = self.next_chunk_id;
        self.next_chunk_id = self
            .next_chunk_id
            .checked_add(1)
            .ok_or_else(|| ObjectError::InvalidArgument("chunk id overflow".to_string()))?;
        let chunk_id = ChunkId::new((id - 1) % self.options.volume_count, id)?;
        self.chunks.insert(
            chunk_id.clone(),
            std::sync::Arc::new(ChunkInfo {
                chunk_id: chunk_id.clone(),
                logical_bytes: 0,
                live_bytes: 0,
                stale_bytes: 0,
                discarded_bytes: 0,
                next_offset: 0,
                stale_ranges: Vec::new(),
                discarded_ranges: Vec::new(),
            }),
        );
        Ok(chunk_id)
    }

    fn mark_extents_stale(&mut self, extents: &[ObjectExtent]) -> Result<(), ObjectError> {
        let mut grouped_ranges: BTreeMap<ChunkId, Vec<DataRange>> = BTreeMap::new();
        for extent in extents {
            extent
                .offset()
                .checked_add(extent.length())
                .ok_or_else(|| ObjectError::InvalidArgument("extent overflows".to_string()))?;
            if !self.chunks.contains_key(&extent.chunk_id) {
                return Err(ObjectError::NotFound("extent chunk missing".to_string()));
            }
            // The range is about to be discarded, so its payload entry goes too:
            // nothing references an extent marked stale (when versioning is on the
            // old metadata is kept in `versions` and this function is not called),
            // and leaving the entry behind would both retain garbage and name a
            // range the page service can no longer return.
            self.payloads.remove(&extent.chunk_id, extent.offset());
            grouped_ranges
                .entry(extent.chunk_id.clone())
                .or_default()
                .push(DataRange {
                    offset: extent.offset(),
                    length: extent.length(),
                });
        }

        let mut discard_requests = Vec::new();
        for (chunk_id, ranges) in grouped_ranges {
            let merged_ranges = merge_ranges(ranges);
            if merged_ranges.is_empty() {
                continue;
            }
            let chunk = self
                .chunks
                .get_mut(&chunk_id)
                .map(std::sync::Arc::make_mut)
                .ok_or_else(|| ObjectError::NotFound("extent chunk missing".to_string()))?;
            let stale_bytes = merged_ranges
                .iter()
                .fold(0u64, |acc, range| saturating_add(acc, range.length));
            chunk.live_bytes = chunk.live_bytes.saturating_sub(stale_bytes);
            chunk.stale_bytes = saturating_add(chunk.stale_bytes, stale_bytes);
            discard_requests.extend(merged_ranges.iter().map(|range| ChunkRangeRequest {
                chunk_id: chunk_id.clone(),
                offset: range.offset,
                length: range.length,
            }));
            chunk.stale_ranges.extend(merged_ranges);
            chunk.stale_ranges = merge_ranges(std::mem::take(&mut chunk.stale_ranges));
        }
        if !discard_requests.is_empty() {
            self.chunk_service
                .discard_page_ranges(discard_requests)?;
        }
        Ok(())
    }

    fn read_extents(
        &self,
        extents: &[ObjectExtent],
        range_offset: u64,
        range_length: u64,
    ) -> Result<Vec<u8>, ObjectError> {
        self.read_extents_bounded(extents, range_offset, range_length, 0)
    }

    fn read_extents_bounded(
        &self,
        extents: &[ObjectExtent],
        range_offset: u64,
        range_length: u64,
        max_payload_bytes: u64,
    ) -> Result<Vec<u8>, ObjectError> {
        self.read_extents_bounded_by_chunks(
            extents,
            range_offset,
            range_length,
            max_payload_bytes,
            0,
        )
        .map(|data| data)
    }

    fn read_extents_bounded_by_chunks(
        &self,
        extents: &[ObjectExtent],
        range_offset: u64,
        range_length: u64,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
    ) -> Result<Vec<u8>, ObjectError> {
        self.read_extents_with_stats_bounded_by_chunks(
            extents,
            range_offset,
            range_length,
            max_payload_bytes,
            max_chunk_read_count,
        )
        .map(|(data, _)| data)
    }

    fn read_extents_with_stats(
        &self,
        extents: &[ObjectExtent],
        range_offset: u64,
        range_length: u64,
    ) -> Result<(Vec<u8>, ChunkReadStats), ObjectError> {
        self.read_extents_with_stats_bounded(extents, range_offset, range_length, 0)
    }

    fn read_extents_with_stats_bounded(
        &self,
        extents: &[ObjectExtent],
        range_offset: u64,
        range_length: u64,
        max_payload_bytes: u64,
    ) -> Result<(Vec<u8>, ChunkReadStats), ObjectError> {
        self.read_extents_with_stats_bounded_by_chunks(
            extents,
            range_offset,
            range_length,
            max_payload_bytes,
            0,
        )
    }

    fn read_extents_with_stats_bounded_by_chunks(
        &self,
        extents: &[ObjectExtent],
        range_offset: u64,
        range_length: u64,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
    ) -> Result<(Vec<u8>, ChunkReadStats), ObjectError> {
        if max_payload_bytes > 0 && range_length > max_payload_bytes {
            return Err(ObjectError::PreconditionFailed(
                "object extent read payload budget exceeded".to_string(),
            ));
        }
        if range_length == 0 {
            return Ok((Vec::new(), ChunkReadStats::default()));
        }
        let range_end = range_offset
            .checked_add(range_length)
            .ok_or_else(|| ObjectError::InvalidArgument("read range overflows".to_string()))?;
        let mut fast_cursor = 0u64;
        for extent in extents {
            let extent_start = fast_cursor;
            let extent_end = fast_cursor.checked_add(extent.length()).ok_or_else(|| {
                ObjectError::InvalidArgument("object extent overflows".to_string())
            })?;
            if range_offset >= extent_start && range_end <= extent_end {
                return self.chunk_service.read_chunk_range_bounded_by_chunks(
                    &extent.chunk_id,
                    extent.offset() + (range_offset - extent_start),
                    range_length,
                    max_payload_bytes,
                    max_chunk_read_count,
                );
            }
            if range_end <= extent_end {
                break;
            }
            fast_cursor = extent_end;
        }
        let capacity = usize::try_from(range_length)
            .map_err(|_| ObjectError::InvalidArgument("read range exceeds memory size".to_string()))?;
        let mut out = Vec::with_capacity(capacity);
        let mut stats = ChunkReadStats::default();
        let mut chunk_reads = 0u64;
        let mut object_cursor = 0u64;
        // A SPAN READ SERVES SEVERAL EXTENTS WITH ONE QUORUM READ. Every extent read pays a whole
        // chunk-range read -- admission, the page map, the head stats and a replica vote that
        // hashes what it returns -- and an object built by appends has one extent per append, so
        // a whole-object read of it was paying that fixed price once per append. It is only
        // taken when the caller set no payload or chunk-count budget, because a span reads the
        // bytes between two extents and a budget is a promise about what a read touches.
        let spans_allowed = max_payload_bytes == 0 && max_chunk_read_count == 0;
        let mut pending: Vec<(ObjectExtent, u64, u64)> = Vec::new();
        let mut pending_end = 0u64;
        macro_rules! flush_pending {
            () => {
                if !pending.is_empty() {
                    let batch = std::mem::take(&mut pending);
                    self.read_extent_batch(&batch, &mut out, &mut stats, &mut chunk_reads)?;
                }
            };
        }
        for extent in extents {
            let extent_start = object_cursor;
            let extent_end = object_cursor.checked_add(extent.length()).ok_or_else(|| {
                ObjectError::InvalidArgument("object extent overflows".to_string())
            })?;
            object_cursor = extent_end;
            if extent_end <= range_offset || extent_start >= range_end {
                continue;
            }
            let read_start = range_offset.max(extent_start);
            let read_end = range_end.min(extent_end);
            let local_offset = read_start - extent_start;
            let local_len = read_end - read_start;
            if spans_allowed {
                let abs = extent.offset() + local_offset;
                let joins = match pending.first() {
                    Some((first, first_abs, _)) => {
                        first.chunk_id == extent.chunk_id
                            && abs >= pending_end
                            && abs - pending_end <= SPAN_MAX_GAP_BYTES
                            && abs + local_len - *first_abs <= SPAN_MAX_BYTES
                    }
                    None => true,
                };
                if !joins {
                    flush_pending!();
                }
                pending.push((extent.clone(), abs, local_len));
                pending_end = abs + local_len;
                continue;
            }
            let remaining_budget = if max_payload_bytes == 0 {
                0
            } else {
                max_payload_bytes.saturating_sub(out.len() as u64)
            };
            let remaining_chunk_budget = if max_chunk_read_count == 0 {
                0
            } else {
                max_chunk_read_count.saturating_sub(chunk_reads)
            };
            let (payload, read_stats) = self.chunk_service.read_chunk_range_bounded_by_chunks(
                &extent.chunk_id,
                extent.offset() + local_offset,
                local_len,
                remaining_budget,
                remaining_chunk_budget,
            )?;
            chunk_reads = chunk_reads.saturating_add(read_stats.chunk_reads);
            merge_page_read_stats(&mut stats, read_stats);
            out.extend_from_slice(&payload);
        }
        flush_pending!();
        Ok((out, stats))
    }

    /// Read one batch of consecutive extents of one chunk with a single chunk-range read when
    /// that is possible, and one read per extent when it is not.
    ///
    /// The batch is `(extent, absolute chunk offset, length)` in object order. A batch of one,
    /// and a span that cannot be read, take the per-extent path -- the bytes between two extents
    /// belong to another object that may since have been discarded, and a failed read of them
    /// must not fail a read of this object, whose own bytes are all still there.
    fn read_extent_batch(
        &self,
        batch: &[(ObjectExtent, u64, u64)],
        out: &mut Vec<u8>,
        stats: &mut ChunkReadStats,
        chunk_reads: &mut u64,
    ) -> Result<(), ObjectError> {
        if batch.len() > 1 {
            let span_start = batch[0].1;
            let (_, last_abs, last_len) = &batch[batch.len() - 1];
            let span_len = last_abs + last_len - span_start;
            if let Ok((span, read_stats)) = self.chunk_service.read_chunk_range_bounded_by_chunks(
                &batch[0].0.chunk_id,
                span_start,
                span_len,
                0,
                0,
            ) {
                let sliced = batch.iter().all(|(_, abs, len)| {
                    let from = abs - span_start;
                    usize::try_from(from + len).map_or(false, |end| end <= span.len())
                });
                if sliced {
                    *chunk_reads = chunk_reads.saturating_add(read_stats.chunk_reads);
                    merge_page_read_stats(stats, read_stats);
                    for (_, abs, len) in batch {
                        let from = (abs - span_start) as usize;
                        out.extend_from_slice(&span[from..from + *len as usize]);
                    }
                    return Ok(());
                }
            }
        }
        for (extent, abs, len) in batch {
            let (payload, read_stats) = self.chunk_service.read_chunk_range_bounded_by_chunks(
                &extent.chunk_id,
                *abs,
                *len,
                0,
                0,
            )?;
            *chunk_reads = chunk_reads.saturating_add(read_stats.chunk_reads);
            merge_page_read_stats(stats, read_stats);
            out.extend_from_slice(&payload);
        }
        Ok(())
    }
}

/// The widest gap between two extents of one chunk that a span read will read across. Appends to
/// two objects that share a chunk interleave, so the neighbours of an object's extents are
/// usually another object's appends, a few hundred bytes each.
const SPAN_MAX_GAP_BYTES: u64 = 4096;
/// The longest span one read covers: a page, which is what one replica read already serves.
const SPAN_MAX_BYTES: u64 = 1 << 20;
