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
                (chunk_id.clone(), offset),
                StoredPayload {
                    len: page_len as u64,
                    etag_fnv1a64: fnv1a64(&data),
                },
            );
            self.physical_written_bytes =
                saturating_add(self.physical_written_bytes, page_len as u64);
            return Ok(vec![ObjectExtent {
                chunk_id,
                offset,
                length: page_len as u64,
            }]);
        }
        let chunk_id = self.ensure_writable_chunk(page_len as u64)?;
        let slab = self
            .slabs
            .get_mut(&chunk_id)
            .ok_or_else(|| ObjectError::NotFound("allocated segment missing".to_string()))?;
        let offset = slab.next_offset;
        slab.next_offset = slab
            .next_offset
            .checked_add(page_len as u64)
            .ok_or_else(|| ObjectError::InvalidArgument("segment offset overflow".to_string()))?;
        slab.logical_bytes = slab.next_offset;
        slab.live_bytes = saturating_add(slab.live_bytes, page_len as u64);
        self.chunk_service
            .write_chunk_range(&chunk_id, offset, &data)?;
        self.payloads.insert(
            (chunk_id.clone(), offset),
            StoredPayload {
                len: page_len as u64,
                etag_fnv1a64: fnv1a64(&data),
            },
        );
        self.physical_written_bytes =
            saturating_add(self.physical_written_bytes, page_len as u64);
        Ok(vec![ObjectExtent {
            chunk_id,
            offset,
            length: page_len as u64,
        }])
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
            let slab = self
                .slabs
                .get_mut(&chunk_id)
                .ok_or_else(|| ObjectError::NotFound("allocated segment missing".to_string()))?;
            let offset = slab.next_offset;
            slab.next_offset = slab
                .next_offset
                .checked_add(page_len as u64)
                .ok_or_else(|| {
                    ObjectError::InvalidArgument("segment offset overflow".to_string())
                })?;
            slab.logical_bytes = slab.next_offset;
            slab.live_bytes = saturating_add(slab.live_bytes, page_len as u64);
            self.chunk_service.write_chunk_range(
                &chunk_id,
                offset,
                &data[cursor..cursor + page_len],
            )?;
            self.payloads.insert(
                (chunk_id.clone(), offset),
                StoredPayload {
                    len: page_len as u64,
                    etag_fnv1a64: fnv1a64(&data[cursor..cursor + page_len]),
                },
            );
            self.physical_written_bytes =
                saturating_add(self.physical_written_bytes, page_len as u64);
            extents.push(ObjectExtent {
                chunk_id,
                offset,
                length: page_len as u64,
            });
            cursor += page_len;
        }
        Ok(extents)
    }

    /// Takes the first freed range that fits, so an overwrite reuses the space
    /// its previous version left instead of growing the slab. Returns `None`
    /// unless the store was asked for this.
    fn take_reusable_stale_range(&mut self, length: u64) -> Option<(ChunkId, u64)> {
        if !self.options.reuse_stale_chunk_space || length == 0 {
            return None;
        }
        let (chunk_id, index) = self.slabs.iter().find_map(|(chunk_id, info)| {
            info.stale_ranges
                .iter()
                .position(|range| range.length >= length)
                .map(|index| (chunk_id.clone(), index))
        })?;
        let info = self.slabs.get_mut(&chunk_id)?;
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
                "extent length exceeds segment_size".to_string(),
            ));
        }
        if let Some((id, _)) = self
            .slabs
            .iter()
            .find(|(_, info)| info.next_offset + length <= self.options.chunk_size)
        {
            return Ok(id.clone());
        }
        let id = self.next_chunk_id;
        self.next_chunk_id = self
            .next_chunk_id
            .checked_add(1)
            .ok_or_else(|| ObjectError::InvalidArgument("segment id overflow".to_string()))?;
        let chunk_id = ChunkId {
            volume_id: (id - 1) % self.options.volume_count,
            id,
        };
        self.slabs.insert(
            chunk_id.clone(),
            ChunkInfo {
                chunk_id: chunk_id.clone(),
                logical_bytes: 0,
                live_bytes: 0,
                stale_bytes: 0,
                discarded_bytes: 0,
                next_offset: 0,
                stale_ranges: Vec::new(),
                discarded_ranges: Vec::new(),
            },
        );
        Ok(chunk_id)
    }

    fn mark_extents_stale(&mut self, extents: &[ObjectExtent]) -> Result<(), ObjectError> {
        let mut grouped_ranges: BTreeMap<ChunkId, Vec<DataRange>> = BTreeMap::new();
        for extent in extents {
            extent
                .offset
                .checked_add(extent.length)
                .ok_or_else(|| ObjectError::InvalidArgument("extent overflows".to_string()))?;
            if !self.slabs.contains_key(&extent.chunk_id) {
                return Err(ObjectError::NotFound("extent segment missing".to_string()));
            }
            // The range is about to be discarded, so its payload entry goes too:
            // nothing references an extent marked stale (when versioning is on the
            // old metadata is kept in `versions` and this function is not called),
            // and leaving the entry behind would both retain garbage and name a
            // range the page service can no longer return.
            self.payloads
                .remove(&(extent.chunk_id.clone(), extent.offset));
            grouped_ranges
                .entry(extent.chunk_id.clone())
                .or_default()
                .push(DataRange {
                    offset: extent.offset,
                    length: extent.length,
                });
        }

        let mut discard_requests = Vec::new();
        for (chunk_id, ranges) in grouped_ranges {
            let merged_ranges = merge_ranges(ranges);
            if merged_ranges.is_empty() {
                continue;
            }
            let slab = self
                .slabs
                .get_mut(&chunk_id)
                .ok_or_else(|| ObjectError::NotFound("extent segment missing".to_string()))?;
            let stale_bytes = merged_ranges
                .iter()
                .fold(0u64, |acc, range| saturating_add(acc, range.length));
            slab.live_bytes = slab.live_bytes.saturating_sub(stale_bytes);
            slab.stale_bytes = saturating_add(slab.stale_bytes, stale_bytes);
            discard_requests.extend(merged_ranges.iter().map(|range| ChunkRangeRequest {
                chunk_id: chunk_id.clone(),
                offset: range.offset,
                length: range.length,
            }));
            slab.stale_ranges.extend(merged_ranges);
            slab.stale_ranges = merge_ranges(std::mem::take(&mut slab.stale_ranges));
        }
        if !discard_requests.is_empty() {
            self.chunk_service
                .discard_slab_ranges(discard_requests)?;
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
            let extent_end = fast_cursor.checked_add(extent.length).ok_or_else(|| {
                ObjectError::InvalidArgument("object extent overflows".to_string())
            })?;
            if range_offset >= extent_start && range_end <= extent_end {
                return self.chunk_service.read_chunk_range_bounded_by_chunks(
                    &extent.chunk_id,
                    extent.offset + (range_offset - extent_start),
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
        for extent in extents {
            let extent_start = object_cursor;
            let extent_end = object_cursor.checked_add(extent.length).ok_or_else(|| {
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
                extent.offset + local_offset,
                local_len,
                remaining_budget,
                remaining_chunk_budget,
            )?;
            chunk_reads = chunk_reads.saturating_add(read_stats.chunk_reads);
            merge_page_read_stats(&mut stats, read_stats);
            out.extend_from_slice(&payload);
        }
        Ok((out, stats))
    }
}
