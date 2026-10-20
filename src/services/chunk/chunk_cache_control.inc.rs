// SPDX-License-Identifier: Apache-2.0
impl MatrixReplicaService {
    pub fn prewarm_chunk_ranges(
        &mut self,
        page_id: &PageId,
        ranges: Vec<DataRange>,
        max_bytes: u64,
    ) -> Result<CacheControlStats, ObjectError> {
        self.prewarm_chunk_ranges_borrowed(page_id, &ranges, max_bytes)
    }

    pub fn prewarm_chunk_ranges_borrowed(
        &mut self,
        page_id: &PageId,
        ranges: &[DataRange],
        max_bytes: u64,
    ) -> Result<CacheControlStats, ObjectError> {
        let mut stats = CacheControlStats::default();
        if ranges.is_empty() {
            return Ok(stats);
        }
        for range in ranges {
            if range.length == 0 {
                continue;
            }
            stats.requested_ranges = stats.requested_ranges.saturating_add(1);
            stats.requested_bytes = stats.requested_bytes.saturating_add(range.length);
        }
        if self.max_read_cache_bytes.get() == 0 {
            stats.skipped_bytes = stats.requested_bytes;
            stats.budget_limited = stats.requested_bytes > 0;
            return Ok(stats);
        }
        let max_cacheable_read_bytes = self.max_cacheable_read_bytes.get();
        let selected = merge_range_slice(ranges);
        let mut budget_left = max_bytes;
        for range in selected {
            let mut selected_range = range;
            if max_bytes > 0 {
                if budget_left == 0 {
                    stats.skipped_bytes = stats.skipped_bytes.saturating_add(selected_range.length);
                    stats.budget_limited = true;
                    continue;
                }
                if selected_range.length > budget_left {
                    stats.skipped_bytes = stats
                        .skipped_bytes
                        .saturating_add(selected_range.length.saturating_sub(budget_left));
                    selected_range.length = budget_left;
                    stats.budget_limited = true;
                }
            }
            if selected_range.length == 0 {
                continue;
            }
            if max_cacheable_read_bytes > 0 && selected_range.length > max_cacheable_read_bytes {
                stats.skipped_bytes = stats.skipped_bytes.saturating_add(selected_range.length);
                stats.budget_limited = true;
                continue;
            }
            let cache_key = (
                page_id.clone(),
                selected_range.offset,
                selected_range.length,
            );
            let last_access_sequence = self.next_read_cache_access_sequence();
            let cache_hit = {
                let mut cache = self.read_cache.borrow_mut();
                if let Some(entry) = cache.get_mut(&cache_key) {
                    entry.last_access_sequence = last_access_sequence;
                    true
                } else {
                    false
                }
            };
            if cache_hit {
                self.record_read_cache_access(last_access_sequence, cache_key);
                stats.cache_hits = stats.cache_hits.saturating_add(1);
                stats.selected_ranges = stats.selected_ranges.saturating_add(1);
                stats.selected_bytes = stats.selected_bytes.saturating_add(selected_range.length);
                if max_bytes > 0 {
                    budget_left = budget_left.saturating_sub(selected_range.length);
                }
                continue;
            }
            let (_, cache_hit) = self.read_chunk_with_cache_status(
                page_id,
                selected_range.offset,
                selected_range.length,
                // WARMING IS THE POINT OF THIS CALL, not a side effect of it: a prefetch
                // that bypassed the cache would read bytes and throw them away.
                ReadCacheIntent::Fill,
            )?;
            if cache_hit {
                stats.cache_hits = stats.cache_hits.saturating_add(1);
            } else {
                stats.physical_reads = stats.physical_reads.saturating_add(1);
            }
            stats.selected_ranges = stats.selected_ranges.saturating_add(1);
            stats.selected_bytes = stats.selected_bytes.saturating_add(selected_range.length);
            if max_bytes > 0 {
                budget_left = budget_left.saturating_sub(selected_range.length);
            }
        }
        Ok(stats)
    }

    pub fn invalidate_chunk_cache(
        &self,
        page_id: &PageId,
        offset: u64,
        length: u64,
    ) -> CacheControlStats {
        self.invalidate_chunk_cache_bounded(page_id, offset, length, 0)
    }

    pub fn invalidate_chunk_cache_bounded(
        &self,
        page_id: &PageId,
        offset: u64,
        length: u64,
        max_bytes: u64,
    ) -> CacheControlStats {
        let mut stats = CacheControlStats::default();
        // The page-validation memo is keyed by (write_sequence, checksum) so a
        // mutation already forces revalidation; dropping it here additionally stops
        // a removed chunk from leaving its entry behind forever.
        self.page_validation.borrow_mut().remove(page_id);
        self.read_cache_invalidations
            .set(self.read_cache_invalidations.get().saturating_add(1));
        if max_bytes == 0 {
            let keys: Vec<ReplicaReadCacheRangeKey> = self
                .read_cache_by_chunk
                .borrow()
                .get(page_id)
                .into_iter()
                .flat_map(|keys| keys.iter())
                .filter(|(cached_offset, cached_length)| {
                    ranges_intersect(offset, length, *cached_offset, *cached_length)
                })
                .copied()
                .collect();
            for (cached_offset, cached_length) in keys {
                let key = (page_id.clone(), cached_offset, cached_length);
                if let Some(entry) = self.remove_read_cache_entry(&key) {
                    stats.invalidated_entries = stats.invalidated_entries.saturating_add(1);
                    let removed_bytes = entry.result.data.len() as u64;
                    self.read_cache_invalidated_entries
                        .set(self.read_cache_invalidated_entries.get().saturating_add(1));
                    self.read_cache_invalidated_bytes.set(
                        self.read_cache_invalidated_bytes
                            .get()
                            .saturating_add(removed_bytes),
                    );
                    stats.selected_bytes = stats.selected_bytes.saturating_add(removed_bytes);
                    stats.selected_ranges = stats.selected_ranges.saturating_add(1);
                }
            }
            return stats;
        }
        let mut keys: Vec<(ReplicaReadCacheRangeKey, u64)> = {
            let cache = self.read_cache.borrow();
            self.read_cache_by_chunk
                .borrow()
                .get(page_id)
                .into_iter()
                .flat_map(|page_keys| page_keys.iter())
                .filter_map(|(cached_offset, cached_length)| {
                    if !ranges_intersect(offset, length, *cached_offset, *cached_length) {
                        return None;
                    }
                    let key = (page_id.clone(), *cached_offset, *cached_length);
                    cache
                        .get(&key)
                        .map(|entry| ((*cached_offset, *cached_length), entry.result.data.len() as u64))
                })
                .collect()
        };
        keys.sort_by(|left, right| {
            left.0.0.cmp(&right.0.0).then(left.0.1.cmp(&right.0.1))
        });
        let mut budget_left = max_bytes;
        for ((cached_offset, cached_length), bytes) in keys {
            if max_bytes > 0 && bytes > budget_left {
                stats.skipped_bytes = stats.skipped_bytes.saturating_add(bytes);
                stats.budget_limited = true;
                continue;
            }
            let key = (page_id.clone(), cached_offset, cached_length);
            if let Some(entry) = self.remove_read_cache_entry(&key) {
                stats.invalidated_entries = stats.invalidated_entries.saturating_add(1);
                let removed_bytes = entry.result.data.len() as u64;
                self.read_cache_invalidated_entries
                    .set(self.read_cache_invalidated_entries.get().saturating_add(1));
                self.read_cache_invalidated_bytes.set(
                    self.read_cache_invalidated_bytes
                        .get()
                        .saturating_add(removed_bytes),
                );
                stats.selected_bytes = stats.selected_bytes.saturating_add(removed_bytes);
                stats.selected_ranges = stats.selected_ranges.saturating_add(1);
                if max_bytes > 0 {
                    budget_left = budget_left.saturating_sub(removed_bytes);
                }
            }
        }
        stats
    }

}
