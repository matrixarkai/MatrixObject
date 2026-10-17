impl MatrixChunkService {
    pub fn set_read_cache_max_bytes(&self, max_bytes: u64) {
        self.max_read_cache_bytes.set(max_bytes);
        self.reserve_read_cache_for_capacity();
        self.trim_read_cache_to_limit();
    }

    pub fn set_read_cache_max_cacheable_read_bytes(&self, max_bytes: u64) {
        self.max_cacheable_read_bytes.set(max_bytes);
    }

    pub fn read_cache_stats(&self) -> ChunkReadCacheStats {
        ChunkReadCacheStats {
            entries: self.read_cache.borrow().len() as u64,
            bytes: self.read_cache_bytes.get(),
            max_bytes: self.max_read_cache_bytes.get(),
            max_cacheable_read_bytes: self.max_cacheable_read_bytes.get(),
            hits: self.read_cache_hits.get(),
            misses: self.read_cache_misses.get(),
            evictions: self.read_cache_evictions.get(),
            corrupt_pages: self.read_cache_corrupt_pages.get(),
            corrupt_bytes: self.read_cache_corrupt_bytes.get(),
            invalidations: self.read_cache_invalidations.get(),
            invalidated_entries: self.read_cache_invalidated_entries.get(),
            invalidated_bytes: self.read_cache_invalidated_bytes.get(),
            clears: self.read_cache_clears.get(),
            cleared_entries: self.read_cache_cleared_entries.get(),
            cleared_bytes: self.read_cache_cleared_bytes.get(),
            direct_fill_pages: self.read_cache_direct_fill_pages.get(),
            direct_fill_bytes: self.read_cache_direct_fill_bytes.get(),
            compressed_fill_pages: self.read_cache_compressed_fill_pages.get(),
            compressed_fill_bytes: self.read_cache_compressed_fill_bytes.get(),
            ec_fill_pages: self.read_cache_ec_fill_pages.get(),
            ec_fill_bytes: self.read_cache_ec_fill_bytes.get(),
            uncacheable_read_pages: self.read_cache_uncacheable_read_pages.get(),
            uncacheable_read_bytes: self.read_cache_uncacheable_read_bytes.get(),
            pressure: self.max_read_cache_bytes.get() > 0
                && self.read_cache_bytes.get() >= self.max_read_cache_bytes.get(),
        }
    }

    pub fn clear_read_cache(&self) {
        let previous_entries = self.read_cache.borrow().len() as u64;
        let previous_bytes = self.read_cache_bytes.get();
        self.read_cache_clears
            .set(self.read_cache_clears.get().saturating_add(1));
        self.read_cache_cleared_entries.set(
            self.read_cache_cleared_entries
                .get()
                .saturating_add(previous_entries),
        );
        self.read_cache_cleared_bytes.set(
            self.read_cache_cleared_bytes
                .get()
                .saturating_add(previous_bytes),
        );
        let mut cache = self.read_cache.borrow_mut();
        cache.clear();
        cache.shrink_to_fit();
        drop(cache);
        let mut by_chunk = self.read_cache_by_chunk.borrow_mut();
        by_chunk.clear();
        by_chunk.shrink_to_fit();
        drop(by_chunk);
        self.read_cache_lru.borrow_mut().clear();
        self.read_cache_bytes.set(0);
    }

    fn insert_read_cache(&self, key: ChunkReadCacheKey, result: ChunkReadResult) {
        let bytes = result.data.len() as u64;
        if !self.can_insert_read_cache_payload(bytes) {
            return;
        }
        let last_access_sequence = self.next_read_cache_access_sequence();
        self.record_read_cache_access(last_access_sequence, key.clone());
        self.reserve_read_cache_for_capacity();
        self.read_cache_by_chunk
            .borrow_mut()
            .entry(key.0.clone())
            .or_default()
            .insert((key.1, key.2));
        let mut cache = self.read_cache.borrow_mut();
        if let Some(previous) = cache.insert(
            key,
            ChunkReadCacheEntry {
                range_checksum: etag(&result.data),
                result,
                last_access_sequence,
            },
        ) {
            self.read_cache_bytes.set(
                self.read_cache_bytes
                    .get()
                    .saturating_sub(previous.result.data.len() as u64),
            );
        }
        self.read_cache_bytes
            .set(self.read_cache_bytes.get().saturating_add(bytes));
        drop(cache);
        self.trim_read_cache_to_limit();
    }

    fn reserve_read_cache_for_capacity(&self) {
        let max_bytes = self.max_read_cache_bytes.get();
        let mut cache = self.read_cache.borrow_mut();
        if max_bytes == 0 {
            if cache.is_empty() {
                cache.shrink_to_fit();
                let mut by_chunk = self.read_cache_by_chunk.borrow_mut();
                if by_chunk.is_empty() {
                    by_chunk.shrink_to_fit();
                }
            }
            return;
        }
        let configured_page_bytes = self.max_cacheable_read_bytes.get();
        let page_bytes = if configured_page_bytes == 0 {
            64 * 1024
        } else {
            configured_page_bytes.min(64 * 1024)
        };
        let estimated_pages = max_bytes.saturating_add(page_bytes - 1) / page_bytes;
        let target_entries = estimated_pages.min(1 << 20).max(1) as usize;
        let current_capacity = cache.capacity();
        if current_capacity < target_entries {
            cache.reserve(target_entries - current_capacity);
        }
        let chunk_index_target_entries = self.chunks.len().max(1);
        let mut by_chunk = self.read_cache_by_chunk.borrow_mut();
        let chunk_index_capacity = by_chunk.capacity();
        if chunk_index_capacity < chunk_index_target_entries {
            by_chunk.reserve(chunk_index_target_entries - chunk_index_capacity);
        }
    }

    /// Whether the bytes backing `[start, end)` still match what was validated.
    ///
    /// The first call for a given (write_sequence, checksum) hashes the whole payload
    /// once and memoises a hash per page; later calls hash only the pages the read
    /// covers. A damaged byte fails its page, so the caller still falls through to EC
    /// recovery exactly as a whole-chunk mismatch did.
    fn chunk_pages_valid(
        &self,
        chunk_id: &ChunkId,
        chunk: &ChunkState,
        start: usize,
        end: usize,
    ) -> bool {
        let page = CHUNK_VALIDATION_PAGE_BYTES;
        {
            let memo = self.page_validation.borrow();
            if let Some((sequence, checksum, hashes)) = memo.get(chunk_id) {
                if *sequence == chunk.write_sequence && checksum == &chunk.checksum {
                    let first = start / page;
                    let last = end.saturating_sub(1) / page;
                    for index in first..=last {
                        let from = index * page;
                        if from >= chunk.data.len() {
                            return false;
                        }
                        let to = (from + page).min(chunk.data.len());
                        match hashes.get(index) {
                            Some(expected) if *expected == fnv1a64(&chunk.data[from..to]) => {}
                            _ => return false,
                        }
                    }
                    return true;
                }
            }
        }
        // cold: validate the whole payload once, then memoise the pages
        if !etag_matches(&chunk.data, &chunk.checksum) {
            self.page_validation.borrow_mut().remove(chunk_id);
            return false;
        }
        let mut hashes = Vec::with_capacity(chunk.data.len() / page + 1);
        let mut from = 0usize;
        while from < chunk.data.len() {
            let to = (from + page).min(chunk.data.len());
            hashes.push(fnv1a64(&chunk.data[from..to]));
            from = to;
        }
        self.page_validation.borrow_mut().insert(
            chunk_id.clone(),
            (chunk.write_sequence, chunk.checksum.clone(), hashes),
        );
        true
    }

    fn can_insert_read_cache_payload(&self, bytes: u64) -> bool {
        let max_bytes = self.max_read_cache_bytes.get();
        let max_cacheable = self.max_cacheable_read_bytes.get();
        bytes > 0
            && max_bytes > 0
            && bytes <= max_bytes
            && (max_cacheable == 0 || bytes <= max_cacheable)
    }

    fn remove_read_cache_entry(&self, key: &ChunkReadCacheKey) -> Option<ChunkReadCacheEntry> {
        let removed = self.read_cache.borrow_mut().remove(key);
        if let Some(entry) = &removed {
            let mut by_chunk = self.read_cache_by_chunk.borrow_mut();
            if let Some(keys) = by_chunk.get_mut(&key.0) {
                keys.remove(&(key.1, key.2));
                if keys.is_empty() {
                    by_chunk.remove(&key.0);
                }
            }
            self.read_cache_bytes.set(
                self.read_cache_bytes
                    .get()
                    .saturating_sub(entry.result.data.len() as u64),
            );
        }
        removed
    }

    fn trim_read_cache_to_limit(&self) {
        let max_bytes = self.max_read_cache_bytes.get();
        let mut cache = self.read_cache.borrow_mut();
        if self.read_cache_bytes.get() <= max_bytes {
            return;
        }
        let mut lru = self.read_cache_lru.borrow_mut();
        while self.read_cache_bytes.get() > max_bytes {
            let Some((last_access_sequence, key)) = lru.pop_front() else {
                self.read_cache_bytes.set(0);
                cache.clear();
                cache.shrink_to_fit();
                let mut by_chunk = self.read_cache_by_chunk.borrow_mut();
                by_chunk.clear();
                by_chunk.shrink_to_fit();
                break;
            };
            if cache
                .get(&key)
                .is_some_and(|entry| entry.last_access_sequence != last_access_sequence)
            {
                continue;
            }
            if let Some(removed) = cache.remove(&key) {
                let mut by_chunk = self.read_cache_by_chunk.borrow_mut();
                if let Some(keys) = by_chunk.get_mut(&key.0) {
                    keys.remove(&(key.1, key.2));
                    if keys.is_empty() {
                        by_chunk.remove(&key.0);
                    }
                }
                self.read_cache_bytes.set(
                    self.read_cache_bytes
                        .get()
                        .saturating_sub(removed.result.data.len() as u64),
                );
                self.read_cache_evictions
                    .set(self.read_cache_evictions.get().saturating_add(1));
            }
        }
    }

    fn next_read_cache_access_sequence(&self) -> u64 {
        let next = self.read_cache_access_sequence.get().saturating_add(1);
        self.read_cache_access_sequence.set(next);
        next
    }

    fn record_read_cache_access(&self, last_access_sequence: u64, key: ChunkReadCacheKey) {
        let mut lru = self.read_cache_lru.borrow_mut();
        lru.push_back((last_access_sequence, key));
        let cache_len = self.read_cache.borrow().len();
        if lru.len() > cache_len.saturating_mul(4).saturating_add(1024) {
            let cache = self.read_cache.borrow();
            lru.retain(|(sequence, key)| {
                cache
                    .get(key)
                    .is_some_and(|entry| entry.last_access_sequence == *sequence)
            });
            let retained_capacity_target = cache_len.saturating_mul(2).saturating_add(1024);
            if lru.capacity() > retained_capacity_target.saturating_mul(2) {
                lru.shrink_to_fit();
            }
        }
    }
}
