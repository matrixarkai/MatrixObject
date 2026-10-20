// SPDX-License-Identifier: Apache-2.0
impl MatrixReplicaService {
    pub fn read_chunk_with_cache_status(
        &self,
        page_id: &PageId,
        offset: u64,
        length: u64,
        intent: ReadCacheIntent,
    ) -> Result<(ReplicaReadResult, bool), ObjectError> {
        let end = validate_pipeline_range("chunk read", offset, length)?;
        let chunk = self
            .pages
            .get(page_id)
            .ok_or_else(|| ObjectError::NotFound("chunk is missing".to_string()))?;
        if end > chunk.logical_length {
            return Err(ObjectError::InvalidArgument(
                "chunk read exceeds logical length".to_string(),
            ));
        }
        for discarded in &chunk.discarded_ranges {
            if discarded.length == 0 {
                continue;
            }
            if discarded.offset >= end {
                break;
            }
            if discarded.offset.saturating_add(discarded.length) <= offset {
                continue;
            }
            if ranges_intersect(offset, length, discarded.offset, discarded.length) {
                return Err(ObjectError::Corruption(
                    "chunk read intersects discarded range".to_string(),
                ));
            }
        }
        // A BYPASSING READ IS NOT A CACHE MISS, which falls out of this rather than
        // being special-cased below: the miss counter, the fill counters and the eviction
        // of a stale entry are all inside `use_read_cache`. Counting a read the store did
        // for itself as a miss would make the hit ratio a statement about the write path.
        let use_read_cache = matches!(intent, ReadCacheIntent::Fill)
            && (self.max_cacheable_read_bytes.get() == 0
                || length <= self.max_cacheable_read_bytes.get());
        let cache_key = use_read_cache.then(|| (page_id.clone(), offset, length));
        let mut remove_cached_entry = false;
        let mut corrupt_cached_entry = false;
        if let Some(cache_key) = cache_key.as_ref() {
            let last_access_sequence = self.next_read_cache_access_sequence();
            let mut cached_result = None;
            let mut cache = self.read_cache.borrow_mut();
            if let Some(entry) = cache.get_mut(&cache_key) {
                let cache_payload_valid =
                    etag_matches(&entry.result.data, &entry.range_checksum);
                if entry.result.write_sequence == chunk.write_sequence
                    && entry.result.checksum == chunk.checksum()
                    && cache_payload_valid
                {
                    entry.last_access_sequence = last_access_sequence;
                    cached_result = Some(entry.result.clone());
                } else {
                    remove_cached_entry = true;
                    corrupt_cached_entry = !cache_payload_valid;
                }
            }
            drop(cache);
            if let Some(result) = cached_result {
                self.record_read_cache_access(last_access_sequence, cache_key.clone());
                self.read_cache_hits
                    .set(self.read_cache_hits.get().saturating_add(1));
                return Ok((result, true));
            }
        }
        if use_read_cache {
            self.read_cache_misses
                .set(self.read_cache_misses.get().saturating_add(1));
        }
        if remove_cached_entry {
            if let Some(entry) = self.remove_read_cache_entry(cache_key.as_ref().ok_or_else(|| {
                ObjectError::Corruption("cacheable chunk read lost cache key".to_string())
            })?) {
                if corrupt_cached_entry {
                    let corrupt_bytes = entry.result.data.len() as u64;
                    self.read_cache_corrupt_pages
                        .set(self.read_cache_corrupt_pages.get().saturating_add(1));
                    self.read_cache_corrupt_bytes.set(
                        self.read_cache_corrupt_bytes
                            .get()
                            .saturating_add(corrupt_bytes),
                    );
                    self.read_cache_evictions
                        .set(self.read_cache_evictions.get().saturating_add(1));
                }
            }
        }
        let mut loaded_from_ec = false;
        let start_index = usize::try_from(offset).map_err(|_| {
            ObjectError::InvalidArgument("chunk read offset exceeds memory size".to_string())
        })?;
        let end_index = usize::try_from(end).map_err(|_| {
            ObjectError::InvalidArgument("chunk read range exceeds memory size".to_string())
        })?;
        let logical_length_index = usize::try_from(chunk.logical_length).map_err(|_| {
            ObjectError::InvalidArgument("chunk logical length exceeds memory size".to_string())
        })?;
        let data = if self.chunk_pages_valid(page_id, chunk, start_index, end_index) {
            if end_index > chunk.data.len() {
                return Err(ObjectError::Corruption(
                    "chunk payload is shorter than logical length".to_string(),
                ));
            }
            chunk.data[start_index..end_index].to_vec()
        } else {
            let reconstructed = match chunk_ec_recover_data(
                &chunk.data,
                chunk.fragment_bytes(),
                chunk.fragment_digests(),
                logical_length_index,
                &chunk.checksum(),
            ) {
                Ok(reconstructed) => reconstructed,
                Err(err) => {
                    self.ec_reconstruction_failures
                        .set(self.ec_reconstruction_failures.get().saturating_add(1));
                    return Err(err);
                }
            };
            if !etag_matches(&reconstructed, &chunk.checksum()) || end_index > reconstructed.len() {
                self.ec_reconstruction_failures
                    .set(self.ec_reconstruction_failures.get().saturating_add(1));
                return Err(ObjectError::Corruption(
                    "chunk payload and EC sidecars are corrupt".to_string(),
                ));
            }
            self.ec_reconstructed_reads
                .set(self.ec_reconstructed_reads.get().saturating_add(1));
            loaded_from_ec = true;
            if start_index == 0 && end_index == logical_length_index {
                reconstructed
            } else {
                reconstructed[start_index..end_index].to_vec()
            }
        };
        let result = ReplicaReadResult {
            data,
            write_sequence: chunk.write_sequence,
            checksum: chunk.checksum().clone(),
        };
        let result_bytes = result.data.len() as u64;
        if use_read_cache {
            if self.can_insert_read_cache_payload(result_bytes) {
                if loaded_from_ec {
                    self.read_cache_ec_fill_pages
                        .set(self.read_cache_ec_fill_pages.get().saturating_add(1));
                    self.read_cache_ec_fill_bytes.set(
                        self.read_cache_ec_fill_bytes
                            .get()
                            .saturating_add(result_bytes),
                    );
                } else {
                    self.read_cache_direct_fill_pages
                        .set(self.read_cache_direct_fill_pages.get().saturating_add(1));
                    self.read_cache_direct_fill_bytes.set(
                        self.read_cache_direct_fill_bytes
                            .get()
                            .saturating_add(result_bytes),
                    );
                }
                let (compressed_chunks, _) = chunk_compression_stats(chunk);
                if compressed_chunks > 0 {
                    self.read_cache_compressed_fill_pages.set(
                        self.read_cache_compressed_fill_pages
                            .get()
                            .saturating_add(1),
                    );
                    self.read_cache_compressed_fill_bytes.set(
                        self.read_cache_compressed_fill_bytes
                            .get()
                            .saturating_add(result_bytes),
                        );
                }
                self.insert_read_cache(
                    cache_key
                        .as_ref()
                        .ok_or_else(|| {
                            ObjectError::Corruption(
                                "cacheable chunk read lost cache key".to_string(),
                            )
                        })?
                        .clone(),
                    result.clone(),
                );
            } else {
                self.read_cache_uncacheable_read_pages.set(
                    self.read_cache_uncacheable_read_pages
                        .get()
                        .saturating_add(1),
                );
                self.read_cache_uncacheable_read_bytes.set(
                    self.read_cache_uncacheable_read_bytes
                        .get()
                        .saturating_add(result_bytes),
                );
            }
        } else {
            self.read_cache_uncacheable_read_pages.set(
                self.read_cache_uncacheable_read_pages
                    .get()
                    .saturating_add(1),
            );
            self.read_cache_uncacheable_read_bytes.set(
                self.read_cache_uncacheable_read_bytes
                    .get()
                    .saturating_add(result_bytes),
            );
        }
        Ok((result, false))
    }
}
