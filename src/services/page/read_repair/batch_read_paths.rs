// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
fn merge_page_range_result_fault_stats(target: &mut ChunkReadStats, source: &ChunkReadStats) {
    target.replica_fallback_reads = target
        .replica_fallback_reads
        .saturating_add(source.replica_fallback_reads);
    target.quorum_verified_chunks = target
        .quorum_verified_chunks
        .saturating_add(source.quorum_verified_chunks);
    target.replica_read_repairs = target
        .replica_read_repairs
        .saturating_add(source.replica_read_repairs);
    target.replica_read_repair_failures = target
        .replica_read_repair_failures
        .saturating_add(source.replica_read_repair_failures);
    target.quorum_read_degraded_chunks = target
        .quorum_read_degraded_chunks
        .saturating_add(source.quorum_read_degraded_chunks);
}

impl MatrixChunkService {
    /// Compatibility spelling; use [`Self::read_page_ranges`].
    #[deprecated(note = "use read_page_ranges")]
    pub fn read_slab_ranges(
        &mut self,
        requests: Vec<ChunkRangeRequest>,
    ) -> Result<(Vec<ChunkRangeReadResult>, ChunkBatchStats), ObjectError> {
        self.read_page_ranges(requests)
    }

    pub fn read_page_ranges(
        &mut self,
        requests: Vec<ChunkRangeRequest>,
    ) -> Result<(Vec<ChunkRangeReadResult>, ChunkBatchStats), ObjectError> {
        self.read_chunk_ranges_bounded(requests, 0)
    }

    pub fn read_chunk_ranges_bounded(
        &mut self,
        requests: Vec<ChunkRangeRequest>,
        max_payload_bytes: u64,
    ) -> Result<(Vec<ChunkRangeReadResult>, ChunkBatchStats), ObjectError> {
        self.read_chunk_ranges_bounded_by_chunks(requests, max_payload_bytes, 0)
    }

    pub fn read_chunk_ranges_bounded_by_chunks(
        &mut self,
        requests: Vec<ChunkRangeRequest>,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
    ) -> Result<(Vec<ChunkRangeReadResult>, ChunkBatchStats), ObjectError> {
        if requests.len() == 1 {
            let request = &requests[0];
            let (data, read_stats) = self.read_chunk_range_bounded_by_chunks(
                &request.chunk_id,
                request.offset,
                request.length,
                max_payload_bytes,
                max_chunk_read_count,
            )?;
            let mapped_chunk_ranges = read_stats.chunk_reads;
            let coalesced_chunk_ranges = read_stats.coalesced_chunk_read_count;
            return Ok((
                vec![ChunkRangeReadResult {
                    data,
                    stats: read_stats.clone(),
                }],
                ChunkBatchStats {
                    input_ranges: 1,
                    mapped_chunk_ranges,
                    coalesced_chunk_ranges,
                    payload_bytes: request.length,
                    read_stats,
                    ..ChunkBatchStats::default()
                },
            ));
        }
        let mut stats = ChunkBatchStats::default();
        let mut request_locations = Vec::with_capacity(requests.len());
        let mut grouped: BTreeMap<PageId, Vec<DataRange>> = BTreeMap::new();
        for request in &requests {
            validate_pipeline_range("block batch read", request.offset, request.length)?;
            stats.input_ranges = stats.input_ranges.saturating_add(1);
            let next_payload =
                stats
                    .payload_bytes
                    .checked_add(request.length)
                    .ok_or_else(|| {
                        ObjectError::InvalidArgument(
                            "page batch read payload byte count overflows".to_string(),
                        )
                    })?;
            if max_payload_bytes > 0 && next_payload > max_payload_bytes {
                return Err(ObjectError::PreconditionFailed(
                    "page batch read payload budget exceeded".to_string(),
                ));
            }
            stats.payload_bytes = next_payload;
            let locations = self.map_range(&request.chunk_id, request.offset, request.length)?;
            stats.mapped_chunk_ranges = stats
                .mapped_chunk_ranges
                .checked_add(locations.len() as u64)
                .ok_or_else(|| {
                    ObjectError::InvalidArgument(
                        "page batch read chunk fanout overflows".to_string(),
                    )
                })?;
            if max_chunk_read_count > 0 && stats.mapped_chunk_ranges > max_chunk_read_count {
                return Err(ObjectError::PreconditionFailed(
                    "page batch read chunk fanout exceeds max_chunk_read_count".to_string(),
                ));
            }
            for loc in &locations {
                grouped
                    .entry(loc.page_id.clone())
                    .or_default()
                    .push(DataRange {
                        offset: loc.page_offset,
                        length: loc.length,
                    });
            }
            request_locations.push(locations);
        }
        self.admit_page_route_qos(
            stats.input_ranges,
            stats.payload_bytes,
            0,
            0,
            0,
            0,
            now_millis(),
        )?;

        let mut page_ids = Vec::with_capacity(grouped.len());
        for page_id in grouped.keys() {
            page_ids.push(page_id.clone());
        }
        merge_page_read_stats(
            &mut stats.read_stats,
            self.collect_metadata_head_admission_stats(&page_ids),
        );

        struct MergedChunkRead {
            offset: u64,
            length: u64,
            ref_count: usize,
            stats: ChunkReadStats,
            payload: Option<Vec<u8>>,
        }

        let mut merged_reads: BTreeMap<PageId, Vec<MergedChunkRead>> = BTreeMap::new();
        for (page_id, ranges) in grouped {
            let merged_ranges = merge_ranges(ranges);
            let mut chunk_payloads = Vec::with_capacity(merged_ranges.len());
            for range in merged_ranges {
                let (read, read_stats) =
                    self.read_chunk_quorum(&page_id, range.offset, range.length)?;
                merge_page_read_stats(&mut stats.read_stats, read_stats.clone());
                stats.coalesced_chunk_ranges = stats.coalesced_chunk_ranges.saturating_add(1);
                chunk_payloads.push(MergedChunkRead {
                    offset: range.offset,
                    length: range.length,
                    ref_count: 0,
                    stats: read_stats,
                    payload: Some(read.data),
                });
            }
            merged_reads.insert(page_id, chunk_payloads);
        }
        let repair_stats_by_chunk = self.probe_batch_read_repairs(&page_ids, &mut stats.read_stats);
        for locations in &request_locations {
            for loc in locations {
                let loc_end = loc.page_offset.checked_add(loc.length).ok_or_else(|| {
                    ObjectError::InvalidArgument("batch read location overflows".to_string())
                })?;
                let Some(chunk_reads) = merged_reads.get_mut(&loc.page_id) else {
                    return Err(ObjectError::Corruption(
                        "batch read missing coalesced chunk payload".to_string(),
                    ));
                };
                let candidate_index = chunk_reads
                    .partition_point(|read| read.offset <= loc.page_offset)
                    .checked_sub(1)
                    .ok_or_else(|| {
                        ObjectError::Corruption(
                            "batch read missing coalesced chunk payload".to_string(),
                        )
                    })?;
                let merged = &chunk_reads[candidate_index];
                let merged_end = merged.offset.checked_add(merged.length).ok_or_else(|| {
                    ObjectError::InvalidArgument("batch merged read overflows".to_string())
                })?;
                if merged_end < loc_end {
                    return Err(ObjectError::Corruption(
                        "batch read missing coalesced chunk payload".to_string(),
                    ));
                }
                let Some(merged) = chunk_reads.get_mut(candidate_index) else {
                    return Err(ObjectError::Corruption(
                        "batch read missing coalesced chunk payload".to_string(),
                    ));
                };
                merged.ref_count = merged.ref_count.saturating_add(1);
            }
        }

        let mut results = Vec::with_capacity(request_locations.len());
        for (request, locations) in requests.iter().zip(&request_locations) {
            let mut data = Vec::new();
            let mut data_reserved = false;
            let mut result_stats = ChunkReadStats::default();
            for loc in locations {
                let loc_end = loc.page_offset.checked_add(loc.length).ok_or_else(|| {
                    ObjectError::InvalidArgument("batch read location overflows".to_string())
                })?;
                let mut found = false;
                let Some(chunk_reads) = merged_reads.get(&loc.page_id) else {
                    return Err(ObjectError::Corruption(
                        "batch read missing coalesced chunk payload".to_string(),
                    ));
                };
                let candidate_index = chunk_reads
                    .partition_point(|read| read.offset <= loc.page_offset)
                    .checked_sub(1);
                if let Some(candidate_index) = candidate_index {
                    let chunk_reads = merged_reads.get_mut(&loc.page_id).ok_or_else(|| {
                        ObjectError::Corruption(
                            "batch read missing coalesced chunk payload".to_string(),
                        )
                    })?;
                    let merged = &mut chunk_reads[candidate_index];
                    let merged_end = merged.offset.checked_add(merged.length).ok_or_else(|| {
                        ObjectError::InvalidArgument("batch merged read overflows".to_string())
                    })?;
                    if merged_end >= loc_end {
                        let start =
                            usize::try_from(loc.page_offset - merged.offset).map_err(|_| {
                                ObjectError::InvalidArgument(
                                    "page batch read chunk offset exceeds memory size".to_string(),
                                )
                            })?;
                        let length = usize::try_from(loc.length).map_err(|_| {
                            ObjectError::InvalidArgument(
                                "page batch read chunk length exceeds memory size".to_string(),
                            )
                        })?;
                        let end = start.checked_add(length).ok_or_else(|| {
                            ObjectError::InvalidArgument(
                                "page batch read chunk slice overflows".to_string(),
                            )
                        })?;
                        let payload_len = merged
                            .payload
                            .as_ref()
                            .ok_or_else(|| {
                                ObjectError::Corruption(
                                    "page batch read coalesced payload was already consumed"
                                        .to_string(),
                                )
                            })?
                            .len();
                        if end > payload_len {
                            return Err(ObjectError::Corruption(
                                "page batch read chunk payload is shorter than mapping"
                                    .to_string(),
                            ));
                        }
                        let exact_single_use = loc.page_offset == merged.offset
                            && loc.length == merged.length
                            && merged.ref_count == 1;
                        if exact_single_use {
                            let mut payload = merged.payload.take().ok_or_else(|| {
                                ObjectError::Corruption(
                                    "page batch read coalesced payload was already consumed"
                                        .to_string(),
                                )
                            })?;
                            if data.is_empty() && payload.len() as u64 == request.length {
                                data = payload;
                            } else {
                                if !data_reserved {
                                    let capacity = usize::try_from(request.length).map_err(|_| {
                                        ObjectError::InvalidArgument(
                                            "page batch read range exceeds memory size".to_string(),
                                        )
                                    })?;
                                    data.reserve_exact(capacity);
                                    data_reserved = true;
                                }
                                data.append(&mut payload);
                            }
                        } else {
                            let payload = merged.payload.as_ref().ok_or_else(|| {
                                ObjectError::Corruption(
                                    "page batch read coalesced payload was already consumed"
                                    .to_string(),
                                )
                            })?;
                            if !data_reserved {
                                let capacity = usize::try_from(request.length).map_err(|_| {
                                    ObjectError::InvalidArgument(
                                        "page batch read range exceeds memory size".to_string(),
                                    )
                                })?;
                                data.reserve_exact(capacity);
                                data_reserved = true;
                            }
                            data.extend_from_slice(&payload[start..end]);
                        }
                        merge_page_range_result_fault_stats(&mut result_stats, &merged.stats);
                        if let Some(repair_stats) = repair_stats_by_chunk.get(&loc.page_id) {
                            merge_page_range_result_fault_stats(&mut result_stats, repair_stats);
                        }
                        found = true;
                    }
                }
                if !found {
                    return Err(ObjectError::Corruption(
                        "batch read missing coalesced chunk payload".to_string(),
                    ));
                }
                result_stats.chunk_reads = result_stats.chunk_reads.saturating_add(1);
            }
            results.push(ChunkRangeReadResult {
                data,
                stats: result_stats,
            });
        }
        self.record_page_read_stats(&stats.read_stats);
        Ok((results, stats))
    }

    fn probe_batch_read_repairs(
        &mut self,
        page_ids: &[PageId],
        stats: &mut ChunkReadStats,
    ) -> BTreeMap<PageId, ChunkReadStats> {
        let mut repair_stats_by_chunk = BTreeMap::new();
        if self.primary_batch_read_repair_probe_limit == 0 || self.replicas.len() <= 1 {
            return repair_stats_by_chunk;
        }
        let mut probes = 0usize;
        for page_id in page_ids {
            let mut chunk_repair_stats = ChunkReadStats::default();
            match self.repair_chunk_from_quorum(page_id) {
                Ok(repair) => {
                    chunk_repair_stats.replica_read_repairs = repair.repaired_replicas;
                    chunk_repair_stats.replica_read_repair_failures = repair.failed_repairs;
                    chunk_repair_stats.quorum_read_degraded_chunks = repair.degraded_chunks;
                    stats.replica_read_repairs = stats
                        .replica_read_repairs
                        .saturating_add(repair.repaired_replicas);
                    stats.replica_read_repair_failures = stats
                        .replica_read_repair_failures
                        .saturating_add(repair.failed_repairs);
                    stats.quorum_read_degraded_chunks = stats
                        .quorum_read_degraded_chunks
                        .saturating_add(repair.degraded_chunks);
                    self.cumulative_replica_read_repairs = self
                        .cumulative_replica_read_repairs
                        .saturating_add(repair.repaired_replicas);
                    self.cumulative_replica_read_repair_failures = self
                        .cumulative_replica_read_repair_failures
                        .saturating_add(repair.failed_repairs);
                }
                Err(_) => {
                    chunk_repair_stats.replica_read_repair_failures = 1;
                    chunk_repair_stats.quorum_read_degraded_chunks = 1;
                    stats.replica_read_repair_failures =
                        stats.replica_read_repair_failures.saturating_add(1);
                    stats.quorum_read_degraded_chunks =
                        stats.quorum_read_degraded_chunks.saturating_add(1);
                    self.cumulative_replica_read_repair_failures = self
                        .cumulative_replica_read_repair_failures
                        .saturating_add(1);
                }
            }
            repair_stats_by_chunk.insert(page_id.clone(), chunk_repair_stats);
            probes = probes.saturating_add(1);
            if probes >= self.primary_batch_read_repair_probe_limit {
                break;
            }
        }
        repair_stats_by_chunk
    }
}
