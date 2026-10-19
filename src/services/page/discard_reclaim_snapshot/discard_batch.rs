impl MatrixChunkService {
    pub fn discard_slab_ranges(
        &mut self,
        requests: Vec<ChunkRangeRequest>,
    ) -> Result<ChunkBatchStats, ObjectError> {
        self.discard_chunk_ranges_with_chunk_receipts(&requests, None)
    }

    pub fn discard_chunk_ranges_bounded_from(
        &mut self,
        requests: Vec<ChunkRangeRequest>,
        max_discard_bytes: u64,
        discard_start_range_index: u64,
    ) -> Result<(ChunkBatchStats, ChunkRangeAdmission), ObjectError> {
        let admission = self.can_accept_slab_discards_from_borrowed(
            &requests,
            max_discard_bytes,
            discard_start_range_index,
        )?;
        let selected = select_admitted_page_discard_requests(requests, &admission)?;
        let stats = self.discard_slab_ranges(selected)?;
        Ok((stats, admission))
    }

    pub fn discard_chunk_ranges_bounded(
        &mut self,
        requests: Vec<ChunkRangeRequest>,
        max_discard_bytes: u64,
    ) -> Result<(ChunkBatchStats, ChunkRangeAdmission), ObjectError> {
        self.discard_chunk_ranges_bounded_from(requests, max_discard_bytes, 0)
    }

    pub fn discard_chunk_ranges_storage_fenced(
        &mut self,
        requests: Vec<ChunkRangeRequest>,
        expected_route_control_checksum: Option<&str>,
        expected_chunk_control_proof_checksum: Option<&str>,
    ) -> Result<ChunkBatchStats, ObjectError> {
        self.check_expected_route_control_checksum(expected_route_control_checksum)?;
        self.check_expected_chunk_control_proof_checksum(expected_chunk_control_proof_checksum)?;
        self.discard_slab_ranges(requests)
    }

    pub fn discard_chunk_ranges_storage_fenced_report(
        &mut self,
        requests: Vec<ChunkRangeRequest>,
        expected_route_control_checksum: Option<&str>,
        expected_chunk_control_proof_checksum: Option<&str>,
    ) -> Result<ChunkStorageMutationReport, ObjectError> {
        let stats = self.discard_chunk_ranges_storage_fenced(
            requests,
            expected_route_control_checksum,
            expected_chunk_control_proof_checksum,
        )?;
        Ok(self.storage_mutation_report(
            stats,
            expected_route_control_checksum,
            expected_chunk_control_proof_checksum,
        ))
    }

    fn discard_chunk_ranges_with_chunk_receipts(
        &mut self,
        requests: &[ChunkRangeRequest],
        idempotency_key: Option<&str>,
    ) -> Result<ChunkBatchStats, ObjectError> {
        let mut stats = ChunkBatchStats::default();
        let mut grouped: BTreeMap<(ChunkId, PageId), Vec<DataRange>> = BTreeMap::new();
        for request in requests {
            validate_pipeline_range("block batch discard", request.offset, request.length)?;
            stats.input_ranges = stats.input_ranges.saturating_add(1);
            stats.payload_bytes =
                stats
                    .payload_bytes
                    .checked_add(request.length)
                    .ok_or_else(|| {
                        ObjectError::InvalidArgument(
                            "block batch discard byte count overflows".to_string(),
                        )
                    })?;
            let locations = self.map_range(&request.chunk_id, request.offset, request.length)?;
            stats.mapped_chunk_ranges = stats
                .mapped_chunk_ranges
                .saturating_add(locations.len() as u64);
            for loc in locations {
                grouped
                    .entry((request.chunk_id.clone(), loc.page_id))
                    .or_default()
                    .push(DataRange {
                        offset: loc.page_offset,
                        length: loc.length,
                    });
            }
        }
        self.admit_page_route_qos(
            0,
            0,
            0,
            0,
            stats.input_ranges,
            stats.payload_bytes,
            now_millis(),
        )?;
        let mut merged_grouped: BTreeMap<(ChunkId, PageId), Vec<DataRange>> = BTreeMap::new();
        let mut discard_ranges: BTreeMap<PageId, Vec<DataRange>> = BTreeMap::new();
        for ((chunk_id, page_id), ranges) in grouped {
            let merged = merge_ranges(ranges);
            if merged.is_empty() {
                continue;
            }
            discard_ranges.insert(page_id.clone(), merged.clone());
            merged_grouped.insert((chunk_id, page_id), merged);
        }
        let accepted_replicas = self.admissible_discard_replicas(&discard_ranges)?;
        for ((_, page_id), merged) in merged_grouped {
            let mut ok = 0usize;
            let mut first_error: Option<ObjectError> = None;
            let mut physical_ranges = 0u64;
            let mut physical_bytes = 0u64;
            for (index, replica) in self.replicas.iter_mut().enumerate() {
                if !accepted_replicas.contains(&index) {
                    continue;
                }
                let discard_result = if let Some(base_key) = idempotency_key {
                    let page_key =
                        replica_receipt_key("bd", base_key, &page_id, index, &merged);
                    replica
                        .discard_chunk_ranges_idempotent_borrowed(&page_id, &merged, &page_key)
                        .map(|(stats, _)| stats)
                } else {
                    replica.discard_chunk_ranges_borrowed(&page_id, &merged)
                };
                match discard_result {
                    Ok(replica_stats) => {
                        ok += 1;
                        physical_ranges =
                            physical_ranges.saturating_add(replica_stats.physical_discard_ranges);
                        physical_bytes =
                            physical_bytes.saturating_add(replica_stats.physical_discard_bytes);
                    }
                    Err(err) if first_error.is_none() => first_error = Some(err),
                    Err(_) => {}
                }
            }
            if ok < self.write_quorum {
                return Err(first_error.unwrap_or_else(|| {
                    ObjectError::Corruption("block discard quorum was not met".to_string())
                }));
            }
            if ok < accepted_replicas.len() {
                self.cumulative_quorum_discard_degraded_chunks = self
                    .cumulative_quorum_discard_degraded_chunks
                    .saturating_add(1);
            }
            stats.coalesced_chunk_ranges = stats
                .coalesced_chunk_ranges
                .saturating_add(merged.len() as u64);
            stats.physical_chunk_discard_ranges = stats
                .physical_chunk_discard_ranges
                .saturating_add(physical_ranges);
            stats.physical_chunk_discard_bytes = stats
                .physical_chunk_discard_bytes
                .saturating_add(physical_bytes);
        }
        Ok(stats)
    }
}
