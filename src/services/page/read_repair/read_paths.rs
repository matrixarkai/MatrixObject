impl MatrixPageService {
    pub fn read_slab_range(
        &self,
        slab_id: &SlabId,
        offset: u64,
        length: u64,
    ) -> Result<(Vec<u8>, PageReadStats), ObjectError> {
        self.read_slab_range_bounded(slab_id, offset, length, 0)
    }

    pub fn read_slab_range_bounded(
        &self,
        slab_id: &SlabId,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
    ) -> Result<(Vec<u8>, PageReadStats), ObjectError> {
        self.read_slab_range_bounded_by_chunks(slab_id, offset, length, max_payload_bytes, 0)
    }

    pub fn read_slab_range_bounded_by_chunks(
        &self,
        slab_id: &SlabId,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
    ) -> Result<(Vec<u8>, PageReadStats), ObjectError> {
        if max_payload_bytes > 0 && length > max_payload_bytes {
            return Err(ObjectError::PreconditionFailed(
                "block read payload budget exceeded".to_string(),
            ));
        }
        if max_chunk_read_count > 0 {
            let mapped_chunk_ranges = self.mapped_chunk_count(offset, length)?;
            if mapped_chunk_ranges > max_chunk_read_count {
                return Err(ObjectError::PreconditionFailed(
                    "block read chunk fanout exceeds max_chunk_read_count".to_string(),
                ));
            }
        }
        self.admit_page_route_qos(1, length, 0, 0, 0, 0, now_millis())?;
        let locations = self.map_range(slab_id, offset, length)?;
        let mut stats = PageReadStats::default();
        if locations.len() == 1 {
            let loc = &locations[0];
            merge_page_read_stats(
                &mut stats,
                self.collect_metadata_head_admission_stats(std::slice::from_ref(&loc.chunk_id)),
            );
            let (selected, read_stats) =
                self.read_chunk_quorum(&loc.chunk_id, loc.chunk_offset, loc.length)?;
            merge_page_read_stats(&mut stats, read_stats);
            self.record_page_read_stats(&stats);
            return Ok((selected.data, stats));
        }
        merge_page_read_stats(
            &mut stats,
            {
                let mut chunk_ids = Vec::with_capacity(locations.len());
                for loc in &locations {
                    chunk_ids.push(loc.chunk_id.clone());
                }
                self.collect_metadata_head_admission_stats(&chunk_ids)
            },
        );
        let capacity = usize::try_from(length).map_err(|_| {
            ObjectError::InvalidArgument("block read range exceeds memory size".to_string())
        })?;
        let mut out = Vec::with_capacity(capacity);
        for loc in locations {
            let (selected, read_stats) =
                self.read_chunk_quorum(&loc.chunk_id, loc.chunk_offset, loc.length)?;
            merge_page_read_stats(&mut stats, read_stats);
            out.extend_from_slice(&selected.data);
        }
        self.record_page_read_stats(&stats);
        Ok((out, stats))
    }

    pub fn read_slab_range_bounded_by_chunks_fenced(
        &self,
        slab_id: &SlabId,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PageRangeReadReport, ObjectError> {
        self.check_expected_route_control_checksum(expected_route_control_checksum)?;
        let route_control_proof = self.route_control_proof();
        let (data, stats) = self.read_slab_range_bounded_by_chunks(
            slab_id,
            offset,
            length,
            max_payload_bytes,
            max_chunk_read_count,
        )?;
        Ok(PageRangeReadReport {
            payload_bytes: data.len() as u64,
            data,
            stats,
            max_payload_bytes,
            max_chunk_read_count,
            route_control_proof: route_control_proof.clone(),
            expected_route_control_checksum: expected_route_control_checksum
                .unwrap_or("")
                .to_string(),
            route_control_fenced: expected_route_control_checksum.is_some(),
            route_control_fence_matched: expected_route_control_checksum
                .map(|expected| expected == route_control_proof.state_checksum)
                .unwrap_or(true),
        })
    }

    fn record_page_read_stats(&self, stats: &PageReadStats) {
        self.cumulative_physical_chunk_read_count.set(
            self.cumulative_physical_chunk_read_count
                .get()
                .saturating_add(stats.physical_chunk_read_count),
        );
        self.cumulative_coalesced_chunk_read_count.set(
            self.cumulative_coalesced_chunk_read_count
                .get()
                .saturating_add(stats.coalesced_chunk_read_count),
        );
    }

    fn read_chunk_quorum(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
        length: u64,
    ) -> Result<(ChunkReadResult, PageReadStats), ObjectError> {
        let mut stats = PageReadStats::default();
        let mut successful =
            Vec::with_capacity(self.replicas.len().min(self.read_quorum.max(1)));
        let mut first_error: Option<ObjectError> = None;
        let mut attempted = 0usize;
        let mut failed_before_quorum = 0usize;
        let mut selected_index: Option<usize> = None;
        let mut selected_count = 0usize;
        let mut votes: BTreeMap<(u64, String, String), (usize, usize)> = BTreeMap::new();
        for (index, replica) in self.replicas.iter().enumerate() {
            if !self.replica_active(index) {
                continue;
            }
            attempted += 1;
            match replica.read_chunk_with_cache_status(chunk_id, offset, length) {
                Ok((read, cache_hit)) => {
                    if cache_hit {
                        stats.cache_hits = stats.cache_hits.saturating_add(1);
                    } else {
                        stats.physical_chunk_read_count =
                            stats.physical_chunk_read_count.saturating_add(1);
                    }
                    let vote_key = (
                        read.write_sequence,
                        read.checksum.clone(),
                        etag(&read.data),
                    );
                    successful.push(read);
                    let candidate_index = successful.len() - 1;
                    let vote = votes.entry(vote_key).or_insert((candidate_index, 0));
                    vote.1 = vote.1.saturating_add(1);
                    if vote.1 > selected_count {
                        selected_index = Some(vote.0);
                        selected_count = vote.1;
                    }
                    if selected_count >= self.read_quorum {
                        break;
                    }
                }
                Err(err) if first_error.is_none() => {
                    failed_before_quorum += 1;
                    first_error = Some(err);
                }
                Err(_) => failed_before_quorum += 1,
            }
        }
        if successful.len() < self.read_quorum {
            return Err(first_error.unwrap_or_else(|| {
                ObjectError::Corruption("block read quorum was not met".to_string())
            }));
        }
        stats.chunk_reads = 1;
        stats.coalesced_chunk_read_count = 1;
        stats.replica_fallback_reads = attempted.saturating_sub(successful.len()) as u64;
        if selected_count < self.read_quorum {
            return Err(ObjectError::Corruption(
                "block read quorum returned divergent chunk data".to_string(),
            ));
        }
        if failed_before_quorum > 0
            || !self.quarantined_replicas.is_empty()
            || selected_count < successful.len()
        {
            stats.quorum_read_degraded_chunks = 1;
        }
        stats.quorum_verified_chunks = 1;
        let selected_index = selected_index.ok_or_else(|| {
            ObjectError::Corruption("block read quorum selection failed".to_string())
        })?;
        let selected = successful.swap_remove(selected_index);
        Ok((selected, stats))
    }
}
