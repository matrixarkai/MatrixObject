impl MatrixChunkService {
    pub fn read_chunk_range_repairing(
        &mut self,
        chunk_id: &ChunkId,
        offset: u64,
        length: u64,
    ) -> Result<(Vec<u8>, ChunkReadStats), ObjectError> {
        self.admit_page_route_qos(1, length, 0, 0, 0, 0, now_millis())?;
        let locations = self.map_range(chunk_id, offset, length)?;
        let capacity = usize::try_from(length).map_err(|_| {
            ObjectError::InvalidArgument("block repair read range exceeds memory size".to_string())
        })?;
        let mut out = Vec::with_capacity(capacity);
        let page_ids = locations
            .iter()
            .map(|loc| loc.page_id.clone())
            .collect::<Vec<_>>();
        let mut stats = self.collect_metadata_head_admission_stats(&page_ids);
        for loc in locations {
            let (selected, read_stats) =
                self.read_chunk_quorum(&loc.page_id, loc.page_offset, loc.length)?;
            merge_page_read_stats(&mut stats, read_stats);
            let repair = self.repair_chunk_from_quorum(&loc.page_id)?;
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
            out.extend_from_slice(&selected.data);
        }
        self.cumulative_quorum_read_degraded_chunks = self
            .cumulative_quorum_read_degraded_chunks
            .saturating_add(stats.quorum_read_degraded_chunks);
        self.cumulative_replica_fallback_reads = self
            .cumulative_replica_fallback_reads
            .saturating_add(stats.replica_fallback_reads);
        self.record_page_read_stats(&stats);
        Ok((out, stats))
    }

    pub fn read_chunk_range_repair_report(
        &mut self,
        chunk_id: &ChunkId,
        offset: u64,
        length: u64,
    ) -> Result<ChunkReadRepairReport, ObjectError> {
        let route_control_proof = self.route_control_proof();
        let (data, stats) = self.read_chunk_range_repairing(chunk_id, offset, length)?;
        Ok(ChunkReadRepairReport {
            chunk_id: chunk_id.clone(),
            offset,
            length,
            payload_checksum: etag(&data),
            repaired_replicas: stats.replica_read_repairs,
            failed_repairs: stats.replica_read_repair_failures,
            degraded_chunks: stats.quorum_read_degraded_chunks,
            route_control_proof,
            expected_route_control_checksum: String::new(),
            data,
            stats,
        })
    }

    pub fn read_chunk_range_repair_report_fenced(
        &mut self,
        chunk_id: &ChunkId,
        offset: u64,
        length: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ChunkReadRepairReport, ObjectError> {
        self.check_expected_route_control_checksum(expected_route_control_checksum)?;
        let mut report = self.read_chunk_range_repair_report(chunk_id, offset, length)?;
        report.expected_route_control_checksum = expected_route_control_checksum
            .unwrap_or_default()
            .to_string();
        Ok(report)
    }

    fn collect_metadata_head_admission_stats(&self, page_ids: &[PageId]) -> ChunkReadStats {
        let mut stats = ChunkReadStats::default();
        if page_ids.is_empty() {
            return stats;
        }
        let mut successful = 0usize;
        for (index, replica) in self.replicas.iter().enumerate() {
            if !self.replica_active(index) {
                continue;
            }
            stats.metadata_head_batch_requests =
                stats.metadata_head_batch_requests.saturating_add(1);
            if let Ok(head_stats) = replica.head_unique_chunk_batch_stats_refs(page_ids) {
                stats.metadata_physical_head_count = stats
                    .metadata_physical_head_count
                    .saturating_add(head_stats.physical_heads);
                stats.metadata_coalesced_head_count = stats
                    .metadata_coalesced_head_count
                    .saturating_add(head_stats.coalesced_heads);
                stats.metadata_cache_hits = stats
                    .metadata_cache_hits
                    .saturating_add(head_stats.metadata_cache_hits);
                stats.metadata_cache_misses = stats
                    .metadata_cache_misses
                    .saturating_add(head_stats.metadata_cache_misses);
                successful += 1;
                if successful >= self.read_quorum {
                    break;
                }
            }
        }
        stats
    }

    pub fn repair_chunk_range(
        &mut self,
        chunk_id: &ChunkId,
        offset: u64,
        length: u64,
    ) -> Result<RepairStats, ObjectError> {
        let mut aggregate = RepairStats::default();
        let mut seen = BTreeSet::new();
        for loc in self.map_range(chunk_id, offset, length)? {
            if seen.insert(loc.page_id.clone()) {
                merge_repair_stats(
                    &mut aggregate,
                    self.repair_chunk_from_quorum(&loc.page_id)?,
                );
            }
        }
        Ok(aggregate)
    }

    pub fn prepare_chunk_state_handoffs(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
        length: u64,
        source_replica_index: usize,
        max_payload_bytes: u64,
    ) -> Result<Vec<ReplicaStateHandoff>, ObjectError> {
        if source_replica_index >= self.replicas.len() {
            return Err(ObjectError::InvalidArgument(
                "source replica index out of range".to_string(),
            ));
        }
        if self.quarantined_replicas.contains(&source_replica_index) {
            return Err(ObjectError::PreconditionFailed(
                "source replica is quarantined".to_string(),
            ));
        }
        let locations = self.map_range(chunk_id, offset, length)?;
        let mut out = Vec::with_capacity(locations.len());
        let mut seen = BTreeSet::new();
        let mut payload_bytes = 0u64;
        for loc in locations {
            if !seen.insert(loc.page_id.clone()) {
                continue;
            }
            let handoff =
                self.replicas[source_replica_index].prepare_chunk_state_handoff(&loc.page_id)?;
            let next_payload = payload_bytes.saturating_add(handoff.state.data.len() as u64);
            if max_payload_bytes > 0 && next_payload > max_payload_bytes {
                if out.is_empty() {
                    return Err(ObjectError::InvalidArgument(
                        "chunk state handoff exceeds payload budget".to_string(),
                    ));
                }
                break;
            }
            payload_bytes = next_payload;
            out.push(handoff);
        }
        if out.is_empty() && length > 0 {
            return Err(ObjectError::NotFound(
                "no chunk state handoff could be prepared".to_string(),
            ));
        }
        Ok(out)
    }

    pub fn apply_chunk_state_handoffs(
        &mut self,
        target_replica_index: usize,
        handoffs: Vec<ReplicaStateHandoff>,
    ) -> Result<RepairStats, ObjectError> {
        self.apply_chunk_state_handoffs_fenced(target_replica_index, handoffs, None)
    }

    pub fn apply_chunk_state_handoffs_fenced(
        &mut self,
        target_replica_index: usize,
        handoffs: Vec<ReplicaStateHandoff>,
        expected_control_checksum: Option<&str>,
    ) -> Result<RepairStats, ObjectError> {
        if target_replica_index >= self.replicas.len() {
            return Err(ObjectError::InvalidArgument(
                "target replica index out of range".to_string(),
            ));
        }
        if self.quarantined_replicas.contains(&target_replica_index) {
            return Err(ObjectError::PreconditionFailed(
                "target replica is quarantined".to_string(),
            ));
        }
        if handoffs.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "chunk state handoff batch must not be empty".to_string(),
            ));
        }
        let mut stats = RepairStats::default();
        let target = &mut self.replicas[target_replica_index];
        for handoff in handoffs {
            stats.checked_chunks = stats.checked_chunks.saturating_add(1);
            if validate_chunk_state_handoff(&handoff).is_err() {
                stats.failed_repairs = stats.failed_repairs.saturating_add(1);
                continue;
            }
            let needs_install = match target.read_chunk_state_validated(&handoff.page_id) {
                Ok(state) => state != handoff.state,
                Err(_) => true,
            };
            if !needs_install {
                continue;
            }
            stats.degraded_chunks = stats.degraded_chunks.saturating_add(1);
            match target.install_chunk_state_handoff_fenced(handoff, expected_control_checksum) {
                Ok(()) => stats.repaired_replicas = stats.repaired_replicas.saturating_add(1),
                Err(_) => stats.failed_repairs = stats.failed_repairs.saturating_add(1),
            }
        }
        self.record_repair_stats(&stats);
        Ok(stats)
    }

    pub fn repair_chunk_range_bounded(
        &mut self,
        chunk_id: &ChunkId,
        offset: u64,
        length: u64,
        max_repair_chunks: usize,
        start_chunk_index: u64,
    ) -> Result<BoundedRepairReport, ObjectError> {
        let mut pages: BTreeMap<PageId, u64> = BTreeMap::new();
        for loc in self.map_range(chunk_id, offset, length)? {
            pages
                .entry(loc.page_id.clone())
                .and_modify(|bytes| *bytes = bytes.saturating_add(loc.length))
                .or_insert(loc.length);
        }
        let mut report = BoundedRepairReport {
            total_chunk_count: pages.len() as u64,
            candidate_repair_bytes: pages
                .values()
                .fold(0u64, |total, bytes| total.saturating_add(*bytes)),
            start_chunk_index,
            next_chunk_index: start_chunk_index,
            route_control_proof: self.route_control_proof(),
            ..BoundedRepairReport::default()
        };
        let limit = if max_repair_chunks == 0 {
            usize::MAX
        } else {
            max_repair_chunks
        };
        let mut selected = 0usize;
        for (page_id, bytes) in pages {
            if page_id.page_index < start_chunk_index {
                continue;
            }
            if selected >= limit {
                report.skipped_chunk_count = report.skipped_chunk_count.saturating_add(1);
                report.skipped_repair_bytes = report.skipped_repair_bytes.saturating_add(bytes);
                if !report.has_more {
                    report.next_chunk_index = page_id.page_index;
                }
                report.has_more = true;
                continue;
            }
            selected += 1;
            report.selected_chunk_count = report.selected_chunk_count.saturating_add(1);
            report.selected_repair_bytes = report.selected_repair_bytes.saturating_add(bytes);
            report.next_chunk_index = page_id.page_index.saturating_add(1);
            match self.repair_chunk_from_quorum(&page_id) {
                Ok(stats) => merge_repair_stats(&mut report.stats, stats),
                Err(_) => {
                    report.stats.failed_repairs = report.stats.failed_repairs.saturating_add(1)
                }
            }
        }
        Ok(report)
    }
}
