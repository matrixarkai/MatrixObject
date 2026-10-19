impl MatrixPageService {
    pub fn prewarm_slab_range_cache(
        &mut self,
        slab_id: &SlabId,
        offset: u64,
        length: u64,
        max_bytes: u64,
    ) -> Result<CacheControlStats, ObjectError> {
        self.prewarm_slab_range_cache_routed(slab_id, offset, length, max_bytes, None, false)
    }

    pub fn prewarm_slab_range_cache_routed(
        &mut self,
        slab_id: &SlabId,
        offset: u64,
        length: u64,
        max_bytes: u64,
        target_replica_index: Option<usize>,
        include_disabled_replicas: bool,
    ) -> Result<CacheControlStats, ObjectError> {
        let mut grouped: BTreeMap<PageId, Vec<DataRange>> = BTreeMap::new();
        for loc in self.map_range(slab_id, offset, length)? {
            grouped.entry(loc.chunk_id).or_default().push(DataRange {
                offset: loc.chunk_offset,
                length: loc.length,
            });
        }
        let replica_indexes =
            self.cache_control_replica_indexes(target_replica_index, include_disabled_replicas)?;
        let mut aggregate = CacheControlStats::default();
        let mut budget_left = max_bytes;
        for (chunk_id, ranges) in grouped {
            for index in replica_indexes.iter().copied() {
                let replica = self.replicas.get_mut(index).ok_or_else(|| {
                    ObjectError::InvalidArgument("replica index out of range".to_string())
                })?;
                let stats = replica.prewarm_chunk_ranges_borrowed(&chunk_id, &ranges, budget_left)?;
                aggregate.contacted_chunk_endpoints =
                    aggregate.contacted_chunk_endpoints.saturating_add(1);
                aggregate.applied_chunk_ranges = aggregate
                    .applied_chunk_ranges
                    .saturating_add(stats.selected_ranges);
                aggregate.failed_chunk_ranges = aggregate
                    .failed_chunk_ranges
                    .saturating_add(stats.requested_ranges.saturating_sub(stats.selected_ranges));
                if max_bytes > 0 {
                    budget_left = budget_left.saturating_sub(stats.selected_bytes);
                }
                merge_cache_stats(&mut aggregate, stats);
                if max_bytes > 0 && budget_left == 0 {
                    break;
                }
            }
            if max_bytes > 0 && budget_left == 0 {
                break;
            }
        }
        self.cumulative_cache_prewarm_applied_ranges = self
            .cumulative_cache_prewarm_applied_ranges
            .saturating_add(aggregate.applied_chunk_ranges);
        self.cumulative_cache_prewarm_failed_ranges = self
            .cumulative_cache_prewarm_failed_ranges
            .saturating_add(aggregate.failed_chunk_ranges);
        Ok(aggregate)
    }

    pub fn invalidate_slab_range_cache(
        &mut self,
        slab_id: &SlabId,
        offset: u64,
        length: u64,
    ) -> Result<CacheControlStats, ObjectError> {
        self.invalidate_slab_range_cache_bounded(slab_id, offset, length, 0)
    }

    pub fn invalidate_slab_range_cache_bounded(
        &mut self,
        slab_id: &SlabId,
        offset: u64,
        length: u64,
        max_bytes: u64,
    ) -> Result<CacheControlStats, ObjectError> {
        self.invalidate_slab_range_cache_routed(
            slab_id, offset, length, max_bytes, None, false,
        )
    }

    pub fn invalidate_slab_range_cache_routed(
        &mut self,
        slab_id: &SlabId,
        offset: u64,
        length: u64,
        max_bytes: u64,
        target_replica_index: Option<usize>,
        include_disabled_replicas: bool,
    ) -> Result<CacheControlStats, ObjectError> {
        let mut aggregate = CacheControlStats::default();
        let mut budget_left = max_bytes;
        let replica_indexes =
            self.cache_control_replica_indexes(target_replica_index, include_disabled_replicas)?;
        for loc in self.map_range(slab_id, offset, length)? {
            for index in replica_indexes.iter().copied() {
                let replica = self.replicas.get_mut(index).ok_or_else(|| {
                    ObjectError::InvalidArgument("replica index out of range".to_string())
                })?;
                let stats = replica.invalidate_chunk_cache_bounded(
                    &loc.chunk_id,
                    loc.chunk_offset,
                    loc.length,
                    budget_left,
                );
                aggregate.contacted_chunk_endpoints =
                    aggregate.contacted_chunk_endpoints.saturating_add(1);
                aggregate.applied_chunk_ranges = aggregate
                    .applied_chunk_ranges
                    .saturating_add(stats.selected_ranges);
                aggregate.failed_chunk_ranges = aggregate
                    .failed_chunk_ranges
                    .saturating_add(stats.requested_ranges.saturating_sub(stats.selected_ranges));
                if max_bytes > 0 {
                    budget_left = budget_left.saturating_sub(stats.selected_bytes);
                }
                merge_cache_stats(&mut aggregate, stats);
                if max_bytes > 0 && budget_left == 0 {
                    aggregate.budget_limited = true;
                    self.cumulative_cache_invalidate_applied_ranges = self
                        .cumulative_cache_invalidate_applied_ranges
                        .saturating_add(aggregate.applied_chunk_ranges);
                    self.cumulative_cache_invalidate_failed_ranges = self
                        .cumulative_cache_invalidate_failed_ranges
                        .saturating_add(aggregate.failed_chunk_ranges);
                    return Ok(aggregate);
                }
            }
        }
        self.cumulative_cache_invalidate_applied_ranges = self
            .cumulative_cache_invalidate_applied_ranges
            .saturating_add(aggregate.applied_chunk_ranges);
        self.cumulative_cache_invalidate_failed_ranges = self
            .cumulative_cache_invalidate_failed_ranges
            .saturating_add(aggregate.failed_chunk_ranges);
        Ok(aggregate)
    }

}
