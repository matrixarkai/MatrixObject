impl MatrixChunkService {
    pub fn configure_chunk_read_cache_control(
        &self,
        max_bytes_per_replica: u64,
    ) -> Result<ReplicaReadCacheControlReport, ObjectError> {
        let before = self.read_cache_stats();
        let replica_indexes = self.cache_control_replica_indexes(None, false)?;
        self.set_read_cache_max_bytes_per_replica(max_bytes_per_replica);
        let after = self.read_cache_stats();
        Ok(ReplicaReadCacheControlReport {
            previous_read_cache_bytes: before.bytes,
            previous_read_cache_entries: before.entries,
            previous_read_cache_max_bytes: before.max_bytes,
            read_cache_bytes: after.bytes,
            read_cache_entries: after.entries,
            read_cache_max_bytes: after.max_bytes,
            remote_chunk_endpoint_count: self.chunk_endpoints.len() as u64,
            configured_chunk_endpoint_count: replica_indexes.len() as u64,
            ..ReplicaReadCacheControlReport::default()
        })
    }

    pub fn clear_chunk_read_cache_control(
        &self,
    ) -> Result<ReplicaReadCacheControlReport, ObjectError> {
        let before = self.read_cache_stats();
        let replica_indexes = self.cache_control_replica_indexes(None, false)?;
        self.clear_read_cache();
        let after = self.read_cache_stats();
        Ok(ReplicaReadCacheControlReport {
            previous_read_cache_bytes: before.bytes,
            previous_read_cache_entries: before.entries,
            previous_read_cache_max_bytes: before.max_bytes,
            read_cache_bytes: after.bytes,
            read_cache_entries: after.entries,
            read_cache_max_bytes: after.max_bytes,
            cleared_read_cache_bytes: before.bytes.saturating_sub(after.bytes),
            cleared_read_cache_entries: before.entries.saturating_sub(after.entries),
            remote_chunk_endpoint_count: self.chunk_endpoints.len() as u64,
            cleared_chunk_endpoint_count: replica_indexes.len() as u64,
            ..ReplicaReadCacheControlReport::default()
        })
    }

    pub fn prewarm_chunk_read_cache_control(
        &mut self,
        chunk_id: PageId,
        offset: u64,
        length: u64,
        max_bytes: u64,
    ) -> Result<ReplicaReadCacheControlReport, ObjectError> {
        self.prewarm_chunk_read_cache_control_batch(
            vec![ReplicaReadCacheRange {
                chunk_id,
                offset,
                length,
            }],
            max_bytes,
        )
    }

    pub fn prewarm_chunk_read_cache_control_batch(
        &mut self,
        ranges: Vec<ReplicaReadCacheRange>,
        max_bytes: u64,
    ) -> Result<ReplicaReadCacheControlReport, ObjectError> {
        let before = self.read_cache_stats();
        let replica_indexes = self.cache_control_replica_indexes(None, false)?;
        let mut grouped: BTreeMap<PageId, Vec<DataRange>> = BTreeMap::new();
        for range in ranges {
            if range.length == 0 {
                continue;
            }
            grouped.entry(range.chunk_id).or_default().push(DataRange {
                offset: range.offset,
                length: range.length,
            });
        }
        let mut aggregate = CacheControlStats::default();
        let mut budget_left = max_bytes;
        let mut selected_chunk_endpoint = None;
        for (chunk_id, chunk_ranges) in grouped {
            for index in replica_indexes.iter().copied() {
                let replica = self.replicas.get_mut(index).ok_or_else(|| {
                    ObjectError::InvalidArgument("replica index out of range".to_string())
                })?;
                let stats =
                    replica.prewarm_chunk_ranges_borrowed(&chunk_id, &chunk_ranges, budget_left)?;
                aggregate.contacted_chunk_endpoints =
                    aggregate.contacted_chunk_endpoints.saturating_add(1);
                aggregate.applied_chunk_ranges = aggregate
                    .applied_chunk_ranges
                    .saturating_add(stats.selected_ranges);
                aggregate.failed_chunk_ranges = aggregate
                    .failed_chunk_ranges
                    .saturating_add(stats.requested_ranges.saturating_sub(stats.selected_ranges));
                if selected_chunk_endpoint.is_none() && stats.selected_ranges > 0 {
                    selected_chunk_endpoint = self.chunk_endpoints.get(index).cloned();
                }
                if max_bytes > 0 {
                    budget_left = budget_left.saturating_sub(stats.selected_bytes);
                }
                merge_cache_stats(&mut aggregate, stats);
                if max_bytes > 0 && budget_left == 0 {
                    aggregate.budget_limited = true;
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
        let after = self.read_cache_stats();
        Ok(ReplicaReadCacheControlReport {
            stats: aggregate,
            previous_read_cache_bytes: before.bytes,
            previous_read_cache_entries: before.entries,
            previous_read_cache_max_bytes: before.max_bytes,
            read_cache_bytes: after.bytes,
            read_cache_entries: after.entries,
            read_cache_max_bytes: after.max_bytes,
            remote_chunk_endpoint_count: self.chunk_endpoints.len() as u64,
            selected_chunk_endpoint,
            ..ReplicaReadCacheControlReport::default()
        })
    }

    pub fn invalidate_chunk_read_cache_control(
        &mut self,
        chunk_id: PageId,
        offset: u64,
        length: u64,
        max_bytes: u64,
    ) -> Result<ReplicaReadCacheControlReport, ObjectError> {
        self.invalidate_chunk_read_cache_control_batch(
            vec![ReplicaReadCacheRange {
                chunk_id,
                offset,
                length,
            }],
            max_bytes,
        )
    }

    pub fn invalidate_chunk_read_cache_control_batch(
        &mut self,
        ranges: Vec<ReplicaReadCacheRange>,
        max_bytes: u64,
    ) -> Result<ReplicaReadCacheControlReport, ObjectError> {
        let before = self.read_cache_stats();
        let replica_indexes = self.cache_control_replica_indexes(None, false)?;
        let mut aggregate = CacheControlStats::default();
        let mut budget_left = max_bytes;
        for range in ranges {
            if range.length == 0 {
                continue;
            }
            for index in replica_indexes.iter().copied() {
                let replica = self.replicas.get_mut(index).ok_or_else(|| {
                    ObjectError::InvalidArgument("replica index out of range".to_string())
                })?;
                let stats = replica.invalidate_chunk_cache_bounded(
                    &range.chunk_id,
                    range.offset,
                    range.length,
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
                    break;
                }
            }
            if max_bytes > 0 && budget_left == 0 {
                break;
            }
        }
        self.cumulative_cache_invalidate_applied_ranges = self
            .cumulative_cache_invalidate_applied_ranges
            .saturating_add(aggregate.applied_chunk_ranges);
        self.cumulative_cache_invalidate_failed_ranges = self
            .cumulative_cache_invalidate_failed_ranges
            .saturating_add(aggregate.failed_chunk_ranges);
        let after = self.read_cache_stats();
        Ok(ReplicaReadCacheControlReport {
            stats: aggregate,
            previous_read_cache_bytes: before.bytes,
            previous_read_cache_entries: before.entries,
            previous_read_cache_max_bytes: before.max_bytes,
            read_cache_bytes: after.bytes,
            read_cache_entries: after.entries,
            read_cache_max_bytes: after.max_bytes,
            remote_chunk_endpoint_count: self.chunk_endpoints.len() as u64,
            invalidated_chunk_endpoint_count: replica_indexes.len() as u64,
            ..ReplicaReadCacheControlReport::default()
        })
    }

}
