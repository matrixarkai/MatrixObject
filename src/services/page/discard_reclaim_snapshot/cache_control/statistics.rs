// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixChunkService {
    pub fn read_cache_stats(&self) -> ReplicaReadCacheStats {
        let mut aggregate = ReplicaReadCacheStats {
            entries: 0,
            bytes: 0,
            max_bytes: 0,
            max_cacheable_read_bytes: 0,
            hits: 0,
            misses: 0,
            evictions: 0,
            corrupt_pages: 0,
            corrupt_bytes: 0,
            invalidations: 0,
            invalidated_entries: 0,
            invalidated_bytes: 0,
            clears: 0,
            cleared_entries: 0,
            cleared_bytes: 0,
            direct_fill_pages: 0,
            direct_fill_bytes: 0,
            compressed_fill_pages: 0,
            compressed_fill_bytes: 0,
            ec_fill_pages: 0,
            ec_fill_bytes: 0,
            uncacheable_read_pages: 0,
            uncacheable_read_bytes: 0,
            pressure: false,
        };
        for replica in &self.replicas {
            let stats = replica.read_cache_stats();
            aggregate.entries = aggregate.entries.saturating_add(stats.entries);
            aggregate.bytes = aggregate.bytes.saturating_add(stats.bytes);
            aggregate.max_bytes = aggregate.max_bytes.saturating_add(stats.max_bytes);
            if stats.max_cacheable_read_bytes > 0 {
                aggregate.max_cacheable_read_bytes = if aggregate.max_cacheable_read_bytes == 0 {
                    stats.max_cacheable_read_bytes
                } else {
                    aggregate
                        .max_cacheable_read_bytes
                        .min(stats.max_cacheable_read_bytes)
                };
            }
            aggregate.hits = aggregate.hits.saturating_add(stats.hits);
            aggregate.misses = aggregate.misses.saturating_add(stats.misses);
            aggregate.evictions = aggregate.evictions.saturating_add(stats.evictions);
            aggregate.corrupt_pages = aggregate.corrupt_pages.saturating_add(stats.corrupt_pages);
            aggregate.corrupt_bytes = aggregate.corrupt_bytes.saturating_add(stats.corrupt_bytes);
            aggregate.invalidations = aggregate.invalidations.saturating_add(stats.invalidations);
            aggregate.invalidated_entries = aggregate
                .invalidated_entries
                .saturating_add(stats.invalidated_entries);
            aggregate.invalidated_bytes = aggregate
                .invalidated_bytes
                .saturating_add(stats.invalidated_bytes);
            aggregate.clears = aggregate.clears.saturating_add(stats.clears);
            aggregate.cleared_entries = aggregate
                .cleared_entries
                .saturating_add(stats.cleared_entries);
            aggregate.cleared_bytes = aggregate.cleared_bytes.saturating_add(stats.cleared_bytes);
            aggregate.direct_fill_pages = aggregate
                .direct_fill_pages
                .saturating_add(stats.direct_fill_pages);
            aggregate.direct_fill_bytes = aggregate
                .direct_fill_bytes
                .saturating_add(stats.direct_fill_bytes);
            aggregate.compressed_fill_pages = aggregate
                .compressed_fill_pages
                .saturating_add(stats.compressed_fill_pages);
            aggregate.compressed_fill_bytes = aggregate
                .compressed_fill_bytes
                .saturating_add(stats.compressed_fill_bytes);
            aggregate.ec_fill_pages = aggregate.ec_fill_pages.saturating_add(stats.ec_fill_pages);
            aggregate.ec_fill_bytes = aggregate.ec_fill_bytes.saturating_add(stats.ec_fill_bytes);
            aggregate.uncacheable_read_pages = aggregate
                .uncacheable_read_pages
                .saturating_add(stats.uncacheable_read_pages);
            aggregate.uncacheable_read_bytes = aggregate
                .uncacheable_read_bytes
                .saturating_add(stats.uncacheable_read_bytes);
            aggregate.pressure |= stats.pressure;
        }
        aggregate
    }

    pub fn clear_read_cache(&self) {
        for replica in &self.replicas {
            replica.clear_read_cache();
        }
    }
}
