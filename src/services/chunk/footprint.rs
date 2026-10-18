// What a chunk service holds besides the chunks themselves.
//
// `ACCOUNTED.byte_buffers` says it is the bytes the store can name, and the
// read cache is bytes: it holds chunk payload, it is bounded by a configured
// size per replica rather than by anything the data implies, and it was in no
// total. A deployment serving reads can hold as much there as it holds data.
//
// The per-page validation hashes beside it are the same shape -- eight bytes
// per page of every chunk the cache has validated -- and the snapshot export
// cache is a whole encoded service.

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ChunkServiceFootprint {
    /// Chunk payload held in the read cache. The service counts this itself,
    /// so the audit reports the same number the cache enforces its bound on.
    pub(crate) read_cache_payload: u64,
    /// The cache's own structures: the entry and key of each cached range, the
    /// checksum string beside it, and the two indexes and the queue that make
    /// eviction possible.
    pub(crate) read_cache_overhead: u64,
    /// Eight bytes per page of every chunk the cache has validated.
    pub(crate) page_validation: u64,
    /// A whole encoded service, cached, plus one being received.
    pub(crate) export_caches: u64,
}

impl MatrixChunkService {
    pub(crate) fn footprint_bytes(&self) -> ChunkServiceFootprint {
        let read_cache = self.read_cache.borrow();
        let mut read_cache_overhead = (read_cache.len() as u64).saturating_mul(
            (std::mem::size_of::<ChunkReadCacheKey>()
                + std::mem::size_of::<ChunkReadCacheEntry>()) as u64,
        );
        for entry in read_cache.values() {
            read_cache_overhead =
                read_cache_overhead.saturating_add(entry.range_checksum_capacity());
        }
        let by_chunk = self.read_cache_by_chunk.borrow();
        read_cache_overhead = read_cache_overhead.saturating_add(
            (by_chunk.len() as u64).saturating_mul(
                (std::mem::size_of::<ChunkId>()
                    + std::mem::size_of::<HashSet<ChunkReadCacheRangeKey>>()) as u64,
            ),
        );
        for ranges in by_chunk.values() {
            read_cache_overhead = read_cache_overhead.saturating_add(
                (ranges.len() as u64)
                    .saturating_mul(std::mem::size_of::<ChunkReadCacheRangeKey>() as u64),
            );
        }
        let lru = self.read_cache_lru.borrow();
        read_cache_overhead = read_cache_overhead.saturating_add(
            (lru.capacity() as u64)
                .saturating_mul(std::mem::size_of::<(u64, ChunkReadCacheKey)>() as u64),
        );

        let validation = self.page_validation.borrow();
        let mut page_validation = (validation.len() as u64).saturating_mul(
            (std::mem::size_of::<ChunkId>() + std::mem::size_of::<(u64, String, Vec<u64>)>())
                as u64,
        );
        for (_, checksum, pages) in validation.values() {
            page_validation = page_validation
                .saturating_add(checksum.capacity() as u64)
                .saturating_add(
                    (pages.capacity() as u64).saturating_mul(std::mem::size_of::<u64>() as u64),
                );
        }

        let export_caches = self
            .snapshot_export_cache
            .borrow()
            .as_ref()
            .map(|cache| cache.footprint_bytes())
            .unwrap_or(0)
            .saturating_add(self.snapshot_assembler.footprint_bytes());

        ChunkServiceFootprint {
            read_cache_payload: self.read_cache_bytes.get(),
            read_cache_overhead,
            page_validation,
            export_caches,
        }
    }
}

impl ChunkSnapshotExportCache {
    fn footprint_bytes(&self) -> u64 {
        (self.bytes.len() as u64)
            .saturating_add(self.stamp.capacity() as u64)
            .saturating_add(self.checksum.capacity() as u64)
    }
}
