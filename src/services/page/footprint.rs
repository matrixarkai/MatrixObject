// What a page service holds besides its replicas' chunks: its own cached
// snapshot and one being received, and the sum of what its replicas hold.
//
// The replicas' read caches are the part that matters: `byte_buffers` claims to
// be the bytes the store can name, and a read cache is bounded by a configured
// size per replica rather than by anything the data implies.

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct PageServiceFootprint {
    pub(crate) read_cache_payload: u64,
    pub(crate) read_cache_overhead: u64,
    pub(crate) page_validation: u64,
    /// Both the page service's own cached snapshot and each replica's.
    pub(crate) export_caches: u64,
}

impl PageServiceFootprint {
    pub(crate) fn total(&self) -> u64 {
        self.read_cache_payload
            .saturating_add(self.read_cache_overhead)
            .saturating_add(self.page_validation)
            .saturating_add(self.export_caches)
    }
}

impl MatrixPageService {
    pub(crate) fn footprint_bytes(&self) -> PageServiceFootprint {
        let mut total = PageServiceFootprint {
            export_caches: self
                .snapshot_export_cache
                .borrow()
                .as_ref()
                .map(|cache| cache.footprint_bytes())
                .unwrap_or(0)
                .saturating_add(self.snapshot_assembler.footprint_bytes()),
            ..PageServiceFootprint::default()
        };
        for replica in &self.replicas {
            let replica = replica.footprint_bytes();
            total.read_cache_payload = total
                .read_cache_payload
                .saturating_add(replica.read_cache_payload);
            total.read_cache_overhead = total
                .read_cache_overhead
                .saturating_add(replica.read_cache_overhead);
            total.page_validation = total.page_validation.saturating_add(replica.page_validation);
            total.export_caches = total.export_caches.saturating_add(replica.export_caches);
        }
        total
    }
}

impl PageSnapshotExportCache {
    fn footprint_bytes(&self) -> u64 {
        (self.bytes.len() as u64)
            .saturating_add(self.stamp.capacity() as u64)
            .saturating_add(self.checksum.capacity() as u64)
    }
}
