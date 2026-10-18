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
    /// The seven receipt maps, each bounded by `receipt_retention`.
    pub(crate) receipts: u64,
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
            receipts: self.receipt_bytes(),
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
// What a chunk service's receipt maps hold.
//
// There are seven, each bounded by `receipt_retention`, which defaults to
// 100,000 -- so a service can hold 700,000 receipts, and a page service with
// three replicas three times that. The audit reported the entry counts and
// none of the bytes, which is the same shape of omission as the metadata
// exclusion: a figure that names what it counts, counting less than it should.
//
// Every receipt is a few short strings and a run of counters, so the cost is
// dominated by the inline struct and the idempotency key it is filed under.

/// Inline plus heap for a receipt map: the key handle and the value per entry,
/// the key's own characters, and whatever the value owns.
fn chunk_receipt_map_bytes<V, F>(map: &BTreeMap<String, V>, value_heap: F) -> u64
where
    F: Fn(&V) -> u64,
{
    let inline = (map.len() as u64)
        .saturating_mul((std::mem::size_of::<String>() + std::mem::size_of::<V>()) as u64);
    map.iter().fold(inline, |total, (key, value)| {
        total
            .saturating_add(key.capacity() as u64)
            .saturating_add(value_heap(value))
    })
}

pub(crate) fn route_control_proof_bytes(proof: &PageRouteControlProof) -> u64 {
    (proof.state_format.capacity() + proof.state_checksum.capacity()) as u64
}

pub(crate) fn chunk_repair_report_bytes(report: &ChunkRepairReport) -> u64 {
    (report
        .first_error
        .as_ref()
        .map(|error| error.capacity())
        .unwrap_or(0)
        + report.expected_route_control_checksum.capacity()) as u64
        + route_control_proof_bytes(&report.route_control_proof)
}

impl MatrixChunkService {
    /// The seven receipt maps, in bytes rather than entries.
    pub(crate) fn receipt_bytes(&self) -> u64 {
        chunk_receipt_map_bytes(&self.write_request_receipts, |receipt| {
            (receipt.request_checksum.capacity()
                + receipt.payload_checksum.capacity()
                + receipt.idempotency_key.capacity()) as u64
        })
        .saturating_add(chunk_receipt_map_bytes(
            &self.discard_request_receipts,
            |receipt| {
                (receipt.request_checksum.capacity() + receipt.idempotency_key.capacity()) as u64
            },
        ))
        .saturating_add(chunk_receipt_map_bytes(
            &self.batch_write_request_receipts,
            |receipt| {
                (receipt.request_checksum.capacity()
                    + receipt.payload_checksum.capacity()
                    + receipt.idempotency_key.capacity()) as u64
            },
        ))
        .saturating_add(chunk_receipt_map_bytes(
            &self.batch_discard_request_receipts,
            |receipt| {
                (receipt.request_checksum.capacity() + receipt.idempotency_key.capacity()) as u64
            },
        ))
        .saturating_add(chunk_receipt_map_bytes(
            &self.state_install_request_receipts,
            |receipt| {
                (receipt.request_checksum.capacity()
                    + receipt.payload_checksum.capacity()
                    + receipt.state_checksum.capacity()
                    + receipt.idempotency_key.capacity()) as u64
            },
        ))
        .saturating_add(chunk_receipt_map_bytes(
            &self.reclaim_request_receipts,
            |receipt| {
                (receipt.request_checksum.capacity() + receipt.idempotency_key.capacity()) as u64
            },
        ))
        .saturating_add(chunk_receipt_map_bytes(
            &self.repair_request_receipts,
            |receipt| {
                (receipt.request_checksum.capacity() + receipt.idempotency_key.capacity()) as u64
                    + chunk_repair_report_bytes(&receipt.report)
            },
        ))
    }
}
