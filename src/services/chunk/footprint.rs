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
pub(crate) struct ReplicaServiceFootprint {
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
    /// Of `receipts`, the part that is a receipt repeating its own map key.
    pub(crate) receipt_duplicate_keys: u64,
    /// How many receipts those bytes come from -- one per receipt that agrees
    /// with its own key.
    pub(crate) receipt_matching_keys: u64,
}

impl MatrixReplicaService {
    pub(crate) fn footprint_bytes(&self) -> ReplicaServiceFootprint {
        let read_cache = self.read_cache.borrow();
        let mut read_cache_overhead = (read_cache.len() as u64).saturating_mul(
            (std::mem::size_of::<ReplicaReadCacheKey>()
                + std::mem::size_of::<ReplicaReadCacheEntry>()) as u64,
        );
        for entry in read_cache.values() {
            read_cache_overhead =
                read_cache_overhead.saturating_add(entry.range_checksum_capacity());
        }
        let by_chunk = self.read_cache_by_chunk.borrow();
        read_cache_overhead = read_cache_overhead.saturating_add(
            (by_chunk.len() as u64).saturating_mul(
                (std::mem::size_of::<PageId>()
                    + std::mem::size_of::<HashSet<ReplicaReadCacheRangeKey>>()) as u64,
            ),
        );
        for ranges in by_chunk.values() {
            read_cache_overhead = read_cache_overhead.saturating_add(
                (ranges.len() as u64)
                    .saturating_mul(std::mem::size_of::<ReplicaReadCacheRangeKey>() as u64),
            );
        }
        let lru = self.read_cache_lru.borrow();
        read_cache_overhead = read_cache_overhead.saturating_add(
            (lru.capacity() as u64)
                .saturating_mul(std::mem::size_of::<(u64, ReplicaReadCacheKey)>() as u64),
        );

        let validation = self.page_validation.borrow();
        let mut page_validation = (validation.len() as u64).saturating_mul(
            (std::mem::size_of::<PageId>() + std::mem::size_of::<(u64, String, Vec<u64>)>())
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

        ReplicaServiceFootprint {
            read_cache_payload: self.read_cache_bytes.get(),
            read_cache_overhead,
            page_validation,
            export_caches,
            receipts: self.receipt_bytes(),
            receipt_duplicate_keys: self.receipt_duplicate_key_bytes(),
            receipt_matching_keys: self.receipt_matching_key_count(),
        }
    }
}

impl ReplicaSnapshotExportCache {
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
pub(crate) fn replica_receipt_map_bytes<V, F>(map: &BTreeMap<String, V>, value_heap: F) -> u64
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

/// Key characters a receipt repeats inside itself.
///
/// Each of these maps is keyed by the idempotency key, and each receipt also
/// carries an `idempotency_key` field holding that same text in a second
/// allocation. The field is part of the receipt's proof, so dropping it is a
/// migration -- the allocation is what can be shared, and this says what that
/// is worth before anything changes.
fn replica_receipt_duplicate_key_bytes<V, F>(map: &BTreeMap<String, V>, key_of: F) -> u64
where
    F: Fn(&V) -> &String,
{
    map.iter().fold(0u64, |total, (key, value)| {
        let repeated = key_of(value);
        if repeated == key {
            total.saturating_add(repeated.capacity() as u64)
        } else {
            total
        }
    })
}

/// Receipts whose own field holds exactly the key they are filed under.
///
/// Reported next to the bytes because sharing one allocation between the two is
/// only sound while they agree, and a receipt filed under a key it does not
/// carry would make that substitution lose the field's value silently.
fn replica_receipt_matching_key_count<V, F>(map: &BTreeMap<String, V>, key_of: F) -> u64
where
    F: Fn(&V) -> &String,
{
    map.iter().fold(0u64, |total, (key, value)| {
        if key_of(value) == key {
            total.saturating_add(1)
        } else {
            total
        }
    })
}

// The heap each receipt kind owns, named once.
//
// Both the live service and a snapshot of it hold these maps, and a snapshot is
// a clone, so the same receipt costs the same in either. Writing the figure
// twice is how the snapshot's half came to count only the inline struct and the
// key, and to miss the strings the value owns -- 69 bytes a receipt on the
// discard path, which is a third of what a copied receipt really costs.

pub(crate) fn replica_write_receipt_heap(receipt: &ReplicaWriteReceipt) -> u64 {
    (receipt.request_checksum.capacity()
        + receipt.payload_checksum.capacity()
        + receipt.idempotency_key.capacity()) as u64
}

pub(crate) fn replica_discard_receipt_heap(receipt: &ReplicaDiscardReceipt) -> u64 {
    (receipt.request_checksum.capacity() + receipt.idempotency_key.capacity()) as u64
}

pub(crate) fn replica_batch_write_receipt_heap(receipt: &ReplicaBatchWriteReceipt) -> u64 {
    (receipt.request_checksum.capacity()
        + receipt.payload_checksum.capacity()
        + receipt.idempotency_key.capacity()) as u64
}

pub(crate) fn replica_batch_discard_receipt_heap(receipt: &ReplicaBatchDiscardReceipt) -> u64 {
    (receipt.request_checksum.capacity() + receipt.idempotency_key.capacity()) as u64
}

pub(crate) fn replica_state_install_receipt_heap(receipt: &ReplicaStateInstallReceipt) -> u64 {
    (receipt.request_checksum.capacity()
        + receipt.payload_checksum.capacity()
        + receipt.state_checksum.capacity()
        + receipt.idempotency_key.capacity()) as u64
}

pub(crate) fn replica_reclaim_receipt_heap(receipt: &ReplicaReclaimReceipt) -> u64 {
    (receipt.request_checksum.capacity() + receipt.idempotency_key.capacity()) as u64
}

pub(crate) fn replica_repair_receipt_heap(receipt: &ReplicaRepairReceipt) -> u64 {
    (receipt.request_checksum.capacity() + receipt.idempotency_key.capacity()) as u64
        + replica_repair_report_bytes(&receipt.report)
}

pub(crate) fn route_control_proof_bytes(proof: &ChunkRouteControlProof) -> u64 {
    (proof.state_format.capacity() + proof.state_checksum.capacity()) as u64
}

pub(crate) fn replica_repair_report_bytes(report: &ReplicaRepairReport) -> u64 {
    (report
        .first_error
        .as_ref()
        .map(|error| error.capacity())
        .unwrap_or(0)
        + report.expected_route_control_checksum.capacity()) as u64
        + route_control_proof_bytes(&report.route_control_proof)
}

/// The seven receipt maps of a chunk service, live or snapshotted.
///
/// A snapshot holds the same seven maps with the same types, and it is a clone,
/// so it costs the same. One function, so the two cannot drift.
#[allow(clippy::too_many_arguments)]
pub(crate) fn replica_receipt_maps_bytes(
    write: &BTreeMap<String, ReplicaWriteReceipt>,
    discard: &BTreeMap<String, ReplicaDiscardReceipt>,
    batch_write: &BTreeMap<String, ReplicaBatchWriteReceipt>,
    batch_discard: &BTreeMap<String, ReplicaBatchDiscardReceipt>,
    state_install: &BTreeMap<String, ReplicaStateInstallReceipt>,
    reclaim: &BTreeMap<String, ReplicaReclaimReceipt>,
    repair: &BTreeMap<String, ReplicaRepairReceipt>,
) -> u64 {
    replica_receipt_map_bytes(write, replica_write_receipt_heap)
        .saturating_add(replica_receipt_map_bytes(discard, replica_discard_receipt_heap))
        .saturating_add(replica_receipt_map_bytes(
            batch_write,
            replica_batch_write_receipt_heap,
        ))
        .saturating_add(replica_receipt_map_bytes(
            batch_discard,
            replica_batch_discard_receipt_heap,
        ))
        .saturating_add(replica_receipt_map_bytes(
            state_install,
            replica_state_install_receipt_heap,
        ))
        .saturating_add(replica_receipt_map_bytes(reclaim, replica_reclaim_receipt_heap))
        .saturating_add(replica_receipt_map_bytes(repair, replica_repair_receipt_heap))
}

impl MatrixReplicaService {
    /// Of `receipt_bytes`, the part that is one receipt holding its own map key
    /// twice, across all seven maps.
    pub(crate) fn receipt_duplicate_key_bytes(&self) -> u64 {
        replica_receipt_duplicate_key_bytes(&self.write_request_receipts, |r| &r.idempotency_key)
            .saturating_add(replica_receipt_duplicate_key_bytes(
                &self.discard_request_receipts,
                |r| &r.idempotency_key,
            ))
            .saturating_add(replica_receipt_duplicate_key_bytes(
                &self.batch_write_request_receipts,
                |r| &r.idempotency_key,
            ))
            .saturating_add(replica_receipt_duplicate_key_bytes(
                &self.batch_discard_request_receipts,
                |r| &r.idempotency_key,
            ))
            .saturating_add(replica_receipt_duplicate_key_bytes(
                &self.state_install_request_receipts,
                |r| &r.idempotency_key,
            ))
            .saturating_add(replica_receipt_duplicate_key_bytes(
                &self.reclaim_request_receipts,
                |r| &r.idempotency_key,
            ))
            .saturating_add(replica_receipt_duplicate_key_bytes(
                &self.repair_request_receipts,
                |r| &r.idempotency_key,
            ))
    }

    pub(crate) fn receipt_matching_key_count(&self) -> u64 {
        replica_receipt_matching_key_count(&self.write_request_receipts, |r| &r.idempotency_key)
            .saturating_add(replica_receipt_matching_key_count(
                &self.discard_request_receipts,
                |r| &r.idempotency_key,
            ))
            .saturating_add(replica_receipt_matching_key_count(
                &self.batch_write_request_receipts,
                |r| &r.idempotency_key,
            ))
            .saturating_add(replica_receipt_matching_key_count(
                &self.batch_discard_request_receipts,
                |r| &r.idempotency_key,
            ))
            .saturating_add(replica_receipt_matching_key_count(
                &self.state_install_request_receipts,
                |r| &r.idempotency_key,
            ))
            .saturating_add(replica_receipt_matching_key_count(
                &self.reclaim_request_receipts,
                |r| &r.idempotency_key,
            ))
            .saturating_add(replica_receipt_matching_key_count(
                &self.repair_request_receipts,
                |r| &r.idempotency_key,
            ))
    }

    /// The seven receipt maps, in bytes rather than entries.
    pub(crate) fn receipt_bytes(&self) -> u64 {
        replica_receipt_maps_bytes(
            &self.write_request_receipts,
            &self.discard_request_receipts,
            &self.batch_write_request_receipts,
            &self.batch_discard_request_receipts,
            &self.state_install_request_receipts,
            &self.reclaim_request_receipts,
            &self.repair_request_receipts,
        )
    }
}
