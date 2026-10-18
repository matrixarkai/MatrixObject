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
    /// Eleven receipt maps here and seven in each replica, every one bounded
    /// by `receipt_retention`, which defaults to 100,000.
    pub(crate) receipts: u64,
    /// Of `receipts`, the part that is a receipt repeating its own map key.
    pub(crate) receipt_duplicate_keys: u64,
    /// How many receipts those bytes come from -- one per receipt that agrees
    /// with its own key.
    pub(crate) receipt_matching_keys: u64,
}

impl PageServiceFootprint {
    pub(crate) fn total(&self) -> u64 {
        self.read_cache_payload
            .saturating_add(self.read_cache_overhead)
            .saturating_add(self.page_validation)
            .saturating_add(self.export_caches)
            .saturating_add(self.receipts)
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
            receipts: self.receipt_bytes(),
            receipt_duplicate_keys: self.receipt_duplicate_key_bytes(),
            receipt_matching_keys: self.receipt_matching_key_count(),
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
            total.receipts = total.receipts.saturating_add(replica.receipts);
            total.receipt_duplicate_keys = total
                .receipt_duplicate_keys
                .saturating_add(replica.receipt_duplicate_keys);
            total.receipt_matching_keys = total
                .receipt_matching_keys
                .saturating_add(replica.receipt_matching_keys);
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
// What a page service's receipt maps hold.
//
// There are eleven, each bounded by `receipt_retention`, which defaults to
// 100,000. The audit reported the entry counts and none of the bytes.

fn page_receipt_map_bytes<V, F>(map: &BTreeMap<String, V>, value_heap: F) -> u64
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
/// Every one of these maps is keyed by the idempotency key, and every receipt
/// in them also carries an `idempotency_key` field holding the same text in a
/// second allocation. The field is in the receipt's proof, so it cannot be
/// dropped without a migration -- but the allocation can be shared, and this
/// says what that is worth before anything changes.
fn page_receipt_duplicate_key_bytes<V, F>(map: &BTreeMap<String, V>, key_of: F) -> u64
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
fn page_receipt_matching_key_count<V, F>(map: &BTreeMap<String, V>, key_of: F) -> u64
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

fn page_batch_allocation_heap(allocations: &[PageAllocation]) -> u64 {
    allocations.iter().fold(
        (allocations.len() as u64).saturating_mul(std::mem::size_of::<PageAllocation>() as u64),
        |total, allocation| total.saturating_add(page_allocation_bytes(allocation)),
    )
}

fn page_write_receipt_heap(receipt: &PageWriteReceipt) -> u64 {
    (receipt.request_checksum.capacity()
        + receipt.payload_checksum.capacity()
        + receipt.idempotency_key.capacity()) as u64
}

fn page_batch_write_receipt_heap(receipt: &PageBatchWriteReceipt) -> u64 {
    (receipt.request_checksum.capacity()
        + receipt.payload_checksum.capacity()
        + receipt.idempotency_key.capacity()) as u64
}

fn page_discard_receipt_heap(receipt: &PageDiscardReceipt) -> u64 {
    (receipt.request_checksum.capacity() + receipt.idempotency_key.capacity()) as u64
}

fn page_batch_discard_receipt_heap(receipt: &PageBatchDiscardReceipt) -> u64 {
    (receipt.request_checksum.capacity() + receipt.idempotency_key.capacity()) as u64
}

fn page_range_repair_receipt_heap(receipt: &PageRangeRepairReceipt) -> u64 {
    (receipt.request_checksum.capacity() + receipt.idempotency_key.capacity()) as u64
        + bounded_repair_report_bytes(&receipt.report)
}

fn page_audit_repair_receipt_heap(receipt: &PageAuditRepairReceipt) -> u64 {
    (receipt.request_checksum.capacity() + receipt.idempotency_key.capacity()) as u64
        + audit_repair_report_bytes(&receipt.report)
}

/// The eleven receipt maps of a page service, live or snapshotted.
///
/// A snapshot holds the same eleven with the same types, and it is a clone, so
/// it costs the same. One function, so the two cannot drift.
#[allow(clippy::too_many_arguments)]
fn page_receipt_maps_bytes(
    pending: &BTreeMap<String, PageAllocation>,
    allocation: &BTreeMap<String, PageAllocation>,
    batch_allocation: &BTreeMap<String, Vec<PageAllocation>>,
    release: &BTreeMap<String, PageAllocation>,
    write: &BTreeMap<String, PageWriteReceipt>,
    batch_write: &BTreeMap<String, PageBatchWriteReceipt>,
    discard: &BTreeMap<String, PageDiscardReceipt>,
    batch_discard: &BTreeMap<String, PageBatchDiscardReceipt>,
    repair: &BTreeMap<String, ChunkRepairReceipt>,
    range_repair: &BTreeMap<String, PageRangeRepairReceipt>,
    audit_repair: &BTreeMap<String, PageAuditRepairReceipt>,
) -> u64 {
    page_receipt_map_bytes(pending, page_allocation_bytes)
        .saturating_add(page_receipt_map_bytes(allocation, page_allocation_bytes))
        .saturating_add(page_receipt_map_bytes(batch_allocation, |allocations| {
            page_batch_allocation_heap(allocations)
        }))
        .saturating_add(page_receipt_map_bytes(release, page_allocation_bytes))
        .saturating_add(page_receipt_map_bytes(write, page_write_receipt_heap))
        .saturating_add(page_receipt_map_bytes(
            batch_write,
            page_batch_write_receipt_heap,
        ))
        .saturating_add(page_receipt_map_bytes(discard, page_discard_receipt_heap))
        .saturating_add(page_receipt_map_bytes(
            batch_discard,
            page_batch_discard_receipt_heap,
        ))
        .saturating_add(page_receipt_map_bytes(repair, chunk_repair_receipt_heap))
        .saturating_add(page_receipt_map_bytes(
            range_repair,
            page_range_repair_receipt_heap,
        ))
        .saturating_add(page_receipt_map_bytes(
            audit_repair,
            page_audit_repair_receipt_heap,
        ))
}

fn page_allocation_bytes(allocation: &PageAllocation) -> u64 {
    allocation.allocation_id.capacity() as u64
}

fn bounded_repair_report_bytes(report: &BoundedRepairReport) -> u64 {
    (report.expected_route_control_checksum.capacity() as u64)
        .saturating_add(route_control_proof_bytes(&report.route_control_proof))
}

fn audit_repair_report_bytes(report: &PageReplicaAuditRepairReport) -> u64 {
    let items = report.audit.items.iter().fold(
        (report.audit.items.capacity() as u64)
            .saturating_mul(std::mem::size_of::<PageReplicaAuditItem>() as u64),
        |total, item| {
            total.saturating_add((item.status.capacity() + item.checksum.capacity()) as u64)
        },
    );
    items
        .saturating_add(report.repair_reason.capacity() as u64)
        .saturating_add(report.expected_route_control_checksum.capacity() as u64)
        .saturating_add(route_control_proof_bytes(&report.route_control_proof))
}

impl MatrixPageService {
    /// The eleven receipt maps, in bytes rather than entries. The pending
    /// allocations are counted here too: they are the same shape and the same
    /// growth, differing only in that a commit removes them.
    ///
    /// The endpoints and the sealed slabs ride along because they grow with the
    /// same configuration rather than with the data.
    pub(crate) fn receipt_bytes(&self) -> u64 {
        page_receipt_maps_bytes(
            &self.pending_allocations,
            &self.allocation_request_receipts,
            &self.batch_allocation_request_receipts,
            &self.release_allocation_request_receipts,
            &self.write_request_receipts,
            &self.batch_write_request_receipts,
            &self.discard_request_receipts,
            &self.batch_discard_request_receipts,
            &self.repair_request_receipts,
            &self.range_repair_request_receipts,
            &self.audit_repair_request_receipts,
        )
        .saturating_add(
            self.chunk_endpoints.iter().fold(
                (self.chunk_endpoints.capacity() as u64)
                    .saturating_mul(std::mem::size_of::<String>() as u64),
                |total, endpoint| total.saturating_add(endpoint.capacity() as u64),
            ),
        )
        .saturating_add(
            (self.sealed_slabs.len() as u64).saturating_mul(std::mem::size_of::<SlabId>() as u64),
        )
    }

    /// Of `receipt_bytes`, the part that is one receipt holding its own map key
    /// twice. Measured across the maps whose receipts carry the field.
    pub(crate) fn receipt_duplicate_key_bytes(&self) -> u64 {
        page_receipt_duplicate_key_bytes(&self.write_request_receipts, |r| &r.idempotency_key)
            .saturating_add(page_receipt_duplicate_key_bytes(
                &self.batch_write_request_receipts,
                |r| &r.idempotency_key,
            ))
            .saturating_add(page_receipt_duplicate_key_bytes(
                &self.discard_request_receipts,
                |r| &r.idempotency_key,
            ))
            .saturating_add(page_receipt_duplicate_key_bytes(
                &self.batch_discard_request_receipts,
                |r| &r.idempotency_key,
            ))
            .saturating_add(page_receipt_duplicate_key_bytes(
                &self.repair_request_receipts,
                |r| &r.idempotency_key,
            ))
            .saturating_add(page_receipt_duplicate_key_bytes(
                &self.range_repair_request_receipts,
                |r| &r.idempotency_key,
            ))
            .saturating_add(page_receipt_duplicate_key_bytes(
                &self.audit_repair_request_receipts,
                |r| &r.idempotency_key,
            ))
    }

    pub(crate) fn receipt_matching_key_count(&self) -> u64 {
        page_receipt_matching_key_count(&self.write_request_receipts, |r| &r.idempotency_key)
            .saturating_add(page_receipt_matching_key_count(
                &self.batch_write_request_receipts,
                |r| &r.idempotency_key,
            ))
            .saturating_add(page_receipt_matching_key_count(
                &self.discard_request_receipts,
                |r| &r.idempotency_key,
            ))
            .saturating_add(page_receipt_matching_key_count(
                &self.batch_discard_request_receipts,
                |r| &r.idempotency_key,
            ))
            .saturating_add(page_receipt_matching_key_count(
                &self.repair_request_receipts,
                |r| &r.idempotency_key,
            ))
            .saturating_add(page_receipt_matching_key_count(
                &self.range_repair_request_receipts,
                |r| &r.idempotency_key,
            ))
            .saturating_add(page_receipt_matching_key_count(
                &self.audit_repair_request_receipts,
                |r| &r.idempotency_key,
            ))
    }

}

// What the snapshot a mutation batch holds costs.
//
// `begin_mutation_batch` boxes a whole `export_snapshot_unsealed()` and holds it
// until the mutation commits or aborts, which is why the store's own undo stopped
// copying the page service: this snapshot owns that job. Nothing measured what
// *it* costs, and it is the one copy on the put path still proportional to how
// much is stored.
//
// The figures separate what the snapshot copies from what it only names. A
// chunk's payload is an `Arc<Vec<u8>>` and its state is now an
// `Arc<ChunkState>`, so cloning the chunk map copies a chunk identity and a
// handle and names everything behind it. Reporting those together would read as
// if a mutation copied every stored byte, which it does not; reporting only the
// shared bytes would hide the part that does grow.
//
// What the copy is per has not changed -- it is still per chunk -- but it fell
// from 128 bytes a chunk to the identity and the handle, because the state the
// handle points at is no longer duplicated.


/// Charges an `Arc<Vec<u8>>` once per allocation, however many holders name it.
///
/// A chunk's payload is named by the live replica and by every snapshot taken of
/// it, and adding its length at each holder would report copies that do not
/// exist.
#[derive(Default)]
struct SharedPayloadLedger {
    charged: std::collections::HashSet<usize>,
}

impl SharedPayloadLedger {
    /// A chunk's state, charged once however many snapshots name it.
    ///
    /// The state lives behind an `Arc` so that taking a snapshot clones a
    /// handle rather than a state. Charging it at every holder would report
    /// copies that do not exist, which is the same rule the payload below
    /// follows.
    fn charge_chunk_state(&mut self, state: &std::sync::Arc<ChunkState>) -> u64 {
        if self
            .charged
            .insert(std::sync::Arc::as_ptr(state) as *const u8 as usize)
        {
            ((std::mem::size_of::<ChunkState>() + 2 * std::mem::size_of::<usize>()) as u64)
                .saturating_add(state.checksum.capacity() as u64)
                .saturating_add(
                    (state.discarded_ranges.capacity() as u64)
                        .saturating_mul(std::mem::size_of::<DataRange>() as u64),
                )
        } else {
            0
        }
    }

    fn charge(&mut self, payload: &std::sync::Arc<Vec<u8>>) -> u64 {
        if self
            .charged
            .insert(std::sync::Arc::as_ptr(payload) as *const u8 as usize)
        {
            (payload.capacity() as u64)
                .saturating_add((2 * std::mem::size_of::<usize>()) as u64)
        } else {
            0
        }
    }
}

impl MatrixPageService {
    /// What an OPEN mutation batch holds, or nothing when none is open.
    ///
    /// `begin_mutation_batch` boxes a whole page-service snapshot and holds it
    /// until the mutation commits or aborts. No total in `Store::footprint_audit`
    /// included any part of it, so the audit was blind to the moment a
    /// deployment's resident bytes peak -- which is precisely while a mutation
    /// is in flight.
    pub(crate) fn open_mutation_batch_footprint_audit(&self) -> Vec<(String, u64)> {
        match &self.active_mutation_batch {
            Some(batch) => batch.snapshot.footprint_audit(),
            None => Vec::new(),
        }
    }
}

impl PageServiceSnapshot {
    /// The PAGE service's half of the footprint, reached through
    /// `open_mutation_batch_footprint_audit` so that `Store::footprint_audit`
    /// reports what an open batch holds. Every total that audit reported before
    /// excluded all of it.
    pub(crate) fn footprint_audit(&self) -> Vec<(String, u64)> {
        let page_receipts = page_receipt_maps_bytes(
            &self.pending_allocations,
            &self.allocation_request_receipts,
            &self.batch_allocation_request_receipts,
            &self.release_allocation_request_receipts,
            &self.write_request_receipts,
            &self.batch_write_request_receipts,
            &self.discard_request_receipts,
            &self.batch_discard_request_receipts,
            &self.repair_request_receipts,
            &self.range_repair_request_receipts,
            &self.audit_repair_request_receipts,
        );

        let mut page_other = (std::mem::size_of::<PageServiceSnapshot>() as u64)
            .saturating_add(self.checksum.capacity() as u64)
            .saturating_add(
                (self.quarantined_replicas.len() as u64)
                    .saturating_mul(std::mem::size_of::<usize>() as u64),
            )
            .saturating_add(
                (self.disabled_chunk_endpoints.len() as u64)
                    .saturating_mul(std::mem::size_of::<usize>() as u64),
            )
            .saturating_add(
                (self.sealed_slabs.len() as u64).saturating_mul(std::mem::size_of::<SlabId>() as u64),
            );
        for endpoint in &self.chunk_endpoints {
            page_other = page_other
                .saturating_add(std::mem::size_of::<String>() as u64)
                .saturating_add(endpoint.capacity() as u64);
        }

        let mut ledger = SharedPayloadLedger::default();
        // A second ledger, because a chunk's state and its payload are
        // separate allocations and one set of pointers cannot stand for both.
        let mut state_ledger = SharedPayloadLedger::default();
        let mut replica_receipts = 0u64;
        let mut chunks_copied = 0u64;
        let mut chunks_shared = 0u64;
        let mut replica_other = 0u64;
        let mut chunk_count = 0u64;
        for replica in &self.replicas {
            replica_receipts = replica_receipts.saturating_add(chunk_receipt_maps_bytes(
                &replica.write_request_receipts,
                &replica.discard_request_receipts,
                &replica.batch_write_request_receipts,
                &replica.batch_discard_request_receipts,
                &replica.state_install_request_receipts,
                &replica.reclaim_request_receipts,
                &replica.repair_request_receipts,
            ));
            replica_other = replica_other
                .saturating_add(std::mem::size_of::<ChunkServiceSnapshot>() as u64)
                .saturating_add(replica.checksum.capacity() as u64)
                .saturating_add(replica.last_partial_reclaim_failure_status.capacity() as u64);
            for (_, state) in replica.chunks.iter() {
                chunk_count = chunk_count.saturating_add(1);
                // The chunk identity and one handle. The state behind the
                // handle is named rather than copied, so it is charged below
                // and once.
                chunks_copied = chunks_copied.saturating_add(
                    (std::mem::size_of::<ChunkId>()
                        + std::mem::size_of::<std::sync::Arc<ChunkState>>()) as u64,
                );
                chunks_shared = chunks_shared
                    .saturating_add(state_ledger.charge_chunk_state(state))
                    .saturating_add(ledger.charge(&state.data))
                    .saturating_add(ledger.charge(&state.ec_parity_fragment));
            }
        }

        let copied = page_receipts
            .saturating_add(page_other)
            .saturating_add(replica_receipts)
            .saturating_add(replica_other)
            .saturating_add(chunks_copied);
        vec![
            ("batch.page_receipts.bytes".to_string(), page_receipts),
            ("batch.page_other.bytes".to_string(), page_other),
            (
                "batch.replica_receipts.bytes".to_string(),
                replica_receipts,
            ),
            ("batch.replica_other.bytes".to_string(), replica_other),
            ("batch.replica_chunks.copied_bytes".to_string(), chunks_copied),
            ("batch.replica_chunks.entries".to_string(), chunk_count),
            // Named by the live replicas as well, so this is what the snapshot
            // points at rather than what it duplicates.
            (
                "batch.replica_chunks.shared_state_and_payload_bytes".to_string(),
                chunks_shared,
            ),
            ("batch.COPIED_bytes".to_string(), copied),
        ]
    }
}
