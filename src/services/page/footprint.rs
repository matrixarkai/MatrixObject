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
    pub(crate) fn receipt_bytes(&self) -> u64 {
        page_receipt_map_bytes(&self.pending_allocations, page_allocation_bytes)
            .saturating_add(page_receipt_map_bytes(
                &self.allocation_request_receipts,
                page_allocation_bytes,
            ))
            .saturating_add(page_receipt_map_bytes(
                &self.batch_allocation_request_receipts,
                |allocations| {
                    allocations.iter().fold(
                        (allocations.capacity() as u64)
                            .saturating_mul(std::mem::size_of::<PageAllocation>() as u64),
                        |total, allocation| {
                            total.saturating_add(page_allocation_bytes(allocation))
                        },
                    )
                },
            ))
            .saturating_add(page_receipt_map_bytes(
                &self.release_allocation_request_receipts,
                page_allocation_bytes,
            ))
            .saturating_add(page_receipt_map_bytes(
                &self.write_request_receipts,
                |receipt| {
                    (receipt.request_checksum.capacity()
                        + receipt.payload_checksum.capacity()
                        + receipt.idempotency_key.capacity()) as u64
                },
            ))
            .saturating_add(page_receipt_map_bytes(
                &self.batch_write_request_receipts,
                |receipt| {
                    (receipt.request_checksum.capacity()
                        + receipt.payload_checksum.capacity()
                        + receipt.idempotency_key.capacity()) as u64
                },
            ))
            .saturating_add(page_receipt_map_bytes(
                &self.discard_request_receipts,
                |receipt| {
                    (receipt.request_checksum.capacity() + receipt.idempotency_key.capacity())
                        as u64
                },
            ))
            .saturating_add(page_receipt_map_bytes(
                &self.batch_discard_request_receipts,
                |receipt| {
                    (receipt.request_checksum.capacity() + receipt.idempotency_key.capacity())
                        as u64
                },
            ))
            .saturating_add(page_receipt_map_bytes(
                &self.repair_request_receipts,
                |receipt| {
                    (receipt.request_checksum.capacity() + receipt.idempotency_key.capacity())
                        as u64
                        + chunk_repair_report_bytes(&receipt.report)
                },
            ))
            .saturating_add(page_receipt_map_bytes(
                &self.range_repair_request_receipts,
                |receipt| {
                    (receipt.request_checksum.capacity() + receipt.idempotency_key.capacity())
                        as u64
                        + bounded_repair_report_bytes(&receipt.report)
                },
            ))
            .saturating_add(page_receipt_map_bytes(
                &self.audit_repair_request_receipts,
                |receipt| {
                    (receipt.request_checksum.capacity() + receipt.idempotency_key.capacity())
                        as u64
                        + audit_repair_report_bytes(&receipt.report)
                },
            ))
            .saturating_add(
                self.chunk_endpoints.iter().fold(
                    (self.chunk_endpoints.capacity() as u64)
                        .saturating_mul(std::mem::size_of::<String>() as u64),
                    |total, endpoint| total.saturating_add(endpoint.capacity() as u64),
                ),
            )
            .saturating_add(
                (self.sealed_slabs.len() as u64)
                    .saturating_mul(std::mem::size_of::<SlabId>() as u64),
            )
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
// chunk's payload is an `Arc<Vec<u8>>`, so cloning the chunk map shares the bytes
// and copies the structure around them -- the map's nodes, the chunk identity,
// the inline state and the checksum text. Reporting those together would read as
// if a mutation copied every stored byte, which it does not; reporting only the
// shared bytes would hide the part that does grow.

/// A `BTreeMap<String, V>` at its inline size plus its key characters.
///
/// Heap a value owns below its inline size is not counted, which is what the
/// `inline_and_key` in these key names says. B-tree node slack is excluded, as
/// it is everywhere in this audit.
fn snapshot_map_inline_and_key_bytes<V>(map: &BTreeMap<String, V>) -> u64 {
    map.keys().fold(0u64, |total, key| {
        total
            .saturating_add((std::mem::size_of::<String>() + std::mem::size_of::<V>()) as u64)
            .saturating_add(key.capacity() as u64)
    })
}

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

impl PageServiceSnapshot {
    pub(crate) fn footprint_audit(&self) -> Vec<(String, u64)> {
        let page_receipts = snapshot_map_inline_and_key_bytes(&self.pending_allocations)
            .saturating_add(snapshot_map_inline_and_key_bytes(&self.allocation_request_receipts))
            .saturating_add(snapshot_map_inline_and_key_bytes(
                &self.batch_allocation_request_receipts,
            ))
            .saturating_add(snapshot_map_inline_and_key_bytes(
                &self.release_allocation_request_receipts,
            ))
            .saturating_add(snapshot_map_inline_and_key_bytes(&self.write_request_receipts))
            .saturating_add(snapshot_map_inline_and_key_bytes(
                &self.batch_write_request_receipts,
            ))
            .saturating_add(snapshot_map_inline_and_key_bytes(&self.discard_request_receipts))
            .saturating_add(snapshot_map_inline_and_key_bytes(
                &self.batch_discard_request_receipts,
            ))
            .saturating_add(snapshot_map_inline_and_key_bytes(&self.repair_request_receipts))
            .saturating_add(snapshot_map_inline_and_key_bytes(
                &self.range_repair_request_receipts,
            ))
            .saturating_add(snapshot_map_inline_and_key_bytes(
                &self.audit_repair_request_receipts,
            ));

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
        let mut replica_receipts = 0u64;
        let mut chunks_copied = 0u64;
        let mut chunks_shared = 0u64;
        let mut replica_other = 0u64;
        let mut chunk_count = 0u64;
        for replica in &self.replicas {
            replica_receipts = replica_receipts
                .saturating_add(snapshot_map_inline_and_key_bytes(&replica.write_request_receipts))
                .saturating_add(snapshot_map_inline_and_key_bytes(
                    &replica.discard_request_receipts,
                ))
                .saturating_add(snapshot_map_inline_and_key_bytes(
                    &replica.batch_write_request_receipts,
                ))
                .saturating_add(snapshot_map_inline_and_key_bytes(
                    &replica.batch_discard_request_receipts,
                ))
                .saturating_add(snapshot_map_inline_and_key_bytes(
                    &replica.state_install_request_receipts,
                ))
                .saturating_add(snapshot_map_inline_and_key_bytes(
                    &replica.reclaim_request_receipts,
                ))
                .saturating_add(snapshot_map_inline_and_key_bytes(
                    &replica.repair_request_receipts,
                ));
            replica_other = replica_other
                .saturating_add(std::mem::size_of::<ChunkServiceSnapshot>() as u64)
                .saturating_add(replica.checksum.capacity() as u64)
                .saturating_add(replica.last_partial_reclaim_failure_status.capacity() as u64);
            for (_, state) in replica.chunks.iter() {
                chunk_count = chunk_count.saturating_add(1);
                chunks_copied = chunks_copied
                    .saturating_add(
                        (std::mem::size_of::<ChunkId>() + std::mem::size_of::<ChunkState>()) as u64,
                    )
                    .saturating_add(state.checksum.capacity() as u64)
                    .saturating_add(
                        (state.discarded_ranges.capacity() as u64)
                            .saturating_mul(std::mem::size_of::<DataRange>() as u64),
                    );
                chunks_shared = chunks_shared
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
            ("batch.page_receipts.inline_and_key_bytes".to_string(), page_receipts),
            ("batch.page_other.bytes".to_string(), page_other),
            (
                "batch.replica_receipts.inline_and_key_bytes".to_string(),
                replica_receipts,
            ),
            ("batch.replica_other.bytes".to_string(), replica_other),
            ("batch.replica_chunks.copied_bytes".to_string(), chunks_copied),
            ("batch.replica_chunks.entries".to_string(), chunk_count),
            // Named by the live replicas as well, so this is what the snapshot
            // points at rather than what it duplicates.
            (
                "batch.replica_chunks.shared_payload_bytes".to_string(),
                chunks_shared,
            ),
            ("batch.COPIED_bytes".to_string(), copied),
        ]
    }
}
