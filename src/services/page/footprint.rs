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
