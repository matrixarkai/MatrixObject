// SPDX-License-Identifier: Apache-2.0
#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct DistributedStorageMutationSnapshot {
    capacity_rejected_writes: u64,
    service_mode_rejected_mutations: u64,
    physical_write_count: u64,
    physical_write_bytes: u64,
    physical_discard_ranges: u64,
    physical_discard_bytes: u64,
    allocation_receipt_count: u64,
    batch_allocation_receipt_count: u64,
    release_allocation_receipt_count: u64,
    write_receipt_count: u64,
    discard_receipt_count: u64,
    batch_write_receipt_count: u64,
    batch_discard_receipt_count: u64,
}

impl DistributedStorageMutationSnapshot {
    fn capture(page: &ChunkStorageReadiness) -> Self {
        let mut snapshot = Self {
            allocation_receipt_count: page.allocation_receipt_count,
            batch_allocation_receipt_count: page.batch_allocation_receipt_count,
            release_allocation_receipt_count: page.release_allocation_receipt_count,
            ..Self::default()
        };
        for replica in &page.replica_readiness {
            snapshot.capacity_rejected_writes = snapshot
                .capacity_rejected_writes
                .saturating_add(replica.capacity_rejected_writes);
            snapshot.service_mode_rejected_mutations = snapshot
                .service_mode_rejected_mutations
                .saturating_add(replica.service_mode_rejected_mutations);
            snapshot.physical_write_count = snapshot
                .physical_write_count
                .saturating_add(replica.physical_write_count);
            snapshot.physical_write_bytes = snapshot
                .physical_write_bytes
                .saturating_add(replica.physical_write_bytes);
            snapshot.physical_discard_ranges = snapshot
                .physical_discard_ranges
                .saturating_add(replica.physical_discard_ranges);
            snapshot.physical_discard_bytes = snapshot
                .physical_discard_bytes
                .saturating_add(replica.physical_discard_bytes);
            snapshot.write_receipt_count = snapshot
                .write_receipt_count
                .saturating_add(replica.write_receipt_count);
            snapshot.discard_receipt_count = snapshot
                .discard_receipt_count
                .saturating_add(replica.discard_receipt_count);
            snapshot.batch_write_receipt_count = snapshot
                .batch_write_receipt_count
                .saturating_add(replica.batch_write_receipt_count);
            snapshot.batch_discard_receipt_count = snapshot
                .batch_discard_receipt_count
                .saturating_add(replica.batch_discard_receipt_count);
        }
        snapshot
    }
}
