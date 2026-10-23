// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ChunkSnapshotExportCache {
    stamp: String,
    bytes: std::sync::Arc<[u8]>,
    checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MatrixChunkService {
    page_size: u64,
    pub(crate) write_quorum: usize,
    pub(crate) read_quorum: usize,
    pub(crate) allocator_chunk_size: u64,
    pub(crate) allocator_next_chunk_id: u64,
    pub(crate) allocator_next_offset: u64,
    pub(crate) receipt_retention: usize,
    pub(crate) pending_allocations: BTreeMap<String, ChunkAllocation>,
    pub(crate) allocation_request_receipts: BTreeMap<String, ChunkAllocation>,
    pub(crate) batch_allocation_request_receipts: BTreeMap<String, Vec<ChunkAllocation>>,
    pub(crate) release_allocation_request_receipts: BTreeMap<String, ChunkAllocation>,
    pub(crate) write_request_receipts: BTreeMap<String, ChunkWriteReceipt>,
    pub(crate) batch_write_request_receipts: BTreeMap<String, ChunkBatchWriteReceipt>,
    pub(crate) discard_request_receipts: BTreeMap<String, ChunkDiscardReceipt>,
    pub(crate) batch_discard_request_receipts: BTreeMap<String, ChunkBatchDiscardReceipt>,
    pub(crate) repair_request_receipts: BTreeMap<String, ReplicaRepairReceipt>,
    pub(crate) range_repair_request_receipts: BTreeMap<String, ChunkRangeRepairReceipt>,
    pub(crate) audit_repair_request_receipts: BTreeMap<String, ChunkAuditRepairReceipt>,
    pub(crate) released_allocation_count: u64,
    pub(crate) quarantined_replicas: BTreeSet<usize>,
    /// The replicas this service knows are behind, and so will not believe.
    ///
    /// # WHY THE AUTHORITY NEEDS A RECORD AT ALL
    ///
    /// A page's `write_sequence` lives on the replica that holds it, and a read takes a
    /// majority over it. That compares replicas against EACH OTHER and never against
    /// anything that knows what was committed, so a replica that missed a write and came
    /// back is merely outvoted -- and when it is not outvoted it wins. Measured, before
    /// this: a read quorum of one served a returning replica's superseded bytes.
    ///
    /// This is the other half, and the replicas cannot write to it. A replica goes in on a
    /// MEMBERSHIP CHANGE -- quarantined, frozen, or taken out of the endpoint set, each of
    /// which stops it receiving writes -- and comes out only through `check_in_replica`,
    /// which checks its pages rather than taking anyone at their word.
    ///
    /// # WHY A SET AND NOT A VERSION NUMBER
    ///
    /// The first version of this was an epoch on the service and one per replica, which
    /// is the general form: a lease number, moved further on a membership change so that a
    /// returning node cannot be mistaken for current. Every use of it was the
    /// one comparison `epoch[i] >= authority` -- a single bit -- and the number cost a
    /// `Vec<u64>` the mutation rollback copies on every clone: the published copy figure
    /// moved 2,485 to 2,517 at 250 objects. An empty `BTreeSet` allocates nothing, so a
    /// store with nothing stale pays nothing, and `quarantined_replicas` above is the same
    /// shape for the same reason. What is given up is HOW FAR behind a replica is, which
    /// nothing read.
    ///
    /// Staleness is a property of a replica, not of a page, so this costs nothing per
    /// page -- which an authoritative sequence per page would not: a 1 TiB store at a
    /// 1 MiB page is a million pages, and a map keyed by `PageId` is tens of megabytes
    /// before the value.
    pub(crate) stale_replicas: BTreeSet<usize>,
    pub(crate) chunk_endpoints: Vec<String>,
    pub(crate) disabled_chunk_endpoints: BTreeSet<usize>,
    chunk_route_control_commit_index: u64,
    chunk_route_control_next_log_index: u64,
    route_qos_config: ChunkRouteQosConfig,
    route_qos_state: RefCell<ChunkRouteQosState>,
    sealed_chunks: BTreeSet<ChunkId>,
    cumulative_quorum_write_degraded_chunks: u64,
    cumulative_quorum_read_degraded_chunks: u64,
    cumulative_quorum_discard_degraded_chunks: u64,
    cumulative_replica_fallback_reads: u64,
    cumulative_physical_chunk_read_count: Cell<u64>,
    cumulative_coalesced_chunk_read_count: Cell<u64>,
    cumulative_cache_prewarm_applied_ranges: u64,
    cumulative_cache_prewarm_failed_ranges: u64,
    cumulative_cache_invalidate_applied_ranges: u64,
    cumulative_cache_invalidate_failed_ranges: u64,
    cumulative_repair_checked_chunks: u64,
    cumulative_repair_degraded_chunks: u64,
    cumulative_repaired_replicas: u64,
    cumulative_failed_repairs: u64,
    cumulative_replica_read_repairs: u64,
    cumulative_replica_read_repair_failures: u64,
    page_read_parallelism: usize,
    page_write_parallelism: usize,
    page_batch_read_parallelism: usize,
    page_batch_write_parallelism: usize,
    page_batch_discard_parallelism: usize,
    page_repair_parallelism: usize,
    replica_fanout_parallelism: usize,
    primary_batch_read_repair_probe_limit: usize,
    local_replica_store_parallelism: usize,
    snapshot_export_cache: RefCell<Option<ChunkSnapshotExportCache>>,
    snapshot_assembler: ChunkServiceSnapshotAssembler,
    active_mutation_batch: Option<ChunkMutationBatch>,
    /// Set when this service spreads a page's bytes over the replicas instead of copying
    /// it to each. `None` is replication, which is the default and what every existing
    /// store is.
    pub(crate) ec_layout: Option<EcStripeLayout>,
    pub(crate) replicas: Vec<MatrixReplicaService>,
}
