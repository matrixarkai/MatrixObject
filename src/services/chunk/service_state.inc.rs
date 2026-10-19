#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ChunkSnapshotExportCache {
    stamp: String,
    bytes: std::sync::Arc<[u8]>,
    checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MatrixReplicaService {
    /// Each chunk's state behind a shared handle, so a snapshot of this
    /// service clones handles rather than states. A write takes
    /// `Arc::make_mut`, which copies the one chunk it touches if a snapshot
    /// still names it -- so the copy is per chunk WRITTEN rather than per chunk
    /// held.
    pub(crate) chunks: BTreeMap<PageId, std::sync::Arc<PageState>>,
    /// Whether this replica keeps the XOR sidecar for its chunks.
    ///
    /// True only when this replica is the only copy. Above one replica a damaged
    /// chunk is recovered from its peers: reads vote across replicas and the repair
    /// path writes the winning state back. Must be uniform across the replica set --
    /// `repair_chunk_from_quorum` compares whole chunk states to find agreement, so
    /// replicas disagreeing about whether a sidecar exists would never agree.
    pub(crate) keep_ec_sidecar: bool,
    pub(crate) receipt_retention: usize,
    /// The seven receipt maps, each behind a shared handle so that a snapshot
    /// of this service clones handles rather than maps. A write takes
    /// `Arc::make_mut`, which copies the one map it is about to change -- so an
    /// open batch copies the maps it WRITES rather than all seven.
    ///
    /// Measured: an ordinary put writes none of them.
    pub(crate) write_request_receipts: std::sync::Arc<BTreeMap<String, ChunkWriteReceipt>>,
    pub(crate) discard_request_receipts: std::sync::Arc<BTreeMap<String, ChunkDiscardReceipt>>,
    pub(crate) batch_write_request_receipts: std::sync::Arc<BTreeMap<String, ChunkBatchWriteReceipt>>,
    pub(crate) batch_discard_request_receipts: std::sync::Arc<BTreeMap<String, ChunkBatchDiscardReceipt>>,
    state_install_request_receipts: std::sync::Arc<BTreeMap<String, ChunkStateInstallReceipt>>,
    reclaim_request_receipts: std::sync::Arc<BTreeMap<String, ChunkReclaimReceipt>>,
    repair_request_receipts: std::sync::Arc<BTreeMap<String, ChunkRepairReceipt>>,
    accepting_writes: bool,
    draining: bool,
    frozen: bool,
    pub(crate) capacity_bytes: u64,
    pub(crate) capacity_rejected_writes: u64,
    pub(crate) service_mode_rejected_mutations: u64,
    metadata_dirty: bool,
    metadata_dirty_mutations: u64,
    metadata_checkpoint_interval: u64,
    defer_metadata_checkpoints: bool,
    deferred_metadata_checkpoint_pending: bool,
    deferred_metadata_checkpoint_requests: u64,
    deferred_metadata_checkpoint_runs: u64,
    trim_flow_rate_bytes_per_sec: u64,
    trim_flow_burst_bytes: u64,
    trim_flow_available_bytes: u64,
    trim_flow_rate_ops_per_sec: u64,
    trim_flow_burst_ops: u64,
    trim_flow_available_ops: u64,
    trim_flow_last_refill_ms: u64,
    trim_flow_throttled_reclaims: u64,
    trim_flow_throttled_ops: u64,
    trim_flow_throttled_bytes: u64,
    partial_reclaim_failures: u64,
    last_partial_reclaim_failure_status: String,
    pub(crate) physical_write_count: u64,
    pub(crate) physical_write_bytes: u64,
    pub(crate) physical_discard_ranges: u64,
    pub(crate) physical_discard_bytes: u64,
    ec_reconstructed_reads: Cell<u64>,
    ec_reconstruction_failures: Cell<u64>,
    chunk_mutation_control_commit_index: u64,
    pub(crate) chunk_mutation_control_next_log_index: u64,
    mutation_qos_config: ChunkMutationQosConfig,
    mutation_qos_state: ChunkMutationQosState,
    pub(crate) read_cache: RefCell<HashMap<ChunkReadCacheKey, ChunkReadCacheEntry>>,
    /// Runtime-only: (write_sequence, checksum, per-page fnv1a64) for chunks whose
    /// payload has been fully validated once, so later reads verify only their own
    /// pages instead of re-hashing the whole chunk. Never persisted.
    page_validation: RefCell<HashMap<PageId, (u64, String, Vec<u64>)>>,
    read_cache_by_chunk: RefCell<HashMap<PageId, HashSet<ChunkReadCacheRangeKey>>>,
    read_cache_lru: RefCell<VecDeque<(u64, ChunkReadCacheKey)>>,
    read_cache_bytes: Cell<u64>,
    max_read_cache_bytes: Cell<u64>,
    max_cacheable_read_bytes: Cell<u64>,
    read_cache_hits: Cell<u64>,
    read_cache_misses: Cell<u64>,
    read_cache_evictions: Cell<u64>,
    read_cache_corrupt_pages: Cell<u64>,
    read_cache_corrupt_bytes: Cell<u64>,
    read_cache_invalidations: Cell<u64>,
    read_cache_invalidated_entries: Cell<u64>,
    read_cache_invalidated_bytes: Cell<u64>,
    read_cache_clears: Cell<u64>,
    read_cache_cleared_entries: Cell<u64>,
    read_cache_cleared_bytes: Cell<u64>,
    read_cache_direct_fill_pages: Cell<u64>,
    read_cache_direct_fill_bytes: Cell<u64>,
    read_cache_compressed_fill_pages: Cell<u64>,
    read_cache_compressed_fill_bytes: Cell<u64>,
    read_cache_ec_fill_pages: Cell<u64>,
    read_cache_ec_fill_bytes: Cell<u64>,
    read_cache_uncacheable_read_pages: Cell<u64>,
    read_cache_uncacheable_read_bytes: Cell<u64>,
    read_cache_access_sequence: Cell<u64>,
    snapshot_export_cache: RefCell<Option<ChunkSnapshotExportCache>>,
    snapshot_assembler: ReplicaServiceSnapshotAssembler,
    active_mutation_batch: Option<ChunkMutationBatch>,
}
