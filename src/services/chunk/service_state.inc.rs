// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ReplicaSnapshotExportCache {
    stamp: String,
    bytes: std::sync::Arc<[u8]>,
    checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MatrixReplicaService {
    /// Each PAGE's state behind a shared handle, so a snapshot of this service
    /// clones handles rather than states. A write takes `Arc::make_mut`, which
    /// copies the one page it touches if a snapshot still names it -- so the
    /// copy is per page WRITTEN rather than per page held.
    ///
    /// Keyed by `PageId` and holding `PageState`: a replica is made of pages,
    /// and the chunk is the container above the replica, which
    /// `MatrixChunkService` owns. This field was called `chunks` while holding
    /// neither.
    pages: BTreeMap<PageId, std::sync::Arc<PageState>>,
    /// Whether this replica keeps the XOR sidecar for its pages.
    ///
    /// True only when this replica is the only copy. Above one replica a damaged
    /// page is recovered from its peers: reads vote across replicas and the repair
    /// path writes the winning state back. Must be uniform across the replica set --
    /// `repair_chunk_from_quorum` compares whole page states to find agreement, so
    /// replicas disagreeing about whether a sidecar exists would never agree.
    keep_ec_sidecar: bool,
    receipt_retention: usize,
    /// The seven receipt maps, each behind a shared handle so that a snapshot
    /// of this service clones handles rather than maps. A write takes
    /// `Arc::make_mut`, which copies the one map it is about to change -- so an
    /// open batch copies the maps it WRITES rather than all seven.
    ///
    /// Measured: an ordinary put writes none of them.
    write_request_receipts: std::sync::Arc<BTreeMap<String, ReplicaWriteReceipt>>,
    discard_request_receipts: std::sync::Arc<BTreeMap<String, ReplicaDiscardReceipt>>,
    batch_write_request_receipts: std::sync::Arc<BTreeMap<String, ReplicaBatchWriteReceipt>>,
    batch_discard_request_receipts: std::sync::Arc<BTreeMap<String, ReplicaBatchDiscardReceipt>>,
    state_install_request_receipts: std::sync::Arc<BTreeMap<String, ReplicaStateInstallReceipt>>,
    reclaim_request_receipts: std::sync::Arc<BTreeMap<String, ReplicaReclaimReceipt>>,
    repair_request_receipts: std::sync::Arc<BTreeMap<String, ReplicaRepairReceipt>>,
    accepting_writes: bool,
    draining: bool,
    frozen: bool,
    capacity_bytes: u64,
    capacity_rejected_writes: u64,
    service_mode_rejected_mutations: u64,
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
    physical_write_count: u64,
    physical_write_bytes: u64,
    physical_discard_ranges: u64,
    physical_discard_bytes: u64,
    ec_reconstructed_reads: Cell<u64>,
    ec_reconstruction_failures: Cell<u64>,
    chunk_mutation_control_commit_index: u64,
    chunk_mutation_control_next_log_index: u64,
    mutation_qos_config: ReplicaMutationQosConfig,
    mutation_qos_state: ReplicaMutationQosState,
    read_cache: RefCell<HashMap<ReplicaReadCacheKey, ReplicaReadCacheEntry>>,
    /// Runtime-only: (write_sequence, checksum, per-page fnv1a64) for pages whose
    /// payload has been fully validated once, so later reads verify only their own
    /// pages instead of re-hashing the whole chunk. Never persisted.
    page_validation: RefCell<HashMap<PageId, (u64, String, Vec<u64>)>>,
    read_cache_by_chunk: RefCell<HashMap<PageId, HashSet<ReplicaReadCacheRangeKey>>>,
    read_cache_lru: RefCell<VecDeque<(u64, ReplicaReadCacheKey)>>,
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
    snapshot_export_cache: RefCell<Option<ReplicaSnapshotExportCache>>,
    snapshot_assembler: ReplicaServiceSnapshotAssembler,
    active_mutation_batch: Option<ReplicaMutationBatch>,
}

#[cfg(feature = "chunk-consensus")]
impl MatrixReplicaService {
    /// Candidate state for one Raft mutation. Runtime read caches are neither
    /// application state nor inputs to the durable outcome. Do not copy their
    /// payloads or validation indexes into a short-lived candidate.
    /// The live service and the public Clone implementation remain unchanged.
    pub(crate) fn clone_for_raft_preparation(&self) -> Self {
        Self {
            pages: self.pages.clone(),
            keep_ec_sidecar: self.keep_ec_sidecar,
            receipt_retention: self.receipt_retention,
            write_request_receipts: self.write_request_receipts.clone(),
            discard_request_receipts: self.discard_request_receipts.clone(),
            batch_write_request_receipts: self.batch_write_request_receipts.clone(),
            batch_discard_request_receipts: self.batch_discard_request_receipts.clone(),
            state_install_request_receipts: self.state_install_request_receipts.clone(),
            reclaim_request_receipts: self.reclaim_request_receipts.clone(),
            repair_request_receipts: self.repair_request_receipts.clone(),
            accepting_writes: self.accepting_writes,
            draining: self.draining,
            frozen: self.frozen,
            capacity_bytes: self.capacity_bytes,
            capacity_rejected_writes: self.capacity_rejected_writes,
            service_mode_rejected_mutations: self.service_mode_rejected_mutations,
            metadata_dirty: self.metadata_dirty,
            metadata_dirty_mutations: self.metadata_dirty_mutations,
            metadata_checkpoint_interval: self.metadata_checkpoint_interval,
            defer_metadata_checkpoints: self.defer_metadata_checkpoints,
            deferred_metadata_checkpoint_pending: self.deferred_metadata_checkpoint_pending,
            deferred_metadata_checkpoint_requests: self.deferred_metadata_checkpoint_requests,
            deferred_metadata_checkpoint_runs: self.deferred_metadata_checkpoint_runs,
            trim_flow_rate_bytes_per_sec: self.trim_flow_rate_bytes_per_sec,
            trim_flow_burst_bytes: self.trim_flow_burst_bytes,
            trim_flow_available_bytes: self.trim_flow_available_bytes,
            trim_flow_rate_ops_per_sec: self.trim_flow_rate_ops_per_sec,
            trim_flow_burst_ops: self.trim_flow_burst_ops,
            trim_flow_available_ops: self.trim_flow_available_ops,
            trim_flow_last_refill_ms: self.trim_flow_last_refill_ms,
            trim_flow_throttled_reclaims: self.trim_flow_throttled_reclaims,
            trim_flow_throttled_ops: self.trim_flow_throttled_ops,
            trim_flow_throttled_bytes: self.trim_flow_throttled_bytes,
            partial_reclaim_failures: self.partial_reclaim_failures,
            last_partial_reclaim_failure_status: self.last_partial_reclaim_failure_status.clone(),
            physical_write_count: self.physical_write_count,
            physical_write_bytes: self.physical_write_bytes,
            physical_discard_ranges: self.physical_discard_ranges,
            physical_discard_bytes: self.physical_discard_bytes,
            ec_reconstructed_reads: self.ec_reconstructed_reads.clone(),
            ec_reconstruction_failures: self.ec_reconstruction_failures.clone(),
            chunk_mutation_control_commit_index: self.chunk_mutation_control_commit_index,
            chunk_mutation_control_next_log_index: self.chunk_mutation_control_next_log_index,
            mutation_qos_config: self.mutation_qos_config.clone(),
            mutation_qos_state: self.mutation_qos_state.clone(),
            read_cache: Default::default(),
            page_validation: Default::default(),
            read_cache_by_chunk: Default::default(),
            read_cache_lru: Default::default(),
            read_cache_bytes: Default::default(),
            max_read_cache_bytes: self.max_read_cache_bytes.clone(),
            max_cacheable_read_bytes: self.max_cacheable_read_bytes.clone(),
            read_cache_hits: self.read_cache_hits.clone(),
            read_cache_misses: self.read_cache_misses.clone(),
            read_cache_evictions: self.read_cache_evictions.clone(),
            read_cache_corrupt_pages: self.read_cache_corrupt_pages.clone(),
            read_cache_corrupt_bytes: self.read_cache_corrupt_bytes.clone(),
            read_cache_invalidations: self.read_cache_invalidations.clone(),
            read_cache_invalidated_entries: self.read_cache_invalidated_entries.clone(),
            read_cache_invalidated_bytes: self.read_cache_invalidated_bytes.clone(),
            read_cache_clears: self.read_cache_clears.clone(),
            read_cache_cleared_entries: self.read_cache_cleared_entries.clone(),
            read_cache_cleared_bytes: self.read_cache_cleared_bytes.clone(),
            read_cache_direct_fill_pages: self.read_cache_direct_fill_pages.clone(),
            read_cache_direct_fill_bytes: self.read_cache_direct_fill_bytes.clone(),
            read_cache_compressed_fill_pages: self.read_cache_compressed_fill_pages.clone(),
            read_cache_compressed_fill_bytes: self.read_cache_compressed_fill_bytes.clone(),
            read_cache_ec_fill_pages: self.read_cache_ec_fill_pages.clone(),
            read_cache_ec_fill_bytes: self.read_cache_ec_fill_bytes.clone(),
            read_cache_uncacheable_read_pages: self.read_cache_uncacheable_read_pages.clone(),
            read_cache_uncacheable_read_bytes: self.read_cache_uncacheable_read_bytes.clone(),
            read_cache_access_sequence: self.read_cache_access_sequence.clone(),
            snapshot_export_cache: self.snapshot_export_cache.clone(),
            snapshot_assembler: self.snapshot_assembler.clone(),
            active_mutation_batch: self.active_mutation_batch.clone(),
        }
    }
}

#[cfg(all(test, feature = "chunk-consensus"))]
mod preparation_clone_tests {
    use super::*;
    #[test]
    fn preparation_clone_preserves_state_without_copying_read_caches() {
        let mut service = MatrixReplicaService::new();
        service.set_capacity_bytes(1024 * 1024).unwrap();
        service.set_read_cache_max_bytes(256 * 1024);
        service.set_read_cache_max_cacheable_read_bytes(1024);
        for i in 0..64 {
            let page = PageId {
                chunk_id: ChunkId::new(1, 1).unwrap(),
                page_index: i,
            };
            service
                .write_chunk(page.clone(), 0, &vec![i as u8; 1024])
                .unwrap();
            service.read_chunk(&page, 0, 1024).unwrap();
        }
        assert_eq!(service.read_cache_stats().entries, 64);
        let saved = service.clone();
        let candidate = service.clone_for_raft_preparation();
        assert_eq!(
            service, saved,
            "preparation clone cannot modify the live caches"
        );
        assert!(candidate.read_cache.borrow().is_empty());
        assert!(candidate.page_validation.borrow().is_empty());
        assert!(candidate.read_cache_by_chunk.borrow().is_empty());
        assert!(candidate.read_cache_lru.borrow().is_empty());
        assert_eq!(candidate.read_cache_bytes.get(), 0);
        for (page, state) in &service.pages {
            assert!(std::sync::Arc::ptr_eq(state, &candidate.pages[page]));
        }
        let mut restored = candidate.clone();
        restored.read_cache = service.read_cache.clone();
        restored.page_validation = service.page_validation.clone();
        restored.read_cache_by_chunk = service.read_cache_by_chunk.clone();
        restored.read_cache_lru = service.read_cache_lru.clone();
        restored.read_cache_bytes = service.read_cache_bytes.clone();
        assert_eq!(restored, service, "only runtime cache contents may differ");
        assert_eq!(candidate.export_snapshot(), service.export_snapshot());
    }
}
