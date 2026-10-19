#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ChunkRouteQosState {
    pub window_start_ms: u64,
    pub read_ops_in_window: u64,
    pub read_bytes_in_window: u64,
    pub write_ops_in_window: u64,
    pub write_bytes_in_window: u64,
    pub discard_ops_in_window: u64,
    pub discard_bytes_in_window: u64,
    pub throttled_ops: u64,
    pub throttled_bytes: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ChunkStorageReadiness {
    pub readable: bool,
    pub writable: bool,
    pub ready: bool,
    pub reason: String,
    pub replica_count: u64,
    pub write_quorum: u64,
    pub read_quorum: u64,
    pub active_replicas: u64,
    pub readable_replicas: u64,
    pub writable_replicas: u64,
    pub reclaimable_replicas: u64,
    pub quarantined_replicas: u64,
    pub frozen_replicas: u64,
    pub draining_replicas: u64,
    pub capacity_pressure_replicas: u64,
    pub trim_flow_limited_replicas: u64,
    pub total_trim_flow_available_ops: u64,
    pub total_trim_flow_throttled_ops: u64,
    pub total_physical_bytes: u64,
    pub total_capacity_bytes: u64,
    pub total_capacity_free_bytes: u64,
    pub metadata_dirty_replicas: u64,
    pub metadata_dirty_mutations: u64,
    pub max_metadata_checkpoint_interval: u64,
    pub deferred_metadata_checkpoint_replicas: u64,
    pub deferred_metadata_checkpoint_requests: u64,
    pub deferred_metadata_checkpoint_runs: u64,
    pub capacity_rejected_writes: u64,
    pub service_mode_rejected_mutations: u64,
    pub physical_write_count: u64,
    pub physical_write_bytes: u64,
    pub physical_discard_ranges: u64,
    pub physical_discard_bytes: u64,
    pub total_reclaimable_bytes: u64,
    pub partial_reclaim_failures: u64,
    pub last_partial_reclaim_failure_status: String,
    pub total_fragmented_chunk_count: u64,
    pub total_fragmented_discarded_bytes: u64,
    pub total_trailing_trim_bytes: u64,
    pub total_read_cache_bytes: u64,
    pub total_read_cache_entries: u64,
    pub total_read_cache_max_bytes: u64,
    pub read_cache_pressure_replicas: u64,
    pub read_cache_pressure: bool,
    pub total_read_cache_hits: u64,
    pub total_read_cache_misses: u64,
    pub total_read_cache_evictions: u64,
    pub total_read_cache_corrupt_pages: u64,
    pub total_read_cache_corrupt_bytes: u64,
    pub total_read_cache_invalidations: u64,
    pub total_read_cache_invalidated_entries: u64,
    pub total_read_cache_invalidated_bytes: u64,
    pub total_read_cache_clears: u64,
    pub total_read_cache_cleared_entries: u64,
    pub total_read_cache_cleared_bytes: u64,
    pub total_read_cache_direct_fill_pages: u64,
    pub total_read_cache_direct_fill_bytes: u64,
    pub total_read_cache_compressed_fill_pages: u64,
    pub total_read_cache_compressed_fill_bytes: u64,
    pub total_read_cache_ec_fill_pages: u64,
    pub total_read_cache_ec_fill_bytes: u64,
    pub total_read_cache_uncacheable_read_pages: u64,
    pub total_read_cache_uncacheable_read_bytes: u64,
    pub cumulative_cache_prewarm_applied_ranges: u64,
    pub cumulative_cache_prewarm_failed_ranges: u64,
    pub cumulative_cache_invalidate_applied_ranges: u64,
    pub cumulative_cache_invalidate_failed_ranges: u64,
    pub min_capacity_free_bytes: u64,
    pub min_reclaimable_bytes: u64,
    pub page_read_parallelism: u64,
    pub page_write_parallelism: u64,
    pub page_batch_read_parallelism: u64,
    pub page_batch_write_parallelism: u64,
    pub page_batch_discard_parallelism: u64,
    pub page_repair_parallelism: u64,
    pub replica_fanout_parallelism: u64,
    pub primary_batch_read_repair_probe_limit: u64,
    pub effective_chunk_fanout_parallelism: u64,
    pub local_replica_store_parallelism: u64,
    pub effective_local_replica_store_parallelism: u64,
    pub chunk_endpoint_count: u64,
    pub disabled_chunk_endpoint_count: u64,
    pub effective_writable_chunk_endpoints: u64,
    pub cumulative_quorum_write_degraded_chunks: u64,
    pub cumulative_quorum_read_degraded_chunks: u64,
    pub cumulative_quorum_discard_degraded_chunks: u64,
    pub cumulative_replica_fallback_reads: u64,
    pub cumulative_physical_chunk_read_count: u64,
    pub cumulative_coalesced_chunk_read_count: u64,
    pub cumulative_repair_checked_chunks: u64,
    pub cumulative_repair_degraded_chunks: u64,
    pub cumulative_repaired_replicas: u64,
    pub cumulative_failed_repairs: u64,
    pub cumulative_replica_read_repairs: u64,
    pub cumulative_replica_read_repair_failures: u64,
    pub receipt_retention: u64,
    pub allocation_receipt_count: u64,
    pub batch_allocation_receipt_count: u64,
    pub release_allocation_receipt_count: u64,
    pub write_receipt_count: u64,
    pub batch_write_receipt_count: u64,
    pub discard_receipt_count: u64,
    pub batch_discard_receipt_count: u64,
    pub route_qos_window_ms: u64,
    pub route_qos_read_ops_in_window: u64,
    pub route_qos_read_bytes_in_window: u64,
    pub route_qos_write_ops_in_window: u64,
    pub route_qos_write_bytes_in_window: u64,
    pub route_qos_discard_ops_in_window: u64,
    pub route_qos_discard_bytes_in_window: u64,
    pub route_qos_throttled_ops: u64,
    pub route_qos_throttled_bytes: u64,
    pub route_qos_pressure: bool,
    pub page_state_fingerprint: String,
    pub page_state_fingerprint_format: String,
    pub page_state_fingerprint_bytes: u64,
    pub chunk_route_control_proof_format: String,
    pub chunk_route_control_proof_checksum: String,
    pub chunk_route_control_proof_bytes: u64,
    pub sealed_slab_count: u64,
    pub allocator_active_slab_sealed: bool,
    pub route_control_proof: ChunkRouteControlProof,
    pub replica_readiness: Vec<ReplicaStorageReadiness>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StorageMaintenanceAction {
    pub action: String,
    pub reason: String,
    pub priority: u64,
    pub replica_index: Option<usize>,
    pub chunk_id: Option<PageId>,
    pub estimated_bytes: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StorageMaintenancePlan {
    pub readiness: ChunkStorageReadiness,
    pub reclaim: ChunkReplicaReclaimPlan,
    pub slab_discards: Vec<DiscardPlan>,
    pub actions: Vec<StorageMaintenanceAction>,
    pub plan_checksum: String,
    pub reclaim_continuation_safe: bool,
    pub reclaim_next_marker_advanced: bool,
    pub capacity_pressure: bool,
    pub reclaim_pressure: bool,
    pub trim_flow_pressure: bool,
    pub quorum_pressure: bool,
    pub max_actions: usize,
    pub truncated: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StorageMaintenanceRunReport {
    pub plan: StorageMaintenancePlan,
    pub repair: ChunkReplicaAuditRepairReport,
    pub reclaim: ReplicaPartialReclaimReport,
    pub slab_discard: SlabDiscardRunReport,
    pub post_readiness: ChunkStorageReadiness,
    pub route_control_proof: ChunkRouteControlProof,
    pub expected_route_control_checksum: String,
    pub route_control_fenced: bool,
    pub route_control_fence_matched: bool,
    pub reclaim_continuation_safe: bool,
    pub reclaim_next_marker_advanced: bool,
    pub reclaim_continuation_blocked: bool,
    pub executed_actions: u64,
    pub skipped_actions: u64,
    pub reclaimed: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SealedSlabPage {
    pub slabs: Vec<SlabId>,
    pub sealed_slab_count: u64,
    pub limit: u64,
    pub truncated: bool,
    pub next_marker: Option<SlabId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkServiceSnapshot {
    pub page_size: u64,
    pub write_quorum: usize,
    pub read_quorum: usize,
    pub allocator_slab_size: u64,
    pub allocator_next_slab_id: u64,
    pub allocator_next_offset: u64,
    pub receipt_retention: usize,
    pub pending_allocations: BTreeMap<String, ChunkAllocation>,
    pub allocation_request_receipts: BTreeMap<String, ChunkAllocation>,
    pub batch_allocation_request_receipts: BTreeMap<String, Vec<ChunkAllocation>>,
    pub release_allocation_request_receipts: BTreeMap<String, ChunkAllocation>,
    pub write_request_receipts: BTreeMap<String, ChunkWriteReceipt>,
    pub batch_write_request_receipts: BTreeMap<String, ChunkBatchWriteReceipt>,
    pub discard_request_receipts: BTreeMap<String, ChunkDiscardReceipt>,
    pub batch_discard_request_receipts: BTreeMap<String, ChunkBatchDiscardReceipt>,
    pub repair_request_receipts: BTreeMap<String, ReplicaRepairReceipt>,
    pub range_repair_request_receipts: BTreeMap<String, ChunkRangeRepairReceipt>,
    pub audit_repair_request_receipts: BTreeMap<String, ChunkAuditRepairReceipt>,
    pub released_allocation_count: u64,
    pub quarantined_replicas: BTreeSet<usize>,
    pub chunk_endpoints: Vec<String>,
    pub disabled_chunk_endpoints: BTreeSet<usize>,
    pub chunk_route_control_commit_index: u64,
    pub chunk_route_control_next_log_index: u64,
    pub route_qos_config: ChunkRouteQosConfig,
    pub route_qos_state: ChunkRouteQosState,
    pub sealed_slabs: BTreeSet<SlabId>,
    pub cumulative_quorum_write_degraded_chunks: u64,
    pub cumulative_quorum_read_degraded_chunks: u64,
    pub cumulative_quorum_discard_degraded_chunks: u64,
    pub cumulative_replica_fallback_reads: u64,
    pub cumulative_physical_chunk_read_count: u64,
    pub cumulative_coalesced_chunk_read_count: u64,
    pub cumulative_repair_checked_chunks: u64,
    pub cumulative_repair_degraded_chunks: u64,
    pub cumulative_repaired_replicas: u64,
    pub cumulative_failed_repairs: u64,
    pub cumulative_replica_read_repairs: u64,
    pub cumulative_replica_read_repair_failures: u64,
    pub replicas: Vec<ReplicaServiceSnapshot>,
    pub checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkServiceSnapshotChunk {
    pub offset: u64,
    pub total_bytes: u64,
    pub chunk_checksum: String,
    pub full_checksum: String,
    pub eof: bool,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ChunkServiceSnapshotAssembler {
    expected_total_bytes: Option<u64>,
    expected_full_checksum: Option<String>,
    buffer: Vec<u8>,
}

impl ChunkServiceSnapshotAssembler {
    /// What a partly received snapshot holds. Its fields are private to this
    /// module, which is why the accounting lives here.
    pub(crate) fn footprint_bytes(&self) -> u64 {
        (self.buffer.capacity() as u64).saturating_add(
            self.expected_full_checksum
                .as_ref()
                .map(|checksum| checksum.capacity() as u64)
                .unwrap_or(0),
        )
    }
}

pub(crate) struct ChunkServiceSnapshotAssembledImage {
    pub bytes: Vec<u8>,
    pub full_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ChunkMutationBatch {
    batch_id: String,
    started_route_commit_index: u64,
    replica_indices: Vec<usize>,
    snapshot: Box<ChunkServiceSnapshot>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ChunkMutationBatchStatus {
    pub active: bool,
    pub batch_id: String,
    pub started_route_commit_index: u64,
    pub current_route_commit_index: u64,
    pub active_replica_count: u64,
    pub opened_replica_batch_count: u64,
    pub chunk_batch_active_replica_count: u64,
    pub slab_allocation_receipt_count: u64,
    pub batch_allocation_receipt_count: u64,
    pub release_allocation_receipt_count: u64,
    pub write_receipt_count: u64,
    pub batch_write_receipt_count: u64,
    pub discard_receipt_count: u64,
    pub batch_discard_receipt_count: u64,
    pub committed: bool,
    pub aborted: bool,
}

fn replica_mutation_batch_id(page_batch_id: &str, replica_index: usize) -> String {
    let prefix = if page_batch_id.is_empty() {
        "block-batch".to_string()
    } else if page_batch_id.len() > 220 {
        etag(page_batch_id.as_bytes())
    } else {
        page_batch_id.to_string()
    };
    format!("{prefix}-chunk-{replica_index}")
}

impl ChunkServiceSnapshotChunk {
    pub fn install_progress(&self) -> SnapshotInstallProgress {
        snapshot_install_progress(
            true,
            !self.eof,
            self.offset.saturating_add(self.payload.len() as u64),
            self.total_bytes,
            self.full_checksum.clone(),
            0,
            0,
        )
    }
}
