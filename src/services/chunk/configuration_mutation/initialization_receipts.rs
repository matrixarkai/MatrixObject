// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
/// Trims a shared receipt map, taking the owned map only when a trim is due.
///
/// `Arc::make_mut` copies on ACCESS rather than on modification, and this runs
/// after every receipt insert across all seven maps. Asking for the owned map
/// first would copy all seven the moment a batch wrote one receipt, which is
/// the cost sharing them exists to avoid. The length is readable through the
/// handle, so the common case -- nothing to trim -- touches nothing.
fn trim_shared_map_to_retention<V: Clone>(
    map: &mut std::sync::Arc<BTreeMap<String, V>>,
    retention: usize,
) -> u64 {
    if map.len() <= retention {
        return 0;
    }
    trim_btree_map_to_retention(std::sync::Arc::make_mut(map), retention)
}

impl MatrixReplicaService {
    pub fn new() -> Self {
        Self {
            pages: BTreeMap::new(),
            // Alone until told otherwise: for a single replica the sidecar is the
            // only recovery there is.
            keep_ec_sidecar: true,
            receipt_retention: DEFAULT_STORAGE_RECEIPT_RETENTION,
            write_request_receipts: std::sync::Arc::new(BTreeMap::new()),
            discard_request_receipts: std::sync::Arc::new(BTreeMap::new()),
            batch_write_request_receipts: std::sync::Arc::new(BTreeMap::new()),
            batch_discard_request_receipts: std::sync::Arc::new(BTreeMap::new()),
            state_install_request_receipts: std::sync::Arc::new(BTreeMap::new()),
            reclaim_request_receipts: std::sync::Arc::new(BTreeMap::new()),
            repair_request_receipts: std::sync::Arc::new(BTreeMap::new()),
            accepting_writes: true,
            draining: false,
            frozen: false,
            capacity_bytes: 0,
            capacity_rejected_writes: 0,
            service_mode_rejected_mutations: 0,
            metadata_dirty: false,
            metadata_dirty_mutations: 0,
            metadata_checkpoint_interval: 1,
            defer_metadata_checkpoints: false,
            deferred_metadata_checkpoint_pending: false,
            deferred_metadata_checkpoint_requests: 0,
            deferred_metadata_checkpoint_runs: 0,
            trim_flow_rate_bytes_per_sec: 0,
            trim_flow_burst_bytes: 0,
            trim_flow_available_bytes: 0,
            trim_flow_rate_ops_per_sec: 0,
            trim_flow_burst_ops: 0,
            trim_flow_available_ops: 0,
            trim_flow_last_refill_ms: 0,
            trim_flow_throttled_reclaims: 0,
            trim_flow_throttled_ops: 0,
            trim_flow_throttled_bytes: 0,
            partial_reclaim_failures: 0,
            last_partial_reclaim_failure_status: "ok".to_string(),
            physical_write_count: 0,
            physical_write_bytes: 0,
            physical_discard_ranges: 0,
            physical_discard_bytes: 0,
            ec_reconstructed_reads: Cell::new(0),
            ec_reconstruction_failures: Cell::new(0),
            chunk_mutation_control_commit_index: 0,
            chunk_mutation_control_next_log_index: 1,
            mutation_qos_config: ReplicaMutationQosConfig::default(),
            mutation_qos_state: ReplicaMutationQosState::default(),
            read_cache: RefCell::new(HashMap::new()),
            page_validation: RefCell::new(HashMap::new()),
            read_cache_by_chunk: RefCell::new(HashMap::new()),
            read_cache_lru: RefCell::new(VecDeque::new()),
            read_cache_bytes: Cell::new(0),
            max_read_cache_bytes: Cell::new(DEFAULT_CHUNK_READ_CACHE_MAX_BYTES),
            max_cacheable_read_bytes: Cell::new(DEFAULT_CHUNK_READ_CACHE_MAX_CACHEABLE_READ_BYTES),
            read_cache_hits: Cell::new(0),
            read_cache_misses: Cell::new(0),
            read_cache_evictions: Cell::new(0),
            read_cache_corrupt_pages: Cell::new(0),
            read_cache_corrupt_bytes: Cell::new(0),
            read_cache_invalidations: Cell::new(0),
            read_cache_invalidated_entries: Cell::new(0),
            read_cache_invalidated_bytes: Cell::new(0),
            read_cache_clears: Cell::new(0),
            read_cache_cleared_entries: Cell::new(0),
            read_cache_cleared_bytes: Cell::new(0),
            read_cache_direct_fill_pages: Cell::new(0),
            read_cache_direct_fill_bytes: Cell::new(0),
            read_cache_compressed_fill_pages: Cell::new(0),
            read_cache_compressed_fill_bytes: Cell::new(0),
            read_cache_ec_fill_pages: Cell::new(0),
            read_cache_ec_fill_bytes: Cell::new(0),
            read_cache_uncacheable_read_pages: Cell::new(0),
            read_cache_uncacheable_read_bytes: Cell::new(0),
            read_cache_access_sequence: Cell::new(0),
            snapshot_export_cache: RefCell::new(None),
            snapshot_assembler: ReplicaServiceSnapshotAssembler::default(),
            active_mutation_batch: None,
            physical_bytes: 0,
            discarded_bytes: 0,
        }
    }

    pub fn set_frozen(&mut self, frozen: bool) {
        self.frozen = frozen;
    }

    pub fn receipt_retention(&self) -> usize {
        self.receipt_retention
    }

    pub fn configure_receipt_retention(&mut self, retention: usize) -> Result<u64, ObjectError> {
        validate_storage_receipt_retention(retention)?;
        self.receipt_retention = retention;
        Ok(self.trim_receipts_to_retention())
    }

    pub(crate) fn trim_receipts_to_retention(&mut self) -> u64 {
        trim_shared_map_to_retention(&mut self.write_request_receipts, self.receipt_retention)
            .saturating_add(trim_shared_map_to_retention(
                &mut self.discard_request_receipts,
                self.receipt_retention,
            ))
            .saturating_add(trim_shared_map_to_retention(
                &mut self.batch_write_request_receipts,
                self.receipt_retention,
            ))
            .saturating_add(trim_shared_map_to_retention(
                &mut self.batch_discard_request_receipts,
                self.receipt_retention,
            ))
            .saturating_add(trim_shared_map_to_retention(
                &mut self.state_install_request_receipts,
                self.receipt_retention,
            ))
            .saturating_add(trim_shared_map_to_retention(
                &mut self.reclaim_request_receipts,
                self.receipt_retention,
            ))
            .saturating_add(trim_shared_map_to_retention(
                &mut self.repair_request_receipts,
                self.receipt_retention,
            ))
    }

}
