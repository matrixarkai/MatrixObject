#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ChunkStorageReadiness {
    pub readable: bool,
    pub writable: bool,
    pub reclaimable: bool,
    pub ready: bool,
    pub reason: String,
    pub chunk_state_fingerprint: String,
    pub accepting_writes: bool,
    pub draining: bool,
    pub frozen: bool,
    pub chunk_count: u64,
    pub physical_bytes: u64,
    pub capacity_bytes: u64,
    pub capacity_free_bytes: u64,
    pub min_capacity_free_bytes: u64,
    pub capacity_pressure: bool,
    pub metadata_dirty: bool,
    pub metadata_dirty_mutations: u64,
    pub metadata_checkpoint_interval: u64,
    pub defer_metadata_checkpoints: bool,
    pub deferred_metadata_checkpoint_pending: bool,
    pub deferred_metadata_checkpoint_requests: u64,
    pub deferred_metadata_checkpoint_runs: u64,
    pub discarded_range_count: u64,
    pub reclaimable_bytes: u64,
    pub trailing_trim_bytes: u64,
    pub fragmented_chunk_count: u64,
    pub fragmented_discarded_bytes: u64,
    pub min_reclaimable_bytes: u64,
    pub trim_flow_enabled: bool,
    pub trim_flow_available_bytes: u64,
    pub trim_flow_available_ops: u64,
    pub trim_flow_throttled_ops: u64,
    pub trim_flow_limited: bool,
    pub partial_reclaim_failures: u64,
    pub last_partial_reclaim_failure_status: String,
    pub mutation_qos_window_ms: u64,
    pub mutation_qos_write_ops_in_window: u64,
    pub mutation_qos_write_bytes_in_window: u64,
    pub mutation_qos_discard_ops_in_window: u64,
    pub mutation_qos_discard_bytes_in_window: u64,
    pub mutation_qos_throttled_ops: u64,
    pub mutation_qos_throttled_bytes: u64,
    pub mutation_qos_pressure: bool,
    pub read_cache_entries: u64,
    pub read_cache_bytes: u64,
    pub read_cache_max_bytes: u64,
    pub read_cache_max_cacheable_read_bytes: u64,
    pub read_cache_hits: u64,
    pub read_cache_misses: u64,
    pub read_cache_evictions: u64,
    pub read_cache_corrupt_pages: u64,
    pub read_cache_corrupt_bytes: u64,
    pub read_cache_invalidations: u64,
    pub read_cache_invalidated_entries: u64,
    pub read_cache_invalidated_bytes: u64,
    pub read_cache_clears: u64,
    pub read_cache_cleared_entries: u64,
    pub read_cache_cleared_bytes: u64,
    pub read_cache_direct_fill_pages: u64,
    pub read_cache_direct_fill_bytes: u64,
    pub read_cache_compressed_fill_pages: u64,
    pub read_cache_compressed_fill_bytes: u64,
    pub read_cache_ec_fill_pages: u64,
    pub read_cache_ec_fill_bytes: u64,
    pub read_cache_uncacheable_read_pages: u64,
    pub read_cache_uncacheable_read_bytes: u64,
    pub capacity_rejected_writes: u64,
    pub service_mode_rejected_mutations: u64,
    pub physical_write_count: u64,
    pub physical_write_bytes: u64,
    pub physical_discard_ranges: u64,
    pub physical_discard_bytes: u64,
    pub compressed_chunk_count: u64,
    pub compression_saved_bytes: u64,
    pub ec_reconstructed_reads: u64,
    pub ec_reconstruction_failures: u64,
    pub receipt_retention: u64,
    pub write_receipt_count: u64,
    pub discard_receipt_count: u64,
    pub batch_write_receipt_count: u64,
    pub batch_discard_receipt_count: u64,
    pub replicaserver_control_proof_format: String,
    pub replicaserver_control_proof_checksum: String,
    pub replicaserver_control_proof_bytes: u64,
    pub control_proof: ReplicaServiceControlProof,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RepairStats {
    pub checked_chunks: u64,
    pub repaired_replicas: u64,
    pub failed_repairs: u64,
    pub degraded_chunks: u64,
}

impl ChunkStorageReadiness {
    pub fn read_cache_lookup_count(&self) -> u64 {
        self.read_cache_hits.saturating_add(self.read_cache_misses)
    }

    pub fn read_cache_hit_ratio_ppm(&self) -> u64 {
        let lookup_count = self.read_cache_lookup_count();
        if lookup_count == 0 {
            return 0;
        }
        const SCALE: u64 = 1_000_000;
        if self.read_cache_hits >= lookup_count {
            return SCALE;
        }
        let quotient = self.read_cache_hits / lookup_count;
        let remainder = self.read_cache_hits % lookup_count;
        quotient * SCALE + (remainder * SCALE) / lookup_count
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("readable", self.readable),
            ("writable", self.writable),
            ("reclaimable", self.reclaimable),
            ("ready", self.ready),
            ("accepting_writes", self.accepting_writes),
            ("draining", self.draining),
            ("frozen", self.frozen),
            ("capacity_pressure", self.capacity_pressure),
            ("metadata_dirty", self.metadata_dirty),
            (
                "defer_metadata_checkpoints",
                self.defer_metadata_checkpoints,
            ),
            (
                "deferred_metadata_checkpoint_pending",
                self.deferred_metadata_checkpoint_pending,
            ),
            ("trim_flow_enabled", self.trim_flow_enabled),
            ("trim_flow_limited", self.trim_flow_limited),
            ("mutation_qos_pressure", self.mutation_qos_pressure),
            ("chunkserver_control_read_ready", self.readable),
            ("chunkserver_control_write_ready", self.writable),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(format!("{field_prefix}reason"), self.reason.clone());
        fields.insert(
            format!("{field_prefix}chunk_state_fingerprint"),
            self.chunk_state_fingerprint.clone(),
        );
        fields.insert(
            format!("{field_prefix}chunkserver_control_proof_format"),
            self.replicaserver_control_proof_format.clone(),
        );
        fields.insert(
            format!("{field_prefix}chunkserver_control_proof_checksum"),
            self.replicaserver_control_proof_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}last_partial_reclaim_failure_status"),
            self.last_partial_reclaim_failure_status.clone(),
        );
        for (name, value) in [
            ("chunk_count", self.chunk_count),
            ("physical_bytes", self.physical_bytes),
            ("capacity_bytes", self.capacity_bytes),
            ("capacity_free_bytes", self.capacity_free_bytes),
            ("min_capacity_free_bytes", self.min_capacity_free_bytes),
            ("metadata_dirty_mutations", self.metadata_dirty_mutations),
            (
                "metadata_checkpoint_interval",
                self.metadata_checkpoint_interval,
            ),
            (
                "deferred_metadata_checkpoint_requests",
                self.deferred_metadata_checkpoint_requests,
            ),
            (
                "deferred_metadata_checkpoint_runs",
                self.deferred_metadata_checkpoint_runs,
            ),
            ("discarded_range_count", self.discarded_range_count),
            ("reclaimable_bytes", self.reclaimable_bytes),
            ("trailing_trim_bytes", self.trailing_trim_bytes),
            ("fragmented_chunk_count", self.fragmented_chunk_count),
            (
                "fragmented_discarded_bytes",
                self.fragmented_discarded_bytes,
            ),
            ("min_reclaimable_bytes", self.min_reclaimable_bytes),
            ("trim_flow_available_bytes", self.trim_flow_available_bytes),
            ("trim_flow_available_ops", self.trim_flow_available_ops),
            ("trim_flow_throttled_ops", self.trim_flow_throttled_ops),
            ("partial_reclaim_failures", self.partial_reclaim_failures),
            ("mutation_qos_window_ms", self.mutation_qos_window_ms),
            (
                "mutation_qos_write_ops_in_window",
                self.mutation_qos_write_ops_in_window,
            ),
            (
                "mutation_qos_write_bytes_in_window",
                self.mutation_qos_write_bytes_in_window,
            ),
            (
                "mutation_qos_discard_ops_in_window",
                self.mutation_qos_discard_ops_in_window,
            ),
            (
                "mutation_qos_discard_bytes_in_window",
                self.mutation_qos_discard_bytes_in_window,
            ),
            (
                "mutation_qos_throttled_ops",
                self.mutation_qos_throttled_ops,
            ),
            (
                "mutation_qos_throttled_bytes",
                self.mutation_qos_throttled_bytes,
            ),
            ("read_cache_entries", self.read_cache_entries),
            ("read_cache_bytes", self.read_cache_bytes),
            ("read_cache_max_bytes", self.read_cache_max_bytes),
            (
                "read_cache_max_cacheable_read_bytes",
                self.read_cache_max_cacheable_read_bytes,
            ),
            ("read_cache_hits", self.read_cache_hits),
            ("read_cache_misses", self.read_cache_misses),
            ("read_cache_lookup_count", self.read_cache_lookup_count()),
            ("read_cache_hit_ratio_ppm", self.read_cache_hit_ratio_ppm()),
            ("read_cache_evictions", self.read_cache_evictions),
            ("read_cache_corrupt_pages", self.read_cache_corrupt_pages),
            ("read_cache_corrupt_bytes", self.read_cache_corrupt_bytes),
            ("read_cache_invalidations", self.read_cache_invalidations),
            (
                "read_cache_invalidated_entries",
                self.read_cache_invalidated_entries,
            ),
            (
                "read_cache_invalidated_bytes",
                self.read_cache_invalidated_bytes,
            ),
            ("read_cache_clears", self.read_cache_clears),
            (
                "read_cache_cleared_entries",
                self.read_cache_cleared_entries,
            ),
            ("read_cache_cleared_bytes", self.read_cache_cleared_bytes),
            (
                "read_cache_direct_fill_pages",
                self.read_cache_direct_fill_pages,
            ),
            (
                "read_cache_direct_fill_bytes",
                self.read_cache_direct_fill_bytes,
            ),
            (
                "read_cache_compressed_fill_pages",
                self.read_cache_compressed_fill_pages,
            ),
            (
                "read_cache_compressed_fill_bytes",
                self.read_cache_compressed_fill_bytes,
            ),
            ("read_cache_ec_fill_pages", self.read_cache_ec_fill_pages),
            ("read_cache_ec_fill_bytes", self.read_cache_ec_fill_bytes),
            (
                "read_cache_uncacheable_read_pages",
                self.read_cache_uncacheable_read_pages,
            ),
            (
                "read_cache_uncacheable_read_bytes",
                self.read_cache_uncacheable_read_bytes,
            ),
            ("capacity_rejected_writes", self.capacity_rejected_writes),
            (
                "service_mode_rejected_mutations",
                self.service_mode_rejected_mutations,
            ),
            ("physical_write_count", self.physical_write_count),
            ("physical_write_bytes", self.physical_write_bytes),
            ("physical_discard_ranges", self.physical_discard_ranges),
            ("physical_discard_bytes", self.physical_discard_bytes),
            ("compressed_chunk_count", self.compressed_chunk_count),
            ("compression_saved_bytes", self.compression_saved_bytes),
            ("ec_reconstructed_reads", self.ec_reconstructed_reads),
            (
                "ec_reconstruction_failures",
                self.ec_reconstruction_failures,
            ),
            ("receipt_retention", self.receipt_retention),
            ("write_receipt_count", self.write_receipt_count),
            ("discard_receipt_count", self.discard_receipt_count),
            ("batch_write_receipt_count", self.batch_write_receipt_count),
            (
                "batch_discard_receipt_count",
                self.batch_discard_receipt_count,
            ),
            (
                "chunkserver_control_proof_bytes",
                self.replicaserver_control_proof_bytes,
            ),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields
    }
}
