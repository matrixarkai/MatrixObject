impl ReplicaServiceControlProof {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("control_receipt", self.control_receipt),
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
            (
                "read_cache_config_raft_backed",
                self.read_cache_config_raft_backed,
            ),
            (
                "capacity_config_raft_backed",
                self.capacity_config_raft_backed,
            ),
            (
                "service_mode_config_raft_backed",
                self.service_mode_config_raft_backed,
            ),
            (
                "trim_flow_config_raft_backed",
                self.trim_flow_config_raft_backed,
            ),
            (
                "mutation_qos_config_raft_backed",
                self.mutation_qos_config_raft_backed,
            ),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        for (name, value) in [
            ("state_bytes", self.state_bytes),
            (
                "chunk_mutation_control_commit_index",
                self.chunk_mutation_control_commit_index,
            ),
            (
                "chunk_mutation_control_next_log_index",
                self.chunk_mutation_control_next_log_index,
            ),
            ("capacity_bytes", self.capacity_bytes),
            ("physical_bytes", self.physical_bytes),
            ("capacity_free_bytes", self.capacity_free_bytes),
            ("capacity_rejected_writes", self.capacity_rejected_writes),
            (
                "service_mode_rejected_mutations",
                self.service_mode_rejected_mutations,
            ),
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
            ("physical_write_count", self.physical_write_count),
            ("physical_write_bytes", self.physical_write_bytes),
            ("physical_discard_ranges", self.physical_discard_ranges),
            ("physical_discard_bytes", self.physical_discard_bytes),
            ("compressed_chunk_count", self.compressed_chunk_count),
            ("compression_saved_bytes", self.compression_saved_bytes),
            ("receipt_retention", self.receipt_retention),
            ("write_receipt_count", self.write_receipt_count),
            ("discard_receipt_count", self.discard_receipt_count),
            ("batch_write_receipt_count", self.batch_write_receipt_count),
            (
                "batch_discard_receipt_count",
                self.batch_discard_receipt_count,
            ),
            ("read_cache_max_bytes", self.read_cache_max_bytes),
            (
                "read_cache_max_cacheable_read_bytes",
                self.read_cache_max_cacheable_read_bytes,
            ),
            (
                "trim_flow_rate_bytes_per_sec",
                self.trim_flow_rate_bytes_per_sec,
            ),
            ("trim_flow_burst_bytes", self.trim_flow_burst_bytes),
            ("trim_flow_available_bytes", self.trim_flow_available_bytes),
            (
                "trim_flow_rate_ops_per_sec",
                self.trim_flow_rate_ops_per_sec,
            ),
            ("trim_flow_burst_ops", self.trim_flow_burst_ops),
            ("trim_flow_available_ops", self.trim_flow_available_ops),
            ("trim_flow_throttled_ops", self.trim_flow_throttled_ops),
            ("mutation_qos_window_ms", self.mutation_qos_window_ms),
            (
                "mutation_qos_max_write_ops_per_window",
                self.mutation_qos_max_write_ops_per_window,
            ),
            (
                "mutation_qos_max_write_bytes_per_window",
                self.mutation_qos_max_write_bytes_per_window,
            ),
            (
                "mutation_qos_max_discard_ops_per_window",
                self.mutation_qos_max_discard_ops_per_window,
            ),
            (
                "mutation_qos_max_discard_bytes_per_window",
                self.mutation_qos_max_discard_bytes_per_window,
            ),
            (
                "mutation_qos_throttled_ops",
                self.mutation_qos_throttled_ops,
            ),
            (
                "mutation_qos_throttled_bytes",
                self.mutation_qos_throttled_bytes,
            ),
            ("read_cache_hits", self.read_cache_hits),
            ("read_cache_misses", self.read_cache_misses),
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
            ("ec_reconstructed_reads", self.ec_reconstructed_reads),
            (
                "ec_reconstruction_failures",
                self.ec_reconstruction_failures,
            ),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(
            format!("{field_prefix}state_format"),
            self.state_format.clone(),
        );
        fields.insert(
            format!("{field_prefix}state_checksum"),
            self.state_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}snapshot_checksum"),
            self.snapshot_checksum.clone(),
        );
        fields
    }
}

