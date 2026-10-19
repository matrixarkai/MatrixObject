impl ChunkStorageReadiness {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("readable", self.readable),
            ("writable", self.writable),
            ("ready", self.ready),
            ("read_cache_pressure", self.read_cache_pressure),
            ("route_qos_pressure", self.route_qos_pressure),
            ("blockserver_route_control_read_ready", self.readable),
            ("blockserver_route_control_write_ready", self.writable),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(format!("{field_prefix}reason"), self.reason.clone());
        for (name, value) in [
            ("replica_count", self.replica_count),
            ("write_quorum", self.write_quorum),
            ("read_quorum", self.read_quorum),
            ("active_replicas", self.active_replicas),
            ("readable_replicas", self.readable_replicas),
            ("writable_replicas", self.writable_replicas),
            ("reclaimable_replicas", self.reclaimable_replicas),
            ("quarantined_replicas", self.quarantined_replicas),
            ("frozen_replicas", self.frozen_replicas),
            ("draining_replicas", self.draining_replicas),
            (
                "capacity_pressure_replicas",
                self.capacity_pressure_replicas,
            ),
            (
                "trim_flow_limited_replicas",
                self.trim_flow_limited_replicas,
            ),
            (
                "total_trim_flow_available_ops",
                self.total_trim_flow_available_ops,
            ),
            (
                "total_trim_flow_throttled_ops",
                self.total_trim_flow_throttled_ops,
            ),
            ("total_physical_bytes", self.total_physical_bytes),
            ("total_capacity_bytes", self.total_capacity_bytes),
            ("total_capacity_free_bytes", self.total_capacity_free_bytes),
            ("metadata_dirty_replicas", self.metadata_dirty_replicas),
            ("metadata_dirty_mutations", self.metadata_dirty_mutations),
            (
                "max_metadata_checkpoint_interval",
                self.max_metadata_checkpoint_interval,
            ),
            (
                "deferred_metadata_checkpoint_replicas",
                self.deferred_metadata_checkpoint_replicas,
            ),
            (
                "deferred_metadata_checkpoint_requests",
                self.deferred_metadata_checkpoint_requests,
            ),
            (
                "deferred_metadata_checkpoint_runs",
                self.deferred_metadata_checkpoint_runs,
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
            ("total_reclaimable_bytes", self.total_reclaimable_bytes),
            (
                "total_fragmented_chunk_count",
                self.total_fragmented_chunk_count,
            ),
            (
                "total_fragmented_discarded_bytes",
                self.total_fragmented_discarded_bytes,
            ),
            ("total_trailing_trim_bytes", self.total_trailing_trim_bytes),
            ("total_read_cache_bytes", self.total_read_cache_bytes),
            ("total_read_cache_entries", self.total_read_cache_entries),
            (
                "total_read_cache_max_bytes",
                self.total_read_cache_max_bytes,
            ),
            (
                "read_cache_pressure_replicas",
                self.read_cache_pressure_replicas,
            ),
            ("total_read_cache_hits", self.total_read_cache_hits),
            ("total_read_cache_misses", self.total_read_cache_misses),
            (
                "total_read_cache_evictions",
                self.total_read_cache_evictions,
            ),
            (
                "total_read_cache_corrupt_pages",
                self.total_read_cache_corrupt_pages,
            ),
            (
                "total_read_cache_corrupt_bytes",
                self.total_read_cache_corrupt_bytes,
            ),
            (
                "total_read_cache_invalidations",
                self.total_read_cache_invalidations,
            ),
            (
                "total_read_cache_invalidated_entries",
                self.total_read_cache_invalidated_entries,
            ),
            (
                "total_read_cache_invalidated_bytes",
                self.total_read_cache_invalidated_bytes,
            ),
            ("total_read_cache_clears", self.total_read_cache_clears),
            (
                "total_read_cache_cleared_entries",
                self.total_read_cache_cleared_entries,
            ),
            (
                "total_read_cache_cleared_bytes",
                self.total_read_cache_cleared_bytes,
            ),
            (
                "total_read_cache_direct_fill_pages",
                self.total_read_cache_direct_fill_pages,
            ),
            (
                "total_read_cache_direct_fill_bytes",
                self.total_read_cache_direct_fill_bytes,
            ),
            (
                "total_read_cache_compressed_fill_pages",
                self.total_read_cache_compressed_fill_pages,
            ),
            (
                "total_read_cache_compressed_fill_bytes",
                self.total_read_cache_compressed_fill_bytes,
            ),
            (
                "total_read_cache_ec_fill_pages",
                self.total_read_cache_ec_fill_pages,
            ),
            (
                "total_read_cache_ec_fill_bytes",
                self.total_read_cache_ec_fill_bytes,
            ),
            (
                "cumulative_cache_prewarm_applied_ranges",
                self.cumulative_cache_prewarm_applied_ranges,
            ),
            (
                "cumulative_cache_prewarm_failed_ranges",
                self.cumulative_cache_prewarm_failed_ranges,
            ),
            (
                "cumulative_cache_invalidate_applied_ranges",
                self.cumulative_cache_invalidate_applied_ranges,
            ),
            (
                "cumulative_cache_invalidate_failed_ranges",
                self.cumulative_cache_invalidate_failed_ranges,
            ),
            ("min_capacity_free_bytes", self.min_capacity_free_bytes),
            ("min_reclaimable_bytes", self.min_reclaimable_bytes),
            ("block_read_parallelism", self.page_read_parallelism),
            ("block_write_parallelism", self.page_write_parallelism),
            (
                "block_batch_read_parallelism",
                self.page_batch_read_parallelism,
            ),
            (
                "block_batch_write_parallelism",
                self.page_batch_write_parallelism,
            ),
            (
                "block_batch_discard_parallelism",
                self.page_batch_discard_parallelism,
            ),
            ("block_repair_parallelism", self.page_repair_parallelism),
            (
                "block_chunk_fanout_parallelism",
                self.replica_fanout_parallelism,
            ),
            (
                "primary_batch_read_repair_probe_limit",
                self.primary_batch_read_repair_probe_limit,
            ),
            (
                "effective_chunk_fanout_parallelism",
                self.effective_chunk_fanout_parallelism,
            ),
            (
                "local_replica_store_parallelism",
                self.local_replica_store_parallelism,
            ),
            (
                "effective_local_replica_store_parallelism",
                self.effective_local_replica_store_parallelism,
            ),
            ("chunk_endpoint_count", self.chunk_endpoint_count),
            (
                "disabled_chunk_endpoint_count",
                self.disabled_chunk_endpoint_count,
            ),
            (
                "effective_writable_chunk_endpoints",
                self.effective_writable_chunk_endpoints,
            ),
            (
                "cumulative_quorum_write_degraded_chunks",
                self.cumulative_quorum_write_degraded_chunks,
            ),
            (
                "cumulative_quorum_read_degraded_chunks",
                self.cumulative_quorum_read_degraded_chunks,
            ),
            (
                "cumulative_quorum_discard_degraded_chunks",
                self.cumulative_quorum_discard_degraded_chunks,
            ),
            (
                "cumulative_replica_fallback_reads",
                self.cumulative_replica_fallback_reads,
            ),
            (
                "cumulative_physical_chunk_read_count",
                self.cumulative_physical_chunk_read_count,
            ),
            (
                "cumulative_coalesced_chunk_read_count",
                self.cumulative_coalesced_chunk_read_count,
            ),
            (
                "cumulative_repair_checked_chunks",
                self.cumulative_repair_checked_chunks,
            ),
            (
                "cumulative_repair_degraded_chunks",
                self.cumulative_repair_degraded_chunks,
            ),
            (
                "cumulative_repaired_replicas",
                self.cumulative_repaired_replicas,
            ),
            ("cumulative_failed_repairs", self.cumulative_failed_repairs),
            (
                "cumulative_replica_read_repairs",
                self.cumulative_replica_read_repairs,
            ),
            (
                "cumulative_replica_read_repair_failures",
                self.cumulative_replica_read_repair_failures,
            ),
            ("receipt_retention", self.receipt_retention),
            ("allocation_receipt_count", self.allocation_receipt_count),
            (
                "batch_allocation_receipt_count",
                self.batch_allocation_receipt_count,
            ),
            (
                "release_allocation_receipt_count",
                self.release_allocation_receipt_count,
            ),
            ("write_receipt_count", self.write_receipt_count),
            ("batch_write_receipt_count", self.batch_write_receipt_count),
            ("discard_receipt_count", self.discard_receipt_count),
            (
                "batch_discard_receipt_count",
                self.batch_discard_receipt_count,
            ),
            ("route_qos_window_ms", self.route_qos_window_ms),
            (
                "route_qos_read_ops_in_window",
                self.route_qos_read_ops_in_window,
            ),
            (
                "route_qos_read_bytes_in_window",
                self.route_qos_read_bytes_in_window,
            ),
            (
                "route_qos_write_ops_in_window",
                self.route_qos_write_ops_in_window,
            ),
            (
                "route_qos_write_bytes_in_window",
                self.route_qos_write_bytes_in_window,
            ),
            (
                "route_qos_discard_ops_in_window",
                self.route_qos_discard_ops_in_window,
            ),
            (
                "route_qos_discard_bytes_in_window",
                self.route_qos_discard_bytes_in_window,
            ),
            ("route_qos_throttled_ops", self.route_qos_throttled_ops),
            ("route_qos_throttled_bytes", self.route_qos_throttled_bytes),
            ("sealed_segment_count", self.sealed_slab_count),
            (
                "allocator_active_segment_sealed",
                self.allocator_active_slab_sealed as u64,
            ),
            (
                "block_state_fingerprint_bytes",
                self.page_state_fingerprint_bytes,
            ),
            (
                "block_route_control_proof_bytes",
                self.chunk_route_control_proof_bytes,
            ),
            (
                "replica_readiness_count",
                self.replica_readiness.len() as u64,
            ),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        for (name, value) in [
            ("block_state_fingerprint", &self.page_state_fingerprint),
            (
                "block_state_fingerprint_format",
                &self.page_state_fingerprint_format,
            ),
            (
                "block_route_control_proof_format",
                &self.chunk_route_control_proof_format,
            ),
            (
                "block_route_control_proof_checksum",
                &self.chunk_route_control_proof_checksum,
            ),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.clone());
        }
        fields.extend(
            self.route_control_proof
                .to_control_fields(&format!("{field_prefix}route_control.")),
        );
        for (index, readiness) in self.replica_readiness.iter().enumerate() {
            fields.extend(
                readiness.to_control_fields(&format!("{field_prefix}replica_readiness.{index}.")),
            );
        }
        fields
    }
}

