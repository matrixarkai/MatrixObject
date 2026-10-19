impl MatrixChunkService {
    pub fn storage_readiness(
        &self,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        now_ms: u64,
    ) -> ChunkStorageReadiness {
        let route_qos_state = self.route_qos_state();
        let route_control_proof = self.route_control_proof();
        let mut readiness = ChunkStorageReadiness {
            replica_count: self.replicas.len() as u64,
            write_quorum: self.write_quorum as u64,
            read_quorum: self.read_quorum as u64,
            quarantined_replicas: self.quarantined_replicas.len() as u64,
            min_capacity_free_bytes,
            min_reclaimable_bytes,
            page_read_parallelism: self.page_read_parallelism as u64,
            page_write_parallelism: self.page_write_parallelism as u64,
            page_batch_read_parallelism: self.page_batch_read_parallelism as u64,
            page_batch_write_parallelism: self.page_batch_write_parallelism as u64,
            page_batch_discard_parallelism: self.page_batch_discard_parallelism as u64,
            page_repair_parallelism: self.page_repair_parallelism as u64,
            replica_fanout_parallelism: self.replica_fanout_parallelism as u64,
            primary_batch_read_repair_probe_limit: self.primary_batch_read_repair_probe_limit
                as u64,
            effective_chunk_fanout_parallelism: bounded_parallelism(
                self.replica_fanout_parallelism,
                self.replicas.len(),
            ) as u64,
            local_replica_store_parallelism: self.local_replica_store_parallelism as u64,
            effective_local_replica_store_parallelism: bounded_parallelism(
                self.local_replica_store_parallelism,
                self.replicas.len(),
            ) as u64,
            chunk_endpoint_count: self.chunk_endpoints.len() as u64,
            disabled_chunk_endpoint_count: self.disabled_chunk_endpoints.len() as u64,
            effective_writable_chunk_endpoints: self
                .chunk_endpoints
                .len()
                .saturating_sub(self.disabled_chunk_endpoints.len())
                as u64,
            cumulative_quorum_write_degraded_chunks: self.cumulative_quorum_write_degraded_chunks,
            cumulative_quorum_read_degraded_chunks: self.cumulative_quorum_read_degraded_chunks,
            cumulative_quorum_discard_degraded_chunks: self
                .cumulative_quorum_discard_degraded_chunks,
            cumulative_replica_fallback_reads: self.cumulative_replica_fallback_reads,
            cumulative_physical_chunk_read_count: self.cumulative_physical_chunk_read_count.get(),
            cumulative_coalesced_chunk_read_count: self.cumulative_coalesced_chunk_read_count.get(),
            cumulative_cache_prewarm_applied_ranges: self.cumulative_cache_prewarm_applied_ranges,
            cumulative_cache_prewarm_failed_ranges: self.cumulative_cache_prewarm_failed_ranges,
            cumulative_cache_invalidate_applied_ranges: self
                .cumulative_cache_invalidate_applied_ranges,
            cumulative_cache_invalidate_failed_ranges: self
                .cumulative_cache_invalidate_failed_ranges,
            cumulative_repair_checked_chunks: self.cumulative_repair_checked_chunks,
            cumulative_repair_degraded_chunks: self.cumulative_repair_degraded_chunks,
            cumulative_repaired_replicas: self.cumulative_repaired_replicas,
            cumulative_failed_repairs: self.cumulative_failed_repairs,
            cumulative_replica_read_repairs: self.cumulative_replica_read_repairs,
            cumulative_replica_read_repair_failures: self.cumulative_replica_read_repair_failures,
            receipt_retention: self.receipt_retention as u64,
            allocation_receipt_count: self.allocation_request_receipts.len() as u64,
            batch_allocation_receipt_count: self.batch_allocation_request_receipts.len() as u64,
            release_allocation_receipt_count: self.release_allocation_request_receipts.len() as u64,
            write_receipt_count: self.write_request_receipts.len() as u64,
            batch_write_receipt_count: self.batch_write_request_receipts.len() as u64,
            discard_receipt_count: self.discard_request_receipts.len() as u64,
            batch_discard_receipt_count: self.batch_discard_request_receipts.len() as u64,
            route_qos_window_ms: self.route_qos_config.window_ms,
            route_qos_read_ops_in_window: route_qos_state.read_ops_in_window,
            route_qos_read_bytes_in_window: route_qos_state.read_bytes_in_window,
            route_qos_write_ops_in_window: route_qos_state.write_ops_in_window,
            route_qos_write_bytes_in_window: route_qos_state.write_bytes_in_window,
            route_qos_discard_ops_in_window: route_qos_state.discard_ops_in_window,
            route_qos_discard_bytes_in_window: route_qos_state.discard_bytes_in_window,
            route_qos_throttled_ops: route_qos_state.throttled_ops,
            route_qos_throttled_bytes: route_qos_state.throttled_bytes,
            route_qos_pressure: self.chunk_route_qos_pressure(),
            chunk_route_control_proof_format: route_control_proof.state_format.clone(),
            chunk_route_control_proof_checksum: route_control_proof.state_checksum.clone(),
            chunk_route_control_proof_bytes: route_control_proof.state_bytes,
            sealed_chunk_count: self.sealed_chunks.len() as u64,
            allocator_active_chunk_sealed: self.allocator_active_chunk_sealed(),
            route_control_proof,
            ..ChunkStorageReadiness::default()
        };
        for (index, replica) in self.replicas.iter().enumerate() {
            let replica_ready =
                replica.storage_readiness(min_capacity_free_bytes, min_reclaimable_bytes, now_ms);
            if self.replica_active(index) {
                readiness.active_replicas = readiness.active_replicas.saturating_add(1);
                if replica_ready.readable {
                    readiness.readable_replicas = readiness.readable_replicas.saturating_add(1);
                }
                if replica_ready.writable {
                    readiness.writable_replicas = readiness.writable_replicas.saturating_add(1);
                }
            }
            if replica_ready.reclaimable {
                readiness.reclaimable_replicas = readiness.reclaimable_replicas.saturating_add(1);
            }
            if replica_ready.frozen {
                readiness.frozen_replicas = readiness.frozen_replicas.saturating_add(1);
            }
            if replica_ready.draining {
                readiness.draining_replicas = readiness.draining_replicas.saturating_add(1);
            }
            if replica_ready.capacity_pressure {
                readiness.capacity_pressure_replicas =
                    readiness.capacity_pressure_replicas.saturating_add(1);
            }
            if replica_ready.trim_flow_limited {
                readiness.trim_flow_limited_replicas =
                    readiness.trim_flow_limited_replicas.saturating_add(1);
            }
            readiness.total_trim_flow_available_ops = readiness
                .total_trim_flow_available_ops
                .saturating_add(replica_ready.trim_flow_available_ops);
            readiness.total_trim_flow_throttled_ops = readiness
                .total_trim_flow_throttled_ops
                .saturating_add(replica_ready.trim_flow_throttled_ops);
            readiness.total_physical_bytes = readiness
                .total_physical_bytes
                .saturating_add(replica_ready.physical_bytes);
            readiness.total_capacity_bytes = readiness
                .total_capacity_bytes
                .saturating_add(replica_ready.capacity_bytes);
            if replica_ready.capacity_bytes == 0 {
                readiness.total_capacity_free_bytes = u64::MAX;
            } else if readiness.total_capacity_free_bytes != u64::MAX {
                readiness.total_capacity_free_bytes = readiness
                    .total_capacity_free_bytes
                    .saturating_add(replica_ready.capacity_free_bytes);
            }
            if replica_ready.metadata_dirty {
                readiness.metadata_dirty_replicas =
                    readiness.metadata_dirty_replicas.saturating_add(1);
            }
            readiness.metadata_dirty_mutations = readiness
                .metadata_dirty_mutations
                .saturating_add(replica_ready.metadata_dirty_mutations);
            readiness.max_metadata_checkpoint_interval = readiness
                .max_metadata_checkpoint_interval
                .max(replica_ready.metadata_checkpoint_interval);
            if replica_ready.deferred_metadata_checkpoint_pending {
                readiness.deferred_metadata_checkpoint_replicas = readiness
                    .deferred_metadata_checkpoint_replicas
                    .saturating_add(1);
            }
            readiness.deferred_metadata_checkpoint_requests = readiness
                .deferred_metadata_checkpoint_requests
                .saturating_add(replica_ready.deferred_metadata_checkpoint_requests);
            readiness.deferred_metadata_checkpoint_runs = readiness
                .deferred_metadata_checkpoint_runs
                .saturating_add(replica_ready.deferred_metadata_checkpoint_runs);
            readiness.capacity_rejected_writes = readiness
                .capacity_rejected_writes
                .saturating_add(replica_ready.capacity_rejected_writes);
            readiness.service_mode_rejected_mutations = readiness
                .service_mode_rejected_mutations
                .saturating_add(replica_ready.service_mode_rejected_mutations);
            readiness.physical_write_count = readiness
                .physical_write_count
                .saturating_add(replica_ready.physical_write_count);
            readiness.physical_write_bytes = readiness
                .physical_write_bytes
                .saturating_add(replica_ready.physical_write_bytes);
            readiness.physical_discard_ranges = readiness
                .physical_discard_ranges
                .saturating_add(replica_ready.physical_discard_ranges);
            readiness.physical_discard_bytes = readiness
                .physical_discard_bytes
                .saturating_add(replica_ready.physical_discard_bytes);
            readiness.total_reclaimable_bytes = readiness
                .total_reclaimable_bytes
                .saturating_add(replica_ready.reclaimable_bytes);
            readiness.partial_reclaim_failures = readiness
                .partial_reclaim_failures
                .saturating_add(replica_ready.partial_reclaim_failures);
            if readiness.last_partial_reclaim_failure_status.is_empty()
                || readiness.last_partial_reclaim_failure_status == "ok"
            {
                readiness.last_partial_reclaim_failure_status =
                    replica_ready.last_partial_reclaim_failure_status.clone();
            }
            readiness.total_fragmented_chunk_count = readiness
                .total_fragmented_chunk_count
                .saturating_add(replica_ready.fragmented_chunk_count);
            readiness.total_fragmented_discarded_bytes = readiness
                .total_fragmented_discarded_bytes
                .saturating_add(replica_ready.fragmented_discarded_bytes);
            readiness.total_trailing_trim_bytes = readiness
                .total_trailing_trim_bytes
                .saturating_add(replica_ready.trailing_trim_bytes);
            readiness.total_read_cache_bytes = readiness
                .total_read_cache_bytes
                .saturating_add(replica_ready.read_cache_bytes);
            readiness.total_read_cache_entries = readiness
                .total_read_cache_entries
                .saturating_add(replica_ready.read_cache_entries);
            readiness.total_read_cache_max_bytes = readiness
                .total_read_cache_max_bytes
                .saturating_add(replica_ready.read_cache_max_bytes);
            if replica_ready.read_cache_max_bytes > 0
                && replica_ready.read_cache_bytes >= replica_ready.read_cache_max_bytes
            {
                readiness.read_cache_pressure_replicas =
                    readiness.read_cache_pressure_replicas.saturating_add(1);
            }
            readiness.total_read_cache_hits = readiness
                .total_read_cache_hits
                .saturating_add(replica_ready.read_cache_hits);
            readiness.total_read_cache_misses = readiness
                .total_read_cache_misses
                .saturating_add(replica_ready.read_cache_misses);
            readiness.total_read_cache_evictions = readiness
                .total_read_cache_evictions
                .saturating_add(replica_ready.read_cache_evictions);
            readiness.total_read_cache_corrupt_pages = readiness
                .total_read_cache_corrupt_pages
                .saturating_add(replica_ready.read_cache_corrupt_pages);
            readiness.total_read_cache_corrupt_bytes = readiness
                .total_read_cache_corrupt_bytes
                .saturating_add(replica_ready.read_cache_corrupt_bytes);
            readiness.total_read_cache_invalidations = readiness
                .total_read_cache_invalidations
                .saturating_add(replica_ready.read_cache_invalidations);
            readiness.total_read_cache_invalidated_entries = readiness
                .total_read_cache_invalidated_entries
                .saturating_add(replica_ready.read_cache_invalidated_entries);
            readiness.total_read_cache_invalidated_bytes = readiness
                .total_read_cache_invalidated_bytes
                .saturating_add(replica_ready.read_cache_invalidated_bytes);
            readiness.total_read_cache_clears = readiness
                .total_read_cache_clears
                .saturating_add(replica_ready.read_cache_clears);
            readiness.total_read_cache_cleared_entries = readiness
                .total_read_cache_cleared_entries
                .saturating_add(replica_ready.read_cache_cleared_entries);
            readiness.total_read_cache_cleared_bytes = readiness
                .total_read_cache_cleared_bytes
                .saturating_add(replica_ready.read_cache_cleared_bytes);
            readiness.total_read_cache_direct_fill_pages = readiness
                .total_read_cache_direct_fill_pages
                .saturating_add(replica_ready.read_cache_direct_fill_pages);
            readiness.total_read_cache_direct_fill_bytes = readiness
                .total_read_cache_direct_fill_bytes
                .saturating_add(replica_ready.read_cache_direct_fill_bytes);
            readiness.total_read_cache_compressed_fill_pages = readiness
                .total_read_cache_compressed_fill_pages
                .saturating_add(replica_ready.read_cache_compressed_fill_pages);
            readiness.total_read_cache_compressed_fill_bytes = readiness
                .total_read_cache_compressed_fill_bytes
                .saturating_add(replica_ready.read_cache_compressed_fill_bytes);
            readiness.total_read_cache_ec_fill_pages = readiness
                .total_read_cache_ec_fill_pages
                .saturating_add(replica_ready.read_cache_ec_fill_pages);
            readiness.total_read_cache_ec_fill_bytes = readiness
                .total_read_cache_ec_fill_bytes
                .saturating_add(replica_ready.read_cache_ec_fill_bytes);
            readiness.total_read_cache_uncacheable_read_pages = readiness
                .total_read_cache_uncacheable_read_pages
                .saturating_add(replica_ready.read_cache_uncacheable_read_pages);
            readiness.total_read_cache_uncacheable_read_bytes = readiness
                .total_read_cache_uncacheable_read_bytes
                .saturating_add(replica_ready.read_cache_uncacheable_read_bytes);
            readiness.replica_readiness.push(replica_ready);
        }
        readiness.readable_replicas = readiness.readable_replicas.min(readiness.active_replicas);
        readiness.writable_replicas = readiness.writable_replicas.min(readiness.active_replicas);
        readiness.read_cache_pressure = readiness.read_cache_pressure_replicas > 0;
        readiness.readable = readiness.readable_replicas >= readiness.read_quorum;
        readiness.writable = readiness.writable_replicas >= readiness.write_quorum;
        readiness.ready = readiness.readable && readiness.writable;
        readiness.reason = if readiness.ready {
            "ready".to_string()
        } else if !readiness.readable {
            "read_quorum_unavailable".to_string()
        } else if readiness.quarantined_replicas > 0 {
            "replica_quarantine".to_string()
        } else if readiness.frozen_replicas > 0 {
            "replica_frozen".to_string()
        } else if readiness.draining_replicas > 0 {
            "replica_draining".to_string()
        } else if readiness.capacity_pressure_replicas > 0 {
            "capacity_pressure".to_string()
        } else {
            "write_quorum_unavailable".to_string()
        };
        let page_fingerprint_material = page_storage_readiness_fingerprint_material(&readiness);
        readiness.page_state_fingerprint = etag(&page_fingerprint_material);
        readiness.page_state_fingerprint_format =
            PAGE_STORAGE_READINESS_FINGERPRINT_FORMAT.to_string();
        readiness.page_state_fingerprint_bytes = page_fingerprint_material.len() as u64;
        readiness
    }


}
