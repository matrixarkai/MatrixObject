pub(crate) fn page_storage_readiness_fingerprint_material(
    readiness: &PageStorageReadiness,
) -> Vec<u8> {
    let mut material = Vec::new();
    append_string(&mut material, PAGE_STORAGE_READINESS_FINGERPRINT_FORMAT);
    append_u64(&mut material, readiness.readable as u64);
    append_u64(&mut material, readiness.writable as u64);
    append_u64(&mut material, readiness.ready as u64);
    append_string(&mut material, &readiness.reason);
    append_u64(&mut material, readiness.replica_count);
    append_u64(&mut material, readiness.write_quorum);
    append_u64(&mut material, readiness.read_quorum);
    append_u64(&mut material, readiness.active_replicas);
    append_u64(&mut material, readiness.readable_replicas);
    append_u64(&mut material, readiness.writable_replicas);
    append_u64(&mut material, readiness.quarantined_replicas);
    append_u64(&mut material, readiness.frozen_replicas);
    append_u64(&mut material, readiness.draining_replicas);
    append_u64(&mut material, readiness.capacity_pressure_replicas);
    append_u64(&mut material, readiness.trim_flow_limited_replicas);
    append_u64(&mut material, readiness.total_trim_flow_available_ops);
    append_u64(&mut material, readiness.total_trim_flow_throttled_ops);
    append_u64(&mut material, readiness.total_physical_bytes);
    append_u64(&mut material, readiness.total_capacity_bytes);
    append_u64(&mut material, readiness.total_capacity_free_bytes);
    append_u64(&mut material, readiness.sealed_slab_count);
    append_u64(
        &mut material,
        readiness.allocator_active_slab_sealed as u64,
    );
    append_u64(&mut material, readiness.metadata_dirty_replicas);
    append_u64(&mut material, readiness.metadata_dirty_mutations);
    append_u64(&mut material, readiness.max_metadata_checkpoint_interval);
    append_u64(
        &mut material,
        readiness.deferred_metadata_checkpoint_replicas,
    );
    append_u64(
        &mut material,
        readiness.deferred_metadata_checkpoint_requests,
    );
    append_u64(&mut material, readiness.deferred_metadata_checkpoint_runs);
    append_u64(&mut material, readiness.capacity_rejected_writes);
    append_u64(&mut material, readiness.service_mode_rejected_mutations);
    append_u64(&mut material, readiness.physical_write_count);
    append_u64(&mut material, readiness.physical_write_bytes);
    append_u64(&mut material, readiness.physical_discard_ranges);
    append_u64(&mut material, readiness.physical_discard_bytes);
    append_u64(&mut material, readiness.total_reclaimable_bytes);
    append_u64(&mut material, readiness.total_fragmented_chunk_count);
    append_u64(&mut material, readiness.total_fragmented_discarded_bytes);
    append_u64(&mut material, readiness.total_trailing_trim_bytes);
    append_u64(&mut material, readiness.total_read_cache_bytes);
    append_u64(&mut material, readiness.total_read_cache_entries);
    append_u64(&mut material, readiness.total_read_cache_max_bytes);
    append_u64(&mut material, readiness.read_cache_pressure_replicas);
    append_u64(&mut material, readiness.read_cache_pressure as u64);
    append_u64(&mut material, readiness.total_read_cache_hits);
    append_u64(&mut material, readiness.total_read_cache_misses);
    append_u64(&mut material, readiness.total_read_cache_evictions);
    append_u64(&mut material, readiness.total_read_cache_corrupt_pages);
    append_u64(&mut material, readiness.total_read_cache_corrupt_bytes);
    append_u64(&mut material, readiness.total_read_cache_invalidations);
    append_u64(
        &mut material,
        readiness.total_read_cache_invalidated_entries,
    );
    append_u64(&mut material, readiness.total_read_cache_invalidated_bytes);
    append_u64(&mut material, readiness.total_read_cache_clears);
    append_u64(&mut material, readiness.total_read_cache_cleared_entries);
    append_u64(&mut material, readiness.total_read_cache_cleared_bytes);
    append_u64(&mut material, readiness.total_read_cache_direct_fill_pages);
    append_u64(&mut material, readiness.total_read_cache_direct_fill_bytes);
    append_u64(
        &mut material,
        readiness.total_read_cache_compressed_fill_pages,
    );
    append_u64(
        &mut material,
        readiness.total_read_cache_compressed_fill_bytes,
    );
    append_u64(&mut material, readiness.total_read_cache_ec_fill_pages);
    append_u64(&mut material, readiness.total_read_cache_ec_fill_bytes);
    append_u64(
        &mut material,
        readiness.total_read_cache_uncacheable_read_pages,
    );
    append_u64(
        &mut material,
        readiness.total_read_cache_uncacheable_read_bytes,
    );
    append_u64(
        &mut material,
        readiness.cumulative_cache_prewarm_applied_ranges,
    );
    append_u64(
        &mut material,
        readiness.cumulative_cache_prewarm_failed_ranges,
    );
    append_u64(
        &mut material,
        readiness.cumulative_cache_invalidate_applied_ranges,
    );
    append_u64(
        &mut material,
        readiness.cumulative_cache_invalidate_failed_ranges,
    );
    append_u64(&mut material, readiness.page_read_parallelism);
    append_u64(&mut material, readiness.page_write_parallelism);
    append_u64(&mut material, readiness.page_batch_read_parallelism);
    append_u64(&mut material, readiness.page_batch_write_parallelism);
    append_u64(&mut material, readiness.page_batch_discard_parallelism);
    append_u64(&mut material, readiness.page_repair_parallelism);
    append_u64(&mut material, readiness.page_chunk_fanout_parallelism);
    append_u64(
        &mut material,
        readiness.primary_batch_read_repair_probe_limit,
    );
    append_u64(&mut material, readiness.effective_chunk_fanout_parallelism);
    append_u64(&mut material, readiness.local_replica_store_parallelism);
    append_u64(
        &mut material,
        readiness.effective_local_replica_store_parallelism,
    );
    append_u64(&mut material, readiness.chunk_endpoint_count);
    append_u64(&mut material, readiness.disabled_chunk_endpoint_count);
    append_u64(&mut material, readiness.effective_writable_chunk_endpoints);
    append_u64(
        &mut material,
        readiness.cumulative_quorum_write_degraded_chunks,
    );
    append_u64(
        &mut material,
        readiness.cumulative_quorum_read_degraded_chunks,
    );
    append_u64(
        &mut material,
        readiness.cumulative_quorum_discard_degraded_chunks,
    );
    append_u64(&mut material, readiness.cumulative_replica_fallback_reads);
    append_u64(
        &mut material,
        readiness.cumulative_physical_chunk_read_count,
    );
    append_u64(
        &mut material,
        readiness.cumulative_coalesced_chunk_read_count,
    );
    append_u64(&mut material, readiness.cumulative_repair_checked_chunks);
    append_u64(&mut material, readiness.cumulative_repair_degraded_chunks);
    append_u64(&mut material, readiness.cumulative_repaired_replicas);
    append_u64(&mut material, readiness.cumulative_failed_repairs);
    append_u64(&mut material, readiness.cumulative_replica_read_repairs);
    append_u64(
        &mut material,
        readiness.cumulative_replica_read_repair_failures,
    );
    append_u64(&mut material, readiness.receipt_retention);
    append_u64(&mut material, readiness.allocation_receipt_count);
    append_u64(&mut material, readiness.batch_allocation_receipt_count);
    append_u64(&mut material, readiness.release_allocation_receipt_count);
    append_u64(&mut material, readiness.write_receipt_count);
    append_u64(&mut material, readiness.batch_write_receipt_count);
    append_u64(&mut material, readiness.discard_receipt_count);
    append_u64(&mut material, readiness.batch_discard_receipt_count);
    append_u64(&mut material, readiness.route_qos_window_ms);
    append_u64(&mut material, readiness.route_qos_read_ops_in_window);
    append_u64(&mut material, readiness.route_qos_read_bytes_in_window);
    append_u64(&mut material, readiness.route_qos_write_ops_in_window);
    append_u64(&mut material, readiness.route_qos_write_bytes_in_window);
    append_u64(&mut material, readiness.route_qos_discard_ops_in_window);
    append_u64(&mut material, readiness.route_qos_discard_bytes_in_window);
    append_u64(&mut material, readiness.route_qos_throttled_ops);
    append_u64(&mut material, readiness.route_qos_throttled_bytes);
    append_u64(&mut material, readiness.route_qos_pressure as u64);
    append_string(&mut material, &readiness.route_control_proof.state_format);
    append_u64(&mut material, readiness.route_control_proof.state_bytes);
    append_string(&mut material, &readiness.route_control_proof.state_checksum);
    append_u64(
        &mut material,
        readiness.route_control_proof.disabled_replica_count,
    );
    append_u64(&mut material, readiness.replica_readiness.len() as u64);
    for replica in &readiness.replica_readiness {
        append_string(&mut material, &replica.chunk_state_fingerprint);
        append_u64(&mut material, replica.readable as u64);
        append_u64(&mut material, replica.writable as u64);
        append_u64(&mut material, replica.ready as u64);
        append_string(&mut material, &replica.reason);
        append_u64(&mut material, replica.chunk_count);
        append_u64(&mut material, replica.physical_bytes);
        append_u64(&mut material, replica.capacity_bytes);
        append_u64(&mut material, replica.capacity_free_bytes);
        append_u64(&mut material, replica.reclaimable_bytes);
        append_u64(&mut material, replica.discarded_range_count);
        append_u64(&mut material, replica.capacity_rejected_writes);
        append_u64(&mut material, replica.service_mode_rejected_mutations);
        append_u64(&mut material, replica.physical_write_count);
        append_u64(&mut material, replica.physical_write_bytes);
        append_u64(&mut material, replica.physical_discard_ranges);
        append_u64(&mut material, replica.physical_discard_bytes);
        append_u64(&mut material, replica.receipt_retention);
        append_u64(&mut material, replica.write_receipt_count);
        append_u64(&mut material, replica.discard_receipt_count);
        append_u64(&mut material, replica.batch_write_receipt_count);
        append_u64(&mut material, replica.batch_discard_receipt_count);
        append_u64(&mut material, replica.compressed_chunk_count);
        append_u64(&mut material, replica.compression_saved_bytes);
        append_u64(&mut material, replica.ec_reconstructed_reads);
        append_u64(&mut material, replica.ec_reconstruction_failures);
    }
    material
}

