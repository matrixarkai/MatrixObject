fn append_storage_status(mut material: &mut Vec<u8>, status: &ObjectStoreModuleStatus) {
    append_u64(&mut material, status.live_bytes);
    append_u64(&mut material, status.stale_bytes);
    append_u64(&mut material, status.pending_discard_bytes);
    append_u64(&mut material, status.discarded_bytes);
    append_u64(&mut material, status.physical_bytes);
    append_u64(&mut material, status.live_bytes_ppm);
    append_u64(&mut material, status.stale_bytes_ppm);
    append_u64(&mut material, status.pending_discard_bytes_ppm);
    append_u64(&mut material, status.discarded_bytes_ppm);
    append_u64(&mut material, status.logical_write_amplification_ppm);
    append_u64(&mut material, status.storage_write_amplification_ppm);
    append_u64(&mut material, status.space_amplification_ppm);
    append_u64(&mut material, status.discard_backlog_ppm);
    append_u64(&mut material, status.discard_candidate_bytes);
    append_u64(&mut material, status.discard_candidate_chunks);
    append_u64(&mut material, status.pending_discard_range_count);
    append_u64(&mut material, status.merged_discard_range_count);
    append_u64(&mut material, status.fragmented_range_count);
    append_u64(&mut material, status.compaction_candidate_chunks);
    append_u64(&mut material, status.compaction_candidate_bytes);
    append_u64(&mut material, status.compaction_candidate_range_count);
    append_u64(&mut material, status.compaction_recommended as u64);
    append_u64(&mut material, status.bucket_count);
    append_u64(&mut material, status.object_count);
    append_u64(&mut material, status.live_object_count);
    append_u64(&mut material, status.deleted_object_count);
    append_u64(&mut material, status.chunk_count);
    append_u64(&mut material, status.page_count);
    append_u64(&mut material, status.logical_written_bytes);
    append_u64(&mut material, status.physical_written_bytes);
    append_u64(&mut material, status.chunk_read_cache_max_bytes_per_replica);
    append_u64(
        &mut material,
        status.chunk_read_cache_max_cacheable_read_bytes_per_replica,
    );
    append_u64(&mut material, status.placement_degraded_objects);
    append_u64(&mut material, status.placement_bytes_to_repair);
    append_u64(&mut material, status.placement_truncated as u64);
    append_u64(&mut material, status.capacity_free_bytes);
    append_u64(&mut material, status.capacity_pressure_replicas);
    append_u64(&mut material, status.volume_count);
    append_u64(&mut material, status.replica_count);
    append_u64(&mut material, status.replica_fallback_reads);
    append_u64(&mut material, status.local_replica_read_repairs);
    append_u64(&mut material, status.local_replica_read_repair_failures);
    append_u64(&mut material, status.storage_replica_fallback_reads);
    append_u64(&mut material, status.storage_local_replica_read_repairs);
    append_u64(
        &mut material,
        status.storage_local_replica_read_repair_failures,
    );
    append_u64(&mut material, status.storage_readable_replicas);
    append_u64(&mut material, status.storage_writable_replicas);
    append_u64(&mut material, status.storage_active_replicas);
    append_u64(&mut material, status.storage_quarantined_replicas);
    append_u64(&mut material, status.storage_frozen_replicas);
    append_u64(&mut material, status.storage_draining_replicas);
    append_u64(&mut material, status.storage_total_capacity_bytes);
    append_u64(&mut material, status.storage_total_capacity_free_bytes);
    append_u64(&mut material, status.read_cache_bytes);
    append_u64(&mut material, status.read_cache_entries);
    append_u64(&mut material, status.read_cache_max_bytes);
    append_u64(&mut material, status.read_cache_max_cacheable_read_bytes);
    append_u64(&mut material, status.read_cache_hits);
    append_u64(&mut material, status.read_cache_misses);
    append_u64(&mut material, status.read_cache_evictions);
    append_u64(&mut material, status.read_cache_pressure as u64);
    append_u64(&mut material, status.read_cache_corrupt_pages);
    append_u64(&mut material, status.read_cache_corrupt_bytes);
    append_u64(&mut material, status.read_cache_invalidations);
    append_u64(&mut material, status.read_cache_invalidated_entries);
    append_u64(&mut material, status.read_cache_invalidated_bytes);
    append_u64(&mut material, status.read_cache_clears);
    append_u64(&mut material, status.read_cache_cleared_entries);
    append_u64(&mut material, status.read_cache_cleared_bytes);
    append_u64(&mut material, status.read_cache_direct_fill_pages);
    append_u64(&mut material, status.read_cache_direct_fill_bytes);
    append_u64(&mut material, status.read_cache_compressed_fill_pages);
    append_u64(&mut material, status.read_cache_compressed_fill_bytes);
    append_u64(&mut material, status.read_cache_ec_fill_pages);
    append_u64(&mut material, status.read_cache_ec_fill_bytes);
    append_u64(&mut material, status.storage_capacity_rejected_writes);
    append_u64(
        &mut material,
        status.storage_service_mode_rejected_mutations,
    );
    append_u64(&mut material, status.storage_physical_read_count);
    append_u64(&mut material, status.storage_coalesced_read_count);
    append_u64(&mut material, status.storage_physical_write_count);
    append_u64(&mut material, status.storage_physical_write_bytes);
    append_u64(&mut material, status.storage_physical_discard_ranges);
    append_u64(&mut material, status.storage_physical_discard_bytes);
    append_u64(&mut material, status.storage_allocation_receipt_count);
    append_u64(&mut material, status.storage_batch_allocation_receipt_count);
    append_u64(
        &mut material,
        status.storage_release_allocation_receipt_count,
    );
    append_u64(&mut material, status.storage_write_receipt_count);
    append_u64(&mut material, status.storage_discard_receipt_count);
    append_u64(&mut material, status.storage_batch_write_receipt_count);
    append_u64(&mut material, status.storage_batch_discard_receipt_count);
    append_u64(&mut material, status.batch_write_input_ranges);
    append_u64(&mut material, status.batch_write_chunk_ranges);
    append_u64(&mut material, status.batch_write_failures);
    append_string(&mut material, &status.last_batch_write_failure_status);
    append_u64(&mut material, status.chunk_batch_write_requests);
    append_u64(&mut material, status.chunk_batch_write_ranges);
    append_u64(&mut material, status.chunk_batch_write_fallbacks);
    append_u64(&mut material, status.fresh_chunk_batch_write_requests);
    append_u64(&mut material, status.fresh_chunk_batch_write_ranges);
    append_u64(&mut material, status.fresh_chunk_batch_write_fallbacks);
    append_u64(&mut material, status.batch_read_input_ranges);
    append_u64(&mut material, status.batch_read_chunk_ranges);
    append_u64(&mut material, status.batch_read_failures);
    append_string(&mut material, &status.last_batch_read_failure_status);
    append_u64(&mut material, status.primary_batch_read_requests);
    append_u64(&mut material, status.primary_batch_read_chunk_ranges);
    append_u64(&mut material, status.primary_batch_read_fallbacks);
    append_u64(&mut material, status.primary_batch_read_repair_probe_limit);
    append_u64(&mut material, status.batch_discard_input_ranges);
    append_u64(&mut material, status.batch_discard_merged_ranges);
    append_u64(&mut material, status.batch_discard_chunk_ranges);
    append_u64(&mut material, status.batch_discard_failures);
    append_string(&mut material, &status.last_batch_discard_failure_status);
    append_u64(&mut material, status.chunk_batch_discard_requests);
    append_u64(&mut material, status.chunk_batch_discard_ranges);
    append_u64(&mut material, status.chunk_batch_discard_fallbacks);
    append_u64(&mut material, status.discard_admission_requests);
    append_u64(&mut material, status.discard_admission_ranges);
    append_u64(&mut material, status.chunk_reclaim_requests);
    append_u64(&mut material, status.chunk_reclaim_rewritten_chunks);
    append_u64(&mut material, status.chunk_reclaim_failures);
    append_string(&mut material, &status.last_chunk_reclaim_failure_status);
    append_u64(
        &mut material,
        status.storage_cumulative_repair_checked_chunks,
    );
    append_u64(
        &mut material,
        status.storage_cumulative_repair_degraded_chunks,
    );
    append_u64(&mut material, status.storage_cumulative_repaired_replicas);
    append_u64(&mut material, status.storage_cumulative_failed_repairs);
    append_u64(&mut material, status.cumulative_repair_checked_chunks);
    append_u64(&mut material, status.cumulative_repair_degraded_chunks);
    append_u64(&mut material, status.cumulative_repaired_replicas);
    append_u64(&mut material, status.cumulative_failed_repairs);
    append_u64(&mut material, status.storage_partial_reclaim_chunks);
    append_u64(&mut material, status.storage_partial_reclaim_bytes);
    append_u64(&mut material, status.storage_partial_reclaim_failures);
    append_u64(&mut material, status.partial_reclaim_chunks);
    append_u64(&mut material, status.partial_reclaim_bytes);
    append_u64(&mut material, status.partial_reclaim_failures);
    append_string(
        &mut material,
        &status.storage_last_partial_reclaim_failure_status,
    );
    append_u64(&mut material, status.storage_total_reclaimable_bytes);
    append_u64(&mut material, status.storage_total_fragmented_chunk_count);
    append_u64(
        &mut material,
        status.storage_total_fragmented_discarded_bytes,
    );
    append_u64(&mut material, status.storage_total_trailing_trim_bytes);
    append_u64(&mut material, status.storage_reclaim_pressure as u64);
    append_u64(&mut material, status.storage_trim_flow_limited_replicas);
    append_u64(&mut material, status.storage_trim_flow_available_ops);
    append_u64(&mut material, status.storage_trim_flow_throttled_ops);
    append_u64(&mut material, status.storage_page_read_parallelism);
    append_u64(&mut material, status.storage_page_write_parallelism);
    append_u64(&mut material, status.storage_page_batch_read_parallelism);
    append_u64(&mut material, status.storage_page_batch_write_parallelism);
    append_u64(
        &mut material,
        status.storage_page_batch_discard_parallelism,
    );
    append_u64(&mut material, status.storage_page_repair_parallelism);
    append_u64(&mut material, status.storage_page_chunk_fanout_parallelism);
    append_u64(
        &mut material,
        status.storage_effective_chunk_fanout_parallelism,
    );
    append_u64(
        &mut material,
        status.storage_local_replica_store_parallelism,
    );
    append_u64(
        &mut material,
        status.storage_effective_local_replica_store_parallelism,
    );
}
