// SPDX-License-Identifier: Apache-2.0
fn append_storage_readiness(mut material: &mut Vec<u8>, report: &ObjectStoreDistributedReadiness) {
    append_u64(&mut material, report.bucket_count);
    append_u64(&mut material, report.object_count);
    append_u64(&mut material, report.live_object_count);
    append_u64(&mut material, report.deleted_object_count);
    append_u64(&mut material, report.chunk_count);
    append_u64(&mut material, report.page_count);
    append_u64(&mut material, report.live_bytes);
    append_u64(&mut material, report.stale_bytes);
    append_u64(&mut material, report.pending_discard_bytes);
    append_u64(&mut material, report.discarded_bytes);
    append_u64(&mut material, report.physical_bytes);
    append_u64(&mut material, report.live_bytes_ppm);
    append_u64(&mut material, report.stale_bytes_ppm);
    append_u64(&mut material, report.pending_discard_bytes_ppm);
    append_u64(&mut material, report.discarded_bytes_ppm);
    append_u64(&mut material, report.logical_write_amplification_ppm);
    append_u64(&mut material, report.storage_write_amplification_ppm);
    append_u64(&mut material, report.space_amplification_ppm);
    append_u64(&mut material, report.discard_backlog_ppm);
    append_u64(&mut material, report.discard_candidate_bytes);
    append_u64(&mut material, report.discard_candidate_chunks);
    append_u64(&mut material, report.pending_discard_range_count);
    append_u64(&mut material, report.merged_discard_range_count);
    append_u64(&mut material, report.fragmented_range_count);
    append_u64(&mut material, report.compaction_candidate_chunks);
    append_u64(&mut material, report.compaction_candidate_bytes);
    append_u64(&mut material, report.compaction_candidate_range_count);
    append_u64(&mut material, report.compaction_recommended as u64);
    append_u64(&mut material, report.read_cache_bytes);
    append_u64(&mut material, report.logical_written_bytes);
    append_u64(&mut material, report.physical_written_bytes);
    append_u64(&mut material, report.chunk_read_cache_max_bytes_per_replica);
    append_u64(
        &mut material,
        report.chunk_read_cache_max_cacheable_read_bytes_per_replica,
    );
    append_u64(&mut material, report.storage_volume_count);
    append_u64(&mut material, report.storage_replica_count);
    append_u64(&mut material, report.storage_replica_fallback_reads);
    append_u64(&mut material, report.storage_local_replica_read_repairs);
    append_u64(
        &mut material,
        report.storage_local_replica_read_repair_failures,
    );
    append_u64(&mut material, report.storage_read_cache_bytes);
    append_u64(&mut material, report.storage_read_cache_entries);
    append_u64(&mut material, report.storage_read_cache_max_bytes);
    append_u64(
        &mut material,
        report.storage_read_cache_max_cacheable_read_bytes,
    );
    append_u64(&mut material, report.storage_read_cache_hits);
    append_u64(&mut material, report.storage_read_cache_misses);
    append_u64(&mut material, report.storage_read_cache_evictions);
    append_u64(&mut material, report.storage_read_cache_pressure as u64);
    append_u64(&mut material, report.storage_read_cache_corrupt_pages);
    append_u64(&mut material, report.storage_read_cache_corrupt_bytes);
    append_u64(&mut material, report.storage_read_cache_invalidations);
    append_u64(&mut material, report.storage_read_cache_invalidated_entries);
    append_u64(&mut material, report.storage_read_cache_invalidated_bytes);
    append_u64(&mut material, report.storage_read_cache_clears);
    append_u64(&mut material, report.storage_read_cache_cleared_entries);
    append_u64(&mut material, report.storage_read_cache_cleared_bytes);
    append_u64(&mut material, report.storage_read_cache_direct_fill_pages);
    append_u64(&mut material, report.storage_read_cache_direct_fill_bytes);
    append_u64(
        &mut material,
        report.storage_read_cache_compressed_fill_pages,
    );
    append_u64(
        &mut material,
        report.storage_read_cache_compressed_fill_bytes,
    );
    append_u64(&mut material, report.storage_read_cache_ec_fill_pages);
    append_u64(&mut material, report.storage_read_cache_ec_fill_bytes);
    append_u64(
        &mut material,
        report.storage_read_cache_uncacheable_read_pages,
    );
    append_u64(
        &mut material,
        report.storage_read_cache_uncacheable_read_bytes,
    );
    append_u64(&mut material, report.storage_capacity_rejected_writes);
    append_u64(
        &mut material,
        report.storage_service_mode_rejected_mutations,
    );
    append_u64(&mut material, report.storage_physical_read_count);
    append_u64(&mut material, report.storage_coalesced_read_count);
    append_u64(&mut material, report.storage_physical_write_count);
    append_u64(&mut material, report.storage_physical_write_bytes);
    append_u64(&mut material, report.storage_physical_discard_ranges);
    append_u64(&mut material, report.storage_physical_discard_bytes);
    append_u64(&mut material, report.storage_allocation_receipt_count);
    append_u64(&mut material, report.storage_batch_allocation_receipt_count);
    append_u64(
        &mut material,
        report.storage_release_allocation_receipt_count,
    );
    append_u64(&mut material, report.storage_write_receipt_count);
    append_u64(&mut material, report.storage_discard_receipt_count);
    append_u64(&mut material, report.storage_batch_write_receipt_count);
    append_u64(&mut material, report.storage_batch_discard_receipt_count);
    append_u64(&mut material, report.batch_write_input_ranges);
    append_u64(&mut material, report.batch_write_chunk_ranges);
    append_u64(&mut material, report.batch_write_failures);
    append_string(&mut material, &report.last_batch_write_failure_status);
    append_u64(&mut material, report.chunk_batch_write_requests);
    append_u64(&mut material, report.chunk_batch_write_ranges);
    append_u64(&mut material, report.chunk_batch_write_fallbacks);
    append_u64(&mut material, report.fresh_chunk_batch_write_requests);
    append_u64(&mut material, report.fresh_chunk_batch_write_ranges);
    append_u64(&mut material, report.fresh_chunk_batch_write_fallbacks);
    append_u64(&mut material, report.batch_read_input_ranges);
    append_u64(&mut material, report.batch_read_chunk_ranges);
    append_u64(&mut material, report.batch_read_failures);
    append_string(&mut material, &report.last_batch_read_failure_status);
    append_u64(&mut material, report.primary_batch_read_requests);
    append_u64(&mut material, report.primary_batch_read_chunk_ranges);
    append_u64(&mut material, report.primary_batch_read_fallbacks);
    append_u64(&mut material, report.primary_batch_read_repair_probe_limit);
    append_u64(&mut material, report.batch_discard_input_ranges);
    append_u64(&mut material, report.batch_discard_merged_ranges);
    append_u64(&mut material, report.batch_discard_chunk_ranges);
    append_u64(&mut material, report.batch_discard_failures);
    append_string(&mut material, &report.last_batch_discard_failure_status);
    append_u64(&mut material, report.chunk_batch_discard_requests);
    append_u64(&mut material, report.chunk_batch_discard_ranges);
    append_u64(&mut material, report.chunk_batch_discard_fallbacks);
    append_u64(&mut material, report.discard_admission_requests);
    append_u64(&mut material, report.discard_admission_ranges);
    append_u64(&mut material, report.chunk_reclaim_requests);
    append_u64(&mut material, report.chunk_reclaim_rewritten_chunks);
    append_u64(&mut material, report.chunk_reclaim_failures);
    append_string(&mut material, &report.last_chunk_reclaim_failure_status);
    append_u64(
        &mut material,
        report.storage_cumulative_repair_checked_chunks,
    );
    append_u64(
        &mut material,
        report.storage_cumulative_repair_degraded_chunks,
    );
    append_u64(&mut material, report.storage_cumulative_repaired_replicas);
    append_u64(&mut material, report.storage_cumulative_failed_repairs);
    append_u64(&mut material, report.storage_compressed_chunk_count);
    append_u64(&mut material, report.storage_compression_saved_bytes);
    append_u64(&mut material, report.storage_ec_reconstructed_reads);
    append_u64(&mut material, report.storage_ec_reconstruction_failures);
    append_u64(&mut material, report.storage_partial_reclaim_chunks);
    append_u64(&mut material, report.storage_partial_reclaim_bytes);
    append_u64(&mut material, report.storage_partial_reclaim_failures);
    append_string(
        &mut material,
        &report.storage_last_partial_reclaim_failure_status,
    );
    append_u64(&mut material, report.storage_readable_replicas);
    append_u64(&mut material, report.storage_writable_replicas);
    append_u64(&mut material, report.storage_active_replicas);
    append_u64(&mut material, report.storage_quarantined_replicas);
    append_u64(&mut material, report.storage_frozen_replicas);
    append_u64(&mut material, report.storage_draining_replicas);
    append_u64(&mut material, report.storage_capacity_pressure_replicas);
    append_u64(&mut material, report.storage_trim_flow_limited_replicas);
    append_u64(&mut material, report.storage_trim_flow_available_ops);
    append_u64(&mut material, report.storage_trim_flow_throttled_ops);
    append_u64(&mut material, report.storage_total_capacity_bytes);
    append_u64(&mut material, report.storage_total_capacity_free_bytes);
    append_u64(&mut material, report.storage_total_reclaimable_bytes);
    append_u64(&mut material, report.storage_total_fragmented_chunk_count);
    append_u64(
        &mut material,
        report.storage_total_fragmented_discarded_bytes,
    );
    append_u64(&mut material, report.storage_total_trailing_trim_bytes);
    append_u64(&mut material, report.storage_page_read_parallelism);
    append_u64(&mut material, report.storage_page_write_parallelism);
    append_u64(&mut material, report.storage_page_batch_read_parallelism);
    append_u64(&mut material, report.storage_page_batch_write_parallelism);
    append_u64(
        &mut material,
        report.storage_page_batch_discard_parallelism,
    );
    append_u64(&mut material, report.storage_page_repair_parallelism);
    append_u64(&mut material, report.storage_page_chunk_fanout_parallelism);
    append_u64(
        &mut material,
        report.storage_effective_chunk_fanout_parallelism,
    );
    append_u64(
        &mut material,
        report.storage_local_replica_store_parallelism,
    );
    append_u64(
        &mut material,
        report.storage_effective_local_replica_store_parallelism,
    );
    append_u64(&mut material, report.storage_reclaim_pressure as u64);
}
