// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
fn populate_storage_module_status(
    status: &mut ObjectStoreModuleStatus,
    readiness: &ObjectStoreDistributedReadiness,
) {
    status.placement_degraded_objects = readiness.placement_degraded_objects;
    status.placement_bytes_to_repair = readiness.placement_bytes_to_repair;
    status.placement_truncated = readiness.placement_truncated;
    status.capacity_free_bytes = readiness.storage_total_capacity_free_bytes;
    status.capacity_pressure_replicas = readiness.storage_capacity_pressure_replicas;
    status.volume_count = readiness.storage_volume_count;
    status.replica_count = readiness.storage_replica_count;
    status.replica_fallback_reads = readiness.storage_replica_fallback_reads;
    status.local_replica_read_repairs = readiness.storage_local_replica_read_repairs;
    status.local_replica_read_repair_failures =
        readiness.storage_local_replica_read_repair_failures;
    status.storage_replica_fallback_reads = readiness.storage_replica_fallback_reads;
    status.storage_local_replica_read_repairs = readiness.storage_local_replica_read_repairs;
    status.storage_local_replica_read_repair_failures =
        readiness.storage_local_replica_read_repair_failures;
    status.storage_readable_replicas = readiness.storage_readable_replicas;
    status.storage_writable_replicas = readiness.storage_writable_replicas;
    status.storage_active_replicas = readiness.storage_active_replicas;
    status.storage_quarantined_replicas = readiness.storage_quarantined_replicas;
    status.storage_frozen_replicas = readiness.storage_frozen_replicas;
    status.storage_draining_replicas = readiness.storage_draining_replicas;
    status.storage_total_capacity_bytes = readiness.storage_total_capacity_bytes;
    status.storage_total_capacity_free_bytes = readiness.storage_total_capacity_free_bytes;
    status.read_cache_bytes = readiness.storage_read_cache_bytes;
    status.read_cache_entries = readiness.storage_read_cache_entries;
    status.read_cache_max_bytes = readiness.storage_read_cache_max_bytes;
    status.read_cache_max_cacheable_read_bytes =
        readiness.storage_read_cache_max_cacheable_read_bytes;
    status.read_cache_hits = readiness.storage_read_cache_hits;
    status.read_cache_misses = readiness.storage_read_cache_misses;
    status.read_cache_evictions = readiness.storage_read_cache_evictions;
    status.read_cache_pressure = readiness.storage_read_cache_pressure;
    status.read_cache_corrupt_pages = readiness.storage_read_cache_corrupt_pages;
    status.read_cache_corrupt_bytes = readiness.storage_read_cache_corrupt_bytes;
    status.read_cache_invalidations = readiness.storage_read_cache_invalidations;
    status.read_cache_invalidated_entries = readiness.storage_read_cache_invalidated_entries;
    status.read_cache_invalidated_bytes = readiness.storage_read_cache_invalidated_bytes;
    status.read_cache_clears = readiness.storage_read_cache_clears;
    status.read_cache_cleared_entries = readiness.storage_read_cache_cleared_entries;
    status.read_cache_cleared_bytes = readiness.storage_read_cache_cleared_bytes;
    status.read_cache_direct_fill_pages = readiness.storage_read_cache_direct_fill_pages;
    status.read_cache_direct_fill_bytes = readiness.storage_read_cache_direct_fill_bytes;
    status.read_cache_compressed_fill_pages = readiness.storage_read_cache_compressed_fill_pages;
    status.read_cache_compressed_fill_bytes = readiness.storage_read_cache_compressed_fill_bytes;
    status.read_cache_ec_fill_pages = readiness.storage_read_cache_ec_fill_pages;
    status.read_cache_ec_fill_bytes = readiness.storage_read_cache_ec_fill_bytes;
    status.read_cache_uncacheable_read_pages =
        readiness.storage_read_cache_uncacheable_read_pages;
    status.read_cache_uncacheable_read_bytes =
        readiness.storage_read_cache_uncacheable_read_bytes;
    status.storage_capacity_rejected_writes = readiness.storage_capacity_rejected_writes;
    status.storage_service_mode_rejected_mutations =
        readiness.storage_service_mode_rejected_mutations;
    status.storage_physical_read_count = readiness.storage_physical_read_count;
    status.storage_coalesced_read_count = readiness.storage_coalesced_read_count;
    status.storage_physical_write_count = readiness.storage_physical_write_count;
    status.storage_physical_write_bytes = readiness.storage_physical_write_bytes;
    status.storage_physical_discard_ranges = readiness.storage_physical_discard_ranges;
    status.storage_physical_discard_bytes = readiness.storage_physical_discard_bytes;
    status.storage_allocation_receipt_count = readiness.storage_allocation_receipt_count;
    status.storage_batch_allocation_receipt_count =
        readiness.storage_batch_allocation_receipt_count;
    status.storage_release_allocation_receipt_count =
        readiness.storage_release_allocation_receipt_count;
    status.storage_write_receipt_count = readiness.storage_write_receipt_count;
    status.storage_discard_receipt_count = readiness.storage_discard_receipt_count;
    status.storage_batch_write_receipt_count = readiness.storage_batch_write_receipt_count;
    status.storage_batch_discard_receipt_count = readiness.storage_batch_discard_receipt_count;
    status.batch_write_input_ranges = readiness.batch_write_input_ranges;
    status.batch_write_chunk_ranges = readiness.batch_write_chunk_ranges;
    status.batch_write_failures = readiness.batch_write_failures;
    status.last_batch_write_failure_status = readiness.last_batch_write_failure_status.clone();
    status.chunk_batch_write_requests = readiness.chunk_batch_write_requests;
    status.chunk_batch_write_ranges = readiness.chunk_batch_write_ranges;
    status.chunk_batch_write_fallbacks = readiness.chunk_batch_write_fallbacks;
    status.fresh_chunk_batch_write_requests = readiness.fresh_chunk_batch_write_requests;
    status.fresh_chunk_batch_write_ranges = readiness.fresh_chunk_batch_write_ranges;
    status.fresh_chunk_batch_write_fallbacks = readiness.fresh_chunk_batch_write_fallbacks;
    status.batch_read_input_ranges = readiness.batch_read_input_ranges;
    status.batch_read_chunk_ranges = readiness.batch_read_chunk_ranges;
    status.batch_read_failures = readiness.batch_read_failures;
    status.last_batch_read_failure_status = readiness.last_batch_read_failure_status.clone();
    status.primary_batch_read_requests = readiness.primary_batch_read_requests;
    status.primary_batch_read_chunk_ranges = readiness.primary_batch_read_chunk_ranges;
    status.primary_batch_read_fallbacks = readiness.primary_batch_read_fallbacks;
    status.primary_batch_read_repair_probe_limit = readiness.primary_batch_read_repair_probe_limit;
    status.batch_discard_input_ranges = readiness.batch_discard_input_ranges;
    status.batch_discard_merged_ranges = readiness.batch_discard_merged_ranges;
    status.batch_discard_chunk_ranges = readiness.batch_discard_chunk_ranges;
    status.batch_discard_failures = readiness.batch_discard_failures;
    status.last_batch_discard_failure_status =
        readiness.last_batch_discard_failure_status.clone();
    status.chunk_batch_discard_requests = readiness.chunk_batch_discard_requests;
    status.chunk_batch_discard_ranges = readiness.chunk_batch_discard_ranges;
    status.chunk_batch_discard_fallbacks = readiness.chunk_batch_discard_fallbacks;
    status.discard_admission_requests = readiness.discard_admission_requests;
    status.discard_admission_ranges = readiness.discard_admission_ranges;
    status.chunk_reclaim_requests = readiness.chunk_reclaim_requests;
    status.chunk_reclaim_rewritten_chunks = readiness.chunk_reclaim_rewritten_chunks;
    status.chunk_reclaim_failures = readiness.chunk_reclaim_failures;
    status.last_chunk_reclaim_failure_status = readiness.last_chunk_reclaim_failure_status.clone();
    status.storage_cumulative_repair_checked_chunks =
        readiness.storage_cumulative_repair_checked_chunks;
    status.storage_cumulative_repair_degraded_chunks =
        readiness.storage_cumulative_repair_degraded_chunks;
    status.storage_cumulative_repaired_replicas = readiness.storage_cumulative_repaired_replicas;
    status.storage_cumulative_failed_repairs = readiness.storage_cumulative_failed_repairs;
    status.cumulative_repair_checked_chunks = readiness.storage_cumulative_repair_checked_chunks;
    status.cumulative_repair_degraded_chunks = readiness.storage_cumulative_repair_degraded_chunks;
    status.cumulative_repaired_replicas = readiness.storage_cumulative_repaired_replicas;
    status.cumulative_failed_repairs = readiness.storage_cumulative_failed_repairs;
    status.storage_partial_reclaim_chunks = readiness.storage_partial_reclaim_chunks;
    status.storage_partial_reclaim_bytes = readiness.storage_partial_reclaim_bytes;
    status.storage_partial_reclaim_failures = readiness.storage_partial_reclaim_failures;
    status.partial_reclaim_chunks = readiness.storage_partial_reclaim_chunks;
    status.partial_reclaim_bytes = readiness.storage_partial_reclaim_bytes;
    status.partial_reclaim_failures = readiness.storage_partial_reclaim_failures;
    status.storage_last_partial_reclaim_failure_status =
        readiness.storage_last_partial_reclaim_failure_status.clone();
    status.storage_total_reclaimable_bytes = readiness.storage_total_reclaimable_bytes;
    status.storage_total_fragmented_chunk_count = readiness.storage_total_fragmented_chunk_count;
    status.storage_total_fragmented_discarded_bytes =
        readiness.storage_total_fragmented_discarded_bytes;
    status.storage_total_trailing_trim_bytes = readiness.storage_total_trailing_trim_bytes;
    status.storage_reclaim_pressure = readiness.storage_reclaim_pressure;
    status.storage_trim_flow_limited_replicas = readiness.storage_trim_flow_limited_replicas;
    status.storage_trim_flow_available_ops = readiness.storage_trim_flow_available_ops;
    status.storage_trim_flow_throttled_ops = readiness.storage_trim_flow_throttled_ops;
    status.storage_page_read_parallelism = readiness.storage_page_read_parallelism;
    status.storage_page_write_parallelism = readiness.storage_page_write_parallelism;
    status.storage_page_batch_read_parallelism = readiness.storage_page_batch_read_parallelism;
    status.storage_page_batch_write_parallelism = readiness.storage_page_batch_write_parallelism;
    status.storage_page_batch_discard_parallelism =
        readiness.storage_page_batch_discard_parallelism;
    status.storage_page_repair_parallelism = readiness.storage_page_repair_parallelism;
    status.storage_page_chunk_fanout_parallelism =
        readiness.storage_page_chunk_fanout_parallelism;
    status.storage_effective_chunk_fanout_parallelism =
        readiness.storage_effective_chunk_fanout_parallelism;
    status.storage_local_replica_store_parallelism =
        readiness.storage_local_replica_store_parallelism;
    status.storage_effective_local_replica_store_parallelism =
        readiness.storage_effective_local_replica_store_parallelism;
    status.repair_degraded_objects = readiness.placement_degraded_objects;
    status.repair_bytes_to_repair = readiness.placement_bytes_to_repair;
    status.repair_degraded_chunks = readiness.storage_cumulative_repair_degraded_chunks;
    status.repair_failed_repairs = readiness.storage_cumulative_failed_repairs;
    status.compressed_chunk_count = readiness.storage_compressed_chunk_count;
    status.compression_saved_bytes = readiness.storage_compression_saved_bytes;
    status.ec_reconstructed_reads = readiness.storage_ec_reconstructed_reads;
    status.ec_reconstruction_failures = readiness.storage_ec_reconstruction_failures;
}
