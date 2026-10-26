// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
pub(crate) fn append_metadata_bootstrap_readiness(
    out: &mut Vec<u8>,
    readiness: &MetadataRaftReadiness,
) {
    append_u64(out, readiness.raft_term);
    append_u64(out, readiness.raft_log_commit_index);
    append_u64(out, readiness.applied_commit_index);
    append_u64(out, readiness.last_applied_metadata_mutation_commit_index);
    append_u64(out, readiness.min_read_index);
    append_u64(out, readiness.safe_read_fence);
    append_u64(out, readiness.read_fence_covered as u64);
    append_u64(out, readiness.read_lag);
    append_u64(out, readiness.raft_applied_lag);
    append_u64(out, readiness.metadata_apply_commit_lag);
    append_u64(out, readiness.readable as u64);
    append_u64(out, readiness.writable as u64);
    append_u64(out, readiness.caught_up as u64);
    append_u64(out, readiness.ready as u64);
    append_string(out, &readiness.reason);
    append_string(out, &readiness.metadata_state_fingerprint);
    append_string(out, &readiness.metadata_control_proof_format);
    append_string(out, &readiness.metadata_control_proof_checksum);
    append_u64(out, readiness.metadata_control_proof_bytes);
    append_string(out, &readiness.coordinator_config_checksum);
    append_string(out, &readiness.coordinator_backend);
    append_u64(out, readiness.coordinator_config_revision);
    append_u64(out, readiness.external_coordinator_required as u64);
    append_u64(out, readiness.external_coordinator_ready as u64);
}

pub(crate) fn append_chunk_storage_bootstrap_readiness(
    out: &mut Vec<u8>,
    readiness: &ReplicaStorageReadiness,
) {
    append_u64(out, readiness.readable as u64);
    append_u64(out, readiness.writable as u64);
    append_u64(out, readiness.reclaimable as u64);
    append_u64(out, readiness.ready as u64);
    append_string(out, &readiness.reason);
    append_string(out, &readiness.chunk_state_fingerprint);
    append_u64(out, readiness.accepting_writes as u64);
    append_u64(out, readiness.draining as u64);
    append_u64(out, readiness.frozen as u64);
    append_u64(out, readiness.page_count);
    append_u64(out, readiness.physical_bytes);
    append_u64(out, readiness.capacity_bytes);
    append_u64(out, readiness.capacity_free_bytes);
    append_u64(out, readiness.min_capacity_free_bytes);
    append_u64(out, readiness.capacity_pressure as u64);
    append_u64(out, readiness.discarded_range_count);
    append_u64(out, readiness.reclaimable_bytes);
    append_u64(out, readiness.trailing_trim_bytes);
    append_u64(out, readiness.fragmented_chunk_count);
    append_u64(out, readiness.fragmented_discarded_bytes);
    append_u64(out, readiness.min_reclaimable_bytes);
    append_u64(out, readiness.trim_flow_enabled as u64);
    append_u64(out, readiness.trim_flow_available_bytes);
    append_u64(out, readiness.trim_flow_available_ops);
    append_u64(out, readiness.trim_flow_throttled_ops);
    append_u64(out, readiness.trim_flow_limited as u64);
    append_u64(out, readiness.mutation_qos_window_ms);
    append_u64(out, readiness.mutation_qos_write_ops_in_window);
    append_u64(out, readiness.mutation_qos_write_bytes_in_window);
    append_u64(out, readiness.mutation_qos_discard_ops_in_window);
    append_u64(out, readiness.mutation_qos_discard_bytes_in_window);
    append_u64(out, readiness.mutation_qos_throttled_ops);
    append_u64(out, readiness.mutation_qos_throttled_bytes);
    append_u64(out, readiness.mutation_qos_pressure as u64);
    append_u64(out, readiness.read_cache_entries);
    append_u64(out, readiness.read_cache_bytes);
    append_u64(out, readiness.read_cache_max_bytes);
    append_u64(out, readiness.read_cache_max_cacheable_read_bytes);
    append_u64(out, readiness.read_cache_hits);
    append_u64(out, readiness.read_cache_misses);
    append_u64(out, readiness.read_cache_evictions);
    append_u64(out, readiness.read_cache_corrupt_pages);
    append_u64(out, readiness.read_cache_corrupt_bytes);
    append_u64(out, readiness.read_cache_invalidations);
    append_u64(out, readiness.read_cache_invalidated_entries);
    append_u64(out, readiness.read_cache_invalidated_bytes);
    append_u64(out, readiness.read_cache_clears);
    append_u64(out, readiness.read_cache_cleared_entries);
    append_u64(out, readiness.read_cache_cleared_bytes);
    append_u64(out, readiness.read_cache_direct_fill_pages);
    append_u64(out, readiness.read_cache_direct_fill_bytes);
    append_u64(out, readiness.read_cache_compressed_fill_pages);
    append_u64(out, readiness.read_cache_compressed_fill_bytes);
    append_u64(out, readiness.read_cache_ec_fill_pages);
    append_u64(out, readiness.read_cache_ec_fill_bytes);
    append_u64(out, readiness.capacity_rejected_writes);
    append_u64(out, readiness.service_mode_rejected_mutations);
    append_u64(out, readiness.metadata_dirty as u64);
    append_u64(out, readiness.metadata_dirty_mutations);
    append_u64(out, readiness.metadata_checkpoint_interval);
    append_u64(out, readiness.defer_metadata_checkpoints as u64);
    append_u64(out, readiness.deferred_metadata_checkpoint_pending as u64);
    append_u64(out, readiness.deferred_metadata_checkpoint_requests);
    append_u64(out, readiness.deferred_metadata_checkpoint_runs);
    append_u64(out, readiness.physical_write_count);
    append_u64(out, readiness.physical_write_bytes);
    append_u64(out, readiness.physical_discard_ranges);
    append_u64(out, readiness.physical_discard_bytes);
    append_u64(out, readiness.compressed_chunk_count);
    append_u64(out, readiness.compression_saved_bytes);
    append_u64(out, readiness.ec_reconstructed_reads);
    append_u64(out, readiness.ec_reconstruction_failures);
    append_u64(out, readiness.receipt_retention);
    append_u64(out, readiness.write_receipt_count);
    append_u64(out, readiness.discard_receipt_count);
    append_u64(out, readiness.batch_write_receipt_count);
    append_u64(out, readiness.batch_discard_receipt_count);
    append_string(out, &readiness.chunkserver_control_proof_format);
    append_string(out, &readiness.chunkserver_control_proof_checksum);
    append_u64(out, readiness.chunkserver_control_proof_bytes);
    append_replica_service_control_proof(out, &readiness.control_proof);
}

