// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
pub(crate) fn root_raft_status_checksum(status: &PlacementRaftStatus) -> String {
    let mut material = Vec::new();
    append_string(&mut material, &status.control_checksum_format);
    append_string(&mut material, &status.leader_node_id);
    append_u64(&mut material, status.term);
    append_u64(&mut material, status.next_log_index);
    append_u64(&mut material, status.last_applied_index);
    append_u64(&mut material, status.commit_index);
    append_u64(&mut material, status.read_index);
    append_u64(&mut material, status.writable as u64);
    append_u64(&mut material, status.linearizable_reads as u64);
    append_u64(&mut material, status.root_metadata_read_ready as u64);
    append_u64(&mut material, status.root_metadata_write_ready as u64);
    append_u64(&mut material, status.root_pageserver_read_ready as u64);
    append_u64(&mut material, status.root_pageserver_write_ready as u64);
    append_u64(&mut material, status.root_replicaserver_read_ready as u64);
    append_u64(&mut material, status.root_replicaserver_write_ready as u64);
    append_u64(&mut material, status.root_readiness_gate_read_ready as u64);
    append_u64(&mut material, status.root_readiness_gate_write_ready as u64);
    append_u64(&mut material, status.node_count);
    append_u64(&mut material, status.object_usage_count);
    append_u64(&mut material, status.root_metadata_status_available as u64);
    append_u64(&mut material, status.root_active_metaserver_count);
    append_u64(
        &mut material,
        status.root_metadata_readable_metaserver_count,
    );
    append_u64(
        &mut material,
        status.root_metadata_writable_metaserver_count,
    );
    append_u64(
        &mut material,
        status.root_metadata_caught_up_metaserver_count,
    );
    append_u64(&mut material, status.root_metadata_quorum);
    append_u64(
        &mut material,
        status.root_metadata_read_quorum_available as u64,
    );
    append_u64(
        &mut material,
        status.root_metadata_write_quorum_available as u64,
    );
    append_u64(
        &mut material,
        status.root_metadata_caught_up_quorum_available as u64,
    );
    append_u64(
        &mut material,
        status.root_metadata_apply_caught_up_metaserver_count,
    );
    append_u64(
        &mut material,
        status.root_metadata_control_apply_caught_up_metaserver_count,
    );
    append_u64(
        &mut material,
        status.root_metadata_read_fence_covered_metaserver_count,
    );
    append_u64(
        &mut material,
        status.root_metadata_apply_quorum_available as u64,
    );
    append_u64(
        &mut material,
        status.root_metadata_read_fence_quorum_available as u64,
    );
    append_u64(
        &mut material,
        status.root_metadata_control_apply_quorum_available as u64,
    );
    append_u64(
        &mut material,
        status.root_metadata_min_last_applied_mutation_commit_index,
    );
    append_u64(&mut material, status.root_metadata_max_apply_commit_lag);
    append_u64(&mut material, status.root_metadata_min_safe_read_fence);
    append_u64(
        &mut material,
        status.root_metadata_max_read_fence_commit_lag,
    );
    append_u64(
        &mut material,
        status.root_metadata_max_read_fence_read_index_lag,
    );
    append_u64(
        &mut material,
        status.root_metadata_max_read_fence_applied_lag,
    );
    append_u64(
        &mut material,
        status.root_metadata_control_min_last_applied_mutation_commit_index,
    );
    append_u64(
        &mut material,
        status.root_metadata_control_max_last_applied_mutation_commit_index,
    );
    append_u64(
        &mut material,
        status.root_metadata_control_max_apply_commit_lag,
    );
    append_u64(
        &mut material,
        status.root_metadata_shard_topology_mismatch as u64,
    );
    append_u64(
        &mut material,
        status.root_metadata_coordinator_backend_mismatch as u64,
    );
    append_u64(
        &mut material,
        status.root_metadata_coordinator_config_mismatch as u64,
    );
    append_u64(&mut material, status.root_metadata_max_shard_topology_epoch);
    append_u64(&mut material, status.root_metadata_control_proof_count);
    append_u64(&mut material, status.root_metadata_control_read_ready_count);
    append_u64(
        &mut material,
        status.root_metadata_control_write_ready_count,
    );
    append_u64(
        &mut material,
        status.root_metadata_external_coordinator_required_metaserver_count,
    );
    append_u64(
        &mut material,
        status.root_metadata_external_coordinator_endpoints_ready_metaserver_count,
    );
    append_u64(
        &mut material,
        status.root_metadata_external_coordinator_owner_sessions_ready_metaserver_count,
    );
    append_u64(
        &mut material,
        status.root_metadata_external_coordinator_ready_metaserver_count,
    );
    append_string(&mut material, &status.root_metadata_control_proof_checksum);
    append_string(&mut material, &status.root_metadata_coverage_status);
    append_u64(&mut material, status.root_active_pageserver_count);
    append_u64(&mut material, status.root_page_allocator_shard_count);
    append_u64(&mut material, status.root_page_allocator_covered_shards);
    append_u64(
        &mut material,
        status.root_page_allocator_shards_complete as u64,
    );
    append_string(&mut material, &status.root_page_allocator_shard_status);
    append_u64(&mut material, status.root_ready_pageserver_count);
    append_u64(&mut material, status.root_read_ready_pageserver_count);
    append_u64(
        &mut material,
        status.root_pageserver_route_control_proof_count,
    );
    append_u64(
        &mut material,
        status.root_pageserver_route_control_read_ready_count,
    );
    append_u64(
        &mut material,
        status.root_pageserver_route_control_write_ready_count,
    );
    append_string(
        &mut material,
        &status.root_pageserver_route_control_proof_checksum,
    );
    append_u64(
        &mut material,
        status.root_pageserver_route_control_min_commit_index,
    );
    append_u64(
        &mut material,
        status.root_pageserver_route_control_max_commit_index,
    );
    append_u64(
        &mut material,
        status.root_pageserver_total_chunk_metadata_dirty_endpoint_count,
    );
    append_u64(
        &mut material,
        status.root_pageserver_total_chunk_metadata_dirty_mutations,
    );
    append_u64(
        &mut material,
        status.root_pageserver_max_chunk_metadata_checkpoint_interval,
    );
    append_u64(
        &mut material,
        status.root_pageserver_total_deferred_metadata_checkpoint_endpoint_count,
    );
    append_u64(
        &mut material,
        status.root_pageserver_total_deferred_metadata_checkpoint_requests,
    );
    append_u64(
        &mut material,
        status.root_pageserver_total_deferred_metadata_checkpoint_runs,
    );
    append_u64(
        &mut material,
        status.root_pageserver_total_chunk_capacity_rejected_writes,
    );
    append_u64(
        &mut material,
        status.root_pageserver_total_replica_service_mode_rejected_mutations,
    );
    append_u64(
        &mut material,
        status.root_pageserver_total_chunk_physical_write_count,
    );
    append_u64(
        &mut material,
        status.root_pageserver_total_chunk_physical_write_bytes,
    );
    append_u64(
        &mut material,
        status.root_pageserver_total_chunk_physical_discard_range_count,
    );
    append_u64(
        &mut material,
        status.root_pageserver_total_chunk_physical_discard_bytes,
    );
    append_u64(
        &mut material,
        status.root_pageserver_total_reclaimable_bytes,
    );
    append_u64(
        &mut material,
        status.root_pageserver_total_trailing_trim_bytes,
    );
    append_u64(
        &mut material,
        status.root_pageserver_total_fragmented_chunk_count,
    );
    append_u64(
        &mut material,
        status.root_pageserver_total_fragmented_discarded_bytes,
    );
    append_u64(
        &mut material,
        status.root_pageserver_total_allocation_receipt_count,
    );
    append_u64(
        &mut material,
        status.root_pageserver_total_batch_allocation_receipt_count,
    );
    append_u64(
        &mut material,
        status.root_pageserver_total_release_allocation_receipt_count,
    );
    append_u64(
        &mut material,
        status.root_pageserver_total_write_receipt_count,
    );
    append_u64(
        &mut material,
        status.root_pageserver_total_batch_write_receipt_count,
    );
    append_u64(
        &mut material,
        status.root_pageserver_total_discard_receipt_count,
    );
    append_u64(
        &mut material,
        status.root_pageserver_total_batch_discard_receipt_count,
    );
    append_u64(&mut material, status.root_pageserver_health_ok_count);
    append_u64(&mut material, status.root_pageserver_health_degraded_count);
    append_u64(
        &mut material,
        status.root_pageserver_health_unavailable_count,
    );
    append_u64(&mut material, status.root_pageserver_health_unknown_count);
    append_u64(
        &mut material,
        status.root_pageserver_not_fault_tolerant_count,
    );
    append_u64(
        &mut material,
        status.root_pageserver_total_health_reason_count,
    );
    append_u64(
        &mut material,
        status.root_pageserver_total_write_quorum,
    );
    append_u64(&mut material, status.root_pageserver_total_read_quorum);
    append_u64(
        &mut material,
        status.root_pageserver_total_write_quorum_deficit,
    );
    append_u64(
        &mut material,
        status.root_pageserver_total_read_quorum_deficit,
    );
    append_u64(
        &mut material,
        status.root_pageserver_total_write_quorum_margin,
    );
    append_u64(
        &mut material,
        status.root_pageserver_total_read_quorum_margin,
    );
    append_string(
        &mut material,
        &status.root_pageserver_first_health_reasons,
    );
    append_string(
        &mut material,
        &status.root_pageserver_service_coverage_status,
    );
    append_u64(&mut material, status.root_active_replicaserver_count);
    append_u64(&mut material, status.root_writable_replicaserver_count);
    append_u64(
        &mut material,
        status.root_placement_eligible_replicaserver_count,
    );
    append_u64(
        &mut material,
        status.root_placement_ineligible_replicaserver_count,
    );
    append_u64(&mut material, status.root_replicaserver_control_proof_count);
    append_u64(
        &mut material,
        status.root_replicaserver_control_read_ready_count,
    );
    append_u64(
        &mut material,
        status.root_replicaserver_control_write_ready_count,
    );
    append_string(
        &mut material,
        &status.root_replicaserver_control_proof_checksum,
    );
    append_u64(
        &mut material,
        status.root_replicaserver_control_min_raft_commit_index,
    );
    append_u64(
        &mut material,
        status.root_replicaserver_control_max_raft_commit_index,
    );
    append_u64(
        &mut material,
        status.root_replicaserver_total_discarded_range_count,
    );
    append_u64(
        &mut material,
        status.root_replicaserver_total_reclaimable_bytes,
    );
    append_u64(
        &mut material,
        status.root_replicaserver_total_trailing_trim_bytes,
    );
    append_u64(
        &mut material,
        status.root_replicaserver_total_fragmented_chunk_count,
    );
    append_u64(
        &mut material,
        status.root_replicaserver_total_fragmented_discarded_bytes,
    );
    append_u64(
        &mut material,
        status.root_reclaim_pressure_replicaserver_count,
    );
    append_string(&mut material, &status.root_replicaserver_coverage_status);
    append_u64(&mut material, status.root_replicaserver_health_ok_count);
    append_u64(
        &mut material,
        status.root_replicaserver_health_degraded_count,
    );
    append_u64(
        &mut material,
        status.root_replicaserver_health_unavailable_count,
    );
    append_u64(&mut material, status.root_replicaserver_health_unknown_count);
    append_u64(
        &mut material,
        status.root_replicaserver_not_fault_tolerant_count,
    );
    append_u64(
        &mut material,
        status.root_replicaserver_total_health_reason_count,
    );
    append_string(
        &mut material,
        &status.root_replicaserver_first_health_reasons,
    );
    etag(&material)
}

