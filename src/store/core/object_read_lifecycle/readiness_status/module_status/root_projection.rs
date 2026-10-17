fn populate_root_module_status(
    status: &mut ObjectStoreModuleStatus,
    readiness: &ObjectStoreDistributedReadiness,
) {
    status.root_active_pageserver_count = readiness.root_active_pageserver_count;
    status.root_active_chunkserver_count = readiness.root_active_chunkserver_count;
    status.root_active_metaserver_count = readiness.root_active_metaserver_count;
    status.root_page_allocator_shard_count = readiness.root_page_allocator_shard_count;
    status.root_page_allocator_covered_shards = readiness.root_page_allocator_covered_shards;
    status.root_page_allocator_shards_complete = readiness.root_page_allocator_shards_complete;
    status.root_page_allocator_shard_status = readiness.root_page_allocator_shard_status.clone();
    status.root_ready_pageserver_count = readiness.root_ready_pageserver_count;
    status.root_read_ready_pageserver_count = readiness.root_read_ready_pageserver_count;
    status.root_pageserver_total_chunk_endpoint_count =
        readiness.root_pageserver_total_chunk_endpoint_count;
    status.root_pageserver_total_writable_chunk_endpoint_count =
        readiness.root_pageserver_total_writable_chunk_endpoint_count;
    status.root_pageserver_total_healthy_chunk_endpoint_count =
        readiness.root_pageserver_total_healthy_chunk_endpoint_count;
    status.root_pageserver_total_raft_writable_chunk_endpoint_count =
        readiness.root_pageserver_total_raft_writable_chunk_endpoint_count;
    status.root_pageserver_total_raft_readable_chunk_endpoint_count =
        readiness.root_pageserver_total_raft_readable_chunk_endpoint_count;
    status.root_pageserver_total_capacity_ready_writable_chunk_endpoint_count =
        readiness.root_pageserver_total_capacity_ready_writable_chunk_endpoint_count;
    status.root_pageserver_total_quarantined_chunk_replicas =
        readiness.root_pageserver_total_quarantined_chunk_replicas;
    status.root_pageserver_total_chunk_capacity_bytes =
        readiness.root_pageserver_total_chunk_capacity_bytes;
    status.root_pageserver_total_chunk_capacity_free_bytes =
        readiness.root_pageserver_total_chunk_capacity_free_bytes;
    status.root_pageserver_total_chunk_metadata_dirty_endpoint_count =
        readiness.root_pageserver_total_chunk_metadata_dirty_endpoint_count;
    status.root_pageserver_total_chunk_metadata_dirty_mutations =
        readiness.root_pageserver_total_chunk_metadata_dirty_mutations;
    status.root_pageserver_max_chunk_metadata_checkpoint_interval =
        readiness.root_pageserver_max_chunk_metadata_checkpoint_interval;
    status.root_pageserver_total_deferred_metadata_checkpoint_endpoint_count =
        readiness.root_pageserver_total_deferred_metadata_checkpoint_endpoint_count;
    status.root_pageserver_total_deferred_metadata_checkpoint_requests =
        readiness.root_pageserver_total_deferred_metadata_checkpoint_requests;
    status.root_pageserver_total_deferred_metadata_checkpoint_runs =
        readiness.root_pageserver_total_deferred_metadata_checkpoint_runs;
    status.root_pageserver_total_chunk_capacity_rejected_writes =
        readiness.root_pageserver_total_chunk_capacity_rejected_writes;
    status.root_pageserver_total_chunk_service_mode_rejected_mutations =
        readiness.root_pageserver_total_chunk_service_mode_rejected_mutations;
    status.root_pageserver_total_chunk_physical_write_count =
        readiness.root_pageserver_total_chunk_physical_write_count;
    status.root_pageserver_total_chunk_physical_write_bytes =
        readiness.root_pageserver_total_chunk_physical_write_bytes;
    status.root_pageserver_total_chunk_physical_discard_range_count =
        readiness.root_pageserver_total_chunk_physical_discard_range_count;
    status.root_pageserver_total_chunk_physical_discard_bytes =
        readiness.root_pageserver_total_chunk_physical_discard_bytes;
    status.root_pageserver_total_reclaimable_bytes =
        readiness.root_pageserver_total_reclaimable_bytes;
    status.root_pageserver_total_trailing_trim_bytes =
        readiness.root_pageserver_total_trailing_trim_bytes;
    status.root_pageserver_total_fragmented_chunk_count =
        readiness.root_pageserver_total_fragmented_chunk_count;
    status.root_pageserver_total_fragmented_discarded_bytes =
        readiness.root_pageserver_total_fragmented_discarded_bytes;
    status.root_pageserver_health_ok_count = readiness.root_pageserver_health_ok_count;
    status.root_pageserver_health_degraded_count =
        readiness.root_pageserver_health_degraded_count;
    status.root_pageserver_health_unavailable_count =
        readiness.root_pageserver_health_unavailable_count;
    status.root_pageserver_health_unknown_count = readiness.root_pageserver_health_unknown_count;
    status.root_pageserver_not_fault_tolerant_count =
        readiness.root_pageserver_not_fault_tolerant_count;
    status.root_pageserver_total_health_reason_count =
        readiness.root_pageserver_total_health_reason_count;
    status.root_pageserver_total_write_quorum =
        readiness.root_pageserver_total_write_quorum;
    status.root_pageserver_total_read_quorum = readiness.root_pageserver_total_read_quorum;
    status.root_pageserver_total_write_quorum_deficit =
        readiness.root_pageserver_total_write_quorum_deficit;
    status.root_pageserver_total_read_quorum_deficit =
        readiness.root_pageserver_total_read_quorum_deficit;
    status.root_pageserver_total_write_quorum_margin =
        readiness.root_pageserver_total_write_quorum_margin;
    status.root_pageserver_total_read_quorum_margin =
        readiness.root_pageserver_total_read_quorum_margin;
    status.root_pageserver_first_health_reasons =
        readiness.root_pageserver_first_health_reasons.clone();
    status.root_pageserver_service_coverage_status =
        readiness.root_pageserver_service_coverage_status.clone();
    status.root_writable_chunkserver_count = readiness.root_writable_chunkserver_count;
    status.root_placement_eligible_chunkserver_count =
        readiness.root_placement_eligible_chunkserver_count;
    status.root_placement_ineligible_chunkserver_count =
        readiness.root_placement_ineligible_chunkserver_count;
    status.root_draining_chunkserver_count = readiness.root_draining_chunkserver_count;
    status.root_capacity_floor_blocked_chunkserver_count =
        readiness.root_capacity_floor_blocked_chunkserver_count;
    status.root_frozen_chunkserver_count = readiness.root_frozen_chunkserver_count;
    status.root_read_only_chunkserver_count = readiness.root_read_only_chunkserver_count;
    status.root_raft_unwritable_chunkserver_count =
        readiness.root_raft_unwritable_chunkserver_count;
    status.root_chunkserver_total_reported_free_bytes =
        readiness.root_chunkserver_total_reported_free_bytes;
    status.root_chunkserver_min_free_bytes = readiness.root_chunkserver_min_free_bytes;
    status.root_chunkserver_coverage_status = readiness.root_chunkserver_coverage_status.clone();
    status.root_chunkserver_health_ok_count = readiness.root_chunkserver_health_ok_count;
    status.root_chunkserver_health_degraded_count =
        readiness.root_chunkserver_health_degraded_count;
    status.root_chunkserver_health_unavailable_count =
        readiness.root_chunkserver_health_unavailable_count;
    status.root_chunkserver_health_unknown_count = readiness.root_chunkserver_health_unknown_count;
    status.root_chunkserver_not_fault_tolerant_count =
        readiness.root_chunkserver_not_fault_tolerant_count;
    status.root_chunkserver_total_health_reason_count =
        readiness.root_chunkserver_total_health_reason_count;
    status.root_chunkserver_first_health_reasons =
        readiness.root_chunkserver_first_health_reasons.clone();
    status.root_node_count = readiness.root_node_count;
    status.root_expected_nodes = readiness.root_expected_nodes;
    status.root_leader_node_id = readiness.root_leader_node_id.clone();
    status.root_lease_epoch = readiness.root_lease_epoch;
    status.root_lease_expire_time_ms = readiness.root_lease_expire_time_ms;
    status.root_read_ready = readiness.root_read_ready;
    status.root_metadata_read_ready = readiness.root_metadata_read_ready;
    status.root_metadata_write_ready = readiness.root_metadata_write_ready;
    status.root_pageserver_read_ready = readiness.root_pageserver_read_ready;
    status.root_pageserver_write_ready = readiness.root_pageserver_write_ready;
    status.root_chunkserver_read_ready = readiness.root_chunkserver_read_ready;
    status.root_chunkserver_write_ready = readiness.root_chunkserver_write_ready;
    status.root_readiness_gate_read_ready = readiness.root_readiness_gate_read_ready;
    status.root_readiness_gate_write_ready = readiness.root_readiness_gate_write_ready;
    status.root_service_health = readiness.root_service_health.clone();
    status.root_service_health_reason_count = readiness.root_service_health_reason_count;
    status.root_service_health_reasons = readiness.root_service_health_reasons.clone();
    status.root_service_fault_tolerant = readiness.root_service_fault_tolerant;
    status.root_snapshot_fault_count = readiness.root_snapshot_fault_count;
    status.root_metadata_status_available = readiness.root_metadata_status_available;
    status.root_metadata_value_bytes = readiness.root_metadata_value_bytes;
    status.root_metadata_readable_metaserver_count =
        readiness.root_metadata_readable_metaserver_count;
    status.root_metadata_writable_metaserver_count =
        readiness.root_metadata_writable_metaserver_count;
    status.root_metadata_caught_up_metaserver_count =
        readiness.root_metadata_caught_up_metaserver_count;
    status.root_metadata_apply_caught_up_metaserver_count =
        readiness.root_metadata_apply_caught_up_metaserver_count;
    status.root_metadata_control_apply_caught_up_metaserver_count =
        readiness.root_metadata_control_apply_caught_up_metaserver_count;
    status.root_metadata_read_fence_covered_metaserver_count =
        readiness.root_metadata_read_fence_covered_metaserver_count;
    status.root_metadata_quorum = readiness.root_metadata_quorum;
    status.root_metadata_read_quorum_available = readiness.root_metadata_read_quorum_available;
    status.root_metadata_write_quorum_available = readiness.root_metadata_write_quorum_available;
    status.root_metadata_caught_up_quorum_available =
        readiness.root_metadata_caught_up_quorum_available;
    status.root_metadata_apply_quorum_available = readiness.root_metadata_apply_quorum_available;
    status.root_metadata_read_fence_quorum_available =
        readiness.root_metadata_read_fence_quorum_available;
    status.root_metadata_control_apply_quorum_available =
        readiness.root_metadata_control_apply_quorum_available;
    status.root_metadata_min_last_applied_mutation_commit_index =
        readiness.root_metadata_min_last_applied_mutation_commit_index;
    status.root_metadata_max_apply_commit_lag = readiness.root_metadata_max_apply_commit_lag;
    status.root_metadata_min_safe_read_fence = readiness.root_metadata_min_safe_read_fence;
    status.root_metadata_max_read_fence_commit_lag =
        readiness.root_metadata_max_read_fence_commit_lag;
    status.root_metadata_max_read_fence_read_index_lag =
        readiness.root_metadata_max_read_fence_read_index_lag;
    status.root_metadata_max_read_fence_applied_lag =
        readiness.root_metadata_max_read_fence_applied_lag;
    status.root_metadata_control_min_last_applied_mutation_commit_index =
        readiness.root_metadata_control_min_last_applied_mutation_commit_index;
    status.root_metadata_shard_topology_mismatch = readiness.root_metadata_shard_topology_mismatch;
    status.root_metadata_coordinator_backend_mismatch =
        readiness.root_metadata_coordinator_backend_mismatch;
    status.root_metadata_coordinator_config_mismatch =
        readiness.root_metadata_coordinator_config_mismatch;
    status.root_metadata_max_shard_topology_epoch =
        readiness.root_metadata_max_shard_topology_epoch;
    status.root_metadata_coverage_status = readiness.root_metadata_coverage_status.clone();
    status.root_metadata_control_proof_count = readiness.root_metadata_control_proof_count;
    status.root_metadata_control_read_ready_count =
        readiness.root_metadata_control_read_ready_count;
    status.root_metadata_control_write_ready_count =
        readiness.root_metadata_control_write_ready_count;
    status.root_metadata_external_coordinator_required_metaserver_count =
        readiness.root_metadata_external_coordinator_required_metaserver_count;
    status.root_metadata_external_coordinator_endpoints_ready_metaserver_count =
        readiness.root_metadata_external_coordinator_endpoints_ready_metaserver_count;
    status.root_metadata_external_coordinator_owner_sessions_ready_metaserver_count =
        readiness.root_metadata_external_coordinator_owner_sessions_ready_metaserver_count;
    status.root_metadata_external_coordinator_ready_metaserver_count =
        readiness.root_metadata_external_coordinator_ready_metaserver_count;
    status.root_metadata_control_proof_checksum =
        readiness.root_metadata_control_proof_checksum.clone();
    status.root_pageserver_route_control_proof_count =
        readiness.root_pageserver_route_control_proof_count;
    status.root_pageserver_route_control_read_ready_count =
        readiness.root_pageserver_route_control_read_ready_count;
    status.root_pageserver_route_control_write_ready_count =
        readiness.root_pageserver_route_control_write_ready_count;
    status.root_pageserver_route_control_proof_checksum = readiness
        .root_pageserver_route_control_proof_checksum
        .clone();
    status.root_chunkserver_control_proof_count = readiness.root_chunkserver_control_proof_count;
    status.root_chunkserver_control_read_ready_count =
        readiness.root_chunkserver_control_read_ready_count;
    status.root_chunkserver_control_write_ready_count =
        readiness.root_chunkserver_control_write_ready_count;
    status.root_chunkserver_control_proof_checksum =
        readiness.root_chunkserver_control_proof_checksum.clone();
    status.root_raft_status_checksum_format = readiness.root_raft_status_checksum_format.clone();
    status.root_raft_status_checksum = readiness.root_raft_status_checksum.clone();
    status.root_pageserver_route_control_min_commit_index =
        readiness.root_pageserver_route_control_min_commit_index;
    status.root_pageserver_route_control_max_commit_index =
        readiness.root_pageserver_route_control_max_commit_index;
    status.root_pageserver_total_allocation_receipt_count =
        readiness.root_pageserver_total_allocation_receipt_count;
    status.root_pageserver_total_batch_allocation_receipt_count =
        readiness.root_pageserver_total_batch_allocation_receipt_count;
    status.root_pageserver_total_release_allocation_receipt_count =
        readiness.root_pageserver_total_release_allocation_receipt_count;
    status.root_pageserver_total_write_receipt_count =
        readiness.root_pageserver_total_write_receipt_count;
    status.root_pageserver_total_batch_write_receipt_count =
        readiness.root_pageserver_total_batch_write_receipt_count;
    status.root_pageserver_total_discard_receipt_count =
        readiness.root_pageserver_total_discard_receipt_count;
    status.root_pageserver_total_batch_discard_receipt_count =
        readiness.root_pageserver_total_batch_discard_receipt_count;
    status.root_chunkserver_control_min_raft_commit_index =
        readiness.root_chunkserver_control_min_raft_commit_index;
    status.root_chunkserver_control_max_raft_commit_index =
        readiness.root_chunkserver_control_max_raft_commit_index;
    status.root_metadata_control_max_last_applied_mutation_commit_index =
        readiness.root_metadata_control_max_last_applied_mutation_commit_index;
    status.root_metadata_control_max_apply_commit_lag =
        readiness.root_metadata_control_max_apply_commit_lag;
}
