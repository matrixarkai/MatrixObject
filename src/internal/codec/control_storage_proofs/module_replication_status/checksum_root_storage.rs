// SPDX-License-Identifier: Apache-2.0
fn append_root_storage_status(mut material: &mut Vec<u8>, status: &ObjectStoreModuleStatus) {
    append_u64(&mut material, status.root_active_pageserver_count);
    append_u64(&mut material, status.root_active_replicaserver_count);
    append_u64(&mut material, status.root_active_metaserver_count);
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
        status.root_pageserver_total_chunk_endpoint_count,
    );
    append_u64(
        &mut material,
        status.root_pageserver_total_writable_chunk_endpoint_count,
    );
    append_u64(
        &mut material,
        status.root_pageserver_total_healthy_chunk_endpoint_count,
    );
    append_u64(
        &mut material,
        status.root_pageserver_total_raft_writable_chunk_endpoint_count,
    );
    append_u64(
        &mut material,
        status.root_pageserver_total_raft_readable_chunk_endpoint_count,
    );
    append_u64(
        &mut material,
        status.root_pageserver_total_capacity_ready_writable_chunk_endpoint_count,
    );
    append_u64(
        &mut material,
        status.root_pageserver_total_quarantined_chunk_replicas,
    );
    append_u64(
        &mut material,
        status.root_pageserver_total_chunk_capacity_bytes,
    );
    append_u64(
        &mut material,
        status.root_pageserver_total_chunk_capacity_free_bytes,
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
    append_u64(&mut material, status.root_writable_replicaserver_count);
    append_u64(
        &mut material,
        status.root_placement_eligible_replicaserver_count,
    );
    append_u64(
        &mut material,
        status.root_placement_ineligible_replicaserver_count,
    );
    append_u64(&mut material, status.root_draining_replicaserver_count);
    append_u64(
        &mut material,
        status.root_capacity_floor_blocked_replicaserver_count,
    );
    append_u64(&mut material, status.root_frozen_replicaserver_count);
    append_u64(&mut material, status.root_read_only_replicaserver_count);
    append_u64(&mut material, status.root_raft_unwritable_replicaserver_count);
    append_u64(
        &mut material,
        status.root_replicaserver_total_reported_free_bytes,
    );
    append_u64(&mut material, status.root_replicaserver_min_free_bytes);
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
    append_u64(&mut material, status.root_node_count);
}
