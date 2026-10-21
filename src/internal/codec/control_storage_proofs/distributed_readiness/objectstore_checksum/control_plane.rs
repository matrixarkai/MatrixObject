// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
fn append_control_plane_readiness(mut material: &mut Vec<u8>, report: &ObjectStoreDistributedReadiness) {
    append_u64(&mut material, report.root_placement_health.degraded_objects);
    append_u64(
        &mut material,
        report.root_placement_health.under_replicated_objects,
    );
    append_u64(
        &mut material,
        report.root_placement_health.capacity_pressure_references,
    );
    append_u64(&mut material, report.root_ready as u64);
    append_u64(&mut material, report.root_leader_ready as u64);
    append_string(&mut material, &report.root_leader_node_id);
    append_u64(&mut material, report.root_lease_epoch);
    append_u64(&mut material, report.root_lease_expire_time_ms);
    append_string(&mut material, &report.root_state_fingerprint);
    append_string(&mut material, &report.root_raft_status_checksum);
    append_u64(&mut material, report.consensus_read_index);
    append_u64(&mut material, report.consensus_read_refreshes);
    append_u64(&mut material, report.consensus_read_cache_hits);
    append_string(&mut material, &report.consensus_read_last_status);
    append_u64(&mut material, report.metadata_commit_index);
    append_u64(&mut material, report.metadata_replication_enabled as u64);
    append_u64(&mut material, report.metadata_replica_count);
    append_u64(&mut material, report.metadata_replication_quorum);
    append_u64(&mut material, report.metadata_replication_acks);
    append_u64(&mut material, report.metadata_replication_failures);
    append_string(&mut material, &report.metadata_replication_last_status);
    append_u64(&mut material, report.metadata_read_ready as u64);
    append_u64(&mut material, report.metadata_write_ready as u64);
    append_u64(&mut material, report.metadata_caught_up as u64);
    append_u64(&mut material, report.metadata_quorum_ready as u64);
    append_u64(&mut material, report.metadata_management_pressure as u64);
    append_u64(&mut material, report.metadata_retention_pressure as u64);
    append_string(&mut material, &report.metadata_coordinator_backend);
    append_string(&mut material, &report.metadata_coordinator_policy_format);
    append_string(
        &mut material,
        &report.metadata_coordinator_preferred_backend,
    );
    append_u64(
        &mut material,
        report.metadata_coordinator_preferred_backend_active as u64,
    );
    append_u64(
        &mut material,
        report.metadata_coordinator_external_compatibility_mode as u64,
    );
    append_u64(
        &mut material,
        report.metadata_coordinator_production_ready as u64,
    );
    append_string(
        &mut material,
        &report.metadata_coordinator_external_dependency,
    );
    append_string(&mut material, &report.metadata_coordinator_config_checksum);
    append_u64(&mut material, report.metadata_coordinator_config_revision);
    append_u64(&mut material, report.metadata_coordinator_endpoint_count);
    append_u64(
        &mut material,
        report.metadata_coordinator_config_receipt_count,
    );
    append_u64(
        &mut material,
        report.metadata_external_coordinator_required as u64,
    );
    append_u64(
        &mut material,
        report.metadata_external_coordinator_endpoints_ready as u64,
    );
    append_u64(
        &mut material,
        report.metadata_external_coordinator_owner_sessions_ready as u64,
    );
    append_u64(
        &mut material,
        report.metadata_external_coordinator_ready as u64,
    );
    append_u64(&mut material, report.metadata_active_coordination_count);
    append_u64(&mut material, report.metadata_expired_coordination_count);
    append_u64(&mut material, report.metadata_open_transaction_count);
    append_u64(&mut material, report.metadata_changefeed_retained_records);
    append_u64(&mut material, report.metadata_change_checkpoint_lag);
    append_u64(&mut material, report.metadata_snapshot_floor_commit_index);
    append_u64(&mut material, report.object_manifest_checkpoint_lag);
    append_u64(&mut material, report.object_metadata_checkpoint_lag);
    append_u64(&mut material, report.object_manifest_checkpoint_interval);
    append_u64(&mut material, report.object_metadata_checkpoint_interval);
    append_u64(
        &mut material,
        report.object_deferred_manifest_checkpoints as u64,
    );
    append_u64(&mut material, report.object_checkpoint_pressure as u64);
    append_u64(&mut material, report.raft_membership_epoch);
    append_u64(&mut material, report.raft_membership_receipt_count);
    append_u64(&mut material, report.placement_bytes_to_repair);
    append_u64(&mut material, report.placement_degraded_objects);
    append_u64(&mut material, report.placement_truncated as u64);
    append_u64(&mut material, report.root_expected_nodes);
    append_u64(&mut material, report.root_metadata_status_available as u64);
    append_u64(&mut material, report.metadata_route_cache.entries);
    append_u64(&mut material, report.metadata_route_cache.capacity);
    append_u64(&mut material, report.metadata_route_cache.ttl_ms);
    append_u64(&mut material, report.metadata_route_cache.hits);
    append_u64(&mut material, report.metadata_route_cache.misses);
    append_u64(&mut material, report.metadata_route_cache.invalidations);
    append_u64(
        &mut material,
        report.metadata_route_cache.expired_invalidations,
    );
    append_u64(
        &mut material,
        report.metadata_route_cache.capacity_evictions,
    );
    append_u64(&mut material, report.metadata_route_cache.pressure as u64);
    append_u64(&mut material, report.metadata_route_cache.topology_epoch);
    append_string(
        &mut material,
        &report.metadata_route_cache.topology_checksum,
    );
    append_string(
        &mut material,
        &report.metadata_route_cache.shard_topology_checksum,
    );
    append_string(&mut material, &report.metadata_route_cache.session_checksum);
    append_u64(&mut material, report.metadata_route_cache.fresh_entries);
    append_u64(&mut material, report.metadata_route_cache.expired_entries);
    append_u64(
        &mut material,
        report.metadata_route_cache.stale_topology_entries,
    );
    append_u64(
        &mut material,
        report.metadata_route_cache.stale_session_entries,
    );
    append_u64(&mut material, report.metadata_route_cache.stale_entries);
    append_u64(&mut material, report.metadata_route_cache.fresh as u64);
    append_string(&mut material, &report.metaserver_endpoint);
    append_string(&mut material, &report.metaserver_commit_last_status);
    append_string(&mut material, &report.metaserver_fence_shard_id);
    append_string(&mut material, &report.metaserver_fence_shard_owner_node_id);
    append_u64(&mut material, report.metaserver_fence_shard_epoch);
    append_string(&mut material, &report.metaserver_fence_lease_namespace);
    append_string(&mut material, &report.metaserver_fence_lease_owner);
    append_u64(&mut material, report.metaserver_fence_lease_epoch);
    append_u64(&mut material, report.metaserver_fence_lease_expire_time_ms);
    append_u64(&mut material, report.metaserver_fence_lease_ttl_ms);
    append_u64(&mut material, report.metaserver_route_cache_entries);
    append_u64(&mut material, report.metaserver_route_cache_hits);
    append_u64(&mut material, report.metaserver_route_cache_misses);
    append_u64(&mut material, report.metaserver_route_cache_invalidations);
    append_u64(
        &mut material,
        report.metaserver_route_cache_expired_invalidations,
    );
    append_u64(&mut material, report.metaserver_route_cache_capacity);
    append_u64(&mut material, report.metaserver_route_cache_ttl_ms);
    append_u64(
        &mut material,
        report.metaserver_route_cache_capacity_evictions,
    );
    append_u64(&mut material, report.metaserver_route_cache_lookup_count);
    append_u64(&mut material, report.metaserver_route_cache_hit_ratio_ppm);
    append_u64(&mut material, report.metaserver_route_cache_miss_ratio_ppm);
    append_u64(
        &mut material,
        report.metaserver_route_cache_remaining_capacity,
    );
    append_u64(&mut material, report.metaserver_route_cache_pressure as u64);
    append_u64(&mut material, report.metaserver_route_cache_fresh_entries);
    append_u64(&mut material, report.metaserver_route_cache_stale_entries);
    append_u64(&mut material, report.metaserver_route_cache_expired_entries);
    append_u64(
        &mut material,
        report.metaserver_route_cache_stale_topology_entries,
    );
    append_u64(
        &mut material,
        report.metaserver_route_cache_stale_session_entries,
    );
    append_u64(&mut material, report.metaserver_route_cache_fresh as u64);
    append_u64(&mut material, report.metaserver_route_topology_epoch);
    append_string(&mut material, &report.metaserver_route_topology_checksum);
    append_string(&mut material, &report.metaserver_route_session_checksum);
}
