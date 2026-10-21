// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
fn append_metaserver_status(mut material: &mut Vec<u8>, status: &ObjectStoreModuleStatus) {
    append_u64(&mut material, status.metadata_commit_index);
    append_u64(&mut material, status.metadata_replication_enabled as u64);
    append_u64(&mut material, status.metadata_replica_count);
    append_u64(&mut material, status.metadata_replication_quorum);
    append_u64(&mut material, status.metadata_replication_acks);
    append_u64(&mut material, status.metadata_replication_failures);
    append_string(&mut material, &status.metadata_replication_last_status);
    append_string(&mut material, &status.metadata_replication_proof_format);
    append_string(&mut material, &status.metadata_replication_proof_checksum);
    append_u64(&mut material, status.metadata_management_pressure as u64);
    append_u64(&mut material, status.metadata_retention_pressure as u64);
    append_u64(&mut material, status.metadata_active_coordination_count);
    append_u64(&mut material, status.metadata_expired_coordination_count);
    append_u64(&mut material, status.metadata_open_transaction_count);
    append_u64(&mut material, status.metadata_changefeed_retained_records);
    append_u64(&mut material, status.metadata_change_checkpoint_lag);
    append_u64(&mut material, status.metadata_snapshot_floor_commit_index);
    append_u64(&mut material, status.object_manifest_checkpoint_lag);
    append_u64(&mut material, status.object_metadata_checkpoint_lag);
    append_u64(&mut material, status.object_manifest_checkpoint_interval);
    append_u64(&mut material, status.object_metadata_checkpoint_interval);
    append_u64(
        &mut material,
        status.object_deferred_manifest_checkpoints as u64,
    );
    append_u64(&mut material, status.object_checkpoint_pressure as u64);
    append_u64(&mut material, status.raft_membership_epoch);
    append_u64(&mut material, status.raft_membership_receipt_count);
    append_u64(&mut material, status.metaserver_route_cache_entries);
    append_u64(&mut material, status.metaserver_route_cache_hits);
    append_u64(&mut material, status.metaserver_route_cache_misses);
    append_u64(&mut material, status.metaserver_route_cache_invalidations);
    append_u64(
        &mut material,
        status.metaserver_route_cache_expired_invalidations,
    );
    append_u64(&mut material, status.metaserver_route_cache_capacity);
    append_u64(&mut material, status.metaserver_route_cache_ttl_ms);
    append_u64(
        &mut material,
        status.metaserver_route_cache_capacity_evictions,
    );
    append_u64(&mut material, status.metaserver_route_cache_lookup_count);
    append_u64(&mut material, status.metaserver_route_cache_hit_ratio_ppm);
    append_u64(&mut material, status.metaserver_route_cache_miss_ratio_ppm);
    append_u64(
        &mut material,
        status.metaserver_route_cache_remaining_capacity,
    );
    append_u64(&mut material, status.metaserver_route_cache_pressure as u64);
    append_u64(&mut material, status.metaserver_route_cache_fresh_entries);
    append_u64(&mut material, status.metaserver_route_cache_stale_entries);
    append_u64(&mut material, status.metaserver_route_cache_expired_entries);
    append_u64(
        &mut material,
        status.metaserver_route_cache_stale_topology_entries,
    );
    append_u64(
        &mut material,
        status.metaserver_route_cache_stale_session_entries,
    );
    append_u64(&mut material, status.metaserver_route_cache_fresh as u64);
    append_u64(&mut material, status.metaserver_route_topology_epoch);
    append_string(&mut material, &status.metaserver_route_topology_checksum);
    append_string(&mut material, &status.metaserver_route_session_checksum);
    append_string(&mut material, &status.metaserver_fence_shard_id);
    append_string(&mut material, &status.metaserver_fence_shard_owner_node_id);
    append_u64(&mut material, status.metaserver_fence_shard_epoch);
    append_string(&mut material, &status.metaserver_fence_lease_namespace);
    append_string(&mut material, &status.metaserver_fence_lease_owner);
    append_u64(&mut material, status.metaserver_fence_lease_epoch);
    append_u64(&mut material, status.metaserver_fence_lease_expire_time_ms);
    append_u64(&mut material, status.metaserver_fence_lease_ttl_ms);
    append_string(&mut material, &status.metaserver_endpoint);
    append_string(&mut material, &status.metaserver_commit_last_status);
    append_u64(&mut material, status.consensus_read_index);
    append_u64(&mut material, status.consensus_read_refreshes);
    append_u64(&mut material, status.consensus_read_cache_hits);
    append_string(&mut material, &status.consensus_read_last_status);
}
