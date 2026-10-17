fn populate_metadata_module_status(
    status: &mut ObjectStoreModuleStatus,
    readiness: &ObjectStoreDistributedReadiness,
    metadata_replication_proof: MetadataReplicationProofReport,
) {
    status.metadata_commit_index = readiness.metadata_commit_index;
    status.metadata_replication_enabled = readiness.metadata_replication_enabled;
    status.metadata_replica_count = readiness.metadata_replica_count;
    status.metadata_replication_quorum = readiness.metadata_replication_quorum;
    status.metadata_replication_acks = readiness.metadata_replication_acks;
    status.metadata_replication_failures = readiness.metadata_replication_failures;
    status.metadata_replication_last_status = readiness.metadata_replication_last_status.clone();
    status.metadata_replication_proof_format = metadata_replication_proof.format;
    status.metadata_replication_proof_checksum = metadata_replication_proof.envelope_checksum;
    status.metadata_coordinator_backend =
        readiness.metadata_control_proof.coordinator_backend.clone();
    status.metadata_coordinator_policy_format = readiness
        .metadata_control_proof
        .coordinator_policy_format
        .clone();
    status.metadata_coordinator_preferred_backend = readiness
        .metadata_control_proof
        .coordinator_preferred_backend
        .clone();
    status.metadata_coordinator_preferred_backend_active = readiness
        .metadata_control_proof
        .coordinator_preferred_backend_active;
    status.metadata_coordinator_external_compatibility_mode = readiness
        .metadata_control_proof
        .coordinator_external_compatibility_mode;
    status.metadata_coordinator_production_ready = readiness
        .metadata_control_proof
        .coordinator_production_ready;
    status.metadata_coordinator_external_dependency = readiness
        .metadata_control_proof
        .coordinator_external_dependency
        .clone();
    status.metadata_coordinator_config_checksum = readiness
        .metadata_control_proof
        .coordinator_config_checksum
        .clone();
    status.metadata_coordinator_config_revision =
        readiness.metadata_control_proof.coordinator_config_revision;
    status.metadata_coordinator_config_endpoint_count = readiness
        .metadata_control_proof
        .coordinator_config_endpoint_count;
    status.metadata_coordinator_config_receipt_count = readiness
        .metadata_control_proof
        .coordinator_config_receipt_count;
    status.metadata_external_coordinator_required =
        readiness.metadata_external_coordinator_required;
    status.metadata_external_coordinator_endpoints_ready = readiness
        .metadata_control_proof
        .external_coordinator_endpoints_ready;
    status.metadata_external_coordinator_owner_sessions_ready = readiness
        .metadata_control_proof
        .external_coordinator_owner_sessions_ready;
    status.metadata_external_coordinator_ready = readiness.metadata_external_coordinator_ready;
    status.metadata_management_pressure = readiness.metadata_management_pressure;
    status.metadata_retention_pressure = readiness.metadata_retention_pressure;
    status.metadata_active_coordination_count = readiness.metadata_active_coordination_count;
    status.metadata_expired_coordination_count = readiness.metadata_expired_coordination_count;
    status.metadata_open_transaction_count = readiness.metadata_open_transaction_count;
    status.metadata_changefeed_retained_records = readiness.metadata_changefeed_retained_records;
    status.metadata_change_checkpoint_lag = readiness.metadata_change_checkpoint_lag;
    status.metadata_snapshot_floor_commit_index = readiness.metadata_snapshot_floor_commit_index;
    status.object_manifest_checkpoint_lag = readiness.object_manifest_checkpoint_lag;
    status.object_metadata_checkpoint_lag = readiness.object_metadata_checkpoint_lag;
    status.object_manifest_checkpoint_interval = readiness.object_manifest_checkpoint_interval;
    status.object_metadata_checkpoint_interval = readiness.object_metadata_checkpoint_interval;
    status.object_deferred_manifest_checkpoints = readiness.object_deferred_manifest_checkpoints;
    status.object_checkpoint_pressure = readiness.object_checkpoint_pressure;
    status.raft_membership_epoch = readiness.raft_membership_epoch;
    status.raft_membership_receipt_count = readiness.raft_membership_receipt_count;
    status.metaserver_route_cache_entries = readiness.metaserver_route_cache_entries;
    status.metaserver_route_cache_hits = readiness.metaserver_route_cache_hits;
    status.metaserver_route_cache_misses = readiness.metaserver_route_cache_misses;
    status.metaserver_route_cache_invalidations = readiness.metaserver_route_cache_invalidations;
    status.metaserver_route_cache_expired_invalidations =
        readiness.metaserver_route_cache_expired_invalidations;
    status.metaserver_route_cache_capacity = readiness.metaserver_route_cache_capacity;
    status.metaserver_route_cache_ttl_ms = readiness.metaserver_route_cache_ttl_ms;
    status.metaserver_route_cache_capacity_evictions =
        readiness.metaserver_route_cache_capacity_evictions;
    status.metaserver_route_cache_lookup_count =
        readiness.metaserver_route_cache_lookup_count;
    status.metaserver_route_cache_hit_ratio_ppm =
        readiness.metaserver_route_cache_hit_ratio_ppm;
    status.metaserver_route_cache_miss_ratio_ppm =
        readiness.metaserver_route_cache_miss_ratio_ppm;
    status.metaserver_route_cache_remaining_capacity =
        readiness.metaserver_route_cache_remaining_capacity;
    status.metaserver_route_cache_pressure = readiness.metaserver_route_cache_pressure;
    status.metaserver_route_cache_fresh_entries = readiness.metaserver_route_cache_fresh_entries;
    status.metaserver_route_cache_stale_entries = readiness.metaserver_route_cache_stale_entries;
    status.metaserver_route_cache_expired_entries =
        readiness.metaserver_route_cache_expired_entries;
    status.metaserver_route_cache_stale_topology_entries =
        readiness.metaserver_route_cache_stale_topology_entries;
    status.metaserver_route_cache_stale_session_entries =
        readiness.metaserver_route_cache_stale_session_entries;
    status.metaserver_route_cache_fresh = readiness.metaserver_route_cache_fresh;
    status.metaserver_route_topology_epoch = readiness.metaserver_route_topology_epoch;
    status.metaserver_route_topology_checksum =
        readiness.metaserver_route_topology_checksum.clone();
    status.metaserver_route_session_checksum = readiness.metaserver_route_session_checksum.clone();
    status.metaserver_fence_shard_id = readiness.metaserver_fence_shard_id.clone();
    status.metaserver_fence_shard_owner_node_id =
        readiness.metaserver_fence_shard_owner_node_id.clone();
    status.metaserver_fence_shard_epoch = readiness.metaserver_fence_shard_epoch;
    status.metaserver_fence_lease_namespace = readiness.metaserver_fence_lease_namespace.clone();
    status.metaserver_fence_lease_owner = readiness.metaserver_fence_lease_owner.clone();
    status.metaserver_fence_lease_epoch = readiness.metaserver_fence_lease_epoch;
    status.metaserver_fence_lease_expire_time_ms = readiness.metaserver_fence_lease_expire_time_ms;
    status.metaserver_fence_lease_ttl_ms = readiness.metaserver_fence_lease_ttl_ms;
    status.metaserver_endpoint = readiness.metaserver_endpoint.clone();
    status.metaserver_commit_last_status = readiness.metaserver_commit_last_status.clone();
    status.consensus_read_index = readiness.consensus_read_index;
    status.consensus_read_refreshes = readiness.consensus_read_refreshes;
    status.consensus_read_cache_hits = readiness.consensus_read_cache_hits;
    status.consensus_read_last_status = readiness.consensus_read_last_status.clone();
}
