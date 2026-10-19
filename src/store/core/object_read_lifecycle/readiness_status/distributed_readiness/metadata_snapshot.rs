// SPDX-License-Identifier: Apache-2.0
struct DistributedMetadataReadinessSnapshot {
    metadata_route_cache: MetadataRouteCacheStatus,
    metadata_control_proof: MetadataControlProof,
    metaserver_endpoint: String,
    metaserver_commit_last_status: String,
    metaserver_fence_shard_id: String,
    metaserver_fence_shard_owner_node_id: String,
    metaserver_fence_shard_epoch: u64,
    metaserver_fence_lease_namespace: String,
    metaserver_fence_lease_owner: String,
    metaserver_fence_lease_epoch: u64,
    metaserver_fence_lease_expire_time_ms: u64,
    metaserver_fence_lease_ttl_ms: u64,
    metaserver_route_cache_entries: u64,
    metaserver_route_cache_hits: u64,
    metaserver_route_cache_misses: u64,
    metaserver_route_cache_invalidations: u64,
    metaserver_route_cache_expired_invalidations: u64,
    metaserver_route_cache_capacity: u64,
    metaserver_route_cache_ttl_ms: u64,
    metaserver_route_cache_capacity_evictions: u64,
    metaserver_route_cache_lookup_count: u64,
    metaserver_route_cache_hit_ratio_ppm: u64,
    metaserver_route_cache_miss_ratio_ppm: u64,
    metaserver_route_cache_remaining_capacity: u64,
    metaserver_route_cache_pressure: bool,
    metaserver_route_cache_fresh_entries: u64,
    metaserver_route_cache_stale_entries: u64,
    metaserver_route_cache_expired_entries: u64,
    metaserver_route_cache_stale_topology_entries: u64,
    metaserver_route_cache_stale_session_entries: u64,
    metaserver_route_cache_fresh: bool,
    metaserver_route_topology_epoch: u64,
    metaserver_route_topology_checksum: String,
    metaserver_route_session_checksum: String,
    metadata_control_read_ready: bool,
    metadata_control_write_ready: bool,
    metadata_ready: bool,
    metadata_coordinator_backend: String,
    metadata_coordinator_policy_format: String,
    metadata_coordinator_preferred_backend: String,
    metadata_coordinator_preferred_backend_active: bool,
    metadata_coordinator_external_compatibility_mode: bool,
    metadata_coordinator_production_ready: bool,
    metadata_coordinator_external_dependency: String,
    metadata_coordinator_config_checksum: String,
    metadata_coordinator_config_revision: u64,
    metadata_coordinator_endpoint_count: u64,
    metadata_coordinator_config_receipt_count: u64,
    metadata_external_coordinator_required: bool,
    metadata_external_coordinator_endpoints_ready: bool,
    metadata_external_coordinator_owner_sessions_ready: bool,
    metadata_external_coordinator_ready: bool,
}

impl DistributedMetadataReadinessSnapshot {
    fn capture(
        store: &MatrixObjectStore,
        metadata: &MetadataRaftReadiness,
        metadata_replication_quorum: u64,
        metadata_replication_acks: u64,
        metadata_replication_ready: bool,
        now_ms: u64,
    ) -> Self {
        let metadata_route_cache = store.metadata_route_cache_status_at(now_ms);
        let readiness_fence_shard = store
            .meta_service
            .shards
            .values()
            .filter(|shard| metadata_shard_overlaps_prefix(shard, "object/"))
            .max_by_key(|shard| (shard.epoch, shard.shard_id.as_str()));
        let readiness_fence_lease = store
            .meta_service
            .leases
            .values()
            .filter(|lease| lease.namespace.starts_with("object/") && lease.expire_time_ms > now_ms)
            .max_by_key(|lease| (lease.epoch, lease.expire_time_ms, lease.namespace.as_str()));
        let metadata_control_proof = metadata_control_proof(
            metadata,
            &metadata_route_cache,
            store.raft_membership_epoch,
            store.raft_membership_receipts.len() as u64,
            metadata_replication_quorum,
            metadata_replication_acks,
        );

        Self {
            metaserver_endpoint: "embedded".to_string(),
            metaserver_commit_last_status: if metadata.ready {
                "ready".to_string()
            } else {
                metadata.reason.clone()
            },
            metaserver_fence_shard_id: readiness_fence_shard
                .map_or_else(String::new, |shard| shard.shard_id.clone()),
            metaserver_fence_shard_owner_node_id: readiness_fence_shard
                .map_or_else(String::new, |shard| shard.owner_node_id.clone()),
            metaserver_fence_shard_epoch: readiness_fence_shard.map_or(0, |shard| shard.epoch),
            metaserver_fence_lease_namespace: readiness_fence_lease
                .map_or_else(String::new, |lease| lease.namespace.clone()),
            metaserver_fence_lease_owner: readiness_fence_lease
                .map_or_else(String::new, |lease| lease.owner.clone()),
            metaserver_fence_lease_epoch: readiness_fence_lease.map_or(0, |lease| lease.epoch),
            metaserver_fence_lease_expire_time_ms: readiness_fence_lease
                .map_or(0, |lease| lease.expire_time_ms),
            metaserver_fence_lease_ttl_ms: readiness_fence_lease.map_or(0, |lease| lease.ttl_ms),
            metaserver_route_cache_entries: metadata_route_cache.entries,
            metaserver_route_cache_hits: metadata_route_cache.hits,
            metaserver_route_cache_misses: metadata_route_cache.misses,
            metaserver_route_cache_invalidations: metadata_route_cache.invalidations,
            metaserver_route_cache_expired_invalidations: metadata_route_cache
                .expired_invalidations,
            metaserver_route_cache_capacity: metadata_route_cache.capacity,
            metaserver_route_cache_ttl_ms: metadata_route_cache.ttl_ms,
            metaserver_route_cache_capacity_evictions: metadata_route_cache.capacity_evictions,
            metaserver_route_cache_lookup_count: metadata_route_cache
                .hits
                .saturating_add(metadata_route_cache.misses),
            metaserver_route_cache_hit_ratio_ppm: {
                let lookups = metadata_route_cache
                    .hits
                    .saturating_add(metadata_route_cache.misses);
                if lookups == 0 {
                    0
                } else {
                    ((metadata_route_cache.hits as u128 * 1_000_000u128)
                        / lookups as u128)
                        .min(u64::MAX as u128) as u64
                }
            },
            metaserver_route_cache_miss_ratio_ppm: {
                let lookups = metadata_route_cache
                    .hits
                    .saturating_add(metadata_route_cache.misses);
                if lookups == 0 {
                    0
                } else {
                    ((metadata_route_cache.misses as u128 * 1_000_000u128)
                        / lookups as u128)
                        .min(u64::MAX as u128) as u64
                }
            },
            metaserver_route_cache_remaining_capacity: metadata_route_cache
                .capacity
                .saturating_sub(metadata_route_cache.entries),
            metaserver_route_cache_pressure: metadata_route_cache.pressure,
            metaserver_route_cache_fresh_entries: metadata_route_cache.fresh_entries,
            metaserver_route_cache_stale_entries: metadata_route_cache.stale_entries,
            metaserver_route_cache_expired_entries: metadata_route_cache.expired_entries,
            metaserver_route_cache_stale_topology_entries: metadata_route_cache
                .stale_topology_entries,
            metaserver_route_cache_stale_session_entries: metadata_route_cache
                .stale_session_entries,
            metaserver_route_cache_fresh: metadata_route_cache.fresh,
            metaserver_route_topology_epoch: metadata_route_cache.topology_epoch,
            metaserver_route_topology_checksum: metadata_route_cache.topology_checksum.clone(),
            metaserver_route_session_checksum: metadata_route_cache.session_checksum.clone(),
            metadata_control_read_ready: metadata_control_proof.read_ready,
            metadata_control_write_ready: metadata_control_proof.write_ready,
            metadata_ready: metadata.ready && metadata_replication_ready,
            metadata_coordinator_backend: store.meta_service.coordinator_config.backend.clone(),
            metadata_coordinator_policy_format: metadata_control_proof
                .coordinator_policy_format
                .clone(),
            metadata_coordinator_preferred_backend: metadata_control_proof
                .coordinator_preferred_backend
                .clone(),
            metadata_coordinator_preferred_backend_active: metadata_control_proof
                .coordinator_preferred_backend_active,
            metadata_coordinator_external_compatibility_mode: metadata_control_proof
                .coordinator_external_compatibility_mode,
            metadata_coordinator_production_ready: metadata_control_proof
                .coordinator_production_ready,
            metadata_coordinator_external_dependency: metadata_control_proof
                .coordinator_external_dependency
                .clone(),
            metadata_coordinator_config_checksum: metadata_control_proof
                .coordinator_config_checksum
                .clone(),
            metadata_coordinator_config_revision: metadata_control_proof
                .coordinator_config_revision,
            metadata_coordinator_endpoint_count: metadata_control_proof
                .coordinator_config_endpoint_count,
            metadata_coordinator_config_receipt_count: metadata_control_proof
                .coordinator_config_receipt_count,
            metadata_external_coordinator_required: metadata.external_coordinator_required,
            metadata_external_coordinator_endpoints_ready: metadata_control_proof
                .external_coordinator_endpoints_ready,
            metadata_external_coordinator_owner_sessions_ready: metadata_control_proof
                .external_coordinator_owner_sessions_ready,
            metadata_external_coordinator_ready: metadata.external_coordinator_ready,
            metadata_route_cache,
            metadata_control_proof,
        }
    }
}
