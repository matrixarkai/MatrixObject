// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
pub(crate) fn metadata_control_proof(
    readiness: &MetadataRaftReadiness,
    route_cache: &MetadataRouteCacheStatus,
    raft_membership_epoch: u64,
    raft_membership_receipt_count: u64,
    metadata_replication_quorum: u64,
    metadata_replication_acks: u64,
) -> MetadataControlProof {
    let read_ready = readiness.readable
        && readiness.metadata_commit_bounds_valid
        && readiness.metadata_apply_caught_up;
    let write_ready = readiness.writable
        && readiness.metadata_commit_bounds_valid
        && readiness.metadata_apply_caught_up
        && !readiness.admission_pressure
        && !readiness.qos_pressure
        && readiness.external_coordinator_ready
        && metadata_replication_acks >= metadata_replication_quorum;

    let mut material = Vec::new();
    append_string(&mut material, "MOSRSMETA-CONTROL12");
    append_u64(&mut material, readiness.raft_term);
    append_u64(&mut material, readiness.raft_log_commit_index);
    append_u64(&mut material, readiness.applied_commit_index);
    append_u64(&mut material, readiness.min_read_index);
    append_u64(&mut material, readiness.safe_read_fence);
    append_u64(&mut material, readiness.read_fence_covered as u64);
    append_u64(&mut material, readiness.read_fence_commit_lag);
    append_u64(&mut material, readiness.read_fence_read_index_lag);
    append_u64(&mut material, readiness.read_fence_applied_lag);
    append_u64(&mut material, readiness.read_lag);
    append_u64(&mut material, readiness.raft_applied_lag);
    append_u64(
        &mut material,
        readiness.last_applied_metadata_mutation_commit_index,
    );
    append_u64(
        &mut material,
        readiness.last_applied_metadata_mutation_time_ms,
    );
    append_u64(
        &mut material,
        readiness.last_applied_metadata_mutation_age_ms,
    );
    append_u64(&mut material, readiness.metadata_apply_commit_lag);
    append_u64(&mut material, readiness.metadata_apply_caught_up as u64);
    append_u64(&mut material, readiness.readable as u64);
    append_u64(&mut material, readiness.writable as u64);
    append_u64(&mut material, readiness.caught_up as u64);
    append_u64(&mut material, readiness.ready as u64);
    append_string(&mut material, &readiness.reason);
    append_u64(&mut material, readiness.metadata_commit_bounds_valid as u64);
    append_u64(&mut material, readiness.max_retained_metadata_commit_index);
    append_u64(&mut material, readiness.total_shards);
    append_u64(&mut material, readiness.writable_shards);
    append_u64(&mut material, readiness.draining_shards);
    append_u64(&mut material, readiness.stale_owner_shards);
    append_u64(&mut material, readiness.active_owner_sessions);
    append_u64(&mut material, readiness.expired_owner_sessions);
    append_u64(&mut material, readiness.active_namespace_leases);
    append_u64(&mut material, readiness.expired_namespace_leases);
    append_u64(&mut material, readiness.open_transactions);
    append_u64(&mut material, readiness.terminal_transactions);
    append_u64(&mut material, readiness.metadata_records);
    append_u64(&mut material, readiness.deleted_metadata_records);
    append_u64(&mut material, readiness.retained_change_records);
    append_u64(&mut material, readiness.change_checkpoint_count);
    append_u64(&mut material, readiness.raft_snapshot_floor_commit_index);
    append_u64(&mut material, readiness.shard_topology_receipt_count);
    append_u64(&mut material, readiness.transaction_receipt_count);
    append_u64(&mut material, readiness.raft_membership_receipt_count);
    append_u64(
        &mut material,
        readiness.raft_leadership_fence_supported as u64,
    );
    append_u64(
        &mut material,
        readiness.raft_apply_envelope_supported as u64,
    );
    append_u64(&mut material, readiness.compacted_tombstone_count);
    append_u64(
        &mut material,
        readiness.last_tombstone_compaction_commit_index,
    );
    append_u64(
        &mut material,
        readiness.raft_snapshot_compacted_change_count,
    );
    append_u64(
        &mut material,
        readiness.raft_snapshot_compacted_batch_receipt_count,
    );
    append_u64(&mut material, readiness.change_log_retention_limit);
    append_u64(&mut material, readiness.mutation_receipt_retention_limit);
    append_u64(&mut material, readiness.retained_mutation_receipts);
    append_u64(&mut material, readiness.change_log_pressure as u64);
    append_u64(&mut material, readiness.mutation_receipt_pressure as u64);
    append_u64(&mut material, readiness.max_change_checkpoint_lag);
    append_u64(&mut material, readiness.max_batch_mutations);
    append_u64(&mut material, readiness.max_batch_value_bytes);
    append_u64(&mut material, readiness.max_open_transactions);
    append_u64(&mut material, readiness.max_namespace_leases);
    append_u64(&mut material, readiness.max_coordinator_sessions);
    append_u64(&mut material, readiness.max_shards);
    append_u64(&mut material, readiness.remaining_open_transactions);
    append_u64(&mut material, readiness.remaining_namespace_leases);
    append_u64(&mut material, readiness.remaining_coordinator_sessions);
    append_u64(&mut material, readiness.remaining_shards);
    append_u64(
        &mut material,
        readiness.open_transaction_admission_pressure as u64,
    );
    append_u64(
        &mut material,
        readiness.namespace_lease_admission_pressure as u64,
    );
    append_u64(
        &mut material,
        readiness.coordinator_session_admission_pressure as u64,
    );
    append_u64(&mut material, readiness.shard_admission_pressure as u64);
    append_u64(&mut material, readiness.admission_pressure as u64);
    append_u64(&mut material, readiness.qos_window_ms);
    append_u64(&mut material, readiness.qos_max_write_ops_per_window);
    append_u64(&mut material, readiness.qos_max_write_bytes_per_window);
    append_u64(&mut material, readiness.qos_window_start_ms);
    append_u64(&mut material, readiness.qos_write_ops_in_window);
    append_u64(&mut material, readiness.qos_write_bytes_in_window);
    append_u64(&mut material, readiness.qos_throttled_write_ops);
    append_u64(&mut material, readiness.qos_throttled_write_bytes);
    append_u64(&mut material, readiness.qos_pressure as u64);
    append_string(&mut material, &readiness.coordinator_config_checksum);
    append_string(&mut material, &readiness.coordinator_backend);
    append_string(&mut material, &readiness.coordinator_normalized_backend);
    append_string(&mut material, &readiness.coordinator_policy_format);
    append_string(&mut material, &readiness.coordinator_policy_checksum);
    append_string(&mut material, &readiness.coordinator_preferred_backend);
    append_u64(
        &mut material,
        readiness.coordinator_preferred_backend_active as u64,
    );
    append_u64(
        &mut material,
        readiness.coordinator_external_compatibility_mode as u64,
    );
    append_u64(&mut material, readiness.coordinator_production_ready as u64);
    append_string(&mut material, &readiness.coordinator_external_dependency);
    append_u64(&mut material, readiness.coordinator_config_revision);
    append_u64(&mut material, readiness.coordinator_config_endpoint_count);
    append_u64(&mut material, readiness.coordinator_config_receipt_count);
    append_u64(
        &mut material,
        readiness.external_coordinator_required as u64,
    );
    append_u64(
        &mut material,
        readiness.external_coordinator_endpoints_ready as u64,
    );
    append_u64(
        &mut material,
        readiness.external_coordinator_owner_sessions_ready as u64,
    );
    append_u64(&mut material, readiness.external_coordinator_ready as u64);
    append_u64(&mut material, route_cache.topology_epoch);
    append_string(&mut material, &route_cache.topology_checksum);
    append_string(&mut material, &route_cache.shard_topology_checksum);
    append_string(&mut material, &route_cache.session_checksum);
    append_u64(&mut material, route_cache.capacity);
    append_u64(&mut material, route_cache.ttl_ms);
    append_u64(&mut material, route_cache.entries);
    append_u64(&mut material, route_cache.fresh_entries);
    append_u64(&mut material, route_cache.expired_entries);
    append_u64(&mut material, route_cache.stale_entries);
    append_u64(&mut material, route_cache.pressure as u64);
    append_u64(&mut material, raft_membership_epoch);
    append_u64(&mut material, raft_membership_receipt_count);
    append_u64(&mut material, metadata_replication_quorum);
    append_u64(&mut material, metadata_replication_acks);
    append_string(&mut material, &readiness.metadata_state_fingerprint);
    append_u64(&mut material, read_ready as u64);
    append_u64(&mut material, write_ready as u64);

    MetadataControlProof {
        state_format: "MOSRSMETA-CONTROL12".to_string(),
        state_bytes: material.len() as u64,
        state_checksum: etag(&material),
        read_ready,
        write_ready,
        commit_bounds_valid: readiness.metadata_commit_bounds_valid,
        admission_pressure: readiness.admission_pressure,
        qos_pressure: readiness.qos_pressure,
        topology_checksum: route_cache.shard_topology_checksum.clone(),
        session_checksum: route_cache.session_checksum.clone(),
        coordinator_config_checksum: readiness.coordinator_config_checksum.clone(),
        coordinator_backend: readiness.coordinator_backend.clone(),
        coordinator_normalized_backend: readiness.coordinator_normalized_backend.clone(),
        coordinator_policy_format: readiness.coordinator_policy_format.clone(),
        coordinator_policy_checksum: readiness.coordinator_policy_checksum.clone(),
        coordinator_preferred_backend: readiness.coordinator_preferred_backend.clone(),
        coordinator_preferred_backend_active: readiness.coordinator_preferred_backend_active,
        coordinator_external_compatibility_mode: readiness.coordinator_external_compatibility_mode,
        coordinator_production_ready: readiness.coordinator_production_ready,
        coordinator_external_dependency: readiness.coordinator_external_dependency.clone(),
        coordinator_config_revision: readiness.coordinator_config_revision,
        coordinator_config_endpoint_count: readiness.coordinator_config_endpoint_count,
        coordinator_config_receipt_count: readiness.coordinator_config_receipt_count,
        external_coordinator_required: readiness.external_coordinator_required,
        external_coordinator_endpoints_ready: readiness.external_coordinator_endpoints_ready,
        external_coordinator_owner_sessions_ready: readiness
            .external_coordinator_owner_sessions_ready,
        external_coordinator_ready: readiness.external_coordinator_ready,
        state_fingerprint: readiness.metadata_state_fingerprint.clone(),
        active_owner_sessions: readiness.active_owner_sessions,
        expired_owner_sessions: readiness.expired_owner_sessions,
        writable_shards: readiness.writable_shards,
        draining_shards: readiness.draining_shards,
        stale_owner_shards: readiness.stale_owner_shards,
        raft_membership_epoch,
        raft_membership_receipt_count,
        metadata_replication_quorum,
        metadata_replication_acks,
        retained_mutation_receipts: readiness.retained_mutation_receipts,
        change_log_pressure: readiness.change_log_pressure,
        mutation_receipt_pressure: readiness.mutation_receipt_pressure,
        max_change_checkpoint_lag: readiness.max_change_checkpoint_lag,
        remaining_open_transactions: readiness.remaining_open_transactions,
        remaining_namespace_leases: readiness.remaining_namespace_leases,
        remaining_coordinator_sessions: readiness.remaining_coordinator_sessions,
        remaining_shards: readiness.remaining_shards,
        open_transaction_admission_pressure: readiness.open_transaction_admission_pressure,
        namespace_lease_admission_pressure: readiness.namespace_lease_admission_pressure,
        coordinator_session_admission_pressure: readiness.coordinator_session_admission_pressure,
        shard_admission_pressure: readiness.shard_admission_pressure,
        last_applied_mutation_commit_index: readiness.last_applied_metadata_mutation_commit_index,
        last_applied_mutation_time_ms: readiness.last_applied_metadata_mutation_time_ms,
        last_applied_mutation_age_ms: readiness.last_applied_metadata_mutation_age_ms,
        apply_commit_lag: readiness.metadata_apply_commit_lag,
        apply_caught_up: readiness.metadata_apply_caught_up,
        requested_read_fence: readiness.min_read_index,
        safe_read_fence: readiness.safe_read_fence,
        read_fence_covered: readiness.read_fence_covered,
        read_fence_commit_lag: readiness.read_fence_commit_lag,
        read_fence_read_index_lag: readiness.read_fence_read_index_lag,
        read_fence_applied_lag: readiness.read_fence_applied_lag,
    }
}
