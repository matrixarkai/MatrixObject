pub(crate) fn metadata_maintenance_plan_from_prepared(
    prepared: PreparedMetadataMaintenance,
    readiness: MetadataRaftReadiness,
) -> MetadataMaintenancePlan {
    metadata_maintenance_plan_from_prepared_fenced(prepared, readiness, "")
}

pub(crate) fn metadata_maintenance_plan_from_prepared_fenced(
    prepared: PreparedMetadataMaintenance,
    readiness: MetadataRaftReadiness,
    expected_plan_checksum: &str,
) -> MetadataMaintenancePlan {
    let planned_lease_expirations = prepared.lease_report.expired_leases;
    let planned_session_expirations = prepared.expired_sessions.len() as u64;
    let planned_transaction_expirations = prepared.expired_transactions.len() as u64;
    let planned_tombstone_compactions = prepared.tombstone_report.compacted_records;
    let planned_tombstone_compaction_bytes = prepared.tombstone_report.compacted_bytes;
    let planned_route_cache_evictions = prepared.route_cache_status.stale_entries;
    let planned_actions = planned_lease_expirations
        .saturating_add(planned_session_expirations)
        .saturating_add(planned_transaction_expirations)
        .saturating_add(planned_tombstone_compactions)
        .saturating_add(planned_route_cache_evictions);
    let needs_maintenance = planned_actions > 0;
    let can_apply = needs_maintenance
        && readiness.metadata_commit_bounds_valid
        && readiness.metadata_apply_caught_up
        && readiness.caught_up
        && readiness.writable;
    let reason = if !needs_maintenance {
        "no_metadata_maintenance_needed"
    } else if !readiness.metadata_commit_bounds_valid {
        "metadata_commit_bounds_exceed_applied_index"
    } else if !readiness.metadata_apply_caught_up {
        "metadata_apply_lag"
    } else if !readiness.caught_up {
        "raft_apply_lag"
    } else if !readiness.writable {
        "stale_shard_owner_session"
    } else {
        "metadata_maintenance_ready"
    }
    .to_string();
    let maintenance_actions =
        metadata_maintenance_actions_from_prepared(&prepared, &readiness, planned_actions);
    let continuation = metadata_maintenance_continuation(
        &prepared.lease_report,
        &prepared.expired_sessions,
        &prepared.expired_transactions,
        &prepared.tombstone_report,
        prepared.lease_limit,
        prepared.session_limit,
        prepared.transaction_limit,
        prepared.payload_budget_limited,
        &prepared.payload_budget_phase,
        &prepared.payload_budget_marker,
    );
    let mut plan_material = Vec::new();
    append_string(&mut plan_material, &prepared.raft_entry_checksum);
    append_u64(&mut plan_material, continuation.has_more as u64);
    append_u64(&mut plan_material, continuation.continuation_safe as u64);
    append_u64(
        &mut plan_material,
        continuation.payload_budget_limited as u64,
    );
    append_string(&mut plan_material, &continuation.phase);
    append_string(&mut plan_material, &continuation.marker);
    append_string(&mut plan_material, &continuation.reason);
    append_u64(
        &mut plan_material,
        continuation.tombstone_marker_advanced as u64,
    );
    append_u64(
        &mut plan_material,
        continuation.lease_marker_advanced as u64,
    );
    append_u64(
        &mut plan_material,
        continuation.coordinator_session_marker_advanced as u64,
    );
    append_u64(
        &mut plan_material,
        continuation.transaction_marker_advanced as u64,
    );
    let plan_checksum = etag(&plan_material);
    MetadataMaintenancePlan {
        now_ms: prepared.now_ms,
        lease_prefix: prepared.lease_prefix.clone(),
        lease_limit: prepared.lease_limit,
        session_limit: prepared.session_limit,
        transaction_limit: prepared.transaction_limit,
        expected_previous_commit_index: prepared.expected_previous_commit_index,
        prepared_commit_index: prepared.prepared_commit_index,
        planned_actions,
        planned_lease_expirations,
        planned_session_expirations,
        planned_transaction_expirations,
        planned_tombstone_compactions,
        planned_tombstone_compaction_bytes,
        planned_route_cache_evictions,
        route_cache_entries: prepared.route_cache_status.entries,
        route_cache_stale_entries: prepared.route_cache_status.stale_entries,
        route_cache_expired_entries: prepared.route_cache_status.expired_entries,
        needs_maintenance,
        can_apply,
        reason,
        plan_checksum: plan_checksum.clone(),
        expected_plan_checksum: expected_plan_checksum.to_string(),
        actual_plan_checksum: plan_checksum,
        readiness,
        maintenance_actions,
        continuation,
        prepared,
    }
}
