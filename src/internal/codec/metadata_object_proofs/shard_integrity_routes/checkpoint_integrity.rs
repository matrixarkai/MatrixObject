pub(crate) fn metadata_change_payload_bytes(change: &MetadataChange) -> u64 {
    change.key.len().saturating_add(40) as u64
}

pub(crate) fn metadata_change_checkpoint_lag_payload_bytes(
    checkpoint: &MetadataChangeCheckpoint,
) -> u64 {
    checkpoint
        .consumer_id
        .len()
        .saturating_add(checkpoint.prefix.len())
        .saturating_add(checkpoint.owner.len())
        .saturating_add(checkpoint.note.len())
        .saturating_add(112) as u64
}

pub(crate) fn metadata_change_checkpoint_lag_plan_checksum(
    plan: &MetadataChangeCheckpointLagPlan,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-META-CHANGE-CHECKPOINT-LAG-PLAN1");
    append_string(&mut material, &plan.prefix);
    append_string(&mut material, plan.marker.as_deref().unwrap_or(""));
    append_string(&mut material, plan.next_marker.as_deref().unwrap_or(""));
    append_u64(&mut material, plan.scanned_checkpoints);
    append_u64(&mut material, plan.matched_checkpoints);
    append_u64(&mut material, plan.checkpoint_count);
    append_u64(&mut material, plan.target_change_sequence);
    append_u64(&mut material, plan.target_commit_index);
    append_u64(&mut material, plan.first_retained_change_sequence);
    append_u64(&mut material, plan.last_retained_change_sequence);
    append_u64(&mut material, plan.first_retained_commit_index);
    append_u64(&mut material, plan.last_retained_commit_index);
    append_u64(&mut material, plan.compacted_change_count);
    append_u64(&mut material, plan.max_change_lag);
    append_u64(&mut material, plan.max_commit_lag);
    append_u64(&mut material, plan.retained_window_compacted_count);
    append_u64(&mut material, plan.payload_bytes);
    append_u64(&mut material, plan.max_payload_bytes);
    append_u64(&mut material, plan.budget_limited as u64);
    append_u64(&mut material, plan.truncated as u64);
    append_u64(&mut material, plan.lag_plan_needed as u64);
    append_u64(&mut material, plan.lag_plan_can_apply as u64);
    append_string(&mut material, &plan.lag_plan_reason);
    append_u64(&mut material, plan.items.len() as u64);
    for item in &plan.items {
        append_metadata_change_checkpoint(&mut material, &item.checkpoint);
        append_u64(&mut material, item.change_lag);
        append_u64(&mut material, item.commit_lag);
        append_u64(&mut material, item.behind_target as u64);
        append_u64(&mut material, item.retained_window_compacted as u64);
        append_u64(&mut material, item.first_retained_change_sequence);
        append_u64(&mut material, item.first_retained_commit_index);
        append_u64(&mut material, item.target_change_sequence);
        append_u64(&mut material, item.target_commit_index);
    }
    etag(&material)
}

pub(crate) fn metadata_integrity_plan_checksum(plan: &MetadataIntegrityPlan) -> (String, u64) {
    let mut material = Vec::new();
    append_string(&mut material, &plan.plan_format);
    append_u64(&mut material, plan.now_ms);
    append_u64(&mut material, plan.max_sample_count as u64);
    append_string(&mut material, &plan.tombstone_prefix);
    append_u64(&mut material, plan.min_read_index);
    append_u64(&mut material, plan.readiness.raft_log_commit_index);
    append_u64(&mut material, plan.readiness.applied_commit_index);
    append_u64(
        &mut material,
        plan.readiness.last_applied_metadata_mutation_commit_index,
    );
    append_string(&mut material, &plan.readiness.metadata_state_fingerprint);
    append_string(&mut material, &plan.control_proof.state_checksum);
    append_string(&mut material, &plan.control_proof.topology_checksum);
    append_string(&mut material, &plan.control_proof.session_checksum);
    append_u64(&mut material, plan.live_record_count);
    append_u64(&mut material, plan.deleted_record_count);
    append_u64(&mut material, plan.compactable_tombstone_count);
    append_u64(&mut material, plan.active_session_count);
    append_u64(&mut material, plan.expired_session_count);
    append_u64(&mut material, plan.active_lease_count);
    append_u64(&mut material, plan.expired_lease_count);
    append_u64(&mut material, plan.orphaned_lease_owner_count);
    append_u64(&mut material, plan.open_transaction_count);
    append_u64(&mut material, plan.expired_transaction_count);
    append_u64(&mut material, plan.writable_shard_count);
    append_u64(&mut material, plan.draining_shard_count);
    append_u64(&mut material, plan.stale_owner_shard_count);
    append_u64(&mut material, plan.change_log_count);
    append_u64(&mut material, plan.change_checkpoint_count);
    append_u64(&mut material, plan.safe_change_sequence);
    append_u64(&mut material, plan.compactable_change_count);
    append_u64(&mut material, plan.route_cache_entry_count);
    append_u64(&mut material, plan.route_cache_stale_entries);
    append_u64(&mut material, plan.route_cache_expired_entries);
    append_u64(&mut material, plan.needs_owner_failover as u64);
    append_u64(&mut material, plan.needs_session_expiry as u64);
    append_u64(&mut material, plan.needs_lease_expiry as u64);
    append_u64(&mut material, plan.needs_transaction_gc as u64);
    append_u64(&mut material, plan.needs_tombstone_compaction as u64);
    append_u64(&mut material, plan.needs_changefeed_compaction as u64);
    append_u64(&mut material, plan.needs_route_cache_eviction as u64);
    append_u64(&mut material, plan.needs_snapshot_compaction as u64);
    append_u64(&mut material, plan.needs_read_repair as u64);
    append_u64(&mut material, plan.needs_action as u64);
    append_u64(&mut material, plan.stale_owner_shards.len() as u64);
    for shard in &plan.stale_owner_shards {
        append_string(&mut material, &shard.shard_id);
        append_string(&mut material, &shard.owner_node_id);
        append_u64(&mut material, shard.epoch);
    }
    append_u64(&mut material, plan.expired_sessions.len() as u64);
    for session in &plan.expired_sessions {
        append_string(&mut material, &session.session_id);
        append_string(&mut material, &session.owner);
        append_u64(&mut material, session.epoch);
        append_u64(&mut material, session.expire_time_ms);
    }
    append_u64(&mut material, plan.expired_leases.len() as u64);
    for lease in &plan.expired_leases {
        append_string(&mut material, &lease.namespace);
        append_string(&mut material, &lease.owner);
        append_u64(&mut material, lease.epoch);
        append_u64(&mut material, lease.expire_time_ms);
    }
    append_u64(&mut material, plan.expired_transactions.len() as u64);
    for transaction in &plan.expired_transactions {
        append_string(&mut material, &transaction.transaction_id);
        append_string(&mut material, &transaction.owner);
        append_u64(&mut material, transaction.expire_time_ms);
    }
    append_u64(&mut material, plan.compactable_tombstones.len() as u64);
    for record in &plan.compactable_tombstones {
        append_string(&mut material, &record.key);
        append_u64(&mut material, record.version);
        append_u64(&mut material, record.deleted as u64);
    }
    (etag(&material), material.len() as u64)
}

