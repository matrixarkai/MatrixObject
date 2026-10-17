fn append_metadata_readiness(mut material: &mut Vec<u8>, report: &ObjectStoreDistributedReadiness) {
    append_u64(&mut material, report.metadata.applied_commit_index);
    append_u64(&mut material, report.metadata.raft_log_commit_index);
    append_u64(&mut material, report.metadata.min_read_index);
    append_u64(&mut material, report.metadata.safe_read_fence);
    append_u64(&mut material, report.metadata.read_fence_covered as u64);
    append_u64(&mut material, report.metadata.read_fence_commit_lag);
    append_u64(&mut material, report.metadata.read_fence_read_index_lag);
    append_u64(&mut material, report.metadata.read_fence_applied_lag);
    append_string(&mut material, &report.metadata.metadata_state_fingerprint);
    append_u64(
        &mut material,
        report.metadata.metadata_commit_bounds_valid as u64,
    );
    append_u64(
        &mut material,
        report.metadata.max_retained_metadata_commit_index,
    );
    append_u64(&mut material, report.metadata.read_lag);
    append_u64(&mut material, report.metadata.raft_applied_lag);
    append_u64(&mut material, report.metadata.shard_topology_receipt_count);
    append_u64(&mut material, report.metadata.transaction_receipt_count);
    append_u64(&mut material, report.metadata.raft_membership_receipt_count);
    append_u64(&mut material, report.metadata.total_shards);
    append_u64(&mut material, report.metadata.writable_shards);
    append_u64(&mut material, report.metadata.draining_shards);
    append_u64(&mut material, report.metadata.stale_owner_shards);
    append_u64(&mut material, report.metadata.active_owner_sessions);
    append_u64(&mut material, report.metadata.expired_owner_sessions);
    append_u64(&mut material, report.metadata.active_namespace_leases);
    append_u64(&mut material, report.metadata.expired_namespace_leases);
    append_u64(&mut material, report.metadata.open_transactions);
    append_u64(&mut material, report.metadata.terminal_transactions);
    append_u64(&mut material, report.metadata.metadata_records);
    append_u64(&mut material, report.metadata.deleted_metadata_records);
    append_u64(&mut material, report.metadata.retained_change_records);
    append_u64(
        &mut material,
        report.metadata.first_retained_change_sequence,
    );
    append_u64(&mut material, report.metadata.last_retained_change_sequence);
    append_u64(
        &mut material,
        report.metadata.first_retained_metadata_commit_index,
    );
    append_u64(
        &mut material,
        report.metadata.last_retained_metadata_commit_index,
    );
    append_u64(&mut material, report.metadata.change_checkpoint_count);
    append_u64(
        &mut material,
        report.metadata.raft_snapshot_floor_commit_index,
    );
    append_u64(&mut material, report.metadata.compacted_tombstone_count);
    append_u64(
        &mut material,
        report.metadata.last_tombstone_compaction_commit_index,
    );
    append_u64(
        &mut material,
        report.metadata.raft_snapshot_compacted_change_count,
    );
    append_u64(
        &mut material,
        report.metadata.raft_snapshot_compacted_batch_receipt_count,
    );
    append_u64(&mut material, report.metadata.max_batch_mutations);
    append_u64(&mut material, report.metadata.max_batch_value_bytes);
    append_u64(&mut material, report.metadata.max_open_transactions);
    append_u64(&mut material, report.metadata.max_namespace_leases);
    append_u64(&mut material, report.metadata.max_coordinator_sessions);
    append_u64(&mut material, report.metadata.max_shards);
    append_u64(&mut material, report.metadata.admission_pressure as u64);
}
