pub(crate) fn metadata_maintenance_actions_from_prepared(
    prepared: &PreparedMetadataMaintenance,
    readiness: &MetadataRaftReadiness,
    planned_actions: u64,
) -> Vec<MetadataMaintenanceAction> {
    let mut actions = metadata_maintenance_work_actions_from_prepared(prepared);
    if planned_actions > 0 && !readiness.writable {
        actions.push(MetadataMaintenanceAction {
            action: "restore_meta_raft_write_quorum".to_string(),
            reason: "metadata_raft_not_writable".to_string(),
            priority: 100,
            key: String::new(),
            marker: readiness.applied_commit_index.to_string(),
            estimated_items: planned_actions,
            estimated_bytes: 0,
        });
    }
    if planned_actions > 0 && !readiness.caught_up {
        actions.push(MetadataMaintenanceAction {
            action: "refresh_meta_raft_read_index".to_string(),
            reason: "metadata_raft_read_index_lag".to_string(),
            priority: 95,
            key: String::new(),
            marker: readiness.min_read_index.to_string(),
            estimated_items: planned_actions,
            estimated_bytes: 0,
        });
    }
    sort_metadata_maintenance_actions(&mut actions);
    actions
}

pub(crate) fn metadata_maintenance_work_actions_from_prepared(
    prepared: &PreparedMetadataMaintenance,
) -> Vec<MetadataMaintenanceAction> {
    let mut actions = Vec::new();
    if let Some(lease) = prepared.lease_report.expired.first() {
        actions.push(MetadataMaintenanceAction {
            action: "expire_meta_leases".to_string(),
            reason: "metadata_lease_expired".to_string(),
            priority: 80,
            key: lease.namespace.clone(),
            marker: String::new(),
            estimated_items: prepared.lease_report.expired_leases,
            estimated_bytes: 0,
        });
    }
    if let Some(session) = prepared.expired_sessions.first() {
        actions.push(MetadataMaintenanceAction {
            action: "expire_meta_coordinator_sessions".to_string(),
            reason: "metadata_coordinator_session_expired".to_string(),
            priority: 75,
            key: session.session_id.clone(),
            marker: String::new(),
            estimated_items: prepared.expired_sessions.len() as u64,
            estimated_bytes: 0,
        });
    }
    if let Some(transaction) = prepared.expired_transactions.first() {
        actions.push(MetadataMaintenanceAction {
            action: "gc_meta_transactions".to_string(),
            reason: "metadata_transaction_gc_needed".to_string(),
            priority: 65,
            key: transaction.transaction_id.clone(),
            marker: String::new(),
            estimated_items: prepared.expired_transactions.len() as u64,
            estimated_bytes: 0,
        });
    }
    if let Some(record) = prepared.compacted_tombstones.first() {
        actions.push(MetadataMaintenanceAction {
            action: "compact_meta_tombstones".to_string(),
            reason: if prepared.tombstone_report.budget_limited {
                "metadata_tombstone_compaction_budget_limited".to_string()
            } else {
                "metadata_tombstone_compaction_needed".to_string()
            },
            priority: 70,
            key: record.key.to_string(),
            marker: prepared
                .tombstone_report
                .next_key_marker
                .clone()
                .unwrap_or_default(),
            estimated_items: prepared.tombstone_report.compacted_records,
            estimated_bytes: prepared.tombstone_report.compacted_bytes,
        });
    }
    if prepared.route_cache_status.stale_entries > 0 {
        actions.push(MetadataMaintenanceAction {
            action: "evict_stale_meta_route_cache".to_string(),
            reason: "metadata_route_cache_stale".to_string(),
            priority: 50,
            key: String::new(),
            marker: String::new(),
            estimated_items: prepared.route_cache_status.stale_entries,
            estimated_bytes: 0,
        });
    }
    sort_metadata_maintenance_actions(&mut actions);
    actions
}
