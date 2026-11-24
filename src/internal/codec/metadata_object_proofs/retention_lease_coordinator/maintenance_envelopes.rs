// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
pub(crate) fn append_metadata_route_cache_status(
    material: &mut Vec<u8>,
    status: &MetadataRouteCacheStatus,
) {
    append_u64(material, status.entries);
    append_u64(material, status.capacity);
    append_u64(material, status.ttl_ms);
    append_u64(material, status.hits);
    append_u64(material, status.misses);
    append_u64(material, status.invalidations);
    append_u64(material, status.expired_invalidations);
    append_u64(material, status.capacity_evictions);
    append_u64(material, status.pressure as u64);
    append_u64(material, status.topology_epoch);
    append_string(material, &status.topology_checksum);
    append_string(material, &status.shard_topology_checksum);
    append_string(material, &status.session_checksum);
    append_u64(material, status.fresh_entries);
    append_u64(material, status.expired_entries);
    append_u64(material, status.stale_topology_entries);
    append_u64(material, status.stale_session_entries);
    append_u64(material, status.stale_entries);
    append_u64(material, status.fresh as u64);
}

pub(crate) fn append_metadata_maintenance_action(
    material: &mut Vec<u8>,
    action: &MetadataMaintenanceAction,
) {
    append_string(material, &action.action);
    append_string(material, &action.reason);
    append_u64(material, action.priority);
    append_string(material, &action.key);
    append_string(material, &action.marker);
    append_u64(material, action.estimated_items);
    append_u64(material, action.estimated_bytes);
}

pub(crate) fn prepared_metadata_maintenance_checksum(
    prepared: &PreparedMetadataMaintenance,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-PREPARED-META-MAINTENANCE7");
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    append_u64(&mut material, prepared.now_ms);
    append_string(&mut material, &prepared.lease_prefix);
    append_u64(&mut material, prepared.lease_limit as u64);
    append_u64(&mut material, prepared.session_limit as u64);
    append_u64(&mut material, prepared.transaction_limit as u64);
    append_u64(&mut material, prepared.max_payload_bytes);
    append_u64(&mut material, prepared.maintenance_payload_bytes);
    append_u64(&mut material, prepared.payload_budget_limited as u64);
    append_string(&mut material, &prepared.payload_budget_phase);
    append_string(&mut material, &prepared.payload_budget_marker);
    append_u64(&mut material, prepared.metadata_read_index);
    append_u64(&mut material, prepared.metadata_raft_term);
    append_string(&mut material, &prepared.metadata_control_proof_format);
    append_string(&mut material, &prepared.metadata_control_proof_checksum);
    append_u64(&mut material, prepared.metadata_control_proof_bytes);
    append_string(&mut material, &prepared.request_checksum);
    append_u64(&mut material, prepared.lease_report.scanned_leases);
    append_u64(&mut material, prepared.lease_report.expired_leases);
    append_u64(&mut material, prepared.lease_report.skipped_leases);
    append_string(&mut material, &prepared.lease_report.prefix);
    append_u64(&mut material, prepared.lease_report.limit as u64);
    append_u64(&mut material, prepared.lease_report.expired.len() as u64);
    for lease in &prepared.lease_report.expired {
        append_metadata_lease(&mut material, lease);
    }
    append_u64(&mut material, prepared.expired_sessions.len() as u64);
    for session in &prepared.expired_sessions {
        append_coordinator_session(&mut material, session);
    }
    append_u64(&mut material, prepared.expired_transactions.len() as u64);
    for transaction in &prepared.expired_transactions {
        append_metadata_transaction(&mut material, transaction);
    }
    append_metadata_tombstone_compaction_report(&mut material, &prepared.tombstone_report);
    append_u64(&mut material, prepared.compacted_tombstones.len() as u64);
    for record in &prepared.compacted_tombstones {
        append_metadata_record(&mut material, record);
    }
    append_metadata_route_cache_status(&mut material, &prepared.route_cache_status);
    let maintenance_actions = metadata_maintenance_work_actions_from_prepared(prepared);
    append_u64(&mut material, maintenance_actions.len() as u64);
    for action in &maintenance_actions {
        append_metadata_maintenance_action(&mut material, action);
    }
    etag(&material)
}

