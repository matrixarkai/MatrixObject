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

pub(crate) fn append_prepared_metadata_maintenance_envelope(
    out: &mut Vec<u8>,
    envelope: &PreparedMetadataMaintenanceEnvelope,
) {
    append_u64(out, envelope.now_ms);
    append_string(out, &envelope.lease_prefix);
    append_u64(out, envelope.lease_limit);
    append_u64(out, envelope.session_limit);
    append_u64(out, envelope.transaction_limit);
    append_u64(out, envelope.max_payload_bytes);
    append_u64(out, envelope.maintenance_payload_bytes);
    append_u64(out, envelope.payload_budget_limited as u64);
    append_string(out, &envelope.payload_budget_phase);
    append_string(out, &envelope.payload_budget_marker);
    append_u64(out, envelope.planned_lease_expirations);
    append_u64(out, envelope.planned_session_expirations);
    append_u64(out, envelope.planned_transaction_expirations);
    append_u64(out, envelope.planned_tombstone_compactions);
    append_u64(out, envelope.planned_tombstone_compaction_bytes);
    append_u64(out, envelope.route_cache_entries);
    append_u64(out, envelope.route_cache_stale_entries);
    append_u64(out, envelope.route_cache_expired_entries);
    append_string(out, &envelope.first_lease_namespace);
    append_string(out, &envelope.first_session_id);
    append_string(out, &envelope.first_transaction_id);
    append_string(out, &envelope.first_tombstone_key);
    append_string(out, &envelope.request_checksum);
    append_u64(out, envelope.expected_previous_commit_index);
    append_u64(out, envelope.prepared_commit_index);
    append_u64(out, envelope.metadata_read_index);
    append_u64(out, envelope.metadata_raft_term);
    append_string(out, &envelope.metadata_control_proof_format);
    append_string(out, &envelope.metadata_control_proof_checksum);
    append_u64(out, envelope.metadata_control_proof_bytes);
    append_string(out, &envelope.raft_entry_checksum);
}

pub(crate) fn prepared_metadata_maintenance_envelope_checksum(
    envelope: &PreparedMetadataMaintenanceEnvelope,
) -> String {
    let mut material = Vec::new();
    append_string(
        &mut material,
        "MOS-RUST-PREPARED-META-MAINTENANCE-ENVELOPE4",
    );
    append_prepared_metadata_maintenance_envelope(&mut material, envelope);
    etag(&material)
}

pub(crate) fn prepared_metadata_maintenance_envelope(
    prepared: &PreparedMetadataMaintenance,
) -> PreparedMetadataMaintenanceEnvelope {
    let mut envelope = PreparedMetadataMaintenanceEnvelope {
        now_ms: prepared.now_ms,
        lease_prefix: prepared.lease_prefix.clone(),
        lease_limit: prepared.lease_limit as u64,
        session_limit: prepared.session_limit as u64,
        transaction_limit: prepared.transaction_limit as u64,
        max_payload_bytes: prepared.max_payload_bytes,
        maintenance_payload_bytes: prepared.maintenance_payload_bytes,
        payload_budget_limited: prepared.payload_budget_limited,
        payload_budget_phase: prepared.payload_budget_phase.clone(),
        payload_budget_marker: prepared.payload_budget_marker.clone(),
        planned_lease_expirations: prepared.lease_report.expired_leases,
        planned_session_expirations: prepared.expired_sessions.len() as u64,
        planned_transaction_expirations: prepared.expired_transactions.len() as u64,
        planned_tombstone_compactions: prepared.tombstone_report.compacted_records,
        planned_tombstone_compaction_bytes: prepared.tombstone_report.compacted_bytes,
        route_cache_entries: prepared.route_cache_status.entries,
        route_cache_stale_entries: prepared.route_cache_status.stale_entries,
        route_cache_expired_entries: prepared.route_cache_status.expired_entries,
        first_lease_namespace: prepared
            .lease_report
            .expired
            .first()
            .map(|lease| lease.namespace.clone())
            .unwrap_or_default(),
        first_session_id: prepared
            .expired_sessions
            .first()
            .map(|session| session.session_id.clone())
            .unwrap_or_default(),
        first_transaction_id: prepared
            .expired_transactions
            .first()
            .map(|transaction| transaction.transaction_id.clone())
            .unwrap_or_default(),
        first_tombstone_key: prepared
            .compacted_tombstones
            .first()
            .map(|record| record.key.clone())
            .unwrap_or_default().to_string(),
        request_checksum: prepared.request_checksum.clone(),
        expected_previous_commit_index: prepared.expected_previous_commit_index,
        prepared_commit_index: prepared.prepared_commit_index,
        metadata_read_index: prepared.metadata_read_index,
        metadata_raft_term: prepared.metadata_raft_term,
        metadata_control_proof_format: prepared.metadata_control_proof_format.clone(),
        metadata_control_proof_checksum: prepared.metadata_control_proof_checksum.clone(),
        metadata_control_proof_bytes: prepared.metadata_control_proof_bytes,
        raft_entry_checksum: prepared.raft_entry_checksum.clone(),
        envelope_checksum: String::new(),
    };
    envelope.envelope_checksum = prepared_metadata_maintenance_envelope_checksum(&envelope);
    envelope
}

