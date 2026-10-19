// SPDX-License-Identifier: Apache-2.0
pub(crate) fn append_prepared_storage_maintenance_envelope(
    out: &mut Vec<u8>,
    envelope: &PreparedStorageMaintenanceEnvelope,
) {
    append_string(out, &envelope.envelope_format);
    append_u64(out, envelope.max_actions);
    append_u64(out, envelope.min_capacity_free_bytes);
    append_u64(out, envelope.min_reclaimable_bytes);
    append_u64(out, envelope.max_reclaim_chunks);
    append_u64(out, envelope.max_rewrite_bytes);
    append_u64(out, envelope.min_discarded_bytes);
    append_u64(out, envelope.min_discard_ratio_ppm);
    append_u64(out, envelope.max_discard_ranges);
    append_u64(out, envelope.max_trim_bytes);
    append_u64(out, envelope.now_ms);
    append_string(out, &envelope.plan_checksum);
    append_string(out, &envelope.page_state_fingerprint);
    append_string(out, &envelope.route_control_proof_format);
    append_string(out, &envelope.route_control_proof_checksum);
    append_u64(out, envelope.route_control_proof_bytes);
    append_u64(out, envelope.route_control_commit_index);
    append_u64(out, envelope.route_control_next_log_index);
    append_u64(out, envelope.selected_action_count);
    append_u64(out, envelope.planned_repair_actions);
    append_u64(out, envelope.planned_reclaim_chunks);
    append_u64(out, envelope.scheduled_reclaim_chunks);
    append_u64(out, envelope.scheduled_rewrite_bytes);
    append_u64(out, envelope.scheduled_trim_ops);
    append_u64(out, envelope.scheduled_trim_bytes);
    append_u64(out, envelope.candidate_discarded_bytes);
    append_u64(out, envelope.planned_chunk_discards);
    append_u64(out, envelope.planned_chunk_discard_bytes);
    append_u64(out, envelope.planned_chunk_compaction_fallbacks);
    append_u64(out, envelope.budget_limited as u64);
    append_u64(out, envelope.trim_flow_limited as u64);
    append_u64(out, envelope.truncated as u64);
    append_u64(out, envelope.reclaim_continuation_safe as u64);
    append_u64(out, envelope.reclaim_next_marker_advanced as u64);
    append_u64(out, envelope.capacity_pressure as u64);
    append_u64(out, envelope.reclaim_pressure as u64);
    append_u64(out, envelope.trim_flow_pressure as u64);
    append_u64(out, envelope.quorum_pressure as u64);
    append_string(out, &envelope.first_action);
    append_string(out, &envelope.first_action_reason);
    append_u64(out, envelope.first_replica_index.is_some() as u64);
    append_u64(out, envelope.first_replica_index.unwrap_or_default() as u64);
    append_u64(out, envelope.first_chunk_id.is_some() as u64);
    if let Some(page_id) = &envelope.first_chunk_id {
        append_page_id(out, page_id);
    }
    append_u64(out, envelope.first_estimated_bytes);
}

pub(crate) fn prepared_storage_maintenance_envelope_checksum(
    envelope: &PreparedStorageMaintenanceEnvelope,
) -> String {
    let mut material = Vec::new();
    append_prepared_storage_maintenance_envelope(&mut material, envelope);
    etag(&material)
}

pub(crate) fn make_prepared_storage_maintenance_envelope(
    plan: &StorageMaintenancePlan,
    max_actions: usize,
    min_capacity_free_bytes: u64,
    min_reclaimable_bytes: u64,
    max_reclaim_chunks: usize,
    max_rewrite_bytes: u64,
    min_discarded_bytes: u64,
    min_discard_ratio_ppm: u64,
    max_discard_ranges: usize,
    max_trim_bytes: u64,
    now_ms: u64,
) -> PreparedStorageMaintenanceEnvelope {
    let first_action = plan.actions.first();
    let planned_repair_actions = plan
        .actions
        .iter()
        .filter(|action| {
            action.action.contains("repair")
                || action.action.contains("restore")
                || action.action.contains("quorum")
        })
        .count() as u64;
    let mut envelope = PreparedStorageMaintenanceEnvelope {
        envelope_format: "MXOSBLOCK-STORAGE-MAINTENANCE-ENVELOPE1".to_string(),
        max_actions: max_actions as u64,
        min_capacity_free_bytes,
        min_reclaimable_bytes,
        max_reclaim_chunks: max_reclaim_chunks as u64,
        max_rewrite_bytes,
        min_discarded_bytes,
        min_discard_ratio_ppm,
        max_discard_ranges: max_discard_ranges as u64,
        max_trim_bytes,
        now_ms,
        plan_checksum: plan.plan_checksum.clone(),
        page_state_fingerprint: plan.readiness.page_state_fingerprint.clone(),
        route_control_proof_format: plan.readiness.route_control_proof.state_format.clone(),
        route_control_proof_checksum: plan.readiness.route_control_proof.state_checksum.clone(),
        route_control_proof_bytes: plan.readiness.route_control_proof.state_bytes,
        route_control_commit_index: plan
            .readiness
            .route_control_proof
            .chunk_route_control_commit_index,
        route_control_next_log_index: plan
            .readiness
            .route_control_proof
            .chunk_route_control_next_log_index,
        selected_action_count: plan.actions.len() as u64,
        planned_repair_actions,
        planned_reclaim_chunks: plan.reclaim.candidate_chunks,
        scheduled_reclaim_chunks: plan.reclaim.scheduled_chunks,
        scheduled_rewrite_bytes: plan.reclaim.scheduled_rewrite_bytes,
        scheduled_trim_ops: plan.reclaim.scheduled_trim_ops,
        scheduled_trim_bytes: plan.reclaim.scheduled_trim_bytes,
        candidate_discarded_bytes: plan.reclaim.candidate_discarded_bytes,
        planned_chunk_discards: plan
            .chunk_discards
            .iter()
            .filter(|discard| !discard.compact_instead)
            .count() as u64,
        planned_chunk_discard_bytes: plan
            .chunk_discards
            .iter()
            .filter(|discard| !discard.compact_instead)
            .map(|discard| discard.discard_bytes)
            .sum(),
        planned_chunk_compaction_fallbacks: plan
            .chunk_discards
            .iter()
            .filter(|discard| discard.compact_instead)
            .count() as u64,
        budget_limited: plan.reclaim.budget_limited
            || plan.reclaim.skipped_by_budget_chunks > 0
            || plan.reclaim.skipped_by_trim_budget_chunks > 0,
        trim_flow_limited: plan.trim_flow_pressure,
        truncated: plan.truncated || plan.reclaim.truncated,
        reclaim_continuation_safe: plan.reclaim_continuation_safe,
        reclaim_next_marker_advanced: plan.reclaim_next_marker_advanced,
        capacity_pressure: plan.capacity_pressure,
        reclaim_pressure: plan.reclaim_pressure,
        trim_flow_pressure: plan.trim_flow_pressure,
        quorum_pressure: plan.quorum_pressure,
        first_action: first_action
            .map(|action| action.action.clone())
            .unwrap_or_default(),
        first_action_reason: first_action
            .map(|action| action.reason.clone())
            .unwrap_or_default(),
        first_replica_index: first_action.and_then(|action| action.replica_index),
        first_chunk_id: first_action.and_then(|action| action.page_id.clone()),
        first_estimated_bytes: first_action
            .map(|action| action.estimated_bytes)
            .unwrap_or_default(),
        envelope_checksum: String::new(),
    };
    envelope.envelope_checksum = prepared_storage_maintenance_envelope_checksum(&envelope);
    envelope
}
