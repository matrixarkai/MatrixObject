pub(crate) fn append_storage_maintenance_action(
    out: &mut Vec<u8>,
    action: &StorageMaintenanceAction,
) {
    append_string(out, &action.action);
    append_string(out, &action.reason);
    append_u64(out, action.priority);
    append_u64(out, action.replica_index.is_some() as u64);
    append_u64(out, action.replica_index.unwrap_or_default() as u64);
    append_u64(out, action.chunk_id.is_some() as u64);
    if let Some(chunk_id) = &action.chunk_id {
        append_chunk_id(out, chunk_id);
    }
    append_u64(out, action.estimated_bytes);
}

pub(crate) fn storage_maintenance_plan_checksum(plan: &StorageMaintenancePlan) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-BLOCK-STORAGE-MAINTENANCE-PLAN1");
    append_string(&mut material, &plan.readiness.page_state_fingerprint);
    append_string(
        &mut material,
        &plan.readiness.route_control_proof.state_checksum,
    );
    append_u64(
        &mut material,
        plan.readiness
            .route_control_proof
            .chunk_route_control_commit_index,
    );
    append_u64(
        &mut material,
        plan.readiness
            .route_control_proof
            .chunk_route_control_next_log_index,
    );
    append_u64(&mut material, plan.readiness.readable as u64);
    append_u64(&mut material, plan.readiness.writable as u64);
    append_u64(&mut material, plan.readiness.ready as u64);
    append_string(&mut material, &plan.readiness.reason);
    append_u64(&mut material, plan.readiness.replica_readiness.len() as u64);
    for replica in &plan.readiness.replica_readiness {
        append_string(&mut material, &replica.chunk_state_fingerprint);
        append_u64(&mut material, replica.readable as u64);
        append_u64(&mut material, replica.writable as u64);
        append_u64(&mut material, replica.reclaimable as u64);
        append_u64(&mut material, replica.ready as u64);
        append_string(&mut material, &replica.reason);
    }
    append_page_replica_reclaim_plan(&mut material, &plan.reclaim);
    append_u64(&mut material, plan.slab_discards.len() as u64);
    for discard in &plan.slab_discards {
        append_slab_id(&mut material, &discard.slab_id);
        append_u64(&mut material, discard.discard_bytes);
        append_u64(&mut material, discard.fragmented as u64);
        append_u64(&mut material, discard.compact_instead as u64);
        append_u64(&mut material, discard.ranges.len() as u64);
        for range in &discard.ranges {
            append_range(&mut material, range);
        }
    }
    append_u64(&mut material, plan.actions.len() as u64);
    for action in &plan.actions {
        append_storage_maintenance_action(&mut material, action);
    }
    append_u64(&mut material, plan.reclaim_continuation_safe as u64);
    append_u64(&mut material, plan.reclaim_next_marker_advanced as u64);
    append_u64(&mut material, plan.capacity_pressure as u64);
    append_u64(&mut material, plan.reclaim_pressure as u64);
    append_u64(&mut material, plan.trim_flow_pressure as u64);
    append_u64(&mut material, plan.quorum_pressure as u64);
    append_u64(&mut material, plan.max_actions as u64);
    append_u64(&mut material, plan.truncated as u64);
    etag(&material)
}
