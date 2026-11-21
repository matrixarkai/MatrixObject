// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
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

