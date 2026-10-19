// SPDX-License-Identifier: Apache-2.0
pub(crate) fn append_chunk_reclaim_candidate(out: &mut Vec<u8>, candidate: &ReplicaReclaimCandidate) {
    append_page_id(out, &candidate.page_id);
    append_u64(out, candidate.logical_length);
    append_u64(out, candidate.discarded_bytes);
    append_u64(out, candidate.discarded_range_count);
    append_u64(out, candidate.discard_ratio_ppm);
    append_u64(out, candidate.rewrite_bytes);
    append_u64(out, candidate.trim_bytes);
    append_u64(out, candidate.reclaim_score);
    append_string(out, &candidate.reason);
}

pub(crate) fn chunk_reclaim_plan_checksum(plan: &ReplicaReclaimPlan) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-CHUNK-RECLAIM-PLAN3");
    append_u64(&mut material, plan.marker.is_some() as u64);
    if let Some(marker) = &plan.marker {
        append_page_id(&mut material, marker);
    }
    append_u64(&mut material, plan.next_marker.is_some() as u64);
    if let Some(next_marker) = &plan.next_marker {
        append_page_id(&mut material, next_marker);
    }
    append_u64(&mut material, plan.continuation_safe as u64);
    append_u64(&mut material, plan.next_marker_advanced as u64);
    append_u64(&mut material, plan.scanned_chunks);
    append_u64(&mut material, plan.candidate_chunks);
    append_u64(&mut material, plan.scheduled_chunks);
    append_u64(&mut material, plan.skipped_chunks);
    append_u64(&mut material, plan.skipped_by_budget_chunks);
    append_u64(&mut material, plan.skipped_by_budget_bytes);
    append_u64(&mut material, plan.skipped_by_trim_budget_chunks);
    append_u64(&mut material, plan.skipped_by_trim_budget_bytes);
    append_u64(&mut material, plan.skipped_by_fragmentation_chunks);
    append_u64(&mut material, plan.skipped_by_fragmentation_discarded_bytes);
    append_u64(&mut material, plan.skipped_by_fragmentation_ranges);
    append_u64(&mut material, plan.candidate_discarded_bytes);
    append_u64(&mut material, plan.scheduled_rewrite_bytes);
    append_u64(&mut material, plan.scheduled_trim_ops);
    append_u64(&mut material, plan.scheduled_trim_bytes);
    append_u64(&mut material, plan.remaining_discard_bytes);
    append_u64(&mut material, plan.budget_limited as u64);
    append_u64(&mut material, plan.truncated as u64);
    append_u64(&mut material, plan.trim_flow_limited as u64);
    append_u64(&mut material, plan.requested_trim_bytes);
    append_u64(&mut material, plan.effective_trim_bytes);
    append_u64(&mut material, plan.trim_flow_available_bytes);
    append_u64(&mut material, plan.trim_flow_throttled_ops);
    append_u64(&mut material, plan.trim_flow_throttled_bytes);
    append_u64(
        &mut material,
        plan.first_budget_skipped_candidate.is_some() as u64,
    );
    if let Some(candidate) = &plan.first_budget_skipped_candidate {
        append_chunk_reclaim_candidate(&mut material, candidate);
    }
    append_u64(
        &mut material,
        plan.first_trim_budget_skipped_candidate.is_some() as u64,
    );
    if let Some(candidate) = &plan.first_trim_budget_skipped_candidate {
        append_chunk_reclaim_candidate(&mut material, candidate);
    }
    append_u64(&mut material, plan.candidates.len() as u64);
    for candidate in &plan.candidates {
        append_chunk_reclaim_candidate(&mut material, candidate);
    }
    append_u64(&mut material, plan.reclaim_needed as u64);
    append_u64(&mut material, plan.reclaim_can_apply as u64);
    append_string(&mut material, &plan.reclaim_reason);
    etag(&material)
}

pub(crate) fn chunk_replica_reclaim_plan_checksum(plan: &ChunkReplicaReclaimPlan) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-BLOCK-REPLICA-RECLAIM-PLAN3");
    append_u64(&mut material, plan.continuation_safe as u64);
    append_u64(&mut material, plan.next_marker_advanced as u64);
    append_u64(&mut material, plan.scanned_replicas);
    append_u64(&mut material, plan.skipped_replicas);
    append_u64(&mut material, plan.frozen_replicas);
    append_u64(&mut material, plan.quarantined_replicas);
    append_u64(&mut material, plan.scanned_chunks);
    append_u64(&mut material, plan.candidate_chunks);
    append_u64(&mut material, plan.scheduled_chunks);
    append_u64(&mut material, plan.skipped_chunks);
    append_u64(&mut material, plan.skipped_by_budget_chunks);
    append_u64(&mut material, plan.skipped_by_budget_bytes);
    append_u64(&mut material, plan.skipped_by_trim_budget_chunks);
    append_u64(&mut material, plan.skipped_by_trim_budget_bytes);
    append_u64(&mut material, plan.skipped_by_fragmentation_chunks);
    append_u64(&mut material, plan.skipped_by_fragmentation_discarded_bytes);
    append_u64(&mut material, plan.skipped_by_fragmentation_ranges);
    append_u64(&mut material, plan.candidate_discarded_bytes);
    append_u64(&mut material, plan.scheduled_rewrite_bytes);
    append_u64(&mut material, plan.scheduled_trim_ops);
    append_u64(&mut material, plan.scheduled_trim_bytes);
    append_u64(&mut material, plan.remaining_discard_bytes);
    append_u64(&mut material, plan.budget_limited as u64);
    append_u64(&mut material, plan.truncated as u64);
    append_u64(&mut material, plan.global_reclaim_budget_enforced as u64);
    append_string(&mut material, &plan.global_reclaim_budget_basis);
    append_u64(
        &mut material,
        plan.first_budget_skipped_candidate.is_some() as u64,
    );
    if let Some(candidate) = &plan.first_budget_skipped_candidate {
        append_u64(&mut material, candidate.replica_index as u64);
        append_chunk_reclaim_candidate(&mut material, &candidate.candidate);
    }
    append_u64(
        &mut material,
        plan.first_trim_budget_skipped_candidate.is_some() as u64,
    );
    if let Some(candidate) = &plan.first_trim_budget_skipped_candidate {
        append_u64(&mut material, candidate.replica_index as u64);
        append_chunk_reclaim_candidate(&mut material, &candidate.candidate);
    }
    append_u64(&mut material, plan.candidates.len() as u64);
    for candidate in &plan.candidates {
        append_u64(&mut material, candidate.replica_index as u64);
        append_chunk_reclaim_candidate(&mut material, &candidate.candidate);
    }
    append_u64(&mut material, plan.reclaim_needed as u64);
    append_u64(&mut material, plan.reclaim_can_apply as u64);
    append_string(&mut material, &plan.reclaim_reason);
    etag(&material)
}
