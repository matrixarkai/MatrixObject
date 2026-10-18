pub(crate) fn append_chunk_reclaim_plan(out: &mut Vec<u8>, plan: &ChunkReclaimPlan) {
    append_u64(out, plan.marker.is_some() as u64);
    if let Some(marker) = &plan.marker {
        append_chunk_id(out, marker);
    }
    append_u64(out, plan.next_marker.is_some() as u64);
    if let Some(next_marker) = &plan.next_marker {
        append_chunk_id(out, next_marker);
    }
    append_u64(out, plan.continuation_safe as u64);
    append_u64(out, plan.next_marker_advanced as u64);
    append_u64(out, plan.scanned_chunks);
    append_u64(out, plan.candidate_chunks);
    append_u64(out, plan.scheduled_chunks);
    append_u64(out, plan.skipped_chunks);
    append_u64(out, plan.skipped_by_budget_chunks);
    append_u64(out, plan.skipped_by_budget_bytes);
    append_u64(out, plan.skipped_by_trim_budget_chunks);
    append_u64(out, plan.skipped_by_trim_budget_bytes);
    append_u64(out, plan.skipped_by_fragmentation_chunks);
    append_u64(out, plan.skipped_by_fragmentation_discarded_bytes);
    append_u64(out, plan.skipped_by_fragmentation_ranges);
    append_u64(out, plan.candidate_discarded_bytes);
    append_u64(out, plan.scheduled_rewrite_bytes);
    append_u64(out, plan.scheduled_trim_ops);
    append_u64(out, plan.scheduled_trim_bytes);
    append_u64(out, plan.remaining_discard_bytes);
    append_u64(out, plan.budget_limited as u64);
    append_u64(out, plan.truncated as u64);
    append_u64(out, plan.trim_flow_limited as u64);
    append_u64(out, plan.requested_trim_bytes);
    append_u64(out, plan.effective_trim_bytes);
    append_u64(out, plan.trim_flow_available_bytes);
    append_u64(out, plan.trim_flow_throttled_bytes);
    append_u64(out, plan.first_budget_skipped_candidate.is_some() as u64);
    if let Some(candidate) = &plan.first_budget_skipped_candidate {
        append_chunk_reclaim_candidate(out, candidate);
    }
    append_u64(
        out,
        plan.first_trim_budget_skipped_candidate.is_some() as u64,
    );
    if let Some(candidate) = &plan.first_trim_budget_skipped_candidate {
        append_chunk_reclaim_candidate(out, candidate);
    }
    append_u64(out, plan.candidates.len() as u64);
    for candidate in &plan.candidates {
        append_chunk_reclaim_candidate(out, candidate);
    }
    append_u64(out, plan.reclaim_needed as u64);
    append_u64(out, plan.reclaim_can_apply as u64);
    append_string(out, &plan.reclaim_reason);
    append_string(out, &plan.plan_checksum);
    append_string(out, &plan.expected_plan_checksum);
}

pub(crate) fn append_page_replica_reclaim_plan(out: &mut Vec<u8>, plan: &PageReplicaReclaimPlan) {
    append_u64(out, plan.continuation_safe as u64);
    append_u64(out, plan.next_marker_advanced as u64);
    append_u64(out, plan.scanned_replicas);
    append_u64(out, plan.skipped_replicas);
    append_u64(out, plan.frozen_replicas);
    append_u64(out, plan.quarantined_replicas);
    append_u64(out, plan.scanned_chunks);
    append_u64(out, plan.candidate_chunks);
    append_u64(out, plan.scheduled_chunks);
    append_u64(out, plan.skipped_chunks);
    append_u64(out, plan.skipped_by_budget_chunks);
    append_u64(out, plan.skipped_by_budget_bytes);
    append_u64(out, plan.skipped_by_trim_budget_chunks);
    append_u64(out, plan.skipped_by_trim_budget_bytes);
    append_u64(out, plan.skipped_by_fragmentation_chunks);
    append_u64(out, plan.skipped_by_fragmentation_discarded_bytes);
    append_u64(out, plan.skipped_by_fragmentation_ranges);
    append_u64(out, plan.candidate_discarded_bytes);
    append_u64(out, plan.scheduled_rewrite_bytes);
    append_u64(out, plan.scheduled_trim_ops);
    append_u64(out, plan.scheduled_trim_bytes);
    append_u64(out, plan.remaining_discard_bytes);
    append_u64(out, plan.budget_limited as u64);
    append_u64(out, plan.truncated as u64);
    append_u64(out, plan.global_reclaim_budget_enforced as u64);
    append_string(out, &plan.global_reclaim_budget_basis);
    append_u64(out, plan.first_budget_skipped_candidate.is_some() as u64);
    if let Some(candidate) = &plan.first_budget_skipped_candidate {
        append_u64(out, candidate.replica_index as u64);
        append_chunk_reclaim_candidate(out, &candidate.candidate);
    }
    append_u64(
        out,
        plan.first_trim_budget_skipped_candidate.is_some() as u64,
    );
    if let Some(candidate) = &plan.first_trim_budget_skipped_candidate {
        append_u64(out, candidate.replica_index as u64);
        append_chunk_reclaim_candidate(out, &candidate.candidate);
    }
    append_u64(out, plan.candidates.len() as u64);
    for candidate in &plan.candidates {
        append_u64(out, candidate.replica_index as u64);
        append_chunk_reclaim_candidate(out, &candidate.candidate);
    }
    append_u64(out, plan.reclaim_needed as u64);
    append_u64(out, plan.reclaim_can_apply as u64);
    append_string(out, &plan.reclaim_reason);
    append_string(out, &plan.plan_checksum);
    append_string(out, &plan.expected_plan_checksum);
}
