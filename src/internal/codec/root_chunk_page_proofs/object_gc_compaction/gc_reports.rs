// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
pub(crate) fn slab_discard_request_checksum(
    chunk_id: &ChunkId,
    ranges: &[DataRange],
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-SEGMENT-DISCARD-REQUEST1");
    append_chunk_id(&mut material, chunk_id);
    append_u64(&mut material, ranges.len() as u64);
    for range in ranges {
        append_range(&mut material, range);
    }
    etag(&material)
}

pub(crate) fn object_gc_report_checksum(report: &GcReport) -> String {
    let mut material = Vec::new();
    append_string(&mut material, report.proof_format());
    append_u64(&mut material, report.chunk_count);
    append_u64(&mut material, report.logical_bytes);
    append_u64(&mut material, report.live_bytes);
    append_u64(&mut material, report.stale_bytes);
    append_u64(&mut material, report.discarded_bytes);
    append_u64(&mut material, report.logical_write_amplification_ppm);
    append_u64(&mut material, report.space_amplification_ppm);
    append_u64(&mut material, report.candidate_count);
    append_u64(&mut material, report.discard_candidate_bytes);
    append_u64(&mut material, report.min_discard_bytes);
    append_u64(&mut material, report.selected_discard_bytes);
    append_u64(&mut material, report.selected_discard_range_count);
    append_u64(&mut material, report.budget_skipped_discard_range_count);
    append_u64(&mut material, report.max_discard_bytes);
    append_u64(&mut material, report.discard_budget_limited as u64);
    append_u64(&mut material, report.pending_discard_range_count);
    append_u64(&mut material, report.merged_discard_range_count);
    append_u64(&mut material, report.fragmented_range_count);
    append_u64(&mut material, report.compaction_candidate_count);
    append_u64(&mut material, report.compaction_candidate_bytes);
    append_u64(&mut material, report.compaction_candidate_range_count);
    append_page_route_control_proof(&mut material, &report.route_control_proof);
    append_string(&mut material, &report.expected_route_control_checksum);
    append_u64(&mut material, report.discard_candidates.len() as u64);
    for candidate in &report.discard_candidates {
        append_chunk_id(&mut material, &candidate.chunk_id);
        append_range(&mut material, &candidate.range);
    }
    etag(&material)
}

pub(crate) fn full_compaction_plan_checksum(plan: &FullCompactionPlan) -> String {
    let mut material = Vec::new();
    append_string(&mut material, plan.proof_format());
    append_u64(&mut material, plan.min_stale_bytes);
    append_u64(&mut material, plan.candidate_count);
    append_u64(&mut material, plan.candidate_stale_bytes);
    append_u64(&mut material, plan.candidate_live_bytes);
    append_u64(&mut material, plan.candidate_logical_bytes);
    append_page_route_control_proof(&mut material, &plan.route_control_proof);
    append_string(&mut material, &plan.expected_route_control_checksum);
    append_string(&mut material, &plan.object_gc_report_proof_checksum);
    append_u64(&mut material, plan.candidate_chunk_ids.len() as u64);
    for chunk_id in &plan.candidate_chunk_ids {
        append_chunk_id(&mut material, chunk_id);
    }
    etag(&material)
}

pub(crate) fn chunk_discard_run_report_checksum(report: &ChunkDiscardRunReport) -> String {
    let mut material = Vec::new();
    append_string(&mut material, report.proof_format());
    append_u64(&mut material, report.planned_chunks);
    append_u64(&mut material, report.planned_discard_bytes);
    append_u64(&mut material, report.skipped_compaction_chunks);
    append_u64(&mut material, report.skipped_limit_chunks);
    append_u64(&mut material, report.attempted_chunks);
    append_u64(&mut material, report.discarded_chunks);
    append_u64(&mut material, report.failed_chunks);
    append_u64(&mut material, report.discard_bytes);
    append_string(&mut material, &report.first_error);
    append_page_route_control_proof(&mut material, &report.route_control_proof);
    append_string(&mut material, &report.expected_route_control_checksum);
    append_u64(&mut material, report.discarded_chunk_ids.len() as u64);
    for chunk_id in &report.discarded_chunk_ids {
        append_chunk_id(&mut material, chunk_id);
    }
    append_u64(&mut material, report.compaction_chunk_ids.len() as u64);
    for chunk_id in &report.compaction_chunk_ids {
        append_chunk_id(&mut material, chunk_id);
    }
    etag(&material)
}
