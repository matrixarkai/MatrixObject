pub(crate) fn slab_discard_request_checksum(
    slab_id: &SlabId,
    ranges: &[DataRange],
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-SEGMENT-DISCARD-REQUEST1");
    append_slab_id(&mut material, slab_id);
    append_u64(&mut material, ranges.len() as u64);
    for range in ranges {
        append_range(&mut material, range);
    }
    etag(&material)
}

pub(crate) fn object_gc_report_checksum(report: &GcReport) -> String {
    let mut material = Vec::new();
    append_string(&mut material, report.proof_format());
    append_u64(&mut material, report.slab_count);
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
        append_slab_id(&mut material, &candidate.slab_id);
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
    append_u64(&mut material, plan.candidate_slab_ids.len() as u64);
    for slab_id in &plan.candidate_slab_ids {
        append_slab_id(&mut material, slab_id);
    }
    etag(&material)
}

pub(crate) fn slab_discard_run_report_checksum(report: &SlabDiscardRunReport) -> String {
    let mut material = Vec::new();
    append_string(&mut material, report.proof_format());
    append_u64(&mut material, report.planned_slabs);
    append_u64(&mut material, report.planned_discard_bytes);
    append_u64(&mut material, report.skipped_compaction_slabs);
    append_u64(&mut material, report.skipped_limit_slabs);
    append_u64(&mut material, report.attempted_slabs);
    append_u64(&mut material, report.discarded_slabs);
    append_u64(&mut material, report.failed_slabs);
    append_u64(&mut material, report.discard_bytes);
    append_string(&mut material, &report.first_error);
    append_page_route_control_proof(&mut material, &report.route_control_proof);
    append_string(&mut material, &report.expected_route_control_checksum);
    append_u64(&mut material, report.discarded_slab_ids.len() as u64);
    for slab_id in &report.discarded_slab_ids {
        append_slab_id(&mut material, slab_id);
    }
    append_u64(&mut material, report.compaction_slab_ids.len() as u64);
    for slab_id in &report.compaction_slab_ids {
        append_slab_id(&mut material, slab_id);
    }
    etag(&material)
}
