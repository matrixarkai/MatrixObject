// SPDX-License-Identifier: Apache-2.0
pub(crate) fn bounded_object_range_end(
    offset: u64,
    length: u64,
    object_length: u64,
) -> Result<u64, ObjectError> {
    if offset > object_length {
        return Err(ObjectError::InvalidArgument(
            "range offset exceeds object length".to_string(),
        ));
    }
    let available = object_length - offset;
    offset
        .checked_add(length.min(available))
        .ok_or_else(|| ObjectError::InvalidArgument("read range overflows".to_string()))
}

pub(crate) fn merge_cache_stats(target: &mut CacheControlStats, next: CacheControlStats) {
    target.requested_ranges = target
        .requested_ranges
        .saturating_add(next.requested_ranges);
    target.requested_bytes = target.requested_bytes.saturating_add(next.requested_bytes);
    target.selected_ranges = target.selected_ranges.saturating_add(next.selected_ranges);
    target.selected_bytes = target.selected_bytes.saturating_add(next.selected_bytes);
    target.skipped_bytes = target.skipped_bytes.saturating_add(next.skipped_bytes);
    target.contacted_chunk_endpoints = target
        .contacted_chunk_endpoints
        .saturating_add(next.contacted_chunk_endpoints);
    target.failed_chunk_endpoints = target
        .failed_chunk_endpoints
        .saturating_add(next.failed_chunk_endpoints);
    target.applied_chunk_ranges = target
        .applied_chunk_ranges
        .saturating_add(next.applied_chunk_ranges);
    target.failed_chunk_ranges = target
        .failed_chunk_ranges
        .saturating_add(next.failed_chunk_ranges);
    target.cache_hits = target.cache_hits.saturating_add(next.cache_hits);
    target.physical_reads = target.physical_reads.saturating_add(next.physical_reads);
    target.invalidated_entries = target
        .invalidated_entries
        .saturating_add(next.invalidated_entries);
    target.budget_limited |= next.budget_limited;
}

pub(crate) fn merge_repair_stats(target: &mut RepairStats, next: RepairStats) {
    target.checked_chunks = target.checked_chunks.saturating_add(next.checked_chunks);
    target.repaired_replicas = target
        .repaired_replicas
        .saturating_add(next.repaired_replicas);
    target.failed_repairs = target.failed_repairs.saturating_add(next.failed_repairs);
    target.degraded_chunks = target.degraded_chunks.saturating_add(next.degraded_chunks);
}

pub(crate) fn finalize_object_replica_repair_plan(plan: &mut ObjectReplicaRepairPlan) {
    plan.repair_needed = plan.repair_objects > 0;
    plan.repair_can_apply = plan.repair_needed && !plan.truncated;
    plan.repair_reason = if !plan.repair_needed {
        "no_object_replica_repair_candidates".to_string()
    } else if plan.truncated {
        "object_replica_repair_plan_truncated".to_string()
    } else {
        "object_replica_repair_ready".to_string()
    };
}

pub(crate) fn finalize_object_capacity_rebalance_report(
    report: &mut ObjectCapacityRebalanceReport,
) {
    report.rebalance_needed = report.rebalance_objects > 0;
    report.rebalance_can_apply =
        report.rebalance_needed && !report.truncated && report.errors.is_empty();
    report.rebalance_reason = if !report.rebalance_needed {
        "no_object_capacity_rebalance_needed".to_string()
    } else if report.truncated {
        "object_capacity_rebalance_plan_truncated".to_string()
    } else if !report.errors.is_empty() {
        "object_capacity_rebalance_plan_errors".to_string()
    } else {
        "object_capacity_rebalance_ready".to_string()
    };
}

pub(crate) fn merge_bounded_repair_report(
    target: &mut BoundedRepairReport,
    next: BoundedRepairReport,
) {
    target.total_chunk_count = target
        .total_chunk_count
        .saturating_add(next.total_chunk_count);
    target.selected_chunk_count = target
        .selected_chunk_count
        .saturating_add(next.selected_chunk_count);
    target.skipped_page_count = target
        .skipped_page_count
        .saturating_add(next.skipped_page_count);
    target.candidate_repair_bytes = target
        .candidate_repair_bytes
        .saturating_add(next.candidate_repair_bytes);
    target.selected_repair_bytes = target
        .selected_repair_bytes
        .saturating_add(next.selected_repair_bytes);
    target.skipped_repair_bytes = target
        .skipped_repair_bytes
        .saturating_add(next.skipped_repair_bytes);
    target.next_chunk_index = target.next_chunk_index.max(next.next_chunk_index);
    target.has_more |= next.has_more;
    merge_repair_stats(&mut target.stats, next.stats);
}

pub(crate) fn merge_chunk_partial_reclaim_report(
    target: &mut ReplicaPartialReclaimReport,
    next: ReplicaPartialReclaimReport,
) {
    target.scanned_chunks = target.scanned_chunks.saturating_add(next.scanned_chunks);
    target.candidate_chunks = target
        .candidate_chunks
        .saturating_add(next.candidate_chunks);
    target.reclaimed_chunks = target
        .reclaimed_chunks
        .saturating_add(next.reclaimed_chunks);
    target.skipped_chunks = target.skipped_chunks.saturating_add(next.skipped_chunks);
    target.reclaimed_discard_bytes = target
        .reclaimed_discard_bytes
        .saturating_add(next.reclaimed_discard_bytes);
    target.rewritten_bytes = target.rewritten_bytes.saturating_add(next.rewritten_bytes);
    target.trimmed_bytes = target.trimmed_bytes.saturating_add(next.trimmed_bytes);
    target.skipped_rewrite_bytes = target
        .skipped_rewrite_bytes
        .saturating_add(next.skipped_rewrite_bytes);
    target.remaining_discard_bytes = target
        .remaining_discard_bytes
        .saturating_add(next.remaining_discard_bytes);
    target.budget_limited |= next.budget_limited;
    target.trim_flow_limited |= next.trim_flow_limited;
    target.requested_trim_bytes = target
        .requested_trim_bytes
        .saturating_add(next.requested_trim_bytes);
    target.effective_trim_bytes = target
        .effective_trim_bytes
        .saturating_add(next.effective_trim_bytes);
    target.trim_flow_available_bytes = target
        .trim_flow_available_bytes
        .saturating_add(next.trim_flow_available_bytes);
    target.trim_flow_throttled_bytes = target
        .trim_flow_throttled_bytes
        .saturating_add(next.trim_flow_throttled_bytes);
    target.trim_flow_throttled_ops = target
        .trim_flow_throttled_ops
        .saturating_add(next.trim_flow_throttled_ops);
    target.partial_reclaim_failures = target
        .partial_reclaim_failures
        .saturating_add(next.partial_reclaim_failures);
    if target.last_partial_reclaim_failure_status.is_empty()
        || target.last_partial_reclaim_failure_status == "ok"
    {
        target.last_partial_reclaim_failure_status = next.last_partial_reclaim_failure_status;
    }
}

pub(crate) fn discarded_range_bytes(ranges: &[DataRange]) -> u64 {
    ranges
        .iter()
        .fold(0u64, |total, range| total.saturating_add(range.length))
}

pub(crate) fn finalize_chunk_reclaim_plan(plan: &mut ReplicaReclaimPlan) {
    plan.next_marker_advanced = match (&plan.marker, &plan.next_marker) {
        (_, None) => false,
        (None, Some(_)) => true,
        (Some(marker), Some(next_marker)) => next_marker > marker,
    };
    plan.continuation_safe = !plan.budget_limited && (!plan.truncated || plan.next_marker_advanced);
    plan.reclaim_needed = plan.scheduled_chunks > 0;
    plan.reclaim_can_apply = plan.reclaim_needed;
    plan.reclaim_reason = if !plan.reclaim_needed && plan.candidate_discarded_bytes == 0 {
        "no_discarded_chunk_reclaim_needed"
    } else if !plan.reclaim_needed && plan.candidate_chunks == 0 {
        "chunkserver_reclaim_below_threshold"
    } else if !plan.reclaim_needed && plan.skipped_by_fragmentation_chunks > 0 {
        "chunkserver_reclaim_fragmented_candidates"
    } else if !plan.reclaim_needed && plan.budget_limited {
        "chunkserver_reclaim_budget_limited"
    } else if !plan.reclaim_needed {
        "chunkserver_reclaim_no_scheduled_candidates"
    } else if plan.truncated {
        "chunkserver_reclaim_ready_truncated"
    } else {
        "chunkserver_reclaim_ready"
    }
    .to_string();
    plan.plan_checksum = chunk_reclaim_plan_checksum(plan);
    plan.actual_plan_checksum = plan.plan_checksum.clone();
}

pub(crate) fn finalize_page_replica_reclaim_plan(plan: &mut ChunkReplicaReclaimPlan) {
    plan.next_marker_advanced = false;
    plan.continuation_safe = !plan.budget_limited && !plan.truncated;
    plan.reclaim_needed = plan.scheduled_chunks > 0;
    plan.reclaim_can_apply = plan.reclaim_needed;
    plan.reclaim_reason = if !plan.reclaim_needed && plan.candidate_discarded_bytes == 0 {
        "no_blockserver_reclaim_needed"
    } else if !plan.reclaim_needed && plan.candidate_chunks == 0 {
        "blockserver_reclaim_below_threshold"
    } else if !plan.reclaim_needed && plan.budget_limited {
        "blockserver_reclaim_budget_limited"
    } else if !plan.reclaim_needed {
        "blockserver_reclaim_no_scheduled_candidates"
    } else if plan.truncated {
        "blockserver_reclaim_ready_truncated"
    } else {
        "blockserver_reclaim_ready"
    }
    .to_string();
    plan.plan_checksum = chunk_replica_reclaim_plan_checksum(plan);
    plan.actual_plan_checksum = plan.plan_checksum.clone();
}

pub(crate) fn finalize_page_audit_repair_report(report: &mut ChunkReplicaAuditRepairReport) {
    report.repair_needed = report.candidate_chunks > 0;
    report.repair_can_apply = report.repair_needed
        && report.unrepaired_chunks == 0
        && report.stats.failed_repairs == 0
        && !report.audit.budget_limited;
    report.repair_reason = if !report.repair_needed {
        "no_blockserver_repair_needed"
    } else if report.unrepaired_chunks > 0 || report.stats.failed_repairs > 0 {
        "blockserver_repair_failures"
    } else if report.audit.budget_limited {
        "blockserver_repair_payload_budget_limited"
    } else if report.audit.has_more {
        "blockserver_repair_ready_more"
    } else {
        "blockserver_repair_ready"
    }
    .to_string();
}

pub(crate) fn discard_ratio_ppm(discarded_bytes: u64, logical_length: u64) -> u64 {
    if logical_length == 0 {
        return 0;
    }
    discarded_bytes
        .saturating_mul(1_000_000)
        .saturating_div(logical_length)
}

pub(crate) fn ratio_ppm(numerator: u64, denominator: u64) -> u64 {
    if denominator == 0 {
        return 0;
    }
    numerator
        .saturating_mul(1_000_000)
        .saturating_div(denominator)
}

pub(crate) fn trailing_discarded_bytes(logical_length: u64, ranges: &[DataRange]) -> u64 {
    let mut trim_start = logical_length;
    for range in ranges.iter().rev() {
        let range_end = range.offset.saturating_add(range.length);
        if range_end < trim_start {
            break;
        }
        if range_end >= trim_start && range.offset < trim_start {
            trim_start = range.offset;
        }
    }
    logical_length.saturating_sub(trim_start)
}

pub(crate) fn merge_page_read_stats(target: &mut ChunkReadStats, next: ChunkReadStats) {
    target.chunk_reads = target.chunk_reads.saturating_add(next.chunk_reads);
    target.cache_hits = target.cache_hits.saturating_add(next.cache_hits);
    target.coalesced_chunk_read_count = target
        .coalesced_chunk_read_count
        .saturating_add(next.coalesced_chunk_read_count);
    target.physical_chunk_read_count = target
        .physical_chunk_read_count
        .saturating_add(next.physical_chunk_read_count);
    target.metadata_head_batch_requests = target
        .metadata_head_batch_requests
        .saturating_add(next.metadata_head_batch_requests);
    target.metadata_physical_head_count = target
        .metadata_physical_head_count
        .saturating_add(next.metadata_physical_head_count);
    target.metadata_coalesced_head_count = target
        .metadata_coalesced_head_count
        .saturating_add(next.metadata_coalesced_head_count);
    target.metadata_cache_hits = target
        .metadata_cache_hits
        .saturating_add(next.metadata_cache_hits);
    target.metadata_cache_misses = target
        .metadata_cache_misses
        .saturating_add(next.metadata_cache_misses);
    target.replica_fallback_reads = target
        .replica_fallback_reads
        .saturating_add(next.replica_fallback_reads);
    target.quorum_verified_chunks = target
        .quorum_verified_chunks
        .saturating_add(next.quorum_verified_chunks);
    target.replica_read_repairs = target
        .replica_read_repairs
        .saturating_add(next.replica_read_repairs);
    target.replica_read_repair_failures = target
        .replica_read_repair_failures
        .saturating_add(next.replica_read_repair_failures);
    target.quorum_read_degraded_chunks = target
        .quorum_read_degraded_chunks
        .saturating_add(next.quorum_read_degraded_chunks);
}
