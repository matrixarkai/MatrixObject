// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
pub(crate) fn append_chunk_partial_reclaim_report(
    out: &mut Vec<u8>,
    report: &ReplicaPartialReclaimReport,
) {
    append_u64(out, report.scanned_chunks);
    append_u64(out, report.candidate_chunks);
    append_u64(out, report.reclaimed_chunks);
    append_u64(out, report.skipped_chunks);
    append_u64(out, report.reclaimed_discard_bytes);
    append_u64(out, report.rewritten_bytes);
    append_u64(out, report.trimmed_bytes);
    append_u64(out, report.skipped_rewrite_bytes);
    append_u64(out, report.remaining_discard_bytes);
    append_u64(out, report.budget_limited as u64);
    append_u64(out, report.trim_flow_limited as u64);
    append_u64(out, report.requested_trim_bytes);
    append_u64(out, report.effective_trim_bytes);
    append_u64(out, report.trim_flow_available_bytes);
    append_u64(out, report.trim_flow_throttled_bytes);
    append_u64(out, report.trim_flow_throttled_ops);
    append_u64(out, report.partial_reclaim_failures);
    append_string(out, &report.last_partial_reclaim_failure_status);
}
