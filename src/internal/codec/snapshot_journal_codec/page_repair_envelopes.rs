// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

pub(crate) fn append_bounded_repair_report(out: &mut Vec<u8>, report: &BoundedRepairReport) {
    append_u64(out, report.total_chunk_count);
    append_u64(out, report.selected_chunk_count);
    append_u64(out, report.skipped_page_count);
    append_u64(out, report.candidate_repair_bytes);
    append_u64(out, report.selected_repair_bytes);
    append_u64(out, report.skipped_repair_bytes);
    append_u64(out, report.start_chunk_index);
    append_u64(out, report.next_chunk_index);
    append_u64(out, report.has_more as u64);
    append_repair_stats(out, &report.stats);
    append_page_route_control_proof(out, &report.route_control_proof);
    append_string(out, &report.expected_route_control_checksum);
}

pub(crate) fn append_chunk_repair_report(out: &mut Vec<u8>, report: &ReplicaRepairReport) {
    append_u64(out, report.page_id.is_some() as u64);
    if let Some(page_id) = &report.page_id {
        append_page_id(out, page_id);
    }
    append_u64(out, report.checked_chunks);
    append_u64(out, report.healthy_chunks);
    append_u64(out, report.degraded_chunks);
    append_u64(out, report.repaired_chunks);
    append_u64(out, report.repaired_replicas);
    append_u64(out, report.failed_repairs);
    append_u64(out, report.missing_replicas);
    append_u64(out, report.corrupt_replicas);
    append_u64(out, report.divergent_replicas);
    append_u64(out, report.frozen_replicas);
    append_u64(out, report.quarantined_replicas);
    append_u64(out, report.repairable_replicas);
    append_u64(out, report.unrepaired_chunks);
    append_u64(out, report.committed as u64);
    append_u64(out, report.first_error.is_some() as u64);
    if let Some(first_error) = &report.first_error {
        append_string(out, first_error);
    }
    append_page_route_control_proof(out, &report.route_control_proof);
    append_string(out, &report.expected_route_control_checksum);
}

pub(crate) fn append_page_replica_audit_repair_report(
    out: &mut Vec<u8>,
    report: &ChunkReplicaAuditRepairReport,
) {
    append_page_replica_audit_page(out, &report.audit);
    append_u64(out, report.candidate_chunks);
    append_u64(out, report.repaired_chunks);
    append_u64(out, report.unrepaired_chunks);
    append_repair_stats(out, &report.stats);
    append_u64(out, report.repair_needed as u64);
    append_u64(out, report.repair_can_apply as u64);
    append_string(out, &report.repair_reason);
    append_page_route_control_proof(out, &report.route_control_proof);
    append_string(out, &report.expected_route_control_checksum);
}

