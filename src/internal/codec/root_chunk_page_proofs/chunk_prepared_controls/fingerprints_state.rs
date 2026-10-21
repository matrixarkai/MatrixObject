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

pub(crate) fn append_replica_service_fingerprint(out: &mut Vec<u8>, service: &MatrixReplicaService) {
    let readiness = service.storage_readiness(0, 0, 0);
    append_string(out, &readiness.chunk_state_fingerprint);
    append_u64(out, readiness.physical_bytes);
    append_u64(out, readiness.reclaimable_bytes);
    append_u64(out, readiness.trailing_trim_bytes);
    append_u64(out, readiness.page_count);
    append_u64(out, readiness.write_receipt_count);
    append_u64(out, readiness.discard_receipt_count);
    append_u64(out, readiness.batch_write_receipt_count);
    append_u64(out, readiness.batch_discard_receipt_count);
    append_u64(out, readiness.ec_reconstructed_reads);
    append_u64(out, readiness.ec_reconstruction_failures);
    append_string(out, &readiness.control_proof.state_checksum);
}

pub(crate) fn replica_service_fingerprint(service: &MatrixReplicaService) -> String {
    let mut material = Vec::new();
    append_replica_service_fingerprint(&mut material, service);
    etag(&material)
}

pub(crate) fn append_chunk_state(out: &mut Vec<u8>, state: &PageState) {
    append_u64(out, state.logical_length);
    append_u64(out, state.write_sequence);
    append_string(out, &state.checksum());
    append_string(out, &etag(&state.data));
    append_u64(out, state.data.len() as u64);
    append_u64(out, state.fragment_bytes().len() as u64);
    append_string(out, &etag(state.fragment_bytes()));
    append_u64(out, state.discarded_ranges.len() as u64);
    for range in &state.discarded_ranges {
        append_range(out, range);
    }
}
