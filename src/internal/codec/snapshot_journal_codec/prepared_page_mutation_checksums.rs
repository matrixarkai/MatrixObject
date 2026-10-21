// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
pub(crate) fn prepared_page_batch_write_checksum(prepared: &PreparedPageBatchWrite) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-PREPARED-BLOCK-WRITE4");
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    append_u64(
        &mut material,
        prepared
            .route_control_proof
            .chunk_route_control_commit_index,
    );
    append_u64(
        &mut material,
        prepared
            .route_control_proof
            .chunk_route_control_next_log_index,
    );
    append_u64(&mut material, prepared.writes.len() as u64);
    for write in &prepared.writes {
        append_chunk_id(&mut material, &write.chunk_id);
        append_u64(&mut material, write.offset);
        append_u64(&mut material, write.data.len() as u64);
        append_string(&mut material, &etag(&write.data));
    }
    append_page_batch_write_receipt(&mut material, &prepared.receipt);
    append_string(&mut material, &prepared.before_page_service_fingerprint);
    append_string(&mut material, &prepared.after_page_service_fingerprint);
    append_page_route_control_proof(&mut material, &prepared.route_control_proof);
    append_string(&mut material, &prepared.expected_route_control_checksum);
    etag(&material)
}

pub(crate) fn prepared_page_allocation_release_checksum(
    prepared: &PreparedPageAllocationRelease,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-PREPARED-BLOCK-ALLOCATION-RELEASE1");
    append_string(&mut material, &prepared.allocation_id);
    append_string(&mut material, &prepared.idempotency_key);
    append_string(&mut material, &prepared.request_checksum);
    append_string(&mut material, &prepared.before_page_service_fingerprint);
    append_string(&mut material, &prepared.after_page_service_fingerprint);
    append_page_route_control_proof(&mut material, &prepared.route_control_proof);
    append_string(&mut material, &prepared.expected_route_control_checksum);
    append_page_allocation_receipt(&mut material, &prepared.receipt);
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    append_u64(&mut material, prepared.pending_allocation_count_after);
    append_u64(&mut material, prepared.release_receipt_count_after);
    etag(&material)
}

pub(crate) fn append_prepared_page_allocation_release_envelope(
    out: &mut Vec<u8>,
    envelope: &PreparedPageAllocationReleaseEnvelope,
) {
    append_string(out, &envelope.envelope_format);
    append_string(out, &envelope.allocation_id);
    append_string(out, &envelope.idempotency_key);
    append_string(out, &envelope.request_checksum);
    append_u64(out, envelope.expected_previous_commit_index);
    append_u64(out, envelope.prepared_commit_index);
    append_string(out, &envelope.before_page_service_fingerprint);
    append_string(out, &envelope.after_page_service_fingerprint);
    append_page_route_control_proof(out, &envelope.route_control_proof);
    append_string(out, &envelope.expected_route_control_checksum);
    append_string(out, &envelope.raft_entry_checksum);
    append_chunk_id(out, &envelope.allocation_chunk_id);
    append_u64(out, envelope.allocation_offset);
    append_u64(out, envelope.allocation_length);
    append_u64(out, envelope.allocation_create_time_ms);
    append_u64(out, envelope.released as u64);
    append_u64(out, envelope.deduped as u64);
    append_u64(out, envelope.pending_allocation_count_after);
    append_u64(out, envelope.release_receipt_count_after);
}

pub(crate) fn prepared_page_allocation_release_envelope_checksum(
    envelope: &PreparedPageAllocationReleaseEnvelope,
) -> String {
    let mut material = Vec::new();
    append_prepared_page_allocation_release_envelope(&mut material, envelope);
    etag(&material)
}

pub(crate) fn make_prepared_page_allocation_release_envelope(
    prepared: &PreparedPageAllocationRelease,
) -> PreparedPageAllocationReleaseEnvelope {
    let mut envelope = PreparedPageAllocationReleaseEnvelope {
        envelope_format: "MXOSBLOCK-ALLOCATION-RELEASE-ENVELOPE1".to_string(),
        allocation_id: prepared.allocation_id.clone(),
        idempotency_key: prepared.idempotency_key.clone(),
        request_checksum: prepared.request_checksum.clone(),
        expected_previous_commit_index: prepared.expected_previous_commit_index,
        prepared_commit_index: prepared.prepared_commit_index,
        before_page_service_fingerprint: prepared.before_page_service_fingerprint.clone(),
        after_page_service_fingerprint: prepared.after_page_service_fingerprint.clone(),
        route_control_proof: prepared.route_control_proof.clone(),
        expected_route_control_checksum: prepared.expected_route_control_checksum.clone(),
        raft_entry_checksum: prepared.raft_entry_checksum.clone(),
        allocation_chunk_id: prepared.receipt.allocation.chunk_id.clone(),
        allocation_offset: prepared.receipt.allocation.offset,
        allocation_length: prepared.receipt.allocation.length,
        allocation_create_time_ms: prepared.receipt.allocation.create_time_ms,
        released: prepared.receipt.released,
        deduped: prepared.receipt.deduped,
        pending_allocation_count_after: prepared.pending_allocation_count_after,
        release_receipt_count_after: prepared.release_receipt_count_after,
        envelope_checksum: String::new(),
    };
    envelope.envelope_checksum = prepared_page_allocation_release_envelope_checksum(&envelope);
    envelope
}

pub(crate) fn prepared_page_batch_discard_checksum(
    prepared: &PreparedPageBatchDiscard,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-PREPARED-BLOCK-DISCARD4");
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    append_u64(
        &mut material,
        prepared
            .route_control_proof
            .chunk_route_control_commit_index,
    );
    append_u64(
        &mut material,
        prepared
            .route_control_proof
            .chunk_route_control_next_log_index,
    );
    append_u64(&mut material, prepared.requests.len() as u64);
    for request in &prepared.requests {
        append_chunk_id(&mut material, &request.chunk_id);
        append_u64(&mut material, request.offset);
        append_u64(&mut material, request.length);
    }
    append_page_batch_discard_receipt(&mut material, &prepared.receipt);
    append_string(&mut material, &prepared.before_page_service_fingerprint);
    append_string(&mut material, &prepared.after_page_service_fingerprint);
    append_page_route_control_proof(&mut material, &prepared.route_control_proof);
    append_string(&mut material, &prepared.expected_route_control_checksum);
    etag(&material)
}

pub(crate) fn prepared_page_reclaim_checksum(prepared: &PreparedPageReplicaReclaim) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-PREPARED-BLOCK-RECLAIM1");
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    append_u64(&mut material, prepared.max_chunks as u64);
    append_u64(&mut material, prepared.max_rewrite_bytes);
    append_u64(&mut material, prepared.min_discarded_bytes);
    append_u64(&mut material, prepared.min_discard_ratio_ppm);
    append_u64(&mut material, prepared.max_discard_ranges as u64);
    append_u64(&mut material, prepared.max_trim_bytes);
    append_u64(&mut material, prepared.now_ms);
    append_page_replica_reclaim_plan(&mut material, &prepared.plan);
    append_chunk_partial_reclaim_report(&mut material, &prepared.report);
    append_string(&mut material, &prepared.before_page_service_fingerprint);
    append_string(&mut material, &prepared.after_page_service_fingerprint);
    append_page_route_control_proof(&mut material, &prepared.route_control_proof);
    append_string(&mut material, &prepared.expected_route_control_checksum);
    etag(&material)
}

pub(crate) fn prepared_page_range_repair_checksum(prepared: &PreparedPageRangeRepair) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-PREPARED-BLOCK-RANGE-REPAIR2");
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    append_chunk_id(&mut material, &prepared.chunk_id);
    append_u64(&mut material, prepared.offset);
    append_u64(&mut material, prepared.length);
    append_u64(&mut material, prepared.max_repair_chunks as u64);
    append_u64(&mut material, prepared.start_chunk_index);
    append_bounded_repair_report(&mut material, &prepared.report);
    append_string(&mut material, &prepared.before_page_service_fingerprint);
    append_string(&mut material, &prepared.after_page_service_fingerprint);
    append_page_route_control_proof(&mut material, &prepared.route_control_proof);
    append_string(&mut material, &prepared.expected_route_control_checksum);
    append_page_chunk_control_proof(&mut material, &prepared.chunk_control_proof);
    etag(&material)
}

pub(crate) fn prepared_page_chunk_repair_checksum(prepared: &PreparedPageChunkRepair) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-PREPARED-BLOCK-CHUNK-REPAIR2");
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    append_page_id(&mut material, &prepared.page_id);
    append_chunk_repair_report(&mut material, &prepared.report);
    append_string(&mut material, &prepared.before_page_service_fingerprint);
    append_string(&mut material, &prepared.after_page_service_fingerprint);
    append_page_route_control_proof(&mut material, &prepared.route_control_proof);
    append_string(&mut material, &prepared.expected_route_control_checksum);
    append_page_chunk_control_proof(&mut material, &prepared.chunk_control_proof);
    etag(&material)
}

pub(crate) fn prepared_page_audit_repair_checksum(prepared: &PreparedPageAuditRepair) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-PREPARED-BLOCK-AUDIT-REPAIR5");
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    append_u64(&mut material, prepared.after_chunk_id.is_some() as u64);
    if let Some(after_chunk_id) = &prepared.after_chunk_id {
        append_page_id(&mut material, after_chunk_id);
    }
    append_u64(&mut material, prepared.max_chunks as u64);
    append_u64(&mut material, prepared.max_payload_bytes);
    append_page_replica_audit_repair_report(&mut material, &prepared.report);
    append_string(&mut material, &prepared.before_page_service_fingerprint);
    append_string(&mut material, &prepared.after_page_service_fingerprint);
    append_page_route_control_proof(&mut material, &prepared.route_control_proof);
    append_string(&mut material, &prepared.expected_route_control_checksum);
    append_page_chunk_control_proof(&mut material, &prepared.chunk_control_proof);
    etag(&material)
}

