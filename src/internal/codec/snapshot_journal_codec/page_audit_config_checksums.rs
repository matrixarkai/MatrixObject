pub(crate) fn append_prepared_page_audit_repair_envelope(
    out: &mut Vec<u8>,
    envelope: &PreparedPageAuditRepairEnvelope,
) {
    append_string(out, &envelope.envelope_format);
    append_u64(out, envelope.after_chunk_id.is_some() as u64);
    if let Some(chunk_id) = &envelope.after_chunk_id {
        append_chunk_id(out, chunk_id);
    }
    append_u64(out, envelope.max_chunks);
    append_u64(out, envelope.max_payload_bytes);
    append_u64(out, envelope.expected_previous_commit_index);
    append_u64(out, envelope.prepared_commit_index);
    append_string(out, &envelope.before_page_service_fingerprint);
    append_string(out, &envelope.after_page_service_fingerprint);
    append_string(out, &envelope.route_control_proof_format);
    append_string(out, &envelope.route_control_proof_checksum);
    append_u64(out, envelope.route_control_proof_bytes);
    append_u64(out, envelope.route_control_commit_index);
    append_u64(out, envelope.route_control_next_log_index);
    append_string(out, &envelope.expected_route_control_checksum);
    append_string(out, &envelope.raft_entry_checksum);
    append_u64(out, envelope.scanned_chunks);
    append_u64(out, envelope.candidate_chunks);
    append_u64(out, envelope.repaired_chunks);
    append_u64(out, envelope.unrepaired_chunks);
    append_u64(out, envelope.missing_replicas);
    append_u64(out, envelope.corrupt_replicas);
    append_u64(out, envelope.divergent_replicas);
    append_u64(out, envelope.quarantined_replicas);
    append_u64(out, envelope.frozen_replicas);
    append_u64(out, envelope.payload_bytes);
    append_u64(out, envelope.checked_chunks);
    append_u64(out, envelope.degraded_chunks);
    append_u64(out, envelope.repaired_replicas);
    append_u64(out, envelope.failed_repairs);
    append_u64(out, envelope.has_more as u64);
    append_u64(out, envelope.repair_needed as u64);
    append_u64(out, envelope.repair_can_apply as u64);
    append_string(out, &envelope.repair_reason);
    append_u64(out, envelope.first_candidate_chunk_id.is_some() as u64);
    if let Some(chunk_id) = &envelope.first_candidate_chunk_id {
        append_chunk_id(out, chunk_id);
    }
    append_u64(out, envelope.first_candidate_replica_index.is_some() as u64);
    append_u64(
        out,
        envelope.first_candidate_replica_index.unwrap_or_default() as u64,
    );
    append_string(out, &envelope.first_candidate_status);
    append_u64(out, envelope.first_candidate_logical_length);
}

pub(crate) fn prepared_page_audit_repair_envelope_checksum(
    envelope: &PreparedPageAuditRepairEnvelope,
) -> String {
    let mut material = Vec::new();
    append_prepared_page_audit_repair_envelope(&mut material, envelope);
    etag(&material)
}

pub(crate) fn make_prepared_page_audit_repair_envelope(
    prepared: &PreparedPageAuditRepair,
) -> PreparedPageAuditRepairEnvelope {
    let first_candidate = prepared.report.audit.items.first();
    let mut envelope = PreparedPageAuditRepairEnvelope {
        envelope_format: "MXOSBLOCK-AUDIT-REPAIR-ENVELOPE1".to_string(),
        after_chunk_id: prepared.after_chunk_id.clone(),
        max_chunks: prepared.max_chunks as u64,
        max_payload_bytes: prepared.max_payload_bytes,
        expected_previous_commit_index: prepared.expected_previous_commit_index,
        prepared_commit_index: prepared.prepared_commit_index,
        before_page_service_fingerprint: prepared.before_page_service_fingerprint.clone(),
        after_page_service_fingerprint: prepared.after_page_service_fingerprint.clone(),
        route_control_proof_format: prepared.route_control_proof.state_format.clone(),
        route_control_proof_checksum: prepared.route_control_proof.state_checksum.clone(),
        route_control_proof_bytes: prepared.route_control_proof.state_bytes,
        route_control_commit_index: prepared
            .route_control_proof
            .page_route_control_commit_index,
        route_control_next_log_index: prepared
            .route_control_proof
            .page_route_control_next_log_index,
        expected_route_control_checksum: prepared.expected_route_control_checksum.clone(),
        raft_entry_checksum: prepared.raft_entry_checksum.clone(),
        scanned_chunks: prepared.report.audit.scanned_chunks,
        candidate_chunks: prepared.report.candidate_chunks,
        repaired_chunks: prepared.report.repaired_chunks,
        unrepaired_chunks: prepared.report.unrepaired_chunks,
        missing_replicas: prepared.report.audit.missing_replicas,
        corrupt_replicas: prepared.report.audit.corrupt_replicas,
        divergent_replicas: prepared.report.audit.divergent_replicas,
        quarantined_replicas: prepared.report.audit.quarantined_replicas,
        frozen_replicas: prepared.report.audit.frozen_replicas,
        payload_bytes: prepared.report.audit.payload_bytes,
        checked_chunks: prepared.report.stats.checked_chunks,
        degraded_chunks: prepared.report.stats.degraded_chunks,
        repaired_replicas: prepared.report.stats.repaired_replicas,
        failed_repairs: prepared.report.stats.failed_repairs,
        has_more: prepared.report.audit.has_more,
        repair_needed: prepared.report.repair_needed,
        repair_can_apply: prepared.report.repair_can_apply,
        repair_reason: prepared.report.repair_reason.clone(),
        first_candidate_chunk_id: first_candidate.map(|item| item.chunk_id.clone()),
        first_candidate_replica_index: first_candidate.map(|item| item.replica_index),
        first_candidate_status: first_candidate
            .map(|item| item.status.clone())
            .unwrap_or_default(),
        first_candidate_logical_length: first_candidate
            .map(|item| item.logical_length)
            .unwrap_or_default(),
        envelope_checksum: String::new(),
    };
    envelope.envelope_checksum = prepared_page_audit_repair_envelope_checksum(&envelope);
    envelope
}

pub(crate) fn prepared_page_route_qos_config_checksum(
    prepared: &PreparedPageRouteQosConfig,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-PREPARED-BLOCK-ROUTE-QOS3");
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    append_page_route_qos_config(&mut material, &prepared.config);
    append_string(&mut material, &prepared.before_page_service_fingerprint);
    append_string(&mut material, &prepared.after_page_service_fingerprint);
    append_page_route_control_proof(&mut material, &prepared.route_control_proof);
    append_string(&mut material, &prepared.expected_route_control_checksum);
    etag(&material)
}

pub(crate) fn prepared_page_receipt_retention_config_checksum(
    prepared: &PreparedPageReceiptRetentionConfig,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-PREPARED-BLOCK-RECEIPT-RETENTION1");
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    append_u64(&mut material, prepared.retention as u64);
    append_u64(&mut material, prepared.trimmed_receipts);
    append_string(&mut material, &prepared.before_page_service_fingerprint);
    append_string(&mut material, &prepared.after_page_service_fingerprint);
    append_page_route_control_proof(&mut material, &prepared.route_control_proof);
    append_string(&mut material, &prepared.expected_route_control_checksum);
    etag(&material)
}

pub(crate) fn prepared_page_quorum_config_checksum(
    prepared: &PreparedPageQuorumConfig,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-PREPARED-BLOCK-QUORUM2");
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    append_u64(&mut material, prepared.write_quorum as u64);
    append_u64(&mut material, prepared.read_quorum as u64);
    append_string(&mut material, &prepared.before_page_service_fingerprint);
    append_string(&mut material, &prepared.after_page_service_fingerprint);
    append_page_route_control_proof(&mut material, &prepared.route_control_proof);
    append_string(&mut material, &prepared.expected_route_control_checksum);
    etag(&material)
}

pub(crate) fn append_page_batch_discard_receipt_legacy(
    out: &mut Vec<u8>,
    receipt: &PageBatchDiscardReceipt,
) {
    append_string(out, &receipt.request_checksum);
    append_string(out, &receipt.idempotency_key);
    append_page_batch_stats_legacy(out, &receipt.stats);
}
