pub(crate) fn append_page_chunk_control_proof(out: &mut Vec<u8>, proof: &ReplicaControlProof) {
    append_u64(out, proof.proof_count);
    append_string(out, &proof.proof_checksum);
    append_u64(out, proof.min_raft_commit_index);
    append_u64(out, proof.max_raft_commit_index);
    append_u64(out, proof.replica_service_state_checksums.len() as u64);
    for (index, checksum) in &proof.replica_service_state_checksums {
        append_u64(out, *index as u64);
        append_string(out, checksum);
    }
}

pub(crate) fn append_bounded_repair_report(out: &mut Vec<u8>, report: &BoundedRepairReport) {
    append_u64(out, report.total_chunk_count);
    append_u64(out, report.selected_chunk_count);
    append_u64(out, report.skipped_chunk_count);
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
        append_chunk_id(out, page_id);
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

pub(crate) fn append_prepared_page_repair_envelope(
    out: &mut Vec<u8>,
    envelope: &PreparedPageRepairEnvelope,
) {
    append_string(out, &envelope.operation);
    append_u64(out, envelope.chunk_id.is_some() as u64);
    if let Some(chunk_id) = &envelope.chunk_id {
        append_slab_id(out, chunk_id);
    }
    append_u64(out, envelope.page_id.is_some() as u64);
    if let Some(page_id) = &envelope.page_id {
        append_chunk_id(out, page_id);
    }
    append_u64(out, envelope.offset);
    append_u64(out, envelope.length);
    append_u64(out, envelope.max_repair_chunks);
    append_u64(out, envelope.start_chunk_index);
    append_u64(out, envelope.total_chunk_count);
    append_u64(out, envelope.selected_chunk_count);
    append_u64(out, envelope.skipped_chunk_count);
    append_u64(out, envelope.selected_repair_bytes);
    append_u64(out, envelope.has_more as u64);
    append_u64(out, envelope.next_chunk_index);
    append_u64(out, envelope.checked_chunks);
    append_u64(out, envelope.degraded_chunks);
    append_u64(out, envelope.repaired_chunks);
    append_u64(out, envelope.repaired_replicas);
    append_u64(out, envelope.failed_repairs);
    append_u64(out, envelope.unrepaired_chunks);
    append_string(out, &envelope.before_page_service_fingerprint);
    append_string(out, &envelope.after_page_service_fingerprint);
    append_u64(out, envelope.expected_previous_commit_index);
    append_u64(out, envelope.prepared_commit_index);
    append_string(out, &envelope.route_control_proof_format);
    append_string(out, &envelope.route_control_proof_checksum);
    append_u64(out, envelope.route_control_proof_bytes);
    append_u64(out, envelope.route_control_commit_index);
    append_u64(out, envelope.route_control_next_log_index);
    append_string(out, &envelope.expected_route_control_checksum);
    append_string(out, &envelope.raft_entry_checksum);
}

pub(crate) fn prepared_page_repair_envelope_checksum(
    envelope: &PreparedPageRepairEnvelope,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-PREPARED-BLOCK-REPAIR-ENVELOPE1");
    append_prepared_page_repair_envelope(&mut material, envelope);
    etag(&material)
}

pub(crate) fn prepared_page_range_repair_envelope(
    prepared: &PreparedPageRangeRepair,
) -> PreparedPageRepairEnvelope {
    let mut envelope = PreparedPageRepairEnvelope {
        operation: "range".to_string(),
        chunk_id: Some(prepared.chunk_id.clone()),
        page_id: None,
        offset: prepared.offset,
        length: prepared.length,
        max_repair_chunks: prepared.max_repair_chunks as u64,
        start_chunk_index: prepared.start_chunk_index,
        total_chunk_count: prepared.report.total_chunk_count,
        selected_chunk_count: prepared.report.selected_chunk_count,
        skipped_chunk_count: prepared.report.skipped_chunk_count,
        selected_repair_bytes: prepared.report.selected_repair_bytes,
        has_more: prepared.report.has_more,
        next_chunk_index: prepared.report.next_chunk_index,
        checked_chunks: prepared.report.stats.checked_chunks,
        degraded_chunks: prepared.report.stats.degraded_chunks,
        repaired_chunks: 0,
        repaired_replicas: prepared.report.stats.repaired_replicas,
        failed_repairs: prepared.report.stats.failed_repairs,
        unrepaired_chunks: 0,
        before_page_service_fingerprint: prepared.before_page_service_fingerprint.clone(),
        after_page_service_fingerprint: prepared.after_page_service_fingerprint.clone(),
        expected_previous_commit_index: prepared.expected_previous_commit_index,
        prepared_commit_index: prepared.prepared_commit_index,
        route_control_proof_format: prepared.route_control_proof.state_format.clone(),
        route_control_proof_checksum: prepared.route_control_proof.state_checksum.clone(),
        route_control_proof_bytes: prepared.route_control_proof.state_bytes,
        route_control_commit_index: prepared
            .route_control_proof
            .chunk_route_control_commit_index,
        route_control_next_log_index: prepared
            .route_control_proof
            .chunk_route_control_next_log_index,
        expected_route_control_checksum: prepared.expected_route_control_checksum.clone(),
        raft_entry_checksum: prepared.raft_entry_checksum.clone(),
        envelope_checksum: String::new(),
    };
    envelope.envelope_checksum = prepared_page_repair_envelope_checksum(&envelope);
    envelope
}

pub(crate) fn prepared_page_chunk_repair_envelope(
    prepared: &PreparedPageChunkRepair,
) -> PreparedPageRepairEnvelope {
    let mut envelope = PreparedPageRepairEnvelope {
        operation: "chunk".to_string(),
        chunk_id: Some(prepared.page_id.chunk_id.clone()),
        page_id: Some(prepared.page_id.clone()),
        offset: 0,
        length: 0,
        max_repair_chunks: 1,
        start_chunk_index: prepared.page_id.page_index,
        total_chunk_count: prepared.report.checked_chunks,
        selected_chunk_count: prepared.report.degraded_chunks,
        skipped_chunk_count: 0,
        selected_repair_bytes: 0,
        has_more: false,
        next_chunk_index: prepared.page_id.page_index.saturating_add(1),
        checked_chunks: prepared.report.checked_chunks,
        degraded_chunks: prepared.report.degraded_chunks,
        repaired_chunks: prepared.report.repaired_chunks,
        repaired_replicas: prepared.report.repaired_replicas,
        failed_repairs: prepared.report.failed_repairs,
        unrepaired_chunks: prepared.report.unrepaired_chunks,
        before_page_service_fingerprint: prepared.before_page_service_fingerprint.clone(),
        after_page_service_fingerprint: prepared.after_page_service_fingerprint.clone(),
        expected_previous_commit_index: prepared.expected_previous_commit_index,
        prepared_commit_index: prepared.prepared_commit_index,
        route_control_proof_format: prepared.route_control_proof.state_format.clone(),
        route_control_proof_checksum: prepared.route_control_proof.state_checksum.clone(),
        route_control_proof_bytes: prepared.route_control_proof.state_bytes,
        route_control_commit_index: prepared
            .route_control_proof
            .chunk_route_control_commit_index,
        route_control_next_log_index: prepared
            .route_control_proof
            .chunk_route_control_next_log_index,
        expected_route_control_checksum: prepared.expected_route_control_checksum.clone(),
        raft_entry_checksum: prepared.raft_entry_checksum.clone(),
        envelope_checksum: String::new(),
    };
    envelope.envelope_checksum = prepared_page_repair_envelope_checksum(&envelope);
    envelope
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

