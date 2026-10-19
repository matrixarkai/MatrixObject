pub(crate) fn append_prepared_chunk_reclaim_envelope(
    out: &mut Vec<u8>,
    envelope: &PreparedChunkReclaimEnvelope,
) {
    append_string(out, &envelope.envelope_format);
    append_u64(out, envelope.replica_index as u64);
    append_u64(out, envelope.max_chunks);
    append_u64(out, envelope.max_rewrite_bytes);
    append_u64(out, envelope.min_discarded_bytes);
    append_u64(out, envelope.min_discard_ratio_ppm);
    append_u64(out, envelope.max_discard_ranges);
    append_u64(out, envelope.max_trim_bytes);
    append_u64(out, envelope.now_ms);
    append_string(out, &envelope.plan_checksum);
    append_u64(out, envelope.expected_previous_commit_index);
    append_u64(out, envelope.prepared_commit_index);
    append_string(out, &envelope.before_replica_service_fingerprint);
    append_string(out, &envelope.after_replica_service_fingerprint);
    append_string(out, &envelope.route_control_proof_format);
    append_string(out, &envelope.route_control_proof_checksum);
    append_u64(out, envelope.route_control_proof_bytes);
    append_u64(out, envelope.route_control_commit_index);
    append_u64(out, envelope.route_control_next_log_index);
    append_string(out, &envelope.expected_route_control_checksum);
    append_string(out, &envelope.raft_entry_checksum);
    append_u64(out, envelope.candidate_chunks);
    append_u64(out, envelope.candidate_discarded_bytes);
    append_u64(out, envelope.scheduled_reclaim_chunks);
    append_u64(out, envelope.scheduled_rewrite_bytes);
    append_u64(out, envelope.scheduled_trim_ops);
    append_u64(out, envelope.scheduled_trim_bytes);
    append_u64(out, envelope.reclaimed_chunks);
    append_u64(out, envelope.reclaimed_discard_bytes);
    append_u64(out, envelope.rewritten_bytes);
    append_u64(out, envelope.trimmed_bytes);
    append_u64(out, envelope.budget_limited as u64);
    append_u64(out, envelope.trim_flow_limited as u64);
    append_u64(out, envelope.truncated as u64);
    append_u64(out, envelope.continuation_safe as u64);
    append_u64(out, envelope.next_marker_advanced as u64);
    append_u64(out, envelope.first_reclaim_chunk_id.is_some() as u64);
    if let Some(page_id) = &envelope.first_reclaim_chunk_id {
        append_chunk_id(out, page_id);
    }
    append_u64(out, envelope.first_reclaim_discarded_bytes);
    append_u64(out, envelope.first_reclaim_discard_ratio_ppm);
    append_u64(out, envelope.first_reclaim_trim_bytes);
}

pub(crate) fn prepared_chunk_reclaim_envelope_checksum(
    envelope: &PreparedChunkReclaimEnvelope,
) -> String {
    let mut material = Vec::new();
    append_prepared_chunk_reclaim_envelope(&mut material, envelope);
    etag(&material)
}

pub(crate) fn make_prepared_chunk_reclaim_envelope(
    prepared: &PreparedChunkReplicaReclaim,
) -> PreparedChunkReclaimEnvelope {
    let first_candidate = prepared.plan.candidates.first();
    let mut envelope = PreparedChunkReclaimEnvelope {
        envelope_format: "MXOSCHUNK-RECLAIM-ENVELOPE1".to_string(),
        replica_index: prepared.replica_index,
        max_chunks: prepared.max_chunks as u64,
        max_rewrite_bytes: prepared.max_rewrite_bytes,
        min_discarded_bytes: prepared.min_discarded_bytes,
        min_discard_ratio_ppm: prepared.min_discard_ratio_ppm,
        max_discard_ranges: prepared.max_discard_ranges as u64,
        max_trim_bytes: prepared.max_trim_bytes,
        now_ms: prepared.now_ms,
        plan_checksum: prepared.plan.plan_checksum.clone(),
        expected_previous_commit_index: prepared.expected_previous_commit_index,
        prepared_commit_index: prepared.prepared_commit_index,
        before_replica_service_fingerprint: prepared.before_replica_service_fingerprint.clone(),
        after_replica_service_fingerprint: prepared.after_replica_service_fingerprint.clone(),
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
        candidate_chunks: prepared.plan.candidate_chunks,
        candidate_discarded_bytes: prepared.plan.candidate_discarded_bytes,
        scheduled_reclaim_chunks: prepared.plan.scheduled_chunks,
        scheduled_rewrite_bytes: prepared.plan.scheduled_rewrite_bytes,
        scheduled_trim_ops: prepared.plan.scheduled_trim_ops,
        scheduled_trim_bytes: prepared.plan.scheduled_trim_bytes,
        reclaimed_chunks: prepared.report.reclaimed_chunks,
        reclaimed_discard_bytes: prepared.report.reclaimed_discard_bytes,
        rewritten_bytes: prepared.report.rewritten_bytes,
        trimmed_bytes: prepared.report.trimmed_bytes,
        budget_limited: prepared.plan.budget_limited || prepared.report.budget_limited,
        trim_flow_limited: prepared.plan.trim_flow_limited || prepared.report.trim_flow_limited,
        truncated: prepared.plan.truncated,
        continuation_safe: prepared.plan.continuation_safe,
        next_marker_advanced: prepared.plan.next_marker_advanced,
        first_reclaim_chunk_id: first_candidate.map(|candidate| candidate.page_id.clone()),
        first_reclaim_discarded_bytes: first_candidate
            .map(|candidate| candidate.discarded_bytes)
            .unwrap_or_default(),
        first_reclaim_discard_ratio_ppm: first_candidate
            .map(|candidate| candidate.discard_ratio_ppm)
            .unwrap_or_default(),
        first_reclaim_trim_bytes: first_candidate
            .map(|candidate| candidate.trim_bytes)
            .unwrap_or_default(),
        envelope_checksum: String::new(),
    };
    envelope.envelope_checksum = prepared_chunk_reclaim_envelope_checksum(&envelope);
    envelope
}
