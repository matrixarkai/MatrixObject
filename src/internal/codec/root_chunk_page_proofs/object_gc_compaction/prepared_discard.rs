pub(crate) fn prepared_chunk_discard_checksum(prepared: &PreparedChunkDiscard) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-PREPARED-SEGMENT-DISCARD1");
    append_chunk_id(&mut material, &prepared.chunk_id);
    append_u64(&mut material, prepared.discard_bytes);
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    append_string(&mut material, &prepared.before_state_fingerprint);
    append_string(&mut material, &prepared.after_state_fingerprint);
    append_string(&mut material, &prepared.request_checksum);
    append_u64(&mut material, prepared.ranges.len() as u64);
    for range in &prepared.ranges {
        append_range(&mut material, range);
    }
    append_u64(&mut material, prepared.source_stale_ranges.len() as u64);
    for range in &prepared.source_stale_ranges {
        append_range(&mut material, range);
    }
    append_u64(&mut material, prepared.source_discarded_ranges.len() as u64);
    for range in &prepared.source_discarded_ranges {
        append_range(&mut material, range);
    }
    append_page_route_control_proof(&mut material, &prepared.route_control_proof);
    append_string(&mut material, &prepared.expected_route_control_checksum);
    etag(&material)
}

pub(crate) fn append_prepared_chunk_discard_envelope(
    out: &mut Vec<u8>,
    envelope: &PreparedChunkDiscardEnvelope,
) {
    append_string(out, &envelope.envelope_format);
    append_chunk_id(out, &envelope.chunk_id);
    append_u64(out, envelope.range_count);
    append_u64(out, envelope.discard_bytes);
    append_u64(out, envelope.source_stale_range_count);
    append_u64(out, envelope.source_discarded_range_count);
    append_u64(out, envelope.first_range_offset);
    append_u64(out, envelope.first_range_length);
    append_string(out, &envelope.before_state_fingerprint);
    append_string(out, &envelope.after_state_fingerprint);
    append_string(out, &envelope.request_checksum);
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

pub(crate) fn prepared_chunk_discard_envelope_checksum(
    envelope: &PreparedChunkDiscardEnvelope,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-SEGMENT-DISCARD-ENVELOPE1");
    append_prepared_chunk_discard_envelope(&mut material, envelope);
    etag(&material)
}

pub(crate) fn make_prepared_chunk_discard_envelope(
    prepared: &PreparedChunkDiscard,
) -> PreparedChunkDiscardEnvelope {
    let first_range = prepared.ranges.first();
    let mut envelope = PreparedChunkDiscardEnvelope {
        envelope_format: "MXOSSEGMENT-DISCARD-ENVELOPE1".to_string(),
        chunk_id: prepared.chunk_id.clone(),
        range_count: prepared.ranges.len() as u64,
        discard_bytes: prepared.discard_bytes,
        source_stale_range_count: prepared.source_stale_ranges.len() as u64,
        source_discarded_range_count: prepared.source_discarded_ranges.len() as u64,
        first_range_offset: first_range.map(|range| range.offset).unwrap_or_default(),
        first_range_length: first_range.map(|range| range.length).unwrap_or_default(),
        before_state_fingerprint: prepared.before_state_fingerprint.clone(),
        after_state_fingerprint: prepared.after_state_fingerprint.clone(),
        request_checksum: prepared.request_checksum.clone(),
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
    envelope.envelope_checksum = prepared_chunk_discard_envelope_checksum(&envelope);
    envelope
}
