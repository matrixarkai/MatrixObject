pub(crate) fn append_prepared_chunk_local_state_handoff_envelope(
    out: &mut Vec<u8>,
    envelope: &PreparedChunkLocalStateHandoffEnvelope,
) {
    append_string(out, &envelope.envelope_format);
    append_page_id(out, &envelope.page_id);
    append_u64(out, envelope.max_payload_bytes);
    append_string(out, &envelope.request_checksum);
    append_u64(out, envelope.payload_bytes);
    append_string(out, &envelope.payload_checksum);
    append_string(out, &envelope.state_checksum);
    append_u64(out, envelope.write_sequence);
    append_u64(out, envelope.discard_range_count);
    append_string(out, &envelope.handoff_token);
    append_string(out, &envelope.handoff_state_checksum);
    append_string(out, &envelope.install_id);
    append_u64(out, envelope.handoff_proof_version);
    append_u64(out, envelope.budget_limited as u64);
}

pub(crate) fn prepared_chunk_local_state_handoff_envelope_checksum(
    envelope: &PreparedChunkLocalStateHandoffEnvelope,
) -> String {
    let mut material = Vec::with_capacity(
            encoded_string_len(&envelope.envelope_format)
            + std::mem::size_of::<u64>() * 9
            + encoded_string_len(&envelope.request_checksum)
            + encoded_string_len(&envelope.payload_checksum)
            + encoded_string_len(&envelope.state_checksum)
            + encoded_string_len(&envelope.handoff_token)
            + encoded_string_len(&envelope.handoff_state_checksum)
            + encoded_string_len(&envelope.install_id),
    );
    append_prepared_chunk_local_state_handoff_envelope(&mut material, envelope);
    etag(&material)
}

pub(crate) fn make_prepared_chunk_local_state_handoff_envelope(
    handoff: &ReplicaStateHandoff,
    max_payload_bytes: u64,
) -> PreparedChunkLocalStateHandoffEnvelope {
    let payload_bytes = handoff.state.data.len() as u64;
    let mut envelope = PreparedChunkLocalStateHandoffEnvelope {
        envelope_format: "MXOSCHUNK-HANDOFF-ENVELOPE1".to_string(),
        page_id: handoff.page_id.clone(),
        max_payload_bytes,
        request_checksum: chunk_local_state_handoff_request_checksum(
            &handoff.page_id,
            max_payload_bytes,
        ),
        payload_bytes,
        payload_checksum: handoff.payload_checksum.clone(),
        state_checksum: chunk_local_state_checksum(&handoff.state),
        write_sequence: handoff.state.write_sequence,
        discard_range_count: handoff.state.discarded_ranges.len() as u64,
        handoff_token: handoff.handoff_token.clone(),
        handoff_state_checksum: handoff.state_checksum.clone(),
        install_id: handoff.install_id.clone(),
        handoff_proof_version: handoff.proof_version,
        budget_limited: max_payload_bytes > 0 && payload_bytes > max_payload_bytes,
        envelope_checksum: String::new(),
    };
    envelope.envelope_checksum = prepared_chunk_local_state_handoff_envelope_checksum(&envelope);
    envelope
}

pub(crate) fn append_prepared_chunk_state_handoff_envelope(
    out: &mut Vec<u8>,
    envelope: &PreparedChunkStateHandoffEnvelope,
) {
    append_string(out, &envelope.envelope_format);
    append_chunk_id(out, &envelope.chunk_id);
    append_u64(out, envelope.offset);
    append_u64(out, envelope.length);
    append_u64(out, envelope.source_replica_index as u64);
    append_u64(out, envelope.max_payload_bytes);
    append_string(out, &envelope.request_checksum);
    append_u64(out, envelope.payload_bytes);
    append_string(out, &envelope.payload_checksum);
    append_u64(out, envelope.handoff_count);
    append_u64(out, envelope.budget_limited as u64);
    append_string(out, &envelope.route_control_proof_format);
    append_string(out, &envelope.route_control_proof_checksum);
    append_u64(out, envelope.route_control_proof_bytes);
    append_u64(out, envelope.route_control_commit_index);
    append_u64(out, envelope.route_control_next_log_index);
    append_u64(out, envelope.first_handoff_chunk_id.is_some() as u64);
    if let Some(page_id) = &envelope.first_handoff_chunk_id {
        append_page_id(out, page_id);
    }
    append_string(out, &envelope.first_handoff_state_checksum);
    append_u64(out, envelope.first_handoff_payload_bytes);
    append_u64(out, envelope.handoff_state_checksums.len() as u64);
    for checksum in &envelope.handoff_state_checksums {
        append_string(out, checksum);
    }
}

pub(crate) fn prepared_chunk_state_handoff_envelope_checksum(
    envelope: &PreparedChunkStateHandoffEnvelope,
) -> String {
    let checksum_bytes = envelope
        .handoff_state_checksums
        .iter()
        .map(|checksum| encoded_string_len(checksum))
        .sum::<usize>();
    let mut material = Vec::with_capacity(
        encoded_string_len(&envelope.envelope_format)
            + std::mem::size_of::<u64>() * 16
            + envelope
                .first_handoff_chunk_id
                .as_ref()
                .map(|_| std::mem::size_of::<u64>() * 3)
                .unwrap_or(0)
            + encoded_string_len(&envelope.request_checksum)
            + encoded_string_len(&envelope.payload_checksum)
            + encoded_string_len(&envelope.route_control_proof_format)
            + encoded_string_len(&envelope.route_control_proof_checksum)
            + encoded_string_len(&envelope.first_handoff_state_checksum)
            + checksum_bytes,
    );
    append_prepared_chunk_state_handoff_envelope(&mut material, envelope);
    etag(&material)
}

pub(crate) fn make_prepared_chunk_state_handoff_envelope(
    chunk_id: &ChunkId,
    offset: u64,
    length: u64,
    source_replica_index: usize,
    max_payload_bytes: u64,
    handoffs: &[ReplicaStateHandoff],
    route_control_proof: &ChunkRouteControlProof,
) -> PreparedChunkStateHandoffEnvelope {
    let payload_bytes = handoffs.iter().fold(0u64, |sum, handoff| {
        sum.saturating_add(handoff.state.data.len() as u64)
    });
    let first_handoff = handoffs.first();
    let mut handoff_state_checksums = Vec::with_capacity(handoffs.len());
    handoff_state_checksums.extend(
        handoffs
            .iter()
            .map(|handoff| handoff.state_checksum.clone()),
    );
    let mut envelope = PreparedChunkStateHandoffEnvelope {
        envelope_format: "MXOSBLOCK-CHUNK-HANDOFF-ENVELOPE1".to_string(),
        chunk_id: chunk_id.clone(),
        offset,
        length,
        source_replica_index,
        max_payload_bytes,
        request_checksum: replica_state_handoff_request_checksum(
            chunk_id,
            offset,
            length,
            source_replica_index,
            max_payload_bytes,
            &route_control_proof.state_checksum,
        ),
        payload_bytes,
        payload_checksum: chunk_state_handoff_payload_checksum(handoffs),
        handoff_count: handoffs.len() as u64,
        budget_limited: max_payload_bytes > 0 && payload_bytes >= max_payload_bytes,
        route_control_proof_format: route_control_proof.state_format.clone(),
        route_control_proof_checksum: route_control_proof.state_checksum.clone(),
        route_control_proof_bytes: route_control_proof.state_bytes,
        route_control_commit_index: route_control_proof.chunk_route_control_commit_index,
        route_control_next_log_index: route_control_proof.chunk_route_control_next_log_index,
        first_handoff_chunk_id: first_handoff.map(|handoff| handoff.page_id.clone()),
        first_handoff_state_checksum: first_handoff
            .map(|handoff| handoff.state_checksum.clone())
            .unwrap_or_default(),
        first_handoff_payload_bytes: first_handoff
            .map(|handoff| handoff.state.data.len() as u64)
            .unwrap_or_default(),
        handoff_state_checksums,
        envelope_checksum: String::new(),
    };
    envelope.envelope_checksum = prepared_chunk_state_handoff_envelope_checksum(&envelope);
    envelope
}

