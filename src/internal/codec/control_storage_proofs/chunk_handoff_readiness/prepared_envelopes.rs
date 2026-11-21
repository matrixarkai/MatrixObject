// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
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

