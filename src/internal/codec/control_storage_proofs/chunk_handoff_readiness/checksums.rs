pub(crate) fn chunk_state_handoff_checksum(
    page_id: &PageId,
    state: &PageState,
    payload_checksum: &str,
) -> String {
    let mut material = Vec::with_capacity(
        std::mem::size_of::<u64>() * 6
            + encoded_string_len(payload_checksum)
            + state.discarded_ranges.len() * std::mem::size_of::<u64>() * 2,
    );
    append_slab_id(&mut material, &page_id.slab_id);
    append_u64(&mut material, page_id.page_index);
    append_u64(&mut material, state.logical_length);
    append_u64(&mut material, state.write_sequence);
    append_string(&mut material, payload_checksum);
    append_u64(&mut material, state.discarded_ranges.len() as u64);
    for range in &state.discarded_ranges {
        append_range(&mut material, range);
    }
    etag(&material)
}

pub(crate) fn chunk_local_state_checksum(state: &PageState) -> String {
    let data_checksum = etag(&state.data);
    let mut material = Vec::with_capacity(
        std::mem::size_of::<u64>() * 4
            + encoded_string_len(&state.checksum)
            + encoded_string_len(&data_checksum)
            + state.discarded_ranges.len() * std::mem::size_of::<u64>() * 2,
    );
    append_u64(&mut material, state.logical_length);
    append_u64(&mut material, state.write_sequence);
    append_string(&mut material, &state.checksum);
    append_string(&mut material, &data_checksum);
    append_u64(&mut material, state.discarded_ranges.len() as u64);
    for range in &state.discarded_ranges {
        append_range(&mut material, range);
    }
    etag(&material)
}

pub(crate) fn chunk_state_handoff_checksum_v2(
    page_id: &PageId,
    handoff_token: &str,
    state: &PageState,
    payload_checksum: &str,
) -> String {
    let legacy_checksum = chunk_state_handoff_checksum(page_id, state, payload_checksum);
    let mut material = Vec::with_capacity(
        encoded_string_len("MOSRS_CHUNK_HANDOFF_V2")
            + encoded_string_len(handoff_token)
            + encoded_string_len(&legacy_checksum),
    );
    append_string(&mut material, "MOSRS_CHUNK_HANDOFF_V2");
    append_string(&mut material, handoff_token);
    append_string(&mut material, &legacy_checksum);
    etag(&material)
}

pub(crate) fn chunk_state_handoff_payload_checksum(handoffs: &[ReplicaStateHandoff]) -> String {
    let payload_bytes = handoffs.iter().fold(0usize, |sum, handoff| {
        sum.saturating_add(handoff.state.data.len())
    });
    let mut payload = Vec::with_capacity(payload_bytes);
    for handoff in handoffs {
        payload.extend_from_slice(&handoff.state.data);
    }
    etag(&payload)
}

pub(crate) fn chunk_local_state_handoff_request_checksum(
    page_id: &PageId,
    max_payload_bytes: u64,
) -> String {
    let mut material = Vec::with_capacity(
        encoded_string_len("MXOSCHUNK-HANDOFF-REQUEST1")
            + std::mem::size_of::<u64>() * 4,
    );
    append_string(&mut material, "MXOSCHUNK-HANDOFF-REQUEST1");
    append_chunk_id(&mut material, page_id);
    append_u64(&mut material, max_payload_bytes);
    etag(&material)
}

pub(crate) fn replica_state_handoff_request_checksum(
    slab_id: &SlabId,
    offset: u64,
    length: u64,
    source_replica_index: usize,
    max_payload_bytes: u64,
    route_control_proof_checksum: &str,
) -> String {
    let mut material = Vec::with_capacity(
        encoded_string_len("MXOSBLOCK-CHUNK-HANDOFF-REQUEST1")
            + std::mem::size_of::<u64>() * 6
            + encoded_string_len(route_control_proof_checksum),
    );
    append_string(&mut material, "MXOSBLOCK-CHUNK-HANDOFF-REQUEST1");
    append_slab_id(&mut material, slab_id);
    append_u64(&mut material, offset);
    append_u64(&mut material, length);
    append_u64(&mut material, source_replica_index as u64);
    append_u64(&mut material, max_payload_bytes);
    append_string(&mut material, route_control_proof_checksum);
    etag(&material)
}

