pub(crate) fn chunk_allocation_reservation_request_checksum(
    length: u64,
    now_ms: u64,
    idempotency_key: &str,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MXOSBLOCK-ALLOCATION-RESERVATION-REQUEST1");
    append_u64(&mut material, length);
    append_u64(&mut material, now_ms);
    append_string(&mut material, idempotency_key);
    etag(&material)
}

pub(crate) fn page_batch_allocation_reservation_request_checksum(
    lengths: &[u64],
    now_ms: u64,
    idempotency_key: &str,
) -> String {
    let mut material = Vec::new();
    append_string(
        &mut material,
        "MXOSBLOCK-BATCH-ALLOCATION-RESERVATION-REQUEST1",
    );
    append_u64(&mut material, lengths.len() as u64);
    for length in lengths {
        append_u64(&mut material, *length);
    }
    append_u64(&mut material, now_ms);
    append_string(&mut material, idempotency_key);
    etag(&material)
}

pub(crate) fn chunk_allocation_release_request_checksum(
    allocation_id: &str,
    idempotency_key: &str,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MXOSBLOCK-ALLOCATION-RELEASE-REQUEST1");
    append_string(&mut material, allocation_id);
    append_string(&mut material, idempotency_key);
    etag(&material)
}

pub(crate) fn prepared_page_allocation_reservation_checksum(
    prepared: &PreparedPageAllocationReservation,
) -> String {
    let mut material = Vec::new();
    append_string(
        &mut material,
        "MOS-RUST-PREPARED-BLOCK-ALLOCATION-RESERVATION1",
    );
    append_u64(&mut material, prepared.length);
    append_u64(&mut material, prepared.now_ms);
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
    append_u64(&mut material, prepared.allocation_receipt_count_after);
    etag(&material)
}

pub(crate) fn append_prepared_page_allocation_reservation_envelope(
    out: &mut Vec<u8>,
    envelope: &PreparedPageAllocationReservationEnvelope,
) {
    append_string(out, &envelope.envelope_format);
    append_u64(out, envelope.length);
    append_u64(out, envelope.now_ms);
    append_string(out, &envelope.idempotency_key);
    append_string(out, &envelope.request_checksum);
    append_u64(out, envelope.expected_previous_commit_index);
    append_u64(out, envelope.prepared_commit_index);
    append_string(out, &envelope.before_page_service_fingerprint);
    append_string(out, &envelope.after_page_service_fingerprint);
    append_page_route_control_proof(out, &envelope.route_control_proof);
    append_string(out, &envelope.expected_route_control_checksum);
    append_string(out, &envelope.raft_entry_checksum);
    append_string(out, &envelope.allocation_id);
    append_slab_id(out, &envelope.allocation_chunk_id);
    append_u64(out, envelope.allocation_offset);
    append_u64(out, envelope.allocation_length);
    append_u64(out, envelope.allocation_create_time_ms);
    append_u64(out, envelope.deduped as u64);
    append_u64(out, envelope.pending_allocation_count_after);
    append_u64(out, envelope.allocation_receipt_count_after);
}

pub(crate) fn prepared_page_allocation_reservation_envelope_checksum(
    envelope: &PreparedPageAllocationReservationEnvelope,
) -> String {
    let mut material = Vec::new();
    append_prepared_page_allocation_reservation_envelope(&mut material, envelope);
    etag(&material)
}

pub(crate) fn make_prepared_page_allocation_reservation_envelope(
    prepared: &PreparedPageAllocationReservation,
) -> PreparedPageAllocationReservationEnvelope {
    let mut envelope = PreparedPageAllocationReservationEnvelope {
        envelope_format: "MXOSBLOCK-ALLOCATION-RESERVATION-ENVELOPE1".to_string(),
        length: prepared.length,
        now_ms: prepared.now_ms,
        idempotency_key: prepared.idempotency_key.clone(),
        request_checksum: prepared.request_checksum.clone(),
        expected_previous_commit_index: prepared.expected_previous_commit_index,
        prepared_commit_index: prepared.prepared_commit_index,
        before_page_service_fingerprint: prepared.before_page_service_fingerprint.clone(),
        after_page_service_fingerprint: prepared.after_page_service_fingerprint.clone(),
        route_control_proof: prepared.route_control_proof.clone(),
        expected_route_control_checksum: prepared.expected_route_control_checksum.clone(),
        raft_entry_checksum: prepared.raft_entry_checksum.clone(),
        allocation_id: prepared.receipt.allocation.allocation_id.clone(),
        allocation_chunk_id: prepared.receipt.allocation.chunk_id.clone(),
        allocation_offset: prepared.receipt.allocation.offset,
        allocation_length: prepared.receipt.allocation.length,
        allocation_create_time_ms: prepared.receipt.allocation.create_time_ms,
        deduped: prepared.receipt.deduped,
        pending_allocation_count_after: prepared.pending_allocation_count_after,
        allocation_receipt_count_after: prepared.allocation_receipt_count_after,
        envelope_checksum: String::new(),
    };
    envelope.envelope_checksum = prepared_page_allocation_reservation_envelope_checksum(&envelope);
    envelope
}

pub(crate) fn validate_page_allocation_batch_lengths(
    lengths: &[u64],
    allocator_chunk_size: u64,
) -> Result<(), ObjectError> {
    if lengths.is_empty() || lengths.len() > 100_000 {
        return Err(ObjectError::InvalidArgument(
            "block batch allocation range_count must be 1..100000".to_string(),
        ));
    }
    for length in lengths {
        if *length == 0 {
            return Err(ObjectError::InvalidArgument(
                "block batch allocation length must be positive".to_string(),
            ));
        }
        if *length > allocator_chunk_size {
            return Err(ObjectError::InvalidArgument(
                "block batch allocation length exceeds allocator segment size".to_string(),
            ));
        }
    }
    Ok(())
}

