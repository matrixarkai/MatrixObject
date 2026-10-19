pub(crate) fn append_page_batch_allocation_receipt(
    out: &mut Vec<u8>,
    receipt: &ChunkBatchAllocationReceipt,
) {
    append_u64(out, receipt.allocations.len() as u64);
    for allocation in &receipt.allocations {
        append_page_allocation(out, allocation);
    }
    append_string(out, &receipt.idempotency_key);
    append_u64(out, receipt.deduped as u64);
}

pub(crate) fn page_batch_allocation_receipt_proof_checksum(
    receipt: &ChunkBatchAllocationReceipt,
) -> String {
    let allocation_bytes: usize = receipt
        .allocations
        .iter()
        .map(chunk_allocation_encoded_len)
        .sum();
    let mut material = Vec::with_capacity(
        encoded_string_len("MXOS-BLOCK-ALLOCATION-BATCH-RECEIPT-PROOF1")
            + std::mem::size_of::<u64>()
            + allocation_bytes
            + encoded_string_len(&receipt.idempotency_key)
            + std::mem::size_of::<u64>(),
    );
    append_string(&mut material, "MXOS-BLOCK-ALLOCATION-BATCH-RECEIPT-PROOF1");
    append_page_batch_allocation_receipt(&mut material, receipt);
    etag(&material)
}

pub(crate) fn page_batch_allocation_receipt_page_proof_checksum(
    page: &PageBatchAllocationReceiptPage,
) -> String {
    let receipt_proof_bytes = page
        .receipts
        .len()
        .saturating_mul(encoded_string_len("0000000000000000"));
    let mut material = Vec::with_capacity(
        encoded_string_len("MXOS-BLOCK-ALLOCATION-BATCH-RECEIPT-PAGE-PROOF1")
            + std::mem::size_of::<u64>() * 16
            + page
                .next_marker
                .as_ref()
                .map(|value| encoded_string_len(value))
                .unwrap_or(0)
            + receipt_proof_bytes,
    );
    append_string(
        &mut material,
        "MXOS-BLOCK-ALLOCATION-BATCH-RECEIPT-PAGE-PROOF1",
    );
    append_page_receipt_page_common(
        &mut material,
        page.receipt_count,
        page.total_receipt_count,
        page.scanned_count,
        page.limit,
        page.truncated,
        &page.next_marker,
        &page.chunk_id,
        page.skipped_slab_count,
    );
    append_page_receipt_page_chunk_filter(
        &mut material,
        page.page_index,
        page.page_size,
        page.skipped_chunk_count,
    );
    append_u64(&mut material, page.receipts.len() as u64);
    for receipt in &page.receipts {
        append_string(&mut material, &receipt.proof_checksum());
    }
    etag(&material)
}

pub(crate) fn prepared_page_batch_allocation_reservation_checksum(
    prepared: &PreparedPageBatchAllocationReservation,
) -> String {
    let allocation_bytes: usize = prepared
        .receipt
        .allocations
        .iter()
        .map(chunk_allocation_encoded_len)
        .sum();
    let mut material = Vec::with_capacity(
        encoded_string_len("MOS-RUST-PREPARED-BLOCK-BATCH-ALLOCATION-RESERVATION1")
            + std::mem::size_of::<u64>() * (40 + prepared.lengths.len())
            + encoded_string_len(&prepared.idempotency_key)
            + encoded_string_len(&prepared.request_checksum)
            + encoded_string_len(&prepared.before_page_service_fingerprint)
            + encoded_string_len(&prepared.after_page_service_fingerprint)
            + encoded_string_len(&prepared.route_control_proof.state_format)
            + encoded_string_len(&prepared.route_control_proof.state_checksum)
            + encoded_string_len(&prepared.expected_route_control_checksum)
            + allocation_bytes
            + encoded_string_len(&prepared.receipt.idempotency_key),
    );
    append_string(
        &mut material,
        "MOS-RUST-PREPARED-BLOCK-BATCH-ALLOCATION-RESERVATION1",
    );
    append_u64(&mut material, prepared.lengths.len() as u64);
    for length in &prepared.lengths {
        append_u64(&mut material, *length);
    }
    append_u64(&mut material, prepared.now_ms);
    append_string(&mut material, &prepared.idempotency_key);
    append_string(&mut material, &prepared.request_checksum);
    append_string(&mut material, &prepared.before_page_service_fingerprint);
    append_string(&mut material, &prepared.after_page_service_fingerprint);
    append_page_route_control_proof(&mut material, &prepared.route_control_proof);
    append_string(&mut material, &prepared.expected_route_control_checksum);
    append_page_batch_allocation_receipt(&mut material, &prepared.receipt);
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    append_u64(&mut material, prepared.pending_allocation_count_after);
    append_u64(&mut material, prepared.allocation_receipt_count_after);
    etag(&material)
}

pub(crate) fn append_prepared_page_batch_allocation_reservation_envelope(
    out: &mut Vec<u8>,
    envelope: &PreparedPageBatchAllocationReservationEnvelope,
) {
    append_string(out, &envelope.envelope_format);
    append_u64(out, envelope.lengths.len() as u64);
    for length in &envelope.lengths {
        append_u64(out, *length);
    }
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
    append_u64(out, envelope.allocation_count);
    for i in 0..envelope.allocation_ids.len() {
        append_string(out, &envelope.allocation_ids[i]);
        append_chunk_id(out, &envelope.allocation_chunks[i]);
        append_u64(out, envelope.allocation_offsets[i]);
        append_u64(out, envelope.allocation_lengths[i]);
        append_u64(out, envelope.allocation_create_time_ms[i]);
    }
    append_u64(out, envelope.pending_allocation_count_after);
    append_u64(out, envelope.allocation_receipt_count_after);
}

pub(crate) fn prepared_page_batch_allocation_reservation_envelope_checksum(
    envelope: &PreparedPageBatchAllocationReservationEnvelope,
) -> String {
    let allocation_id_bytes: usize = envelope
        .allocation_ids
        .iter()
        .map(|allocation_id| encoded_string_len(allocation_id))
        .sum();
    let mut material = Vec::with_capacity(
        encoded_string_len(&envelope.envelope_format)
            + std::mem::size_of::<u64>() * (8 + envelope.lengths.len() + 4 * envelope.allocation_ids.len())
            + encoded_string_len(&envelope.idempotency_key)
            + encoded_string_len(&envelope.request_checksum)
            + encoded_string_len(&envelope.before_page_service_fingerprint)
            + encoded_string_len(&envelope.after_page_service_fingerprint)
            + encoded_string_len(&envelope.route_control_proof.state_format)
            + encoded_string_len(&envelope.route_control_proof.state_checksum)
            + encoded_string_len(&envelope.expected_route_control_checksum)
            + encoded_string_len(&envelope.raft_entry_checksum)
            + allocation_id_bytes,
    );
    append_prepared_page_batch_allocation_reservation_envelope(&mut material, envelope);
    etag(&material)
}

pub(crate) fn make_prepared_page_batch_allocation_reservation_envelope(
    prepared: &PreparedPageBatchAllocationReservation,
) -> PreparedPageBatchAllocationReservationEnvelope {
    let mut envelope = PreparedPageBatchAllocationReservationEnvelope {
        envelope_format: "MXOSBLOCK-BATCH-ALLOCATION-RESERVATION-ENVELOPE1".to_string(),
        lengths: prepared.lengths.clone(),
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
        allocation_count: prepared.receipt.allocations.len() as u64,
        allocation_ids: prepared
            .receipt
            .allocations
            .iter()
            .map(|item| item.allocation_id.clone())
            .collect(),
        allocation_chunks: prepared
            .receipt
            .allocations
            .iter()
            .map(|item| item.chunk_id.clone())
            .collect(),
        allocation_offsets: prepared
            .receipt
            .allocations
            .iter()
            .map(|item| item.offset)
            .collect(),
        allocation_lengths: prepared
            .receipt
            .allocations
            .iter()
            .map(|item| item.length)
            .collect(),
        allocation_create_time_ms: prepared
            .receipt
            .allocations
            .iter()
            .map(|item| item.create_time_ms)
            .collect(),
        pending_allocation_count_after: prepared.pending_allocation_count_after,
        allocation_receipt_count_after: prepared.allocation_receipt_count_after,
        envelope_checksum: String::new(),
    };
    envelope.envelope_checksum =
        prepared_page_batch_allocation_reservation_envelope_checksum(&envelope);
    envelope
}
