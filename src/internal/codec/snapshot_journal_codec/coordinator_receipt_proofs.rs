pub(crate) fn metadata_coordinator_config_receipt_page_proof_checksum(
    page: &MetadataCoordinatorConfigReceiptPage,
) -> String {
    let mut material = Vec::new();
    append_string(
        &mut material,
        "MXOS-META-COORDINATOR-CONFIG-RECEIPT-PAGE-PROOF1",
    );
    metadata_receipt_page_common(
        &mut material,
        page.receipt_count,
        page.total_receipt_count,
        page.scanned_count,
        page.limit,
        page.truncated,
        &page.next_marker,
    );
    append_u64(&mut material, page.backend.is_some() as u64);
    if let Some(backend) = &page.backend {
        append_string(&mut material, backend);
    }
    append_u64(&mut material, page.receipts.len() as u64);
    for receipt in &page.receipts {
        append_string(&mut material, &receipt.proof_checksum());
    }
    etag(&material)
}

pub(crate) fn coordinator_session_payload_bytes(
    session: &CoordinatorSession,
) -> Result<u64, ObjectError> {
    (session.session_id.len() as u64)
        .checked_add(session.owner.len() as u64)
        .and_then(|total| total.checked_add(session.endpoint.len() as u64))
        .ok_or_else(|| {
            ObjectError::InvalidArgument(
                "metadata coordinator session payload bytes overflow".to_string(),
            )
        })
}

pub(crate) fn append_coordinator_session_receipt(
    out: &mut Vec<u8>,
    receipt: &CoordinatorSessionReceipt,
) {
    append_string(out, &receipt.idempotency_key);
    append_string(out, &receipt.operation);
    append_string(out, &receipt.request_checksum);
    append_coordinator_session(out, &receipt.session);
    append_u64(out, receipt.released as u64);
    append_u64(out, receipt.commit_index);
    append_u64(out, receipt.update_time_ms);
}

pub(crate) fn coordinator_session_receipt_proof_checksum(
    receipt: &CoordinatorSessionReceipt,
) -> String {
    let mut material = Vec::new();
    append_string(
        &mut material,
        "MXOS-META-COORDINATOR-SESSION-RECEIPT-PROOF1",
    );
    append_coordinator_session_receipt(&mut material, receipt);
    etag(&material)
}

pub(crate) fn coordinator_session_receipt_page_proof_checksum(
    page: &CoordinatorSessionReceiptPage,
) -> String {
    let mut material = Vec::new();
    append_string(
        &mut material,
        "MXOS-META-COORDINATOR-SESSION-RECEIPT-PAGE-PROOF1",
    );
    append_u64(&mut material, page.receipt_count);
    append_u64(&mut material, page.total_receipt_count);
    append_u64(&mut material, page.scanned_count);
    append_u64(&mut material, page.matched_count);
    append_u64(&mut material, page.payload_bytes);
    append_u64(&mut material, page.max_payload_bytes);
    append_u64(&mut material, page.budget_limited as u64);
    append_u64(&mut material, page.truncated as u64);
    append_u64(&mut material, page.marker.is_some() as u64);
    if let Some(marker) = &page.marker {
        append_string(&mut material, marker);
    }
    append_u64(&mut material, page.next_marker.is_some() as u64);
    if let Some(next_marker) = &page.next_marker {
        append_string(&mut material, next_marker);
    }
    append_u64(&mut material, page.operation.is_some() as u64);
    if let Some(operation) = &page.operation {
        append_string(&mut material, operation);
    }
    append_u64(&mut material, page.session_id.is_some() as u64);
    if let Some(session_id) = &page.session_id {
        append_string(&mut material, session_id);
    }
    append_u64(&mut material, page.owner.is_some() as u64);
    if let Some(owner) = &page.owner {
        append_string(&mut material, owner);
    }
    append_u64(&mut material, page.released.is_some() as u64);
    if let Some(released) = page.released {
        append_u64(&mut material, released as u64);
    }
    append_u64(&mut material, page.skipped_operation_count);
    append_u64(&mut material, page.skipped_session_count);
    append_u64(&mut material, page.skipped_owner_count);
    append_u64(&mut material, page.skipped_released_count);
    append_u64(&mut material, page.receipts.len() as u64);
    for receipt in &page.receipts {
        append_string(&mut material, &receipt.proof_checksum());
    }
    etag(&material)
}

