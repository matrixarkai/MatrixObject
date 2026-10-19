pub(crate) fn append_page_receipt_page_common(
    material: &mut Vec<u8>,
    receipt_count: u64,
    total_receipt_count: u64,
    scanned_count: u64,
    limit: u64,
    truncated: bool,
    next_marker: &Option<String>,
    chunk_id: &Option<ChunkId>,
    skipped_slab_count: u64,
) {
    append_u64(material, receipt_count);
    append_u64(material, total_receipt_count);
    append_u64(material, scanned_count);
    append_u64(material, limit);
    append_u64(material, truncated as u64);
    append_u64(material, next_marker.is_some() as u64);
    if let Some(next_marker) = next_marker {
        append_string(material, next_marker);
    }
    append_u64(material, chunk_id.is_some() as u64);
    if let Some(chunk_id) = chunk_id {
        append_chunk_id(material, chunk_id);
    }
    append_u64(material, skipped_slab_count);
}

pub(crate) fn append_page_receipt_page_chunk_filter(
    material: &mut Vec<u8>,
    page_index: Option<u64>,
    page_size: Option<u64>,
    skipped_chunk_count: u64,
) {
    append_u64(material, page_index.is_some() as u64);
    if let Some(page_index) = page_index {
        append_u64(material, page_index);
        append_u64(material, page_size.unwrap_or(0));
    }
    append_u64(material, skipped_chunk_count);
}

fn receipt_page_proof_capacity(
    proof_format: &str,
    next_marker: &Option<String>,
    chunk_id: &Option<ChunkId>,
    receipt_count: usize,
    extra_u64_fields: usize,
) -> usize {
    encoded_string_len(proof_format)
        + std::mem::size_of::<u64>() * (10 + extra_u64_fields)
        + next_marker
            .as_ref()
            .map(|value| encoded_string_len(value))
            .unwrap_or(0)
        + if chunk_id.is_some() {
            std::mem::size_of::<u64>() * 2
        } else {
            0
        }
        + receipt_count.saturating_mul(encoded_string_len("0000000000000000"))
}

fn chunk_receipt_page_proof_capacity(
    proof_format: &str,
    next_marker: &Option<String>,
    page_id: &Option<PageId>,
    receipt_count: usize,
) -> usize {
    encoded_string_len(proof_format)
        + std::mem::size_of::<u64>() * 10
        + next_marker
            .as_ref()
            .map(|value| encoded_string_len(value))
            .unwrap_or(0)
        + if page_id.is_some() {
            std::mem::size_of::<u64>() * 3
        } else {
            0
        }
        + receipt_count.saturating_mul(encoded_string_len("0000000000000000"))
}

pub(crate) fn page_write_receipt_page_proof_checksum(page: &PageWriteReceiptPage) -> String {
    const PROOF_FORMAT: &str = "MXOS-BLOCK-WRITE-RECEIPT-PAGE-PROOF1";
    let mut material = Vec::with_capacity(receipt_page_proof_capacity(
        PROOF_FORMAT,
        &page.next_marker,
        &page.chunk_id,
        page.receipts.len(),
        3,
    ));
    append_string(&mut material, PROOF_FORMAT);
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

pub(crate) fn page_batch_write_receipt_page_proof_checksum(
    page: &PageBatchWriteReceiptPage,
) -> String {
    const PROOF_FORMAT: &str = "MXOS-BLOCK-WRITE-BATCH-RECEIPT-PAGE-PROOF1";
    let mut material = Vec::with_capacity(receipt_page_proof_capacity(
        PROOF_FORMAT,
        &page.next_marker,
        &None,
        page.receipts.len(),
        0,
    ));
    append_string(&mut material, PROOF_FORMAT);
    append_page_receipt_page_common(
        &mut material,
        page.receipt_count,
        page.total_receipt_count,
        page.scanned_count,
        page.limit,
        page.truncated,
        &page.next_marker,
        &None,
        0,
    );
    append_u64(&mut material, page.receipts.len() as u64);
    for receipt in &page.receipts {
        append_string(&mut material, &receipt.proof_checksum());
    }
    etag(&material)
}

pub(crate) fn page_discard_receipt_page_proof_checksum(page: &PageDiscardReceiptPage) -> String {
    const PROOF_FORMAT: &str = "MXOS-BLOCK-DISCARD-RECEIPT-PAGE-PROOF1";
    let mut material = Vec::with_capacity(receipt_page_proof_capacity(
        PROOF_FORMAT,
        &page.next_marker,
        &page.chunk_id,
        page.receipts.len(),
        3,
    ));
    append_string(&mut material, PROOF_FORMAT);
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

pub(crate) fn page_batch_discard_receipt_page_proof_checksum(
    page: &PageBatchDiscardReceiptPage,
) -> String {
    const PROOF_FORMAT: &str = "MXOS-BLOCK-DISCARD-BATCH-RECEIPT-PAGE-PROOF1";
    let mut material = Vec::with_capacity(receipt_page_proof_capacity(
        PROOF_FORMAT,
        &page.next_marker,
        &None,
        page.receipts.len(),
        0,
    ));
    append_string(&mut material, PROOF_FORMAT);
    append_page_receipt_page_common(
        &mut material,
        page.receipt_count,
        page.total_receipt_count,
        page.scanned_count,
        page.limit,
        page.truncated,
        &page.next_marker,
        &None,
        0,
    );
    append_u64(&mut material, page.receipts.len() as u64);
    for receipt in &page.receipts {
        append_string(&mut material, &receipt.proof_checksum());
    }
    etag(&material)
}

pub(crate) fn page_range_repair_receipt_page_proof_checksum(
    page: &PageRangeRepairReceiptPage,
) -> String {
    const PROOF_FORMAT: &str = "MXOS-BLOCK-RANGE-REPAIR-RECEIPT-PAGE-PROOF1";
    let mut material = Vec::with_capacity(receipt_page_proof_capacity(
        PROOF_FORMAT,
        &page.next_marker,
        &page.chunk_id,
        page.receipts.len(),
        0,
    ));
    append_string(&mut material, PROOF_FORMAT);
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
    append_u64(&mut material, page.receipts.len() as u64);
    for receipt in &page.receipts {
        append_string(&mut material, &receipt.proof_checksum());
    }
    etag(&material)
}

pub(crate) fn page_audit_repair_receipt_page_proof_checksum(
    page: &PageAuditRepairReceiptPage,
) -> String {
    const PROOF_FORMAT: &str = "MXOS-BLOCK-AUDIT-REPAIR-RECEIPT-PAGE-PROOF1";
    let mut material = Vec::with_capacity(chunk_receipt_page_proof_capacity(
        PROOF_FORMAT,
        &page.next_marker,
        &page.after_chunk_id,
        page.receipts.len(),
    ));
    append_string(&mut material, PROOF_FORMAT);
    append_chunk_receipt_page_common(
        &mut material,
        page.receipt_count,
        page.total_receipt_count,
        page.scanned_count,
        page.limit,
        page.truncated,
        &page.next_marker,
        &page.after_chunk_id,
        page.skipped_after_chunk_count,
    );
    append_u64(&mut material, page.receipts.len() as u64);
    for receipt in &page.receipts {
        append_string(&mut material, &receipt.proof_checksum());
    }
    etag(&material)
}

