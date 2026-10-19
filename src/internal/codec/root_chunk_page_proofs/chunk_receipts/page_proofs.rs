pub(crate) fn append_chunk_receipt_page_common(
    material: &mut Vec<u8>,
    receipt_count: u64,
    total_receipt_count: u64,
    scanned_count: u64,
    limit: u64,
    truncated: bool,
    next_marker: &Option<String>,
    chunk_id: &Option<PageId>,
    skipped_chunk_count: u64,
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
    append_u64(material, skipped_chunk_count);
}

fn root_chunk_receipt_page_proof_capacity(
    proof_format: &str,
    next_marker: &Option<String>,
    chunk_id: &Option<PageId>,
    receipt_count: usize,
) -> usize {
    encoded_string_len(proof_format)
        + std::mem::size_of::<u64>() * 10
        + next_marker
            .as_ref()
            .map(|value| encoded_string_len(value))
            .unwrap_or(0)
        + if chunk_id.is_some() {
            std::mem::size_of::<u64>() * 3
        } else {
            0
        }
        + receipt_count.saturating_mul(encoded_string_len("0000000000000000"))
}

pub(crate) fn chunk_write_receipt_page_proof_checksum(page: &ChunkWriteReceiptPage) -> String {
    const PROOF_FORMAT: &str = "MXOS-CHUNK-WRITE-RECEIPT-PAGE-PROOF1";
    let mut material = Vec::with_capacity(root_chunk_receipt_page_proof_capacity(
        PROOF_FORMAT,
        &page.next_marker,
        &page.chunk_id,
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
        &page.chunk_id,
        page.skipped_chunk_count,
    );
    append_u64(&mut material, page.receipts.len() as u64);
    for receipt in &page.receipts {
        append_string(&mut material, &receipt.proof_checksum());
    }
    etag(&material)
}

pub(crate) fn chunk_batch_write_receipt_page_proof_checksum(
    page: &ChunkBatchWriteReceiptPage,
) -> String {
    const PROOF_FORMAT: &str = "MXOS-CHUNK-WRITE-BATCH-RECEIPT-PAGE-PROOF1";
    let mut material = Vec::with_capacity(root_chunk_receipt_page_proof_capacity(
        PROOF_FORMAT,
        &page.next_marker,
        &page.chunk_id,
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
        &page.chunk_id,
        page.skipped_chunk_count,
    );
    append_u64(&mut material, page.receipts.len() as u64);
    for receipt in &page.receipts {
        append_string(&mut material, &receipt.proof_checksum());
    }
    etag(&material)
}

pub(crate) fn chunk_discard_receipt_page_proof_checksum(page: &ChunkDiscardReceiptPage) -> String {
    const PROOF_FORMAT: &str = "MXOS-CHUNK-DISCARD-RECEIPT-PAGE-PROOF1";
    let mut material = Vec::with_capacity(root_chunk_receipt_page_proof_capacity(
        PROOF_FORMAT,
        &page.next_marker,
        &page.chunk_id,
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
        &page.chunk_id,
        page.skipped_chunk_count,
    );
    append_u64(&mut material, page.receipts.len() as u64);
    for receipt in &page.receipts {
        append_string(&mut material, &receipt.proof_checksum());
    }
    etag(&material)
}

pub(crate) fn chunk_batch_discard_receipt_page_proof_checksum(
    page: &ChunkBatchDiscardReceiptPage,
) -> String {
    const PROOF_FORMAT: &str = "MXOS-CHUNK-DISCARD-BATCH-RECEIPT-PAGE-PROOF1";
    let mut material = Vec::with_capacity(root_chunk_receipt_page_proof_capacity(
        PROOF_FORMAT,
        &page.next_marker,
        &page.chunk_id,
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
        &page.chunk_id,
        page.skipped_chunk_count,
    );
    append_u64(&mut material, page.receipts.len() as u64);
    for receipt in &page.receipts {
        append_string(&mut material, &receipt.proof_checksum());
    }
    etag(&material)
}

pub(crate) fn chunk_repair_receipt_page_proof_checksum(page: &ChunkRepairReceiptPage) -> String {
    const PROOF_FORMAT: &str = "MXOS-CHUNK-REPAIR-RECEIPT-PAGE-PROOF1";
    let mut material = Vec::with_capacity(root_chunk_receipt_page_proof_capacity(
        PROOF_FORMAT,
        &page.next_marker,
        &page.chunk_id,
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
        &page.chunk_id,
        page.skipped_chunk_count,
    );
    append_u64(&mut material, page.receipts.len() as u64);
    for receipt in &page.receipts {
        append_string(&mut material, &receipt.proof_checksum());
    }
    etag(&material)
}
