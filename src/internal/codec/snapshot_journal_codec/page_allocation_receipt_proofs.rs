// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
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
        page.skipped_chunk_count,
    );
    append_page_receipt_page_chunk_filter(
        &mut material,
        page.page_index,
        page.page_size,
        page.skipped_page_count,
    );
    append_u64(&mut material, page.receipts.len() as u64);
    for receipt in &page.receipts {
        append_string(&mut material, &receipt.proof_checksum());
    }
    etag(&material)
}
