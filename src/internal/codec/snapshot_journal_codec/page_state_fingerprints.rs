// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

pub(crate) fn append_page_allocation(out: &mut Vec<u8>, allocation: &ChunkAllocation) {
    append_string(out, &allocation.allocation_id);
    append_chunk_id(out, &allocation.chunk_id);
    append_u64(out, allocation.offset);
    append_u64(out, allocation.length);
    append_u64(out, allocation.create_time_ms);
}

pub(crate) fn chunk_allocation_encoded_len(allocation: &ChunkAllocation) -> usize {
    encoded_string_len(&allocation.allocation_id) + std::mem::size_of::<u64>() * 5
}

pub(crate) fn append_page_allocation_receipt(out: &mut Vec<u8>, receipt: &ChunkAllocationReceipt) {
    append_page_allocation(out, &receipt.allocation);
    append_string(out, &receipt.idempotency_key);
    append_u64(out, receipt.released as u64);
    append_u64(out, receipt.deduped as u64);
}

pub(crate) fn chunk_allocation_receipt_encoded_len(receipt: &ChunkAllocationReceipt) -> usize {
    chunk_allocation_encoded_len(&receipt.allocation)
        + encoded_string_len(&receipt.idempotency_key)
        + std::mem::size_of::<u64>() * 2
}

pub(crate) fn chunk_allocation_receipt_proof_checksum(receipt: &ChunkAllocationReceipt) -> String {
    let mut material = Vec::with_capacity(
        encoded_string_len(receipt.proof_format()) + chunk_allocation_receipt_encoded_len(receipt),
    );
    append_string(&mut material, receipt.proof_format());
    append_page_allocation_receipt(&mut material, receipt);
    etag(&material)
}

pub(crate) fn page_allocation_receipt_page_proof_checksum(
    page: &PageAllocationReceiptPage,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, page.proof_format());
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

