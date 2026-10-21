// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
pub(crate) fn append_page_id(out: &mut Vec<u8>, page_id: &PageId) {
    append_chunk_id(out, &page_id.chunk_id);
    append_u64(out, page_id.page_index);
}

fn request_chunk_id_encoded_len() -> usize {
    std::mem::size_of::<u64>() * 2
}

fn request_page_id_encoded_len() -> usize {
    std::mem::size_of::<u64>() * 3
}

pub(crate) fn chunk_write_receipt_request_checksum(
    page_id: &PageId,
    offset: u64,
    length: u64,
    payload_checksum: &str,
) -> String {
    let mut material = Vec::with_capacity(
        request_page_id_encoded_len()
            + std::mem::size_of::<u64>() * 2
            + encoded_string_len(payload_checksum),
    );
    append_page_id(&mut material, page_id);
    append_u64(&mut material, offset);
    append_u64(&mut material, length);
    append_string(&mut material, payload_checksum);
    etag(&material)
}

pub(crate) fn chunk_discard_request_checksum(
    page_id: &PageId,
    offset: u64,
    length: u64,
) -> String {
    let mut material =
        Vec::with_capacity(request_page_id_encoded_len() + std::mem::size_of::<u64>() * 2);
    append_page_id(&mut material, page_id);
    append_u64(&mut material, offset);
    append_u64(&mut material, length);
    etag(&material)
}

pub(crate) fn chunk_write_receipt_proof_checksum(receipt: &ReplicaWriteReceipt) -> String {
    let mut material = Vec::with_capacity(
        encoded_string_len("MXOS-CHUNK-WRITE-RECEIPT-PROOF1")
            + request_page_id_encoded_len()
            + std::mem::size_of::<u64>() * 2
            + encoded_string_len(&receipt.request_checksum)
            + encoded_string_len(&receipt.payload_checksum)
            + encoded_string_len(&receipt.idempotency_key),
    );
    append_string(&mut material, "MXOS-CHUNK-WRITE-RECEIPT-PROOF1");
    append_page_id(&mut material, &receipt.page_id);
    append_u64(&mut material, receipt.offset);
    append_u64(&mut material, receipt.length);
    append_string(&mut material, &receipt.request_checksum);
    append_string(&mut material, &receipt.payload_checksum);
    append_string(&mut material, &receipt.idempotency_key);
    etag(&material)
}

pub(crate) fn chunk_discard_receipt_proof_checksum(receipt: &ReplicaDiscardReceipt) -> String {
    let mut material = Vec::with_capacity(
        encoded_string_len("MXOS-CHUNK-DISCARD-RECEIPT-PROOF1")
            + request_page_id_encoded_len()
            + std::mem::size_of::<u64>() * 2
            + encoded_string_len(&receipt.request_checksum)
            + encoded_string_len(&receipt.idempotency_key),
    );
    append_string(&mut material, "MXOS-CHUNK-DISCARD-RECEIPT-PROOF1");
    append_page_id(&mut material, &receipt.page_id);
    append_u64(&mut material, receipt.offset);
    append_u64(&mut material, receipt.length);
    append_string(&mut material, &receipt.request_checksum);
    append_string(&mut material, &receipt.idempotency_key);
    etag(&material)
}

pub(crate) fn chunk_batch_write_payload_checksum(writes: &[(u64, Vec<u8>)]) -> String {
    let mut borrowed = Vec::with_capacity(writes.len());
    borrowed.extend(writes.iter().map(|(offset, data)| (*offset, data.as_slice())));
    chunk_batch_write_payload_checksum_borrowed(&borrowed)
}

pub(crate) fn chunk_batch_write_payload_checksum_borrowed(writes: &[(u64, &[u8])]) -> String {
    let mut material = Vec::with_capacity(
        std::mem::size_of::<u64>() + writes.len() * (std::mem::size_of::<u64>() * 3 + 24),
    );
    append_u64(&mut material, writes.len() as u64);
    for (offset, data) in writes {
        append_u64(&mut material, *offset);
        append_u64(&mut material, data.len() as u64);
        append_string(&mut material, &etag(data));
    }
    etag(&material)
}

pub(crate) fn chunk_batch_write_request_checksum(
    page_id: &PageId,
    writes: &[(u64, Vec<u8>)],
) -> String {
    let mut borrowed = Vec::with_capacity(writes.len());
    borrowed.extend(writes.iter().map(|(offset, data)| (*offset, data.as_slice())));
    chunk_batch_write_request_checksum_borrowed(page_id, &borrowed)
}

pub(crate) fn chunk_batch_write_request_checksum_borrowed(
    page_id: &PageId,
    writes: &[(u64, &[u8])],
) -> String {
    let payload_checksum = chunk_batch_write_payload_checksum_borrowed(writes);
    let mut material = Vec::with_capacity(
        request_page_id_encoded_len() + encoded_string_len(&payload_checksum),
    );
    append_page_id(&mut material, page_id);
    append_string(&mut material, &payload_checksum);
    etag(&material)
}

pub(crate) fn chunk_batch_discard_request_checksum(
    page_id: &PageId,
    ranges: &[DataRange],
) -> String {
    let mut material = Vec::with_capacity(
        request_page_id_encoded_len()
            + std::mem::size_of::<u64>()
            + ranges.len() * std::mem::size_of::<u64>() * 2,
    );
    append_page_id(&mut material, page_id);
    append_u64(&mut material, ranges.len() as u64);
    for range in ranges {
        append_range(&mut material, range);
    }
    etag(&material)
}

pub(crate) fn chunk_batch_discard_nonempty_request_checksum(
    page_id: &PageId,
    ranges: &[DataRange],
) -> String {
    let nonempty_count = ranges.iter().filter(|range| range.length > 0).count();
    let mut material = Vec::with_capacity(
        request_page_id_encoded_len()
            + std::mem::size_of::<u64>()
            + nonempty_count * std::mem::size_of::<u64>() * 2,
    );
    append_page_id(&mut material, page_id);
    append_u64(&mut material, nonempty_count as u64);
    for range in ranges {
        if range.length > 0 {
            append_range(&mut material, range);
        }
    }
    etag(&material)
}

pub(crate) fn chunk_state_install_request_checksum(
    page_id: &PageId,
    state: &PageState,
    payload_checksum: &str,
    state_checksum: &str,
) -> String {
    let mut material = Vec::with_capacity(
        request_page_id_encoded_len()
            + std::mem::size_of::<u64>() * 3
            + encoded_string_len(payload_checksum)
            + encoded_string_len(state_checksum),
    );
    append_page_id(&mut material, page_id);
    append_u64(&mut material, state.logical_length);
    append_u64(&mut material, state.write_sequence);
    append_string(&mut material, payload_checksum);
    append_string(&mut material, state_checksum);
    append_u64(&mut material, state.discarded_ranges.len() as u64);
    etag(&material)
}

pub(crate) fn chunk_reclaim_request_checksum(
    max_chunks: usize,
    max_rewrite_bytes: u64,
    min_discarded_bytes: u64,
    min_discard_ratio_ppm: u64,
    max_discard_ranges: usize,
    max_trim_bytes: u64,
) -> String {
    let mut material = Vec::with_capacity(std::mem::size_of::<u64>() * 6);
    append_u64(&mut material, max_chunks as u64);
    append_u64(&mut material, max_rewrite_bytes);
    append_u64(&mut material, min_discarded_bytes);
    append_u64(&mut material, min_discard_ratio_ppm);
    append_u64(&mut material, max_discard_ranges as u64);
    append_u64(&mut material, max_trim_bytes);
    etag(&material)
}

pub(crate) fn chunk_repair_request_checksum(page_id: &PageId) -> String {
    let mut material = Vec::with_capacity(request_page_id_encoded_len());
    append_page_id(&mut material, page_id);
    etag(&material)
}

pub(crate) fn page_range_repair_request_checksum(
    chunk_id: &ChunkId,
    offset: u64,
    length: u64,
    max_repair_chunks: u64,
    start_chunk_index: u64,
) -> String {
    let mut material =
        Vec::with_capacity(request_chunk_id_encoded_len() + std::mem::size_of::<u64>() * 4);
    append_chunk_id(&mut material, chunk_id);
    append_u64(&mut material, offset);
    append_u64(&mut material, length);
    append_u64(&mut material, max_repair_chunks);
    append_u64(&mut material, start_chunk_index);
    etag(&material)
}

pub(crate) fn page_audit_repair_request_checksum(
    after_chunk_id: &Option<PageId>,
    max_chunks: u64,
    max_payload_bytes: u64,
) -> String {
    let mut material = Vec::with_capacity(
        std::mem::size_of::<u64>() * 3
            + after_chunk_id
                .as_ref()
                .map(|_| request_page_id_encoded_len())
                .unwrap_or(0),
    );
    append_u64(&mut material, after_chunk_id.is_some() as u64);
    if let Some(after_chunk_id) = after_chunk_id {
        append_page_id(&mut material, after_chunk_id);
    }
    append_u64(&mut material, max_chunks);
    append_u64(&mut material, max_payload_bytes);
    etag(&material)
}

pub(crate) fn chunk_state_install_receipt_request_checksum(
    receipt: &ReplicaStateInstallReceipt,
) -> String {
    let mut material = Vec::with_capacity(
        request_page_id_encoded_len()
            + std::mem::size_of::<u64>() * 3
            + encoded_string_len(&receipt.payload_checksum)
            + encoded_string_len(&receipt.state_checksum),
    );
    append_page_id(&mut material, &receipt.page_id);
    append_u64(&mut material, receipt.logical_length);
    append_u64(&mut material, receipt.write_sequence);
    append_string(&mut material, &receipt.payload_checksum);
    append_string(&mut material, &receipt.state_checksum);
    append_u64(&mut material, receipt.discard_range_count);
    etag(&material)
}
