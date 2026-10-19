pub(crate) fn page_chunk_receipt_key<T: std::fmt::Debug>(
    prefix: &str,
    base_key: &str,
    chunk_id: &PageId,
    replica_index: usize,
    request: &T,
) -> String {
    let request_debug = format!("{:?}", request);
    let mut material = Vec::with_capacity(
        encoded_string_len(base_key)
            + std::mem::size_of::<u64>() * 4
            + encoded_string_len(&request_debug),
    );
    append_string(&mut material, base_key);
    append_chunk_id(&mut material, chunk_id);
    append_u64(&mut material, replica_index as u64);
    append_string(&mut material, &request_debug);
    format!("{prefix}:{}", etag(&material))
}

pub(crate) fn page_write_receipt_request_checksum(
    slab_id: &SlabId,
    offset: u64,
    length: u64,
    payload_checksum: &str,
) -> String {
    let mut material = Vec::with_capacity(
        std::mem::size_of::<u64>() * 4 + encoded_string_len(payload_checksum),
    );
    append_slab_id(&mut material, slab_id);
    append_u64(&mut material, offset);
    append_u64(&mut material, length);
    append_string(&mut material, payload_checksum);
    etag(&material)
}

pub(crate) fn page_write_request_checksum(
    slab_id: &SlabId,
    offset: u64,
    data: &[u8],
) -> String {
    page_write_receipt_request_checksum(slab_id, offset, data.len() as u64, &etag(data))
}

pub(crate) fn page_batch_write_payload_checksum(writes: &[PageRangeWrite]) -> String {
    let mut material = Vec::with_capacity(
        std::mem::size_of::<u64>() + writes.len() * (std::mem::size_of::<u64>() + encoded_string_len("")),
    );
    append_u64(&mut material, writes.len() as u64);
    for write in writes {
        append_u64(&mut material, write.data.len() as u64);
        append_string(&mut material, &etag(&write.data));
    }
    etag(&material)
}

pub(crate) fn page_batch_write_request_checksum(writes: &[PageRangeWrite]) -> String {
    let mut material = Vec::with_capacity(
        std::mem::size_of::<u64>()
            + writes.len() * (std::mem::size_of::<u64>() * 4 + encoded_string_len("")),
    );
    append_u64(&mut material, writes.len() as u64);
    for write in writes {
        append_slab_id(&mut material, &write.slab_id);
        append_u64(&mut material, write.offset);
        append_u64(&mut material, write.data.len() as u64);
        append_string(&mut material, &etag(&write.data));
    }
    etag(&material)
}

pub(crate) fn append_page_write_receipt(out: &mut Vec<u8>, receipt: &PageWriteReceipt) {
    append_slab_id(out, &receipt.slab_id);
    append_u64(out, receipt.offset);
    append_u64(out, receipt.length);
    append_string(out, &receipt.request_checksum);
    append_string(out, &receipt.payload_checksum);
    append_string(out, &receipt.idempotency_key);
}

pub(crate) fn page_write_receipt_encoded_len(receipt: &PageWriteReceipt) -> usize {
    std::mem::size_of::<u64>() * 4
        + encoded_string_len(&receipt.request_checksum)
        + encoded_string_len(&receipt.payload_checksum)
        + encoded_string_len(&receipt.idempotency_key)
}

pub(crate) fn page_write_receipt_proof_checksum(receipt: &PageWriteReceipt) -> String {
    let mut material = Vec::with_capacity(
        encoded_string_len("MXOS-BLOCK-WRITE-RECEIPT-PROOF1")
            + std::mem::size_of::<u64>() * 4
            + encoded_string_len(&receipt.request_checksum)
            + encoded_string_len(&receipt.payload_checksum)
            + encoded_string_len(&receipt.idempotency_key),
    );
    append_string(&mut material, "MXOS-BLOCK-WRITE-RECEIPT-PROOF1");
    append_slab_id(&mut material, &receipt.slab_id);
    append_u64(&mut material, receipt.offset);
    append_u64(&mut material, receipt.length);
    append_string(&mut material, &receipt.request_checksum);
    append_string(&mut material, &receipt.payload_checksum);
    append_string(&mut material, &receipt.idempotency_key);
    etag(&material)
}

pub(crate) fn append_page_batch_write_receipt(
    out: &mut Vec<u8>,
    receipt: &PageBatchWriteReceipt,
) {
    append_string(out, &receipt.request_checksum);
    append_string(out, &receipt.payload_checksum);
    append_string(out, &receipt.idempotency_key);
    append_page_batch_stats(out, &receipt.stats);
}

pub(crate) fn page_batch_write_receipt_encoded_len(
    receipt: &PageBatchWriteReceipt,
) -> usize {
    encoded_string_len(&receipt.request_checksum)
        + encoded_string_len(&receipt.payload_checksum)
        + encoded_string_len(&receipt.idempotency_key)
        + std::mem::size_of::<u64>() * 22
}

pub(crate) fn append_page_batch_write_receipt_legacy(
    out: &mut Vec<u8>,
    receipt: &PageBatchWriteReceipt,
) {
    append_string(out, &receipt.request_checksum);
    append_string(out, &receipt.payload_checksum);
    append_string(out, &receipt.idempotency_key);
    append_page_batch_stats_legacy(out, &receipt.stats);
}

pub(crate) fn page_batch_write_receipt_proof_checksum(receipt: &PageBatchWriteReceipt) -> String {
    let mut material = Vec::with_capacity(
        encoded_string_len("MXOS-BLOCK-WRITE-BATCH-PROOF1")
            + encoded_string_len(&receipt.request_checksum)
            + encoded_string_len(&receipt.payload_checksum)
            + encoded_string_len(&receipt.idempotency_key)
            + std::mem::size_of::<u64>() * 22,
    );
    append_string(&mut material, "MXOS-BLOCK-WRITE-BATCH-PROOF1");
    append_string(&mut material, &receipt.request_checksum);
    append_string(&mut material, &receipt.payload_checksum);
    append_string(&mut material, &receipt.idempotency_key);
    append_page_batch_stats(&mut material, &receipt.stats);
    etag(&material)
}

pub(crate) fn page_discard_request_checksum(
    slab_id: &SlabId,
    offset: u64,
    length: u64,
) -> String {
    let mut material = Vec::with_capacity(std::mem::size_of::<u64>() * 4);
    append_slab_id(&mut material, slab_id);
    append_u64(&mut material, offset);
    append_u64(&mut material, length);
    etag(&material)
}

pub(crate) fn append_page_discard_receipt(out: &mut Vec<u8>, receipt: &PageDiscardReceipt) {
    append_slab_id(out, &receipt.slab_id);
    append_u64(out, receipt.offset);
    append_u64(out, receipt.length);
    append_string(out, &receipt.request_checksum);
    append_string(out, &receipt.idempotency_key);
}

pub(crate) fn page_discard_receipt_encoded_len(receipt: &PageDiscardReceipt) -> usize {
    std::mem::size_of::<u64>() * 4
        + encoded_string_len(&receipt.request_checksum)
        + encoded_string_len(&receipt.idempotency_key)
}

pub(crate) fn page_discard_receipt_proof_checksum(receipt: &PageDiscardReceipt) -> String {
    let mut material = Vec::with_capacity(
        encoded_string_len("MXOS-BLOCK-DISCARD-RECEIPT-PROOF1")
            + std::mem::size_of::<u64>() * 4
            + encoded_string_len(&receipt.request_checksum)
            + encoded_string_len(&receipt.idempotency_key),
    );
    append_string(&mut material, "MXOS-BLOCK-DISCARD-RECEIPT-PROOF1");
    append_slab_id(&mut material, &receipt.slab_id);
    append_u64(&mut material, receipt.offset);
    append_u64(&mut material, receipt.length);
    append_string(&mut material, &receipt.request_checksum);
    append_string(&mut material, &receipt.idempotency_key);
    etag(&material)
}

pub(crate) fn page_batch_discard_request_checksum(requests: &[PageRangeRequest]) -> String {
    let mut material = Vec::with_capacity(
        std::mem::size_of::<u64>() + requests.len() * std::mem::size_of::<u64>() * 4,
    );
    append_u64(&mut material, requests.len() as u64);
    for request in requests {
        append_slab_id(&mut material, &request.slab_id);
        append_u64(&mut material, request.offset);
        append_u64(&mut material, request.length);
    }
    etag(&material)
}

pub(crate) fn append_page_batch_stats(out: &mut Vec<u8>, stats: &PageBatchStats) {
    append_u64(out, stats.input_ranges);
    append_u64(out, stats.mapped_chunk_ranges);
    append_u64(out, stats.coalesced_chunk_ranges);
    append_u64(out, stats.physical_chunk_write_count);
    append_u64(out, stats.physical_chunk_write_bytes);
    append_u64(out, stats.physical_chunk_discard_ranges);
    append_u64(out, stats.physical_chunk_discard_bytes);
    append_u64(out, stats.payload_bytes);
    append_page_read_stats(out, &stats.read_stats);
}

pub(crate) fn append_page_batch_stats_legacy(out: &mut Vec<u8>, stats: &PageBatchStats) {
    append_u64(out, stats.input_ranges);
    append_u64(out, stats.mapped_chunk_ranges);
    append_u64(out, stats.coalesced_chunk_ranges);
    append_u64(out, stats.physical_chunk_write_count);
    append_u64(out, stats.physical_chunk_write_bytes);
    append_u64(out, stats.physical_chunk_discard_ranges);
    append_u64(out, stats.physical_chunk_discard_bytes);
    append_u64(out, stats.payload_bytes);
    append_page_read_stats_legacy(out, &stats.read_stats);
}

pub(crate) fn append_page_read_stats(out: &mut Vec<u8>, stats: &PageReadStats) {
    append_u64(out, stats.chunk_reads);
    append_u64(out, stats.cache_hits);
    append_u64(out, stats.coalesced_chunk_read_count);
    append_u64(out, stats.physical_chunk_read_count);
    append_u64(out, stats.metadata_head_batch_requests);
    append_u64(out, stats.metadata_physical_head_count);
    append_u64(out, stats.metadata_coalesced_head_count);
    append_u64(out, stats.metadata_cache_hits);
    append_u64(out, stats.metadata_cache_misses);
    append_u64(out, stats.replica_fallback_reads);
    append_u64(out, stats.quorum_verified_chunks);
    append_u64(out, stats.replica_read_repairs);
    append_u64(out, stats.replica_read_repair_failures);
    append_u64(out, stats.quorum_read_degraded_chunks);
}

pub(crate) fn append_page_read_stats_legacy(out: &mut Vec<u8>, stats: &PageReadStats) {
    append_u64(out, stats.chunk_reads);
    append_u64(out, stats.cache_hits);
    append_u64(out, stats.coalesced_chunk_read_count);
    append_u64(out, stats.physical_chunk_read_count);
    append_u64(out, stats.replica_fallback_reads);
    append_u64(out, stats.quorum_verified_chunks);
    append_u64(out, stats.replica_read_repairs);
    append_u64(out, stats.replica_read_repair_failures);
    append_u64(out, stats.quorum_read_degraded_chunks);
}

pub(crate) fn append_page_batch_discard_receipt(
    out: &mut Vec<u8>,
    receipt: &PageBatchDiscardReceipt,
) {
    append_string(out, &receipt.request_checksum);
    append_string(out, &receipt.idempotency_key);
    append_page_batch_stats(out, &receipt.stats);
}

pub(crate) fn page_batch_discard_receipt_encoded_len(
    receipt: &PageBatchDiscardReceipt,
) -> usize {
    encoded_string_len(&receipt.request_checksum)
        + encoded_string_len(&receipt.idempotency_key)
        + std::mem::size_of::<u64>() * 22
}

pub(crate) fn page_batch_discard_receipt_proof_checksum(
    receipt: &PageBatchDiscardReceipt,
) -> String {
    let mut material = Vec::with_capacity(
        encoded_string_len("MXOS-BLOCK-DISCARD-BATCH-PROOF1")
            + encoded_string_len(&receipt.request_checksum)
            + encoded_string_len(&receipt.idempotency_key)
            + std::mem::size_of::<u64>() * 22,
    );
    append_string(&mut material, "MXOS-BLOCK-DISCARD-BATCH-PROOF1");
    append_string(&mut material, &receipt.request_checksum);
    append_string(&mut material, &receipt.idempotency_key);
    append_page_batch_stats(&mut material, &receipt.stats);
    etag(&material)
}

