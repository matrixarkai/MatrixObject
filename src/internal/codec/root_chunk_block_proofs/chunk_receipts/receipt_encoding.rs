pub(crate) fn append_chunk_write_batch_stats(out: &mut Vec<u8>, stats: &ChunkWriteBatchStats) {
    append_u64(out, stats.input_ranges);
    append_u64(out, stats.input_bytes);
    append_u64(out, stats.coalesced_ranges);
    append_u64(out, stats.coalesced_bytes);
    append_u64(out, stats.physical_write_count);
    append_u64(out, stats.physical_write_bytes);
    append_u64(out, stats.invalidated_cache_entries);
}

pub(crate) fn append_chunk_discard_batch_stats(out: &mut Vec<u8>, stats: &ChunkDiscardBatchStats) {
    append_u64(out, stats.input_ranges);
    append_u64(out, stats.input_bytes);
    append_u64(out, stats.coalesced_ranges);
    append_u64(out, stats.coalesced_bytes);
    append_u64(out, stats.physical_discard_ranges);
    append_u64(out, stats.physical_discard_bytes);
    append_u64(out, stats.invalidated_cache_entries);
}

pub(crate) fn append_chunk_batch_write_receipt(
    out: &mut Vec<u8>,
    receipt: &ChunkBatchWriteReceipt,
) {
    append_chunk_id(out, &receipt.chunk_id);
    append_string(out, &receipt.request_checksum);
    append_string(out, &receipt.payload_checksum);
    append_string(out, &receipt.idempotency_key);
    append_chunk_write_batch_stats(out, &receipt.stats);
}

pub(crate) fn append_chunk_write_receipt(out: &mut Vec<u8>, receipt: &ChunkWriteReceipt) {
    append_chunk_id(out, &receipt.chunk_id);
    append_u64(out, receipt.offset);
    append_u64(out, receipt.length);
    append_string(out, &receipt.request_checksum);
    append_string(out, &receipt.payload_checksum);
    append_string(out, &receipt.idempotency_key);
}

pub(crate) fn append_chunk_discard_receipt(out: &mut Vec<u8>, receipt: &ChunkDiscardReceipt) {
    append_chunk_id(out, &receipt.chunk_id);
    append_u64(out, receipt.offset);
    append_u64(out, receipt.length);
    append_string(out, &receipt.request_checksum);
    append_string(out, &receipt.idempotency_key);
}

pub(crate) fn append_chunk_batch_discard_receipt(
    out: &mut Vec<u8>,
    receipt: &ChunkBatchDiscardReceipt,
) {
    append_chunk_id(out, &receipt.chunk_id);
    append_string(out, &receipt.request_checksum);
    append_string(out, &receipt.idempotency_key);
    append_chunk_discard_batch_stats(out, &receipt.stats);
}

pub(crate) fn append_chunk_state_install_receipt(
    out: &mut Vec<u8>,
    receipt: &ChunkStateInstallReceipt,
) {
    append_chunk_id(out, &receipt.chunk_id);
    append_string(out, &receipt.request_checksum);
    append_string(out, &receipt.payload_checksum);
    append_string(out, &receipt.state_checksum);
    append_string(out, &receipt.idempotency_key);
    append_u64(out, receipt.logical_length);
    append_u64(out, receipt.write_sequence);
    append_u64(out, receipt.discard_range_count);
}

pub(crate) fn append_chunk_reclaim_receipt(out: &mut Vec<u8>, receipt: &ChunkReclaimReceipt) {
    append_string(out, &receipt.request_checksum);
    append_string(out, &receipt.idempotency_key);
    append_u64(out, receipt.max_chunks);
    append_u64(out, receipt.max_rewrite_bytes);
    append_u64(out, receipt.min_discarded_bytes);
    append_u64(out, receipt.min_discard_ratio_ppm);
    append_u64(out, receipt.max_discard_ranges);
    append_u64(out, receipt.max_trim_bytes);
    append_chunk_partial_reclaim_report(out, &receipt.report);
}

pub(crate) fn append_chunk_repair_receipt(out: &mut Vec<u8>, receipt: &ChunkRepairReceipt) {
    append_chunk_id(out, &receipt.chunk_id);
    append_string(out, &receipt.request_checksum);
    append_string(out, &receipt.idempotency_key);
    append_chunk_repair_report(out, &receipt.report);
}

pub(crate) fn append_page_range_repair_receipt(
    out: &mut Vec<u8>,
    receipt: &PageRangeRepairReceipt,
) {
    append_slab_id(out, &receipt.slab_id);
    append_u64(out, receipt.offset);
    append_u64(out, receipt.length);
    append_u64(out, receipt.max_repair_chunks);
    append_u64(out, receipt.start_chunk_index);
    append_string(out, &receipt.request_checksum);
    append_string(out, &receipt.idempotency_key);
    append_bounded_repair_report(out, &receipt.report);
}

pub(crate) fn append_page_audit_repair_receipt(
    out: &mut Vec<u8>,
    receipt: &PageAuditRepairReceipt,
) {
    append_u64(out, receipt.after_chunk_id.is_some() as u64);
    if let Some(after_chunk_id) = &receipt.after_chunk_id {
        append_chunk_id(out, after_chunk_id);
    }
    append_u64(out, receipt.max_chunks);
    append_u64(out, receipt.max_payload_bytes);
    append_string(out, &receipt.request_checksum);
    append_string(out, &receipt.idempotency_key);
    append_page_replica_audit_repair_report(out, &receipt.report);
}
