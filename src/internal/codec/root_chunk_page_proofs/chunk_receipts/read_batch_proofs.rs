pub(crate) fn chunk_head_batch_read_proof_checksum(
    page_ids: &[PageId],
    heads: &[ReplicaHead],
    stats: &ReplicaHeadBatchStats,
    proof: &StorageReadProof,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, CHUNK_HEAD_BATCH_PROOF_FORMAT);
    append_u64(&mut material, page_ids.len() as u64);
    append_u64(&mut material, heads.len() as u64);
    append_u64(&mut material, stats.input_heads);
    append_u64(&mut material, stats.physical_heads);
    append_u64(&mut material, stats.coalesced_heads);
    append_u64(&mut material, stats.metadata_cache_hits);
    append_u64(&mut material, stats.metadata_cache_misses);
    append_u64(&mut material, proof.requested_storage_read_index);
    append_u64(&mut material, proof.requested_storage_commit_index);
    append_u64(&mut material, proof.storage_applied_index);
    append_u64(&mut material, proof.storage_read_index_lag);
    append_u64(&mut material, proof.storage_commit_index_lag);
    append_string(&mut material, &proof.control_kind);
    append_string(&mut material, &proof.control_checksum);
    for (page_id, head) in page_ids.iter().zip(heads.iter()) {
        append_page_id(&mut material, page_id);
        append_u64(&mut material, head.logical_length);
        append_u64(&mut material, head.write_sequence);
        append_string(&mut material, &head.checksum);
        append_string(&mut material, &head.state_checksum);
        append_u64(&mut material, head.discarded_ranges.len() as u64);
        for range in &head.discarded_ranges {
            append_range(&mut material, range);
        }
    }
    etag(&material)
}

pub(crate) fn chunk_read_batch_read_proof_checksum(
    page_id: &PageId,
    ranges: &[ReplicaRangeReadResult],
    stats: &ReplicaReadBatchStats,
    proof: &StorageReadProof,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, CHUNK_READ_BATCH_PROOF_FORMAT);
    append_page_id(&mut material, page_id);
    append_u64(&mut material, ranges.len() as u64);
    append_u64(&mut material, stats.input_ranges);
    append_u64(&mut material, stats.input_bytes);
    append_u64(&mut material, stats.coalesced_ranges);
    append_u64(&mut material, stats.coalesced_bytes);
    append_u64(&mut material, stats.payload_bytes);
    append_u64(&mut material, stats.cache_hits);
    append_u64(&mut material, stats.physical_reads);
    append_u64(&mut material, stats.budget_limited as u64);
    append_u64(&mut material, proof.requested_storage_read_index);
    append_u64(&mut material, proof.requested_storage_commit_index);
    append_u64(&mut material, proof.storage_applied_index);
    append_u64(&mut material, proof.storage_read_index_lag);
    append_u64(&mut material, proof.storage_commit_index_lag);
    append_string(&mut material, &proof.control_kind);
    append_string(&mut material, &proof.control_checksum);
    for range in ranges {
        append_range(&mut material, &range.range);
        append_string(&mut material, &etag(&range.data));
        append_u64(&mut material, range.cache_hit as u64);
    }
    etag(&material)
}
