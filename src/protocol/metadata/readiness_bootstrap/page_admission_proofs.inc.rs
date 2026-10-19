pub(crate) fn page_range_admission_proof_checksum(
    operation: &str,
    admission: &ChunkRangeAdmission,
) -> String {
    let mut material = Vec::with_capacity(
        std::mem::size_of::<u64>() * 48
            + encoded_string_len(STORAGE_ADMISSION_PROOF_FORMAT)
            + encoded_string_len(operation)
            + encoded_string_len(&admission.partial_status)
            + encoded_string_len(&admission.admission_reason),
    );
    append_string(&mut material, STORAGE_ADMISSION_PROOF_FORMAT);
    append_string(&mut material, operation);
    append_u64(&mut material, admission.can_accept as u64);
    append_u64(&mut material, admission.input_ranges);
    append_u64(&mut material, admission.mapped_chunk_ranges);
    append_u64(&mut material, admission.coalesced_chunk_ranges);
    append_u64(&mut material, admission.payload_bytes);
    append_u64(&mut material, admission.max_payload_bytes);
    append_u64(&mut material, admission.min_read_index);
    append_u64(&mut material, admission.min_commit_index);
    append_u64(&mut material, admission.max_chunk_read_count);
    append_u64(&mut material, admission.max_write_bytes);
    append_u64(&mut material, admission.max_chunk_write_count);
    append_u64(&mut material, admission.selected_write_range_count);
    append_u64(&mut material, admission.selected_write_payload_bytes);
    append_u64(&mut material, admission.write_start_range_index);
    append_u64(&mut material, admission.next_write_range_index);
    append_u64(&mut material, admission.write_has_more as u64);
    append_u64(&mut material, admission.max_discard_bytes);
    append_u64(&mut material, admission.budget_limited as u64);
    append_u64(&mut material, admission.ready_replica_count);
    append_u64(&mut material, admission.required_quorum);
    append_u64(&mut material, admission.writable_replica_count);
    append_u64(&mut material, admission.readable_chunk_count);
    append_u64(&mut material, admission.failed_chunk_count);
    append_u64(&mut material, admission.selected_discard_bytes);
    append_u64(&mut material, admission.skipped_discard_bytes);
    append_u64(&mut material, admission.skipped_discard_range_count);
    append_u64(&mut material, admission.discard_start_range_index);
    append_u64(&mut material, admission.next_discard_range_index);
    append_u64(&mut material, admission.discard_has_more as u64);
    append_u64(&mut material, admission.read_stats.chunk_reads);
    append_u64(&mut material, admission.read_stats.cache_hits);
    append_u64(
        &mut material,
        admission.read_stats.coalesced_chunk_read_count,
    );
    append_u64(
        &mut material,
        admission.read_stats.physical_chunk_read_count,
    );
    append_u64(
        &mut material,
        admission.read_stats.metadata_head_batch_requests,
    );
    append_u64(
        &mut material,
        admission.read_stats.metadata_physical_head_count,
    );
    append_u64(
        &mut material,
        admission.read_stats.metadata_coalesced_head_count,
    );
    append_u64(&mut material, admission.read_stats.metadata_cache_hits);
    append_u64(&mut material, admission.read_stats.metadata_cache_misses);
    append_u64(&mut material, admission.read_stats.replica_fallback_reads);
    append_u64(&mut material, admission.read_stats.quorum_verified_chunks);
    append_u64(&mut material, admission.read_stats.replica_read_repairs);
    append_u64(
        &mut material,
        admission.read_stats.replica_read_repair_failures,
    );
    append_u64(
        &mut material,
        admission.read_stats.quorum_read_degraded_chunks,
    );
    append_string(&mut material, &admission.partial_status);
    append_string(&mut material, &admission.admission_reason);
    append_u64(&mut material, admission.read_quorum_limited as u64);
    append_u64(&mut material, admission.write_quorum_limited as u64);
    append_u64(&mut material, admission.discard_budget_limited as u64);
    etag(&material)
}

pub(crate) fn stamp_page_range_admission_proof(
    operation: &str,
    admission: &mut ChunkRangeAdmission,
) {
    admission.admission_proof_format = STORAGE_ADMISSION_PROOF_FORMAT.to_string();
    admission.admission_proof_checksum = page_range_admission_proof_checksum(operation, admission);
}
