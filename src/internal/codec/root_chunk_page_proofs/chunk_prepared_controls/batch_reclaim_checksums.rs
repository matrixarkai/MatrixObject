pub(crate) fn prepared_chunk_batch_write_checksum(prepared: &PreparedChunkBatchWrite) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-PREPARED-CHUNK-WRITE2");
    append_u64(&mut material, prepared.replica_index as u64);
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    append_u64(&mut material, prepared.writes.len() as u64);
    for (offset, data) in &prepared.writes {
        append_u64(&mut material, *offset);
        append_u64(&mut material, data.len() as u64);
        append_string(&mut material, &etag(data));
    }
    append_chunk_state(&mut material, &prepared.state);
    append_chunk_batch_write_receipt(&mut material, &prepared.receipt);
    append_replica_service_control_proof(&mut material, &prepared.control_proof);
    append_string(&mut material, &prepared.expected_control_checksum);
    etag(&material)
}

pub(crate) fn prepared_chunk_batch_discard_checksum(
    prepared: &PreparedChunkBatchDiscard,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-PREPARED-CHUNK-DISCARD2");
    append_u64(&mut material, prepared.replica_index as u64);
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    append_u64(&mut material, prepared.ranges.len() as u64);
    for range in &prepared.ranges {
        append_range(&mut material, range);
    }
    append_chunk_state(&mut material, &prepared.state);
    append_chunk_batch_discard_receipt(&mut material, &prepared.receipt);
    append_replica_service_control_proof(&mut material, &prepared.control_proof);
    append_string(&mut material, &prepared.expected_control_checksum);
    etag(&material)
}

pub(crate) fn prepared_chunk_reclaim_checksum(prepared: &PreparedChunkReplicaReclaim) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-PREPARED-CHUNK-RECLAIM3");
    append_u64(&mut material, prepared.replica_index as u64);
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    append_u64(&mut material, prepared.max_chunks as u64);
    append_u64(&mut material, prepared.max_rewrite_bytes);
    append_u64(&mut material, prepared.min_discarded_bytes);
    append_u64(&mut material, prepared.min_discard_ratio_ppm);
    append_u64(&mut material, prepared.max_discard_ranges as u64);
    append_u64(&mut material, prepared.max_trim_bytes);
    append_u64(&mut material, prepared.now_ms);
    append_chunk_reclaim_plan(&mut material, &prepared.plan);
    append_chunk_partial_reclaim_report(&mut material, &prepared.report);
    append_string(&mut material, &prepared.before_replica_service_fingerprint);
    append_string(&mut material, &prepared.after_replica_service_fingerprint);
    append_page_route_control_proof(&mut material, &prepared.route_control_proof);
    append_string(&mut material, &prepared.expected_route_control_checksum);
    etag(&material)
}
