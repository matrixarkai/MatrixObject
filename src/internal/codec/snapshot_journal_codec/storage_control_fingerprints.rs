pub(crate) fn append_replica_service_control_proof(
    out: &mut Vec<u8>,
    proof: &ReplicaServiceControlProof,
) {
    append_u64(out, proof.control_receipt as u64);
    append_string(out, &proof.state_format);
    append_u64(out, proof.state_bytes);
    append_string(out, &proof.state_checksum);
    append_u64(out, proof.chunk_mutation_control_commit_index);
    append_u64(out, proof.chunk_mutation_control_next_log_index);
    append_u64(out, proof.accepting_writes as u64);
    append_u64(out, proof.draining as u64);
    append_u64(out, proof.frozen as u64);
    append_u64(out, proof.capacity_bytes);
    append_u64(out, proof.physical_bytes);
    append_u64(out, proof.capacity_free_bytes);
    append_u64(out, proof.capacity_pressure as u64);
    append_u64(out, proof.capacity_rejected_writes);
    append_u64(out, proof.service_mode_rejected_mutations);
    append_u64(out, proof.metadata_dirty as u64);
    append_u64(out, proof.metadata_dirty_mutations);
    append_u64(out, proof.metadata_checkpoint_interval);
    append_u64(out, proof.defer_metadata_checkpoints as u64);
    append_u64(out, proof.deferred_metadata_checkpoint_pending as u64);
    append_u64(out, proof.deferred_metadata_checkpoint_requests);
    append_u64(out, proof.deferred_metadata_checkpoint_runs);
    append_u64(out, proof.physical_write_count);
    append_u64(out, proof.physical_write_bytes);
    append_u64(out, proof.physical_discard_ranges);
    append_u64(out, proof.physical_discard_bytes);
    append_u64(out, proof.compressed_chunk_count);
    append_u64(out, proof.compression_saved_bytes);
    append_u64(out, proof.receipt_retention);
    append_u64(out, proof.write_receipt_count);
    append_u64(out, proof.discard_receipt_count);
    append_u64(out, proof.batch_write_receipt_count);
    append_u64(out, proof.batch_discard_receipt_count);
    append_u64(out, proof.read_cache_max_bytes);
    append_u64(out, proof.read_cache_config_raft_backed as u64);
    append_u64(out, proof.capacity_config_raft_backed as u64);
    append_u64(out, proof.service_mode_config_raft_backed as u64);
    append_u64(out, proof.trim_flow_config_raft_backed as u64);
    append_u64(out, proof.mutation_qos_config_raft_backed as u64);
    append_u64(out, proof.trim_flow_rate_bytes_per_sec);
    append_u64(out, proof.trim_flow_burst_bytes);
    append_u64(out, proof.trim_flow_available_bytes);
    append_u64(out, proof.trim_flow_rate_ops_per_sec);
    append_u64(out, proof.trim_flow_burst_ops);
    append_u64(out, proof.trim_flow_available_ops);
    append_u64(out, proof.trim_flow_throttled_ops);
    append_u64(out, proof.mutation_qos_window_ms);
    append_u64(out, proof.mutation_qos_max_write_ops_per_window);
    append_u64(out, proof.mutation_qos_max_write_bytes_per_window);
    append_u64(out, proof.mutation_qos_max_discard_ops_per_window);
    append_u64(out, proof.mutation_qos_max_discard_bytes_per_window);
    append_u64(out, proof.mutation_qos_throttled_ops);
    append_u64(out, proof.mutation_qos_throttled_bytes);
    append_u64(out, proof.read_cache_hits);
    append_u64(out, proof.read_cache_misses);
    append_u64(out, proof.read_cache_evictions);
    append_u64(out, proof.read_cache_corrupt_pages);
    append_u64(out, proof.read_cache_corrupt_bytes);
    append_u64(out, proof.read_cache_invalidations);
    append_u64(out, proof.read_cache_invalidated_entries);
    append_u64(out, proof.read_cache_invalidated_bytes);
    append_u64(out, proof.read_cache_clears);
    append_u64(out, proof.read_cache_cleared_entries);
    append_u64(out, proof.read_cache_cleared_bytes);
    append_u64(out, proof.read_cache_direct_fill_pages);
    append_u64(out, proof.read_cache_direct_fill_bytes);
    append_u64(out, proof.read_cache_compressed_fill_pages);
    append_u64(out, proof.read_cache_compressed_fill_bytes);
    append_u64(out, proof.read_cache_ec_fill_pages);
    append_u64(out, proof.read_cache_ec_fill_bytes);
    append_u64(out, proof.read_cache_uncacheable_read_pages);
    append_u64(out, proof.read_cache_uncacheable_read_bytes);
    append_u64(out, proof.ec_reconstructed_reads);
    append_u64(out, proof.ec_reconstruction_failures);
    append_string(out, &proof.snapshot_checksum);
}

pub(crate) fn page_read_cache_control_image_fingerprint(
    chunk_service: &MatrixChunkService,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-BLOCK-READ-CACHE-CONTROL-IMAGE1");
    append_page_service_fingerprint(&mut material, chunk_service);
    append_u64(&mut material, chunk_service.replicas.len() as u64);
    for (replica_index, replica) in chunk_service.replicas.iter().enumerate() {
        let stats = replica.read_cache_stats();
        append_u64(&mut material, replica_index as u64);
        append_u64(&mut material, stats.max_bytes);
    }
    etag(&material)
}

pub(crate) fn page_capacity_control_image_fingerprint(
    chunk_service: &MatrixChunkService,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-BLOCK-CAPACITY-CONTROL-IMAGE1");
    append_u64(&mut material, chunk_service.write_quorum as u64);
    append_u64(&mut material, chunk_service.read_quorum as u64);
    append_u64(&mut material, chunk_service.chunk_endpoints.len() as u64);
    for (index, endpoint) in chunk_service.chunk_endpoints.iter().enumerate() {
        append_u64(&mut material, index as u64);
        append_string(&mut material, endpoint);
        append_u64(
            &mut material,
            chunk_service.disabled_chunk_endpoints.contains(&index) as u64,
        );
    }
    append_u64(
        &mut material,
        chunk_service.quarantined_replicas.len() as u64,
    );
    for replica_index in &chunk_service.quarantined_replicas {
        append_u64(&mut material, *replica_index as u64);
    }
    append_u64(&mut material, chunk_service.replicas.len() as u64);
    for (replica_index, replica) in chunk_service.replicas.iter().enumerate() {
        append_u64(&mut material, replica_index as u64);
        append_u64(&mut material, replica.accepting_writes() as u64);
        append_u64(&mut material, replica.is_draining() as u64);
        append_u64(&mut material, replica.is_frozen() as u64);
        append_u64(&mut material, replica.capacity_bytes());
    }
    etag(&material)
}

pub(crate) fn page_trim_flow_control_image_fingerprint(
    chunk_service: &MatrixChunkService,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-BLOCK-TRIM-FLOW-CONTROL-IMAGE1");
    append_page_service_fingerprint(&mut material, chunk_service);
    append_u64(&mut material, chunk_service.replicas.len() as u64);
    for (replica_index, replica) in chunk_service.replicas.iter().enumerate() {
        append_u64(&mut material, replica_index as u64);
        append_u64(&mut material, replica.trim_flow_rate_bytes_per_sec());
        append_u64(&mut material, replica.trim_flow_burst_bytes());
        append_u64(&mut material, replica.trim_flow_available_bytes());
        append_u64(&mut material, replica.trim_flow_rate_ops_per_sec());
        append_u64(&mut material, replica.trim_flow_burst_ops());
        append_u64(&mut material, replica.trim_flow_available_ops());
        append_u64(&mut material, replica.trim_flow_throttled_reclaims());
        append_u64(&mut material, replica.trim_flow_throttled_ops());
        append_u64(&mut material, replica.trim_flow_throttled_bytes());
    }
    etag(&material)
}

pub(crate) fn append_repair_stats(out: &mut Vec<u8>, stats: &RepairStats) {
    append_u64(out, stats.checked_chunks);
    append_u64(out, stats.repaired_replicas);
    append_u64(out, stats.failed_repairs);
    append_u64(out, stats.degraded_chunks);
}

pub(crate) fn append_page_replica_audit_item(out: &mut Vec<u8>, item: &ChunkReplicaAuditItem) {
    append_chunk_id(out, &item.page_id);
    append_u64(out, item.replica_index as u64);
    append_string(out, &item.status);
    append_u64(out, item.logical_length);
    append_u64(out, item.write_sequence);
    append_string(out, &item.checksum);
    append_u64(out, item.discarded_range_count);
    append_u64(out, item.frozen as u64);
    append_u64(out, item.quarantined as u64);
}

pub(crate) fn append_page_replica_audit_page(out: &mut Vec<u8>, page: &ChunkReplicaAuditChunk) {
    append_u64(out, page.items.len() as u64);
    for item in &page.items {
        append_page_replica_audit_item(out, item);
    }
    append_u64(out, page.marker.is_some() as u64);
    if let Some(marker) = &page.marker {
        append_chunk_id(out, marker);
    }
    append_u64(out, page.next_marker.is_some() as u64);
    if let Some(next_marker) = &page.next_marker {
        append_chunk_id(out, next_marker);
    }
    append_u64(out, page.scanned_chunks);
    append_u64(out, page.healthy_chunks);
    append_u64(out, page.degraded_chunks);
    append_u64(out, page.missing_replicas);
    append_u64(out, page.corrupt_replicas);
    append_u64(out, page.divergent_replicas);
    append_u64(out, page.quarantined_replicas);
    append_u64(out, page.frozen_replicas);
    append_u64(out, page.payload_bytes);
    append_u64(out, page.budget_limited as u64);
    append_u64(out, page.has_more as u64);
}

pub(crate) fn append_page_route_control_proof(out: &mut Vec<u8>, proof: &ChunkRouteControlProof) {
    append_u64(out, proof.control_receipt as u64);
    append_string(out, &proof.state_format);
    append_u64(out, proof.state_bytes);
    append_string(out, &proof.state_checksum);
    append_u64(out, proof.chunk_route_control_commit_index);
    append_u64(out, proof.chunk_route_control_next_log_index);
    append_u64(out, proof.chunk_endpoint_count);
    append_u64(out, proof.disabled_chunk_endpoint_count);
    append_u64(out, proof.write_quorum);
    append_u64(out, proof.configured_write_quorum);
    append_u64(out, proof.read_quorum);
    append_u64(out, proof.configured_read_quorum);
    append_u64(out, proof.disabled_replica_count);
    append_u64(out, proof.qos_window_ms);
    append_u64(out, proof.qos_max_read_ops_per_window);
    append_u64(out, proof.qos_max_read_bytes_per_window);
    append_u64(out, proof.qos_max_write_ops_per_window);
    append_u64(out, proof.qos_max_write_bytes_per_window);
    append_u64(out, proof.qos_max_discard_ops_per_window);
    append_u64(out, proof.qos_max_discard_bytes_per_window);
    append_u64(out, proof.qos_throttled_ops);
    append_u64(out, proof.qos_throttled_bytes);
    append_u64(out, proof.receipt_retention);
    append_u64(out, proof.primary_batch_read_repair_probe_limit);
    append_u64(out, proof.cumulative_quorum_write_degraded_chunks);
    append_u64(out, proof.cumulative_quorum_read_degraded_chunks);
    append_u64(out, proof.cumulative_quorum_discard_degraded_chunks);
    append_u64(out, proof.cumulative_replica_fallback_reads);
    append_u64(out, proof.cumulative_physical_chunk_read_count);
    append_u64(out, proof.cumulative_coalesced_chunk_read_count);
    append_u64(out, proof.cumulative_repair_checked_chunks);
    append_u64(out, proof.cumulative_repair_degraded_chunks);
    append_u64(out, proof.cumulative_repaired_replicas);
    append_u64(out, proof.cumulative_failed_repairs);
    append_u64(out, proof.cumulative_replica_read_repairs);
    append_u64(out, proof.cumulative_replica_read_repair_failures);
}

pub(crate) fn append_optional_metadata(out: &mut Vec<u8>, metadata: &Option<ObjectMetadata>) {
    match metadata {
        Some(metadata) => {
            append_u64(out, 1);
            append_metadata(out, metadata);
        }
        None => append_u64(out, 0),
    }
}

