pub(crate) fn chunk_storage_readiness_fingerprint_material(
    replicas: &[ReplicaStorageReadiness],
) -> Vec<u8> {
    let mut material = Vec::new();
    append_string(&mut material, CHUNK_STORAGE_READINESS_FINGERPRINT_FORMAT);
    append_u64(&mut material, replicas.len() as u64);
    for replica in replicas {
        append_string(&mut material, &replica.chunk_state_fingerprint);
        append_u64(&mut material, replica.readable as u64);
        append_u64(&mut material, replica.writable as u64);
        append_u64(&mut material, replica.ready as u64);
        append_string(&mut material, &replica.reason);
        append_u64(&mut material, replica.page_count);
        append_u64(&mut material, replica.physical_bytes);
        append_u64(&mut material, replica.capacity_bytes);
        append_u64(&mut material, replica.capacity_free_bytes);
        append_u64(&mut material, replica.reclaimable_bytes);
        append_u64(&mut material, replica.discarded_range_count);
        append_u64(&mut material, replica.capacity_rejected_writes);
        append_u64(&mut material, replica.service_mode_rejected_mutations);
        append_u64(&mut material, replica.physical_write_count);
        append_u64(&mut material, replica.physical_write_bytes);
        append_u64(&mut material, replica.physical_discard_ranges);
        append_u64(&mut material, replica.physical_discard_bytes);
        append_u64(&mut material, replica.receipt_retention);
        append_u64(&mut material, replica.write_receipt_count);
        append_u64(&mut material, replica.discard_receipt_count);
        append_u64(&mut material, replica.batch_write_receipt_count);
        append_u64(&mut material, replica.batch_discard_receipt_count);
        append_u64(&mut material, replica.compressed_chunk_count);
        append_u64(&mut material, replica.compression_saved_bytes);
        append_u64(&mut material, replica.ec_reconstructed_reads);
        append_u64(&mut material, replica.ec_reconstruction_failures);
        append_u64(&mut material, replica.read_cache_invalidations);
        append_u64(&mut material, replica.read_cache_invalidated_entries);
        append_u64(&mut material, replica.read_cache_invalidated_bytes);
        append_u64(&mut material, replica.read_cache_clears);
        append_u64(&mut material, replica.read_cache_cleared_entries);
        append_u64(&mut material, replica.read_cache_cleared_bytes);
        append_u64(&mut material, replica.read_cache_direct_fill_pages);
        append_u64(&mut material, replica.read_cache_direct_fill_bytes);
        append_u64(&mut material, replica.read_cache_ec_fill_pages);
        append_u64(&mut material, replica.read_cache_ec_fill_bytes);
        append_u64(&mut material, replica.read_cache_uncacheable_read_pages);
        append_u64(&mut material, replica.read_cache_uncacheable_read_bytes);
        append_u64(&mut material, replica.mutation_qos_window_ms);
        append_u64(&mut material, replica.mutation_qos_write_ops_in_window);
        append_u64(&mut material, replica.mutation_qos_write_bytes_in_window);
        append_u64(&mut material, replica.mutation_qos_discard_ops_in_window);
        append_u64(&mut material, replica.mutation_qos_discard_bytes_in_window);
        append_u64(&mut material, replica.mutation_qos_throttled_ops);
        append_u64(&mut material, replica.mutation_qos_throttled_bytes);
        append_u64(&mut material, replica.mutation_qos_pressure as u64);
    }
    material
}
