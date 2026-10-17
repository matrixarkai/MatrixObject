fn append_health_fingerprints(mut material: &mut Vec<u8>, status: &ObjectStoreModuleStatus) {
    append_u64(&mut material, status.repair_degraded_objects);
    append_u64(&mut material, status.repair_bytes_to_repair);
    append_u64(&mut material, status.repair_degraded_chunks);
    append_u64(&mut material, status.repair_failed_repairs);
    append_u64(&mut material, status.compressed_chunk_count);
    append_u64(&mut material, status.compression_saved_bytes);
    append_u64(&mut material, status.ec_reconstructed_reads);
    append_u64(&mut material, status.ec_reconstruction_failures);
    append_string(&mut material, &status.metadata_state_fingerprint);
    append_string(&mut material, &status.root_state_fingerprint);
    append_string(&mut material, &status.page_state_fingerprint);
    append_string(&mut material, &status.chunk_state_fingerprint);
    append_string(&mut material, &status.distributed_readiness_checksum);
}
