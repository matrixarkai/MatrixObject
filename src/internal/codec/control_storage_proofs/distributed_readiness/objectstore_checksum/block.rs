fn append_page_readiness(mut material: &mut Vec<u8>, report: &ObjectStoreDistributedReadiness) {
    append_u64(&mut material, report.page.ready as u64);
    append_u64(&mut material, report.page.write_quorum);
    append_u64(&mut material, report.page.read_quorum);
    append_u64(&mut material, report.page.page_read_parallelism);
    append_u64(&mut material, report.page.page_write_parallelism);
    append_u64(&mut material, report.page.page_batch_read_parallelism);
    append_u64(&mut material, report.page.page_batch_write_parallelism);
    append_u64(&mut material, report.page.page_batch_discard_parallelism);
    append_u64(&mut material, report.page.page_repair_parallelism);
    append_u64(&mut material, report.page.page_chunk_fanout_parallelism);
    append_u64(
        &mut material,
        report.page.effective_chunk_fanout_parallelism,
    );
    append_u64(&mut material, report.page.local_replica_store_parallelism);
    append_u64(
        &mut material,
        report.page.effective_local_replica_store_parallelism,
    );
    append_u64(
        &mut material,
        report.page.cumulative_quorum_write_degraded_chunks,
    );
    append_u64(
        &mut material,
        report.page.cumulative_quorum_read_degraded_chunks,
    );
    append_u64(
        &mut material,
        report.page.cumulative_quorum_discard_degraded_chunks,
    );
    append_u64(
        &mut material,
        report.page.cumulative_replica_fallback_reads,
    );
    append_u64(&mut material, report.page.cumulative_repair_checked_chunks);
    append_u64(
        &mut material,
        report.page.cumulative_repair_degraded_chunks,
    );
    append_u64(&mut material, report.page.cumulative_repaired_replicas);
    append_u64(&mut material, report.page.cumulative_failed_repairs);
    append_string(
        &mut material,
        &report.page.route_control_proof.state_checksum,
    );
    append_string(&mut material, &report.page.page_state_fingerprint);
    append_u64(&mut material, report.page.replica_readiness.len() as u64);
    for replica in &report.page.replica_readiness {
        append_string(&mut material, &replica.chunk_state_fingerprint);
        append_u64(&mut material, replica.chunk_count);
        append_u64(&mut material, replica.physical_bytes);
        append_u64(&mut material, replica.discarded_range_count);
        append_u64(&mut material, replica.reclaimable_bytes);
    }
}
