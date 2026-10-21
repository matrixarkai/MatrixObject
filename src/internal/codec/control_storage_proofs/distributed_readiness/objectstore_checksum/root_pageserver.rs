// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
fn append_root_pageserver_topology(mut material: &mut Vec<u8>, report: &ObjectStoreDistributedReadiness) {
    append_u64(&mut material, report.root_node_count);
    append_u64(&mut material, report.root_active_pageserver_count);
    append_u64(&mut material, report.root_page_allocator_shard_count);
    append_u64(&mut material, report.root_page_allocator_covered_shards);
    append_u64(
        &mut material,
        report.root_page_allocator_shards_complete as u64,
    );
    append_string(&mut material, &report.root_page_allocator_shard_status);
    append_u64(&mut material, report.root_ready_pageserver_count);
    append_u64(&mut material, report.root_read_ready_pageserver_count);
    append_u64(
        &mut material,
        report.root_pageserver_total_chunk_endpoint_count,
    );
    append_u64(
        &mut material,
        report.root_pageserver_total_writable_chunk_endpoint_count,
    );
    append_u64(
        &mut material,
        report.root_pageserver_total_healthy_chunk_endpoint_count,
    );
    append_u64(
        &mut material,
        report.root_pageserver_total_raft_writable_chunk_endpoint_count,
    );
    append_u64(
        &mut material,
        report.root_pageserver_total_raft_readable_chunk_endpoint_count,
    );
    append_u64(
        &mut material,
        report.root_pageserver_total_capacity_ready_writable_chunk_endpoint_count,
    );
    append_u64(
        &mut material,
        report.root_pageserver_total_quarantined_chunk_replicas,
    );
    append_u64(
        &mut material,
        report.root_pageserver_total_chunk_capacity_bytes,
    );
    append_u64(
        &mut material,
        report.root_pageserver_total_chunk_capacity_free_bytes,
    );
    append_u64(
        &mut material,
        report.root_pageserver_total_chunk_metadata_dirty_endpoint_count,
    );
    append_u64(
        &mut material,
        report.root_pageserver_total_chunk_metadata_dirty_mutations,
    );
    append_u64(
        &mut material,
        report.root_pageserver_max_chunk_metadata_checkpoint_interval,
    );
    append_u64(
        &mut material,
        report.root_pageserver_total_deferred_metadata_checkpoint_endpoint_count,
    );
    append_u64(
        &mut material,
        report.root_pageserver_total_deferred_metadata_checkpoint_requests,
    );
    append_u64(
        &mut material,
        report.root_pageserver_total_deferred_metadata_checkpoint_runs,
    );
    append_u64(
        &mut material,
        report.root_pageserver_total_chunk_capacity_rejected_writes,
    );
    append_u64(
        &mut material,
        report.root_pageserver_total_replica_service_mode_rejected_mutations,
    );
    append_u64(
        &mut material,
        report.root_pageserver_total_chunk_physical_write_count,
    );
    append_u64(
        &mut material,
        report.root_pageserver_total_chunk_physical_write_bytes,
    );
    append_u64(
        &mut material,
        report.root_pageserver_total_chunk_physical_discard_range_count,
    );
    append_u64(
        &mut material,
        report.root_pageserver_total_chunk_physical_discard_bytes,
    );
    append_u64(
        &mut material,
        report.root_pageserver_total_reclaimable_bytes,
    );
    append_u64(
        &mut material,
        report.root_pageserver_total_trailing_trim_bytes,
    );
    append_u64(
        &mut material,
        report.root_pageserver_total_fragmented_chunk_count,
    );
    append_u64(
        &mut material,
        report.root_pageserver_total_fragmented_discarded_bytes,
    );
    append_u64(
        &mut material,
        report.root_pageserver_total_allocation_receipt_count,
    );
    append_u64(
        &mut material,
        report.root_pageserver_total_batch_allocation_receipt_count,
    );
    append_u64(
        &mut material,
        report.root_pageserver_total_release_allocation_receipt_count,
    );
    append_u64(
        &mut material,
        report.root_pageserver_total_write_receipt_count,
    );
    append_u64(
        &mut material,
        report.root_pageserver_total_batch_write_receipt_count,
    );
    append_u64(
        &mut material,
        report.root_pageserver_total_discard_receipt_count,
    );
    append_u64(
        &mut material,
        report.root_pageserver_total_batch_discard_receipt_count,
    );
    append_u64(&mut material, report.root_pageserver_health_ok_count);
    append_u64(&mut material, report.root_pageserver_health_degraded_count);
    append_u64(
        &mut material,
        report.root_pageserver_health_unavailable_count,
    );
    append_u64(&mut material, report.root_pageserver_health_unknown_count);
    append_u64(
        &mut material,
        report.root_pageserver_not_fault_tolerant_count,
    );
    append_u64(
        &mut material,
        report.root_pageserver_total_health_reason_count,
    );
    append_u64(
        &mut material,
        report.root_pageserver_total_write_quorum,
    );
    append_u64(&mut material, report.root_pageserver_total_read_quorum);
    append_u64(
        &mut material,
        report.root_pageserver_total_write_quorum_deficit,
    );
    append_u64(
        &mut material,
        report.root_pageserver_total_read_quorum_deficit,
    );
    append_u64(
        &mut material,
        report.root_pageserver_total_write_quorum_margin,
    );
    append_u64(
        &mut material,
        report.root_pageserver_total_read_quorum_margin,
    );
    append_string(
        &mut material,
        &report.root_pageserver_first_health_reasons,
    );
    append_u64(
        &mut material,
        report.root_pageserver_route_control_proof_count,
    );
    append_u64(
        &mut material,
        report.root_pageserver_route_control_read_ready_count,
    );
    append_u64(
        &mut material,
        report.root_pageserver_route_control_write_ready_count,
    );
    append_string(
        &mut material,
        &report.root_pageserver_route_control_proof_checksum,
    );
    append_u64(
        &mut material,
        report.root_pageserver_route_control_min_commit_index,
    );
    append_u64(
        &mut material,
        report.root_pageserver_route_control_max_commit_index,
    );
    append_string(
        &mut material,
        &report.root_pageserver_service_coverage_status,
    );
}
