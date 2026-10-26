// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
fn append_placement_metadata_topology(mut material: &mut Vec<u8>, report: &ObjectStoreDistributedReadiness) {
    append_u64(&mut material, report.root_active_metaserver_count);
    append_u64(&mut material, report.root_metadata_value_bytes);
    append_u64(
        &mut material,
        report.root_metadata_readable_metaserver_count,
    );
    append_u64(
        &mut material,
        report.root_metadata_writable_metaserver_count,
    );
    append_u64(
        &mut material,
        report.root_metadata_caught_up_metaserver_count,
    );
    append_u64(
        &mut material,
        report.root_metadata_apply_caught_up_metaserver_count,
    );
    append_u64(
        &mut material,
        report.root_metadata_control_apply_caught_up_metaserver_count,
    );
    append_u64(
        &mut material,
        report.root_metadata_read_fence_covered_metaserver_count,
    );
    append_u64(&mut material, report.root_metadata_quorum);
    append_u64(
        &mut material,
        report.root_metadata_min_last_applied_mutation_commit_index,
    );
    append_u64(&mut material, report.root_metadata_max_apply_commit_lag);
    append_u64(&mut material, report.root_metadata_min_safe_read_fence);
    append_u64(
        &mut material,
        report.root_metadata_max_read_fence_commit_lag,
    );
    append_u64(
        &mut material,
        report.root_metadata_max_read_fence_read_index_lag,
    );
    append_u64(
        &mut material,
        report.root_metadata_max_read_fence_applied_lag,
    );
    append_u64(
        &mut material,
        report.root_metadata_control_min_last_applied_mutation_commit_index,
    );
    append_u64(
        &mut material,
        report.root_metadata_control_max_last_applied_mutation_commit_index,
    );
    append_u64(
        &mut material,
        report.root_metadata_control_max_apply_commit_lag,
    );
    append_u64(
        &mut material,
        report.root_metadata_read_quorum_available as u64,
    );
    append_u64(
        &mut material,
        report.root_metadata_write_quorum_available as u64,
    );
    append_u64(
        &mut material,
        report.root_metadata_caught_up_quorum_available as u64,
    );
    append_u64(
        &mut material,
        report.root_metadata_apply_quorum_available as u64,
    );
    append_u64(
        &mut material,
        report.root_metadata_read_fence_quorum_available as u64,
    );
    append_u64(
        &mut material,
        report.root_metadata_control_apply_quorum_available as u64,
    );
    append_u64(
        &mut material,
        report.root_metadata_shard_topology_mismatch as u64,
    );
    append_u64(
        &mut material,
        report.root_metadata_coordinator_backend_mismatch as u64,
    );
    append_u64(
        &mut material,
        report.root_metadata_coordinator_config_mismatch as u64,
    );
    append_u64(&mut material, report.root_metadata_max_shard_topology_epoch);
    append_u64(&mut material, report.root_metadata_control_proof_count);
    append_u64(&mut material, report.root_metadata_control_read_ready_count);
    append_u64(
        &mut material,
        report.root_metadata_control_write_ready_count,
    );
    append_u64(
        &mut material,
        report.root_metadata_external_coordinator_required_metaserver_count,
    );
    append_u64(
        &mut material,
        report.root_metadata_external_coordinator_endpoints_ready_metaserver_count,
    );
    append_u64(
        &mut material,
        report.root_metadata_external_coordinator_owner_sessions_ready_metaserver_count,
    );
    append_u64(
        &mut material,
        report.root_metadata_external_coordinator_ready_metaserver_count,
    );
    append_string(&mut material, &report.root_metadata_control_proof_checksum);
    append_string(&mut material, &report.root_metadata_coverage_status);
}
