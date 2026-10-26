// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
fn append_placement_chunkserver_topology(mut material: &mut Vec<u8>, report: &ObjectStoreDistributedReadiness) {
    append_u64(&mut material, report.root_active_chunkserver_count);
    append_u64(&mut material, report.root_writable_chunkserver_count);
    append_u64(
        &mut material,
        report.root_placement_eligible_chunkserver_count,
    );
    append_u64(
        &mut material,
        report.root_placement_ineligible_chunkserver_count,
    );
    append_u64(&mut material, report.root_draining_chunkserver_count);
    append_u64(
        &mut material,
        report.root_capacity_floor_blocked_chunkserver_count,
    );
    append_u64(&mut material, report.root_frozen_chunkserver_count);
    append_u64(&mut material, report.root_read_only_chunkserver_count);
    append_u64(&mut material, report.root_raft_unwritable_chunkserver_count);
    append_u64(
        &mut material,
        report.root_chunkserver_total_reported_free_bytes,
    );
    append_u64(&mut material, report.root_chunkserver_min_free_bytes);
    append_u64(&mut material, report.root_chunkserver_control_proof_count);
    append_u64(
        &mut material,
        report.root_chunkserver_control_read_ready_count,
    );
    append_u64(
        &mut material,
        report.root_chunkserver_control_write_ready_count,
    );
    append_string(
        &mut material,
        &report.root_chunkserver_control_proof_checksum,
    );
    append_u64(
        &mut material,
        report.root_chunkserver_control_min_raft_commit_index,
    );
    append_u64(
        &mut material,
        report.root_chunkserver_control_max_raft_commit_index,
    );
    append_string(&mut material, &report.root_chunkserver_coverage_status);
    append_u64(&mut material, report.root_chunkserver_health_ok_count);
    append_u64(
        &mut material,
        report.root_chunkserver_health_degraded_count,
    );
    append_u64(
        &mut material,
        report.root_chunkserver_health_unavailable_count,
    );
    append_u64(&mut material, report.root_chunkserver_health_unknown_count);
    append_u64(
        &mut material,
        report.root_chunkserver_not_fault_tolerant_count,
    );
    append_u64(
        &mut material,
        report.root_chunkserver_total_health_reason_count,
    );
    append_string(
        &mut material,
        &report.root_chunkserver_first_health_reasons,
    );
}
