// SPDX-License-Identifier: Apache-2.0
fn append_root_replicaserver_topology(mut material: &mut Vec<u8>, report: &ObjectStoreDistributedReadiness) {
    append_u64(&mut material, report.root_active_replicaserver_count);
    append_u64(&mut material, report.root_writable_replicaserver_count);
    append_u64(
        &mut material,
        report.root_placement_eligible_replicaserver_count,
    );
    append_u64(
        &mut material,
        report.root_placement_ineligible_replicaserver_count,
    );
    append_u64(&mut material, report.root_draining_replicaserver_count);
    append_u64(
        &mut material,
        report.root_capacity_floor_blocked_replicaserver_count,
    );
    append_u64(&mut material, report.root_frozen_replicaserver_count);
    append_u64(&mut material, report.root_read_only_replicaserver_count);
    append_u64(&mut material, report.root_raft_unwritable_replicaserver_count);
    append_u64(
        &mut material,
        report.root_replicaserver_total_reported_free_bytes,
    );
    append_u64(&mut material, report.root_replicaserver_min_free_bytes);
    append_u64(&mut material, report.root_replicaserver_control_proof_count);
    append_u64(
        &mut material,
        report.root_replicaserver_control_read_ready_count,
    );
    append_u64(
        &mut material,
        report.root_replicaserver_control_write_ready_count,
    );
    append_string(
        &mut material,
        &report.root_replicaserver_control_proof_checksum,
    );
    append_u64(
        &mut material,
        report.root_replicaserver_control_min_raft_commit_index,
    );
    append_u64(
        &mut material,
        report.root_replicaserver_control_max_raft_commit_index,
    );
    append_string(&mut material, &report.root_replicaserver_coverage_status);
    append_u64(&mut material, report.root_replicaserver_health_ok_count);
    append_u64(
        &mut material,
        report.root_replicaserver_health_degraded_count,
    );
    append_u64(
        &mut material,
        report.root_replicaserver_health_unavailable_count,
    );
    append_u64(&mut material, report.root_replicaserver_health_unknown_count);
    append_u64(
        &mut material,
        report.root_replicaserver_not_fault_tolerant_count,
    );
    append_u64(
        &mut material,
        report.root_replicaserver_total_health_reason_count,
    );
    append_string(
        &mut material,
        &report.root_replicaserver_first_health_reasons,
    );
}
