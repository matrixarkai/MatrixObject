// SPDX-License-Identifier: Apache-2.0
fn append_metadata_control_proof(mut material: &mut Vec<u8>, report: &ObjectStoreDistributedReadiness) {
    append_string(&mut material, &report.metadata_control_proof.state_format);
    append_u64(&mut material, report.metadata_control_proof.state_bytes);
    append_string(&mut material, &report.metadata_control_proof.state_checksum);
    append_u64(
        &mut material,
        report.metadata_control_proof.read_ready as u64,
    );
    append_u64(
        &mut material,
        report.metadata_control_proof.write_ready as u64,
    );
    append_u64(
        &mut material,
        report.metadata_control_proof.commit_bounds_valid as u64,
    );
    append_u64(
        &mut material,
        report.metadata_control_proof.admission_pressure as u64,
    );
    append_u64(
        &mut material,
        report.metadata_control_proof.qos_pressure as u64,
    );
    append_string(
        &mut material,
        &report.metadata_control_proof.topology_checksum,
    );
    append_string(
        &mut material,
        &report.metadata_control_proof.session_checksum,
    );
    append_string(
        &mut material,
        &report.metadata_control_proof.coordinator_config_checksum,
    );
    append_string(
        &mut material,
        &report.metadata_control_proof.coordinator_backend,
    );
    append_u64(
        &mut material,
        report.metadata_control_proof.coordinator_config_revision,
    );
    append_u64(
        &mut material,
        report
            .metadata_control_proof
            .coordinator_config_endpoint_count,
    );
    append_u64(
        &mut material,
        report
            .metadata_control_proof
            .coordinator_config_receipt_count,
    );
    append_string(
        &mut material,
        &report.metadata_control_proof.state_fingerprint,
    );
    append_u64(
        &mut material,
        report.metadata_control_proof.active_owner_sessions,
    );
    append_u64(
        &mut material,
        report.metadata_control_proof.expired_owner_sessions,
    );
    append_u64(&mut material, report.metadata_control_proof.writable_shards);
    append_u64(&mut material, report.metadata_control_proof.draining_shards);
    append_u64(
        &mut material,
        report.metadata_control_proof.stale_owner_shards,
    );
    append_u64(
        &mut material,
        report.metadata_control_proof.raft_membership_epoch,
    );
    append_u64(
        &mut material,
        report.metadata_control_proof.raft_membership_receipt_count,
    );
    append_u64(
        &mut material,
        report.metadata_control_proof.metadata_replication_quorum,
    );
    append_u64(
        &mut material,
        report.metadata_control_proof.metadata_replication_acks,
    );
    append_u64(
        &mut material,
        report.metadata_control_proof.retained_mutation_receipts,
    );
    append_u64(
        &mut material,
        report.metadata_control_proof.change_log_pressure as u64,
    );
    append_u64(
        &mut material,
        report.metadata_control_proof.mutation_receipt_pressure as u64,
    );
    append_u64(
        &mut material,
        report.metadata_control_proof.max_change_checkpoint_lag,
    );
}
