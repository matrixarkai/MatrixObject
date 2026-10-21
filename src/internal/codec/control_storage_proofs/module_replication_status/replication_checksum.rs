// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
pub(crate) fn validate_storage_parallelism(value: usize, field: &str) -> Result<(), ObjectError> {
    if value == 0 || value > MAX_STORAGE_PARALLELISM {
        return Err(ObjectError::InvalidArgument(format!(
            "{field} must be 1..{MAX_STORAGE_PARALLELISM}"
        )));
    }
    Ok(())
}

pub(crate) fn bounded_parallelism(configured: usize, work_items: usize) -> usize {
    if work_items == 0 {
        0
    } else {
        configured.min(work_items).max(1)
    }
}

pub(crate) fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as u64)
}

pub(crate) fn metadata_replication_proof_checksum(
    report: &MetadataReplicationProofReport,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MXOS-RUST-METADATA-REPLICATION-PROOF5");
    append_u64(&mut material, report.ready as u64);
    append_string(&mut material, &report.reason);
    append_u64(&mut material, report.root_ready as u64);
    append_string(&mut material, &report.root_status);
    append_string(&mut material, &report.root_raft_status_checksum_format);
    append_string(&mut material, &report.root_raft_status_checksum);
    append_u64(&mut material, report.metadata_commit_index);
    append_u64(&mut material, report.metadata_snapshot_floor_commit_index);
    append_u64(&mut material, report.metadata_change_checkpoint_lag);
    append_u64(&mut material, report.object_manifest_checkpoint_lag);
    append_u64(&mut material, report.object_metadata_checkpoint_lag);
    append_u64(
        &mut material,
        report.object_deferred_manifest_checkpoints as u64,
    );
    append_u64(&mut material, report.object_checkpoint_pressure as u64);
    append_u64(&mut material, report.metadata_replication_enabled as u64);
    append_u64(&mut material, report.metadata_replica_count);
    append_u64(&mut material, report.metadata_replication_quorum);
    append_u64(&mut material, report.metadata_replication_acks);
    append_u64(&mut material, report.metadata_replication_failures);
    append_string(&mut material, &report.metadata_replication_last_status);
    append_u64(&mut material, report.metadata_read_ready as u64);
    append_u64(&mut material, report.metadata_write_ready as u64);
    append_u64(&mut material, report.metadata_caught_up as u64);
    append_u64(&mut material, report.metadata_quorum_ready as u64);
    append_string(&mut material, &report.metadata_state_fingerprint);
    append_u64(&mut material, report.metadata_route_cache_pressure as u64);
    append_u64(&mut material, report.metadata_route_cache_fresh as u64);
    append_string(&mut material, &report.metadata_route_topology_checksum);
    append_string(&mut material, &report.metadata_route_session_checksum);
    append_string(&mut material, &report.metaserver_route_topology_checksum);
    append_string(&mut material, &report.metaserver_route_session_checksum);
    append_string(&mut material, &report.distributed_readiness_checksum);
    etag(&material)
}
