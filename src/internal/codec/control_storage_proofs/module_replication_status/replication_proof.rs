// SPDX-License-Identifier: Apache-2.0
pub fn metadata_replication_proof_report(
    readiness: &ObjectStoreDistributedReadiness,
) -> MetadataReplicationProofReport {
    let mut report = MetadataReplicationProofReport {
        format: "MXOS-RUST-METADATA-REPLICATION-PROOF5".to_string(),
        ready: readiness.metadata_ready,
        reason: readiness.reason.clone(),
        root_ready: readiness.root_ready,
        root_status: readiness.root_status.clone(),
        root_raft_status_checksum_format: readiness.root_raft_status_checksum_format.clone(),
        root_raft_status_checksum: readiness.root_raft_status_checksum.clone(),
        metadata_commit_index: readiness.metadata_commit_index,
        metadata_snapshot_floor_commit_index: readiness.metadata_snapshot_floor_commit_index,
        metadata_change_checkpoint_lag: readiness.metadata_change_checkpoint_lag,
        object_manifest_checkpoint_lag: readiness.object_manifest_checkpoint_lag,
        object_metadata_checkpoint_lag: readiness.object_metadata_checkpoint_lag,
        object_deferred_manifest_checkpoints: readiness.object_deferred_manifest_checkpoints,
        object_checkpoint_pressure: readiness.object_checkpoint_pressure,
        metadata_replication_enabled: readiness.metadata_replication_enabled,
        metadata_replica_count: readiness.metadata_replica_count,
        metadata_replication_quorum: readiness.metadata_replication_quorum,
        metadata_replication_acks: readiness.metadata_replication_acks,
        metadata_replication_failures: readiness.metadata_replication_failures,
        metadata_replication_last_status: readiness.metadata_replication_last_status.clone(),
        metadata_read_ready: readiness.metadata_read_ready,
        metadata_write_ready: readiness.metadata_write_ready,
        metadata_caught_up: readiness.metadata_caught_up,
        metadata_quorum_ready: readiness.metadata_quorum_ready,
        metadata_state_fingerprint: readiness.metadata_state_fingerprint.clone(),
        metadata_route_cache_pressure: readiness.metaserver_route_cache_pressure,
        metadata_route_cache_fresh: readiness.metaserver_route_cache_fresh,
        metadata_route_topology_checksum: readiness.metadata_route_cache.topology_checksum.clone(),
        metadata_route_session_checksum: readiness.metadata_route_cache.session_checksum.clone(),
        metaserver_route_topology_checksum: readiness.metaserver_route_topology_checksum.clone(),
        metaserver_route_session_checksum: readiness.metaserver_route_session_checksum.clone(),
        distributed_readiness_checksum: readiness.control_checksum.clone(),
        envelope_checksum: String::new(),
    };
    report.envelope_checksum = metadata_replication_proof_checksum(&report);
    report
}

pub fn verify_metadata_replication_proof_report(
    expected: &MetadataReplicationProofReport,
    readiness: &ObjectStoreDistributedReadiness,
) -> Result<(), ObjectError> {
    if expected.format != "MXOS-RUST-METADATA-REPLICATION-PROOF5" {
        return Err(ObjectError::PreconditionFailed(format!(
            "metadata replication proof format mismatch: {}",
            expected.format
        )));
    }
    let actual = metadata_replication_proof_report(readiness);
    if expected != &actual {
        return Err(ObjectError::PreconditionFailed(format!(
            "metadata replication proof mismatch: expected envelope {} got {}",
            expected.envelope_checksum, actual.envelope_checksum
        )));
    }
    Ok(())
}
