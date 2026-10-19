// SPDX-License-Identifier: Apache-2.0
fn root_raft_status_chunk_fields(
    status: &RootRaftStatus,
    field_prefix: &str,
) -> BTreeMap<String, String> {
    let mut fields = BTreeMap::new();
    for (name, value) in [
        (
            "root_active_chunkserver_count",
            status.root_active_replicaserver_count,
        ),
        (
            "root_writable_chunkserver_count",
            status.root_writable_replicaserver_count,
        ),
        (
            "root_placement_eligible_chunkserver_count",
            status.root_placement_eligible_replicaserver_count,
        ),
        (
            "root_placement_ineligible_chunkserver_count",
            status.root_placement_ineligible_replicaserver_count,
        ),
        (
            "root_chunkserver_control_proof_count",
            status.root_replicaserver_control_proof_count,
        ),
        (
            "root_chunkserver_control_read_ready_count",
            status.root_replicaserver_control_read_ready_count,
        ),
        (
            "root_chunkserver_control_write_ready_count",
            status.root_replicaserver_control_write_ready_count,
        ),
        (
            "root_chunkserver_control_min_raft_commit_index",
            status.root_replicaserver_control_min_raft_commit_index,
        ),
        (
            "root_chunkserver_control_max_raft_commit_index",
            status.root_replicaserver_control_max_raft_commit_index,
        ),
        (
            "root_chunkserver_total_discarded_range_count",
            status.root_replicaserver_total_discarded_range_count,
        ),
        (
            "root_chunkserver_total_reclaimable_bytes",
            status.root_replicaserver_total_reclaimable_bytes,
        ),
        (
            "root_chunkserver_total_trailing_trim_bytes",
            status.root_replicaserver_total_trailing_trim_bytes,
        ),
        (
            "root_chunkserver_total_fragmented_chunk_count",
            status.root_replicaserver_total_fragmented_chunk_count,
        ),
        (
            "root_chunkserver_total_fragmented_discarded_bytes",
            status.root_replicaserver_total_fragmented_discarded_bytes,
        ),
        (
            "root_reclaim_pressure_chunkserver_count",
            status.root_reclaim_pressure_replicaserver_count,
        ),
        (
            "root_chunkserver_health_ok_count",
            status.root_replicaserver_health_ok_count,
        ),
        (
            "root_chunkserver_health_degraded_count",
            status.root_replicaserver_health_degraded_count,
        ),
        (
            "root_chunkserver_health_unavailable_count",
            status.root_replicaserver_health_unavailable_count,
        ),
        (
            "root_chunkserver_health_unknown_count",
            status.root_replicaserver_health_unknown_count,
        ),
        (
            "root_chunkserver_not_fault_tolerant_count",
            status.root_replicaserver_not_fault_tolerant_count,
        ),
        (
            "root_chunkserver_total_health_reason_count",
            status.root_replicaserver_total_health_reason_count,
        ),
    ] {
        fields.insert(format!("{field_prefix}{name}"), value.to_string());
    }
    fields
}
