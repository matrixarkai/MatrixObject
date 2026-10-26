// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
fn placement_raft_status_metadata_fields(
    status: &PlacementRaftStatus,
    field_prefix: &str,
) -> BTreeMap<String, String> {
    let mut fields = BTreeMap::new();
    for (name, value) in [
        ("term", status.term),
        ("next_log_index", status.next_log_index),
        ("last_applied_index", status.last_applied_index),
        ("commit_index", status.commit_index),
        ("read_index", status.read_index),
        ("node_count", status.node_count),
        ("object_usage_count", status.object_usage_count),
        (
            "placement_active_metaserver_count",
            status.placement_active_metaserver_count,
        ),
        (
            "placement_metadata_readable_metaserver_count",
            status.placement_metadata_readable_metaserver_count,
        ),
        (
            "placement_metadata_writable_metaserver_count",
            status.placement_metadata_writable_metaserver_count,
        ),
        (
            "placement_metadata_caught_up_metaserver_count",
            status.placement_metadata_caught_up_metaserver_count,
        ),
        (
            "placement_metadata_apply_caught_up_metaserver_count",
            status.placement_metadata_apply_caught_up_metaserver_count,
        ),
        (
            "placement_metadata_control_apply_caught_up_metaserver_count",
            status.placement_metadata_control_apply_caught_up_metaserver_count,
        ),
        (
            "placement_metadata_read_fence_covered_metaserver_count",
            status.placement_metadata_read_fence_covered_metaserver_count,
        ),
        ("placement_metadata_quorum", status.placement_metadata_quorum),
        (
            "placement_metadata_min_last_applied_mutation_commit_index",
            status.placement_metadata_min_last_applied_mutation_commit_index,
        ),
        (
            "placement_metadata_max_apply_commit_lag",
            status.placement_metadata_max_apply_commit_lag,
        ),
        (
            "placement_metadata_min_safe_read_fence",
            status.placement_metadata_min_safe_read_fence,
        ),
        (
            "placement_metadata_max_read_fence_commit_lag",
            status.placement_metadata_max_read_fence_commit_lag,
        ),
        (
            "placement_metadata_max_read_fence_read_index_lag",
            status.placement_metadata_max_read_fence_read_index_lag,
        ),
        (
            "placement_metadata_max_read_fence_applied_lag",
            status.placement_metadata_max_read_fence_applied_lag,
        ),
        (
            "placement_metadata_control_min_last_applied_mutation_commit_index",
            status.placement_metadata_control_min_last_applied_mutation_commit_index,
        ),
        (
            "placement_metadata_control_max_last_applied_mutation_commit_index",
            status.placement_metadata_control_max_last_applied_mutation_commit_index,
        ),
        (
            "placement_metadata_control_max_apply_commit_lag",
            status.placement_metadata_control_max_apply_commit_lag,
        ),
        (
            "placement_metadata_max_shard_topology_epoch",
            status.placement_metadata_max_shard_topology_epoch,
        ),
        (
            "placement_metadata_control_proof_count",
            status.placement_metadata_control_proof_count,
        ),
        (
            "placement_metadata_control_read_ready_count",
            status.placement_metadata_control_read_ready_count,
        ),
        (
            "placement_metadata_control_write_ready_count",
            status.placement_metadata_control_write_ready_count,
        ),
        (
            "placement_metadata_external_coordinator_required_metaserver_count",
            status.placement_metadata_external_coordinator_required_metaserver_count,
        ),
        (
            "placement_metadata_external_coordinator_endpoints_ready_metaserver_count",
            status.placement_metadata_external_coordinator_endpoints_ready_metaserver_count,
        ),
        (
            "placement_metadata_external_coordinator_owner_sessions_ready_metaserver_count",
            status.placement_metadata_external_coordinator_owner_sessions_ready_metaserver_count,
        ),
        (
            "placement_metadata_external_coordinator_ready_metaserver_count",
            status.placement_metadata_external_coordinator_ready_metaserver_count,
        ),
    ] {
        fields.insert(format!("{field_prefix}{name}"), value.to_string());
    }
    fields
}
