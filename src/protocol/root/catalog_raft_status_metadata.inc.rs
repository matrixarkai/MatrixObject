fn root_raft_status_metadata_fields(
    status: &RootRaftStatus,
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
            "root_active_metaserver_count",
            status.root_active_metaserver_count,
        ),
        (
            "root_metadata_readable_metaserver_count",
            status.root_metadata_readable_metaserver_count,
        ),
        (
            "root_metadata_writable_metaserver_count",
            status.root_metadata_writable_metaserver_count,
        ),
        (
            "root_metadata_caught_up_metaserver_count",
            status.root_metadata_caught_up_metaserver_count,
        ),
        (
            "root_metadata_apply_caught_up_metaserver_count",
            status.root_metadata_apply_caught_up_metaserver_count,
        ),
        (
            "root_metadata_control_apply_caught_up_metaserver_count",
            status.root_metadata_control_apply_caught_up_metaserver_count,
        ),
        (
            "root_metadata_read_fence_covered_metaserver_count",
            status.root_metadata_read_fence_covered_metaserver_count,
        ),
        ("root_metadata_quorum", status.root_metadata_quorum),
        (
            "root_metadata_min_last_applied_mutation_commit_index",
            status.root_metadata_min_last_applied_mutation_commit_index,
        ),
        (
            "root_metadata_max_apply_commit_lag",
            status.root_metadata_max_apply_commit_lag,
        ),
        (
            "root_metadata_min_safe_read_fence",
            status.root_metadata_min_safe_read_fence,
        ),
        (
            "root_metadata_max_read_fence_commit_lag",
            status.root_metadata_max_read_fence_commit_lag,
        ),
        (
            "root_metadata_max_read_fence_read_index_lag",
            status.root_metadata_max_read_fence_read_index_lag,
        ),
        (
            "root_metadata_max_read_fence_applied_lag",
            status.root_metadata_max_read_fence_applied_lag,
        ),
        (
            "root_metadata_control_min_last_applied_mutation_commit_index",
            status.root_metadata_control_min_last_applied_mutation_commit_index,
        ),
        (
            "root_metadata_control_max_last_applied_mutation_commit_index",
            status.root_metadata_control_max_last_applied_mutation_commit_index,
        ),
        (
            "root_metadata_control_max_apply_commit_lag",
            status.root_metadata_control_max_apply_commit_lag,
        ),
        (
            "root_metadata_max_shard_topology_epoch",
            status.root_metadata_max_shard_topology_epoch,
        ),
        (
            "root_metadata_control_proof_count",
            status.root_metadata_control_proof_count,
        ),
        (
            "root_metadata_control_read_ready_count",
            status.root_metadata_control_read_ready_count,
        ),
        (
            "root_metadata_control_write_ready_count",
            status.root_metadata_control_write_ready_count,
        ),
        (
            "root_metadata_external_coordinator_required_metaserver_count",
            status.root_metadata_external_coordinator_required_metaserver_count,
        ),
        (
            "root_metadata_external_coordinator_endpoints_ready_metaserver_count",
            status.root_metadata_external_coordinator_endpoints_ready_metaserver_count,
        ),
        (
            "root_metadata_external_coordinator_owner_sessions_ready_metaserver_count",
            status.root_metadata_external_coordinator_owner_sessions_ready_metaserver_count,
        ),
        (
            "root_metadata_external_coordinator_ready_metaserver_count",
            status.root_metadata_external_coordinator_ready_metaserver_count,
        ),
    ] {
        fields.insert(format!("{field_prefix}{name}"), value.to_string());
    }
    fields
}
