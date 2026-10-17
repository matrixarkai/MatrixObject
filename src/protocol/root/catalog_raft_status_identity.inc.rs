fn root_raft_status_identity_fields(
    status: &RootRaftStatus,
    field_prefix: &str,
) -> BTreeMap<String, String> {
    let self_ = status;
    let mut fields = BTreeMap::new();
    for (name, value) in [
        ("leader_node_id", self_.leader_node_id.as_str()),
        (
            "root_metadata_coverage_status",
            self_.root_metadata_coverage_status.as_str(),
        ),
        (
            "root_metadata_control_proof_checksum",
            self_.root_metadata_control_proof_checksum.as_str(),
        ),
        (
            "root_block_allocator_shard_status",
            self_.root_page_allocator_shard_status.as_str(),
        ),
        (
            "root_blockserver_route_control_proof_checksum",
            self_.root_pageserver_route_control_proof_checksum.as_str(),
        ),
        (
            "root_blockserver_service_coverage_status",
            self_.root_pageserver_service_coverage_status.as_str(),
        ),
        (
            "root_chunkserver_control_proof_checksum",
            self_.root_chunkserver_control_proof_checksum.as_str(),
        ),
        (
            "root_chunkserver_coverage_status",
            self_.root_chunkserver_coverage_status.as_str(),
        ),
        (
            "root_chunkserver_first_health_reasons",
            self_.root_chunkserver_first_health_reasons.as_str(),
        ),
        (
            "control_checksum_format",
            self_.control_checksum_format.as_str(),
        ),
        ("control_checksum", self_.control_checksum.as_str()),
    ] {
        fields.insert(format!("{field_prefix}{name}"), value.to_string());
    }
    fields
}
