// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
fn placement_raft_status_readiness_fields(
    status: &PlacementRaftStatus,
    field_prefix: &str,
) -> BTreeMap<String, String> {
    let self_ = status;
    let mut fields = BTreeMap::new();
    for (name, value) in [
        ("writable", self_.writable),
        ("linearizable_reads", self_.linearizable_reads),
        (
            "placement_metadata_read_ready",
            self_.placement_metadata_read_ready,
        ),
        (
            "placement_metadata_write_ready",
            self_.placement_metadata_write_ready,
        ),
        (
            "root_blockserver_read_ready",
            self_.root_pageserver_read_ready,
        ),
        (
            "root_blockserver_write_ready",
            self_.root_pageserver_write_ready,
        ),
        (
            "root_chunkserver_read_ready",
            self_.root_replicaserver_read_ready,
        ),
        (
            "root_chunkserver_write_ready",
            self_.root_replicaserver_write_ready,
        ),
        (
            "placement_readiness_gate_read_ready",
            self_.placement_readiness_gate_read_ready,
        ),
        (
            "placement_readiness_gate_write_ready",
            self_.placement_readiness_gate_write_ready,
        ),
        (
            "placement_metadata_status_available",
            self_.placement_metadata_status_available,
        ),
        (
            "placement_metadata_read_quorum_available",
            self_.placement_metadata_read_quorum_available,
        ),
        (
            "placement_metadata_write_quorum_available",
            self_.placement_metadata_write_quorum_available,
        ),
        (
            "placement_metadata_caught_up_quorum_available",
            self_.placement_metadata_caught_up_quorum_available,
        ),
        (
            "placement_metadata_apply_quorum_available",
            self_.placement_metadata_apply_quorum_available,
        ),
        (
            "placement_metadata_read_fence_quorum_available",
            self_.placement_metadata_read_fence_quorum_available,
        ),
        (
            "placement_metadata_control_apply_quorum_available",
            self_.placement_metadata_control_apply_quorum_available,
        ),
        (
            "placement_metadata_shard_topology_mismatch",
            self_.placement_metadata_shard_topology_mismatch,
        ),
        (
            "placement_metadata_coordinator_backend_mismatch",
            self_.placement_metadata_coordinator_backend_mismatch,
        ),
        (
            "placement_metadata_coordinator_config_mismatch",
            self_.placement_metadata_coordinator_config_mismatch,
        ),
        (
            "placement_block_allocator_shards_complete",
            self_.root_page_allocator_shards_complete,
        ),
    ] {
        fields.insert(format!("{field_prefix}{name}"), value.to_string());
    }
    fields
}
