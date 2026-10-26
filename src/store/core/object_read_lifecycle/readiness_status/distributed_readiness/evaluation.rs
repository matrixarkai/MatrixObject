// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
struct DistributedReadinessEvaluation<'a> {
    ready: bool,
    metadata_replication_ready: bool,
    metadata_ready: bool,
    page_ready: bool,
    root_leader_ready: bool,
    root_metadata_coverage: &'a PlacementMetadataServiceCoverage,
    root_metadata_external_coordinator_required: bool,
    root_metadata_external_coordinator_endpoints_ready: bool,
    root_metadata_external_coordinator_owner_sessions_ready: bool,
    root_metadata_external_coordinator_ready: bool,
    root_metadata_control_ready: bool,
    root_metadata_coverage_ready: bool,
    root_page_allocator_ready: bool,
    root_pageserver_service_coverage: &'a PlacementPageServerServiceCoverage,
    root_pageserver_service_ready: bool,
    root_pageserver_route_control_ready: bool,
    root_replicaserver_coverage: &'a PlacementReplicaServerCoverage,
    root_replicaserver_ready: bool,
    root_replicaserver_control_ready: bool,
    rootserver_required: bool,
    root_ready: bool,
    root_leader_node_id: &'a str,
    placement_degraded_objects: u64,
}

impl DistributedReadinessEvaluation<'_> {
    fn reason(&self) -> String {
        let reason = if self.ready {
            "ready"
        } else if !self.metadata_replication_ready {
            "metadata_replication_quorum_unavailable"
        } else if !self.metadata_ready {
            "metadata_not_ready"
        } else if !self.page_ready {
            "block_not_ready"
        } else if !self.root_leader_ready {
            "root_leader_not_ready"
        } else if self.root_metadata_coverage.active_metaserver_count > 0
            && (!self.root_metadata_coverage.metadata_read_quorum_available
                || !self.root_metadata_coverage.metadata_write_quorum_available
                || !self
                    .root_metadata_coverage
                    .metadata_caught_up_quorum_available
                || !self.root_metadata_coverage.metadata_apply_quorum_available
                || !self
                    .root_metadata_coverage
                    .metadata_read_fence_quorum_available
                || !self
                    .root_metadata_coverage
                    .metadata_control_apply_quorum_available)
        {
            self.placement_metadata_quorum_reason()
        } else if self.root_metadata_external_coordinator_required
            && !self.root_metadata_external_coordinator_endpoints_ready
        {
            "root_metaserver_external_coordinator_endpoints_unready"
        } else if self.root_metadata_external_coordinator_required
            && !self.root_metadata_external_coordinator_owner_sessions_ready
        {
            "root_metaserver_external_coordinator_owner_sessions_unready"
        } else if self.root_metadata_external_coordinator_required
            && !self.root_metadata_external_coordinator_ready
        {
            "root_metaserver_external_coordinator_unready"
        } else if self
            .root_metadata_coverage
            .metadata_coordinator_backend_mismatch
        {
            "root_metaserver_coordinator_backend_mismatch"
        } else if self
            .root_metadata_coverage
            .metadata_coordinator_config_mismatch
        {
            "root_metaserver_coordinator_config_mismatch"
        } else if !self.root_metadata_control_ready {
            control_proof_readiness_label(
                self.root_metadata_coverage.metadata_control_proof_count,
                self.root_metadata_coverage
                    .metadata_control_read_ready_count,
                self.root_metadata_coverage
                    .metadata_control_write_ready_count,
                "root_metaserver_control_unready",
                "root_metaserver_control_read_unready",
                "root_metaserver_control_write_unready",
            )
            .unwrap_or("root_metaserver_control_unready")
        } else if !self.root_metadata_coverage_ready {
            self.placement_metadata_quorum_reason()
        } else if !self.root_page_allocator_ready {
            "root_block_allocator_shards_incomplete"
        } else if !self.root_pageserver_service_ready {
            service_read_write_readiness_label(
                self.root_pageserver_service_coverage
                    .read_ready_pageserver_count,
                self.root_pageserver_service_coverage
                    .ready_pageserver_count,
                "root_blockservers_unready",
                "root_blockservers_read_unavailable",
                "root_blockservers_write_unavailable",
            )
        } else if !self.root_pageserver_route_control_ready {
            control_proof_readiness_label(
                self.root_pageserver_service_coverage
                    .route_control_proof_count,
                self.root_pageserver_service_coverage
                    .route_control_read_ready_count,
                self.root_pageserver_service_coverage
                    .route_control_write_ready_count,
                "root_blockserver_route_control_unready",
                "root_blockserver_route_control_read_unready",
                "root_blockserver_route_control_write_unready",
            )
            .unwrap_or("root_blockserver_route_control_unready")
        } else if !self.root_replicaserver_ready {
            replicaserver_readiness_label(
                self.root_replicaserver_coverage
                    .placement_eligible_replicaserver_count,
                self.root_replicaserver_coverage.draining_replicaserver_count,
                self.root_replicaserver_coverage
                    .capacity_floor_blocked_replicaserver_count,
                self.root_replicaserver_coverage.frozen_replicaserver_count,
                self.root_replicaserver_coverage.read_only_replicaserver_count,
                self.root_replicaserver_coverage
                    .raft_unwritable_replicaserver_count,
                "root_chunkservers_unwritable",
                "root_chunkservers_frozen_unwritable",
                "root_chunkservers_read_only_unwritable",
                "root_chunkservers_raft_unwritable",
                "root_chunkservers_capacity_floor_unavailable",
                "root_chunkservers_draining_unwritable",
            )
        } else if !self.root_replicaserver_control_ready {
            control_proof_readiness_label(
                self.root_replicaserver_coverage.control_proof_count,
                self.root_replicaserver_coverage.control_read_ready_count,
                self.root_replicaserver_coverage.control_write_ready_count,
                "root_chunkserver_control_unready",
                "root_chunkserver_control_read_unready",
                "root_chunkserver_control_write_unready",
            )
            .unwrap_or("root_chunkserver_control_unready")
        } else {
            "root_placement_degraded"
        };
        reason.to_string()
    }

    fn root_status(&self, reason: &str) -> String {
        let status = if !self.rootserver_required && self.root_ready {
            if self.root_leader_node_id.is_empty() {
                "disabled"
            } else {
                "embedded"
            }
        } else if self.root_ready {
            "ready"
        } else if !self.root_leader_ready {
            "leader_unavailable"
        } else if self.placement_degraded_objects != 0 {
            "placement_degraded"
        } else {
            placement_reason_status(reason)
        };
        status.to_string()
    }

    fn placement_metadata_quorum_reason(&self) -> &'static str {
        metadata_quorum_readiness_label(
            self.root_metadata_coverage.metadata_read_quorum_available,
            self.root_metadata_coverage.metadata_write_quorum_available,
            self.root_metadata_coverage
                .metadata_caught_up_quorum_available,
            self.root_metadata_coverage.metadata_apply_quorum_available,
            self.root_metadata_coverage
                .metadata_read_fence_quorum_available,
            self.root_metadata_coverage
                .metadata_control_apply_quorum_available,
            "root_metaserver_read_quorum_unavailable",
            "root_metaserver_write_quorum_unavailable",
            "root_metaserver_caught_up_quorum_unavailable",
            "root_metaserver_apply_quorum_unavailable",
            "root_metaserver_read_fence_quorum_unavailable",
            "root_metaserver_control_apply_quorum_unavailable",
        )
        .unwrap_or("root_metaserver_degraded")
    }
}

fn placement_reason_status(reason: &str) -> &'static str {
    match reason {
        "root_metaserver_external_coordinator_endpoints_unready" => {
            "metaserver_external_coordinator_endpoints_unready"
        }
        "root_metaserver_external_coordinator_owner_sessions_unready" => {
            "metaserver_external_coordinator_owner_sessions_unready"
        }
        "root_metaserver_external_coordinator_unready" => "metaserver_external_coordinator_unready",
        "root_metaserver_coordinator_backend_mismatch" => "metaserver_coordinator_backend_mismatch",
        "root_metaserver_coordinator_config_mismatch" => "metaserver_coordinator_config_mismatch",
        "root_metaserver_control_unready" => "metaserver_control_unready",
        "root_metaserver_control_read_unready" => "metaserver_control_read_unready",
        "root_metaserver_control_write_unready" => "metaserver_control_write_unready",
        "root_metaserver_read_quorum_unavailable" => "metaserver_read_quorum_unavailable",
        "root_metaserver_write_quorum_unavailable" => "metaserver_write_quorum_unavailable",
        "root_metaserver_caught_up_quorum_unavailable" => "metaserver_caught_up_quorum_unavailable",
        "root_metaserver_apply_quorum_unavailable" => "metaserver_apply_quorum_unavailable",
        "root_metaserver_read_fence_quorum_unavailable" => {
            "metaserver_read_fence_quorum_unavailable"
        }
        "root_metaserver_control_apply_quorum_unavailable" => {
            "metaserver_control_apply_quorum_unavailable"
        }
        "root_metaserver_degraded" => "metaserver_degraded",
        "root_block_allocator_shards_incomplete" => "block_allocator_shards_incomplete",
        "root_blockservers_unready" => "blockservers_unready",
        "root_blockservers_read_unavailable" => "blockservers_read_unavailable",
        "root_blockservers_write_unavailable" => "blockservers_write_unavailable",
        "root_blockserver_route_control_unready" => "blockserver_route_control_unready",
        "root_blockserver_route_control_read_unready" => "blockserver_route_control_read_unready",
        "root_blockserver_route_control_write_unready" => "blockserver_route_control_write_unready",
        "root_chunkservers_unwritable" => "chunkservers_unwritable",
        "root_chunkservers_frozen_unwritable" => "chunkservers_frozen_unwritable",
        "root_chunkservers_read_only_unwritable" => "chunkservers_read_only_unwritable",
        "root_chunkservers_raft_unwritable" => "chunkservers_raft_unwritable",
        "root_chunkservers_capacity_floor_unavailable" => "chunkservers_capacity_floor_unavailable",
        "root_chunkservers_draining_unwritable" => "chunkservers_draining_unwritable",
        "root_chunkserver_control_unready" => "chunkserver_control_unready",
        "root_chunkserver_control_read_unready" => "chunkserver_control_read_unready",
        "root_chunkserver_control_write_unready" => "chunkserver_control_write_unready",
        _ => "not_ready",
    }
}
