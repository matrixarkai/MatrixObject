// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
struct DistributedPlacementReadinessSnapshot {
    root_node_count: u64,
    placement_required: bool,
    control_plane_mode: String,
    placement_expected_nodes: u64,
    root_metadata_coverage: PlacementMetadataServiceCoverage,
    placement_metadata_status_available: bool,
    root_metadata_external_coordinator_required: bool,
    root_metadata_external_coordinator_endpoints_ready: bool,
    root_metadata_external_coordinator_owner_sessions_ready: bool,
    root_metadata_external_coordinator_ready: bool,
    root_metadata_control_ready: bool,
    root_metadata_coverage_ready: bool,
    root_page_allocator_coverage: PlacementPageAllocatorShardCoverage,
    root_page_allocator_ready: bool,
    root_pageserver_service_coverage: PlacementPageServerServiceCoverage,
    root_pageserver_service_ready: bool,
    root_pageserver_route_control_ready: bool,
    root_replicaserver_coverage: PlacementReplicaServerCoverage,
    root_replicaserver_ready: bool,
    root_replicaserver_control_ready: bool,
    root_leader_node_id: String,
    root_lease_epoch: u64,
    root_lease_expire_time_ms: u64,
    placement_state_fingerprint: String,
    placement_raft_status_checksum_format: String,
    root_raft_status_checksum: String,
    placement_metadata_read_ready: bool,
    placement_metadata_write_ready: bool,
    root_pageserver_read_ready: bool,
    root_pageserver_write_ready: bool,
    root_replicaserver_read_ready: bool,
    root_replicaserver_write_ready: bool,
    placement_readiness_gate_read_ready: bool,
    placement_readiness_gate_write_ready: bool,
    placement_leader_ready: bool,
    placement_read_ready: bool,
    root_write_ready: bool,
    placement_ready: bool,
}

impl DistributedPlacementReadinessSnapshot {
    fn capture(
        store: &MatrixObjectStore,
        metadata_replication_quorum: u64,
        min_capacity_free_bytes: u64,
        now_ms: u64,
        root_placement_ready: bool,
    ) -> Self {
        let placement_nodes = store.placement_nodes();
        let root_node_count = placement_nodes.len() as u64;
        let placement_required = store.placement_required();
        let control_plane_mode = store.effective_control_plane_mode().to_string();
        let placement_expected_nodes = root_node_count;
        let root_metadata_coverage =
            compute_placement_metadata_service_coverage(&placement_nodes, metadata_replication_quorum);
        let placement_metadata_status_available = root_metadata_coverage.active_metaserver_count > 0;
        let root_observed_metadata = root_metadata_coverage.active_metaserver_count > 0;
        let root_metadata_external_coordinator_required =
            root_metadata_coverage.metadata_external_coordinator_required_metaserver_count > 0;
        let root_metadata_external_coordinator_endpoints_ready =
            !root_metadata_external_coordinator_required
                || root_metadata_coverage
                    .metadata_external_coordinator_endpoints_ready_metaserver_count
                    >= root_metadata_coverage
                        .metadata_external_coordinator_required_metaserver_count;
        let root_metadata_external_coordinator_owner_sessions_ready =
            !root_metadata_external_coordinator_required
                || root_metadata_coverage
                    .metadata_external_coordinator_owner_sessions_ready_metaserver_count
                    >= root_metadata_coverage
                        .metadata_external_coordinator_required_metaserver_count;
        let root_metadata_external_coordinator_ready =
            root_metadata_external_coordinator_endpoints_ready
                && root_metadata_external_coordinator_owner_sessions_ready
                && (!root_metadata_external_coordinator_required
                    || root_metadata_coverage.metadata_external_coordinator_ready_metaserver_count
                        >= root_metadata_coverage
                            .metadata_external_coordinator_required_metaserver_count);
        let root_metadata_control_ready = !root_observed_metadata
            || root_metadata_coverage.metadata_control_proof_count == 0
            || (root_metadata_coverage.metadata_control_read_ready_count > 0
                && root_metadata_coverage.metadata_control_write_ready_count > 0);
        let root_metadata_coverage_ready = !root_observed_metadata
            || (root_metadata_coverage.status == "healthy"
                && root_metadata_coverage.metadata_apply_quorum_available
                && root_metadata_coverage.metadata_read_fence_quorum_available
                && root_metadata_coverage.metadata_control_apply_quorum_available
                && root_metadata_control_ready
                && root_metadata_external_coordinator_ready);
        let root_page_allocator_coverage = store.meta_service.page_allocator_shard_coverage();
        let root_page_allocator_ready = root_page_allocator_coverage.active_pageserver_count
            == 0
            || root_page_allocator_coverage.complete;
        let root_pageserver_service_coverage = store.meta_service.pageserver_service_coverage();
        let root_pageserver_service_ready =
            root_pageserver_service_coverage.active_pageserver_count == 0
                || (root_pageserver_service_coverage.ready_pageserver_count > 0
                    && root_pageserver_service_coverage.read_ready_pageserver_count > 0);
        let root_pageserver_route_control_ready =
            root_pageserver_service_coverage.route_control_proof_count == 0
                || (root_pageserver_service_coverage.route_control_write_ready_count > 0
                    && root_pageserver_service_coverage.route_control_read_ready_count > 0);
        let root_replicaserver_coverage = store
            .meta_service
            .replicaserver_coverage(min_capacity_free_bytes);
        let root_replicaserver_ready = root_replicaserver_coverage.active_replicaserver_count == 0
            || root_replicaserver_coverage.writable_replicaserver_count > 0;
        let root_replicaserver_control_ready = root_replicaserver_coverage.control_proof_count == 0
            || (root_replicaserver_coverage.control_read_ready_count > 0
                && root_replicaserver_coverage.control_write_ready_count > 0);
        let root_lease = store.root_leader_lease();
        let root_leader_node_id = root_lease
            .as_ref()
            .map_or_else(String::new, |lease| lease.leader_node_id.clone());
        let root_lease_epoch = root_lease.as_ref().map_or(0, |lease| lease.epoch);
        let root_lease_expire_time_ms = root_lease.as_ref().map_or(0, |lease| lease.expire_time_ms);
        let placement_state_fingerprint = store.meta_service.placement_state_fingerprint();
        let placement_raft_status = store.meta_service.raft_status_from_coverage(
            &root_metadata_coverage,
            &root_page_allocator_coverage,
            &root_pageserver_service_coverage,
            &root_replicaserver_coverage,
        );
        let placement_raft_status_checksum_format = placement_raft_status.control_checksum_format;
        let root_raft_status_checksum = placement_raft_status.control_checksum;
        let placement_metadata_read_ready = placement_raft_status.placement_metadata_read_ready;
        let placement_metadata_write_ready = placement_raft_status.placement_metadata_write_ready;
        let root_pageserver_read_ready = placement_raft_status.root_pageserver_read_ready;
        let root_pageserver_write_ready = placement_raft_status.root_pageserver_write_ready;
        let root_replicaserver_read_ready = placement_raft_status.root_replicaserver_read_ready;
        let root_replicaserver_write_ready = placement_raft_status.root_replicaserver_write_ready;
        let placement_readiness_gate_read_ready = placement_raft_status.placement_readiness_gate_read_ready;
        let placement_readiness_gate_write_ready = placement_raft_status.placement_readiness_gate_write_ready;
        let placement_read_ready = placement_raft_status.linearizable_reads;
        let root_write_ready = placement_raft_status.writable;
        let placement_leader_ready = if !placement_required {
            true
        } else {
            root_lease.as_ref().is_some_and(|lease| {
                lease.expire_time_ms > now_ms
                    && placement_nodes.iter().any(|node| {
                        node.node_id == lease.leader_node_id
                            && is_control_plane_leader_candidate(node)
                            && node.online
                            && !node.draining
                    })
            })
        };
        let root_observed_services_ready = root_metadata_coverage_ready
            && root_page_allocator_ready
            && root_pageserver_service_ready
            && root_pageserver_route_control_ready
            && root_replicaserver_ready
            && root_replicaserver_control_ready;
        let placement_ready = if placement_required {
            placement_leader_ready && root_placement_ready && root_observed_services_ready
        } else {
            root_observed_services_ready
        };

        Self {
            root_node_count,
            placement_required,
            control_plane_mode,
            placement_expected_nodes,
            root_metadata_coverage,
            placement_metadata_status_available,
            root_metadata_external_coordinator_required,
            root_metadata_external_coordinator_endpoints_ready,
            root_metadata_external_coordinator_owner_sessions_ready,
            root_metadata_external_coordinator_ready,
            root_metadata_control_ready,
            root_metadata_coverage_ready,
            root_page_allocator_coverage,
            root_page_allocator_ready,
            root_pageserver_service_coverage,
            root_pageserver_service_ready,
            root_pageserver_route_control_ready,
            root_replicaserver_coverage,
            root_replicaserver_ready,
            root_replicaserver_control_ready,
            root_leader_node_id,
            root_lease_epoch,
            root_lease_expire_time_ms,
            placement_state_fingerprint,
            placement_raft_status_checksum_format,
            root_raft_status_checksum,
            placement_metadata_read_ready,
            placement_metadata_write_ready,
            root_pageserver_read_ready,
            root_pageserver_write_ready,
            root_replicaserver_read_ready,
            root_replicaserver_write_ready,
            placement_readiness_gate_read_ready,
            placement_readiness_gate_write_ready,
            placement_leader_ready,
            placement_read_ready,
            root_write_ready,
            placement_ready,
        }
    }
}
