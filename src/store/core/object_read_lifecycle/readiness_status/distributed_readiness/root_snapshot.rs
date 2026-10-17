struct DistributedRootReadinessSnapshot {
    root_node_count: u64,
    rootserver_required: bool,
    control_plane_mode: String,
    root_expected_nodes: u64,
    root_metadata_coverage: RootMetadataServiceCoverage,
    root_metadata_status_available: bool,
    root_metadata_external_coordinator_required: bool,
    root_metadata_external_coordinator_endpoints_ready: bool,
    root_metadata_external_coordinator_owner_sessions_ready: bool,
    root_metadata_external_coordinator_ready: bool,
    root_metadata_control_ready: bool,
    root_metadata_coverage_ready: bool,
    root_page_allocator_coverage: RootPageAllocatorShardCoverage,
    root_page_allocator_ready: bool,
    root_pageserver_service_coverage: RootPageServerServiceCoverage,
    root_pageserver_service_ready: bool,
    root_pageserver_route_control_ready: bool,
    root_chunkserver_coverage: RootChunkServerCoverage,
    root_chunkserver_ready: bool,
    root_chunkserver_control_ready: bool,
    root_leader_node_id: String,
    root_lease_epoch: u64,
    root_lease_expire_time_ms: u64,
    root_state_fingerprint: String,
    root_raft_status_checksum_format: String,
    root_raft_status_checksum: String,
    root_metadata_read_ready: bool,
    root_metadata_write_ready: bool,
    root_pageserver_read_ready: bool,
    root_pageserver_write_ready: bool,
    root_chunkserver_read_ready: bool,
    root_chunkserver_write_ready: bool,
    root_readiness_gate_read_ready: bool,
    root_readiness_gate_write_ready: bool,
    root_leader_ready: bool,
    root_read_ready: bool,
    root_write_ready: bool,
    root_ready: bool,
}

impl DistributedRootReadinessSnapshot {
    fn capture(
        store: &MatrixObjectStore,
        metadata_replication_quorum: u64,
        min_capacity_free_bytes: u64,
        now_ms: u64,
        root_placement_ready: bool,
    ) -> Self {
        let root_nodes = store.root_nodes();
        let root_node_count = root_nodes.len() as u64;
        let rootserver_required = store.rootserver_required();
        let control_plane_mode = store.effective_control_plane_mode().to_string();
        let root_expected_nodes = root_node_count;
        let root_metadata_coverage =
            compute_root_metadata_service_coverage(&root_nodes, metadata_replication_quorum);
        let root_metadata_status_available = root_metadata_coverage.active_metaserver_count > 0;
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
        let root_page_allocator_coverage = store.root_service.page_allocator_shard_coverage();
        let root_page_allocator_ready = root_page_allocator_coverage.active_pageserver_count
            == 0
            || root_page_allocator_coverage.complete;
        let root_pageserver_service_coverage = store.root_service.pageserver_service_coverage();
        let root_pageserver_service_ready =
            root_pageserver_service_coverage.active_pageserver_count == 0
                || (root_pageserver_service_coverage.ready_pageserver_count > 0
                    && root_pageserver_service_coverage.read_ready_pageserver_count > 0);
        let root_pageserver_route_control_ready =
            root_pageserver_service_coverage.route_control_proof_count == 0
                || (root_pageserver_service_coverage.route_control_write_ready_count > 0
                    && root_pageserver_service_coverage.route_control_read_ready_count > 0);
        let root_chunkserver_coverage = store
            .root_service
            .chunkserver_coverage(min_capacity_free_bytes);
        let root_chunkserver_ready = root_chunkserver_coverage.active_chunkserver_count == 0
            || root_chunkserver_coverage.writable_chunkserver_count > 0;
        let root_chunkserver_control_ready = root_chunkserver_coverage.control_proof_count == 0
            || (root_chunkserver_coverage.control_read_ready_count > 0
                && root_chunkserver_coverage.control_write_ready_count > 0);
        let root_lease = store.root_leader_lease();
        let root_leader_node_id = root_lease
            .as_ref()
            .map_or_else(String::new, |lease| lease.leader_node_id.clone());
        let root_lease_epoch = root_lease.as_ref().map_or(0, |lease| lease.epoch);
        let root_lease_expire_time_ms = root_lease.as_ref().map_or(0, |lease| lease.expire_time_ms);
        let root_state_fingerprint = store.root_service.root_state_fingerprint();
        let root_raft_status = store.root_service.raft_status_from_coverage(
            &root_metadata_coverage,
            &root_page_allocator_coverage,
            &root_pageserver_service_coverage,
            &root_chunkserver_coverage,
        );
        let root_raft_status_checksum_format = root_raft_status.control_checksum_format;
        let root_raft_status_checksum = root_raft_status.control_checksum;
        let root_metadata_read_ready = root_raft_status.root_metadata_read_ready;
        let root_metadata_write_ready = root_raft_status.root_metadata_write_ready;
        let root_pageserver_read_ready = root_raft_status.root_pageserver_read_ready;
        let root_pageserver_write_ready = root_raft_status.root_pageserver_write_ready;
        let root_chunkserver_read_ready = root_raft_status.root_chunkserver_read_ready;
        let root_chunkserver_write_ready = root_raft_status.root_chunkserver_write_ready;
        let root_readiness_gate_read_ready = root_raft_status.root_readiness_gate_read_ready;
        let root_readiness_gate_write_ready = root_raft_status.root_readiness_gate_write_ready;
        let root_read_ready = root_raft_status.linearizable_reads;
        let root_write_ready = root_raft_status.writable;
        let root_leader_ready = if !rootserver_required {
            true
        } else {
            root_lease.as_ref().is_some_and(|lease| {
                lease.expire_time_ms > now_ms
                    && root_nodes.iter().any(|node| {
                        node.node_id == lease.leader_node_id
                            && node.service_kind == "rootserver"
                            && node.online
                            && !node.draining
                    })
            })
        };
        let root_observed_services_ready = root_metadata_coverage_ready
            && root_page_allocator_ready
            && root_pageserver_service_ready
            && root_pageserver_route_control_ready
            && root_chunkserver_ready
            && root_chunkserver_control_ready;
        let root_ready = if rootserver_required {
            root_leader_ready && root_placement_ready && root_observed_services_ready
        } else {
            root_observed_services_ready
        };

        Self {
            root_node_count,
            rootserver_required,
            control_plane_mode,
            root_expected_nodes,
            root_metadata_coverage,
            root_metadata_status_available,
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
            root_chunkserver_coverage,
            root_chunkserver_ready,
            root_chunkserver_control_ready,
            root_leader_node_id,
            root_lease_epoch,
            root_lease_expire_time_ms,
            root_state_fingerprint,
            root_raft_status_checksum_format,
            root_raft_status_checksum,
            root_metadata_read_ready,
            root_metadata_write_ready,
            root_pageserver_read_ready,
            root_pageserver_write_ready,
            root_chunkserver_read_ready,
            root_chunkserver_write_ready,
            root_readiness_gate_read_ready,
            root_readiness_gate_write_ready,
            root_leader_ready,
            root_read_ready,
            root_write_ready,
            root_ready,
        }
    }
}
