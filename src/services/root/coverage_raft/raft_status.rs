#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct RootServingGates {
    metadata_read_ready: bool,
    metadata_write_ready: bool,
    page_read_ready: bool,
    page_write_ready: bool,
    chunk_read_ready: bool,
    chunk_write_ready: bool,
    linearizable_reads: bool,
    writable: bool,
}

fn root_serving_gates(
    last_applied_index: u64,
    leader_active: bool,
    metadata: &RootMetadataServiceCoverage,
    page_allocator: &RootPageAllocatorShardCoverage,
    pageserver: &RootPageServerServiceCoverage,
    chunkserver: &RootChunkServerCoverage,
) -> RootServingGates {
    let metadata_read_ready = metadata.metadata_read_quorum_available
        && metadata.metadata_read_fence_quorum_available
        && metadata.metadata_apply_quorum_available
        && metadata.metadata_control_apply_quorum_available
        && !metadata.metadata_shard_topology_mismatch
        && !metadata.metadata_coordinator_backend_mismatch
        && !metadata.metadata_coordinator_config_mismatch;
    let metadata_write_ready = metadata_read_ready
        && metadata.metadata_write_quorum_available
        && metadata.metadata_caught_up_quorum_available;
    let page_read_ready = pageserver.ready_pageserver_count > 0
        && pageserver.read_ready_pageserver_count > 0
        && pageserver.route_control_read_ready_count > 0
        && pageserver.total_read_quorum_deficit == 0;
    let page_write_ready = page_read_ready
        && page_allocator.complete
        && pageserver.route_control_write_ready_count > 0
        && pageserver.total_write_quorum_deficit == 0;
    let chunk_read_ready = chunkserver.active_chunkserver_count > 0
        && chunkserver.control_read_ready_count > 0
        && chunkserver.health_unavailable_count == 0;
    let chunk_write_ready = chunk_read_ready
        && chunkserver.writable_chunkserver_count > 0
        && chunkserver.placement_eligible_chunkserver_count > 0
        && chunkserver.control_write_ready_count > 0;
    let linearizable_reads = leader_active
        && last_applied_index > 0
        && metadata_read_ready
        && page_read_ready
        && chunk_read_ready;
    let writable = linearizable_reads && metadata_write_ready && page_write_ready && chunk_write_ready;
    RootServingGates {
        metadata_read_ready,
        metadata_write_ready,
        page_read_ready,
        page_write_ready,
        chunk_read_ready,
        chunk_write_ready,
        linearizable_reads,
        writable,
    }
}

impl MatrixMetaService {
    pub fn raft_status(
        &self,
        metadata_replication_quorum: u64,
        min_chunk_free_bytes: u64,
    ) -> RootRaftStatus {
        let nodes: Vec<RootNode> = self.nodes.values().cloned().collect();
        let metadata = compute_root_metadata_service_coverage(&nodes, metadata_replication_quorum);
        let page_allocator = self.page_allocator_shard_coverage();
        let pageserver = self.pageserver_service_coverage();
        let chunkserver = self.chunkserver_coverage(min_chunk_free_bytes);
        self.raft_status_from_coverage(&metadata, &page_allocator, &pageserver, &chunkserver)
    }

    pub(crate) fn raft_status_from_coverage(
        &self,
        metadata: &RootMetadataServiceCoverage,
        page_allocator: &RootPageAllocatorShardCoverage,
        pageserver: &RootPageServerServiceCoverage,
        chunkserver: &RootChunkServerCoverage,
    ) -> RootRaftStatus {
        let last_applied_index = self.placement_commit_index.saturating_sub(1);
        let leader_active = self
            .leader_lease
            .as_ref()
            .and_then(|lease| self.nodes.get(&lease.leader_node_id))
            .is_some_and(|node| node.service_kind == "rootserver" && node.online && !node.draining);
        let gates = root_serving_gates(
            last_applied_index,
            leader_active,
            metadata,
            page_allocator,
            pageserver,
            chunkserver,
        );
        let mut status = RootRaftStatus {
            leader_node_id: self
                .leader_lease
                .as_ref()
                .map_or_else(String::new, |lease| lease.leader_node_id.clone()),
            term: self.leader_lease.as_ref().map_or(0, |lease| lease.epoch),
            next_log_index: self.placement_commit_index,
            last_applied_index,
            commit_index: last_applied_index,
            read_index: last_applied_index,
            writable: gates.writable,
            linearizable_reads: gates.linearizable_reads,
            root_metadata_read_ready: gates.metadata_read_ready,
            root_metadata_write_ready: gates.metadata_write_ready,
            root_pageserver_read_ready: gates.page_read_ready,
            root_pageserver_write_ready: gates.page_write_ready,
            root_chunkserver_read_ready: gates.chunk_read_ready,
            root_chunkserver_write_ready: gates.chunk_write_ready,
            root_readiness_gate_read_ready: gates.linearizable_reads,
            root_readiness_gate_write_ready: gates.writable,
            node_count: self.nodes.len() as u64,
            object_usage_count: self.placements.len() as u64,
            root_metadata_status_available: metadata.active_metaserver_count > 0,
            root_active_metaserver_count: metadata.active_metaserver_count,
            root_metadata_readable_metaserver_count: metadata.metadata_readable_metaserver_count,
            root_metadata_writable_metaserver_count: metadata.metadata_writable_metaserver_count,
            root_metadata_caught_up_metaserver_count: metadata.metadata_caught_up_metaserver_count,
            root_metadata_apply_caught_up_metaserver_count: metadata
                .metadata_apply_caught_up_metaserver_count,
            root_metadata_control_apply_caught_up_metaserver_count: metadata
                .metadata_control_apply_caught_up_metaserver_count,
            root_metadata_read_fence_covered_metaserver_count: metadata
                .metadata_read_fence_covered_metaserver_count,
            root_metadata_quorum: metadata.metadata_quorum,
            root_metadata_read_quorum_available: metadata.metadata_read_quorum_available,
            root_metadata_write_quorum_available: metadata.metadata_write_quorum_available,
            root_metadata_caught_up_quorum_available: metadata.metadata_caught_up_quorum_available,
            root_metadata_apply_quorum_available: metadata.metadata_apply_quorum_available,
            root_metadata_read_fence_quorum_available: metadata
                .metadata_read_fence_quorum_available,
            root_metadata_control_apply_quorum_available: metadata
                .metadata_control_apply_quorum_available,
            root_metadata_min_last_applied_mutation_commit_index: metadata
                .metadata_min_last_applied_mutation_commit_index,
            root_metadata_max_apply_commit_lag: metadata.metadata_max_apply_commit_lag,
            root_metadata_min_safe_read_fence: metadata.metadata_min_safe_read_fence,
            root_metadata_max_read_fence_commit_lag: metadata.metadata_max_read_fence_commit_lag,
            root_metadata_max_read_fence_read_index_lag: metadata
                .metadata_max_read_fence_read_index_lag,
            root_metadata_max_read_fence_applied_lag: metadata.metadata_max_read_fence_applied_lag,
            root_metadata_control_min_last_applied_mutation_commit_index: metadata
                .metadata_control_min_last_applied_mutation_commit_index,
            root_metadata_control_max_last_applied_mutation_commit_index: metadata
                .metadata_control_max_last_applied_mutation_commit_index,
            root_metadata_control_max_apply_commit_lag: metadata
                .metadata_control_max_apply_commit_lag,
            root_metadata_shard_topology_mismatch: metadata.metadata_shard_topology_mismatch,
            root_metadata_coordinator_backend_mismatch: metadata
                .metadata_coordinator_backend_mismatch,
            root_metadata_coordinator_config_mismatch: metadata
                .metadata_coordinator_config_mismatch,
            root_metadata_max_shard_topology_epoch: metadata.metadata_max_shard_topology_epoch,
            root_metadata_control_proof_count: metadata.metadata_control_proof_count,
            root_metadata_control_read_ready_count: metadata.metadata_control_read_ready_count,
            root_metadata_control_write_ready_count: metadata.metadata_control_write_ready_count,
            root_metadata_external_coordinator_required_metaserver_count: metadata
                .metadata_external_coordinator_required_metaserver_count,
            root_metadata_external_coordinator_endpoints_ready_metaserver_count: metadata
                .metadata_external_coordinator_endpoints_ready_metaserver_count,
            root_metadata_external_coordinator_owner_sessions_ready_metaserver_count: metadata
                .metadata_external_coordinator_owner_sessions_ready_metaserver_count,
            root_metadata_external_coordinator_ready_metaserver_count: metadata
                .metadata_external_coordinator_ready_metaserver_count,
            root_metadata_control_proof_checksum: metadata.metadata_control_proof_checksum.clone(),
            root_metadata_coverage_status: metadata.status.clone(),
            root_active_pageserver_count: page_allocator.active_pageserver_count,
            root_page_allocator_shard_count: page_allocator.shard_count,
            root_page_allocator_covered_shards: page_allocator.covered_shards,
            root_page_allocator_shards_complete: page_allocator.complete,
            root_page_allocator_shard_status: page_allocator.status.clone(),
            root_ready_pageserver_count: pageserver.ready_pageserver_count,
            root_read_ready_pageserver_count: pageserver.read_ready_pageserver_count,
            root_pageserver_route_control_proof_count: pageserver.route_control_proof_count,
            root_pageserver_route_control_read_ready_count: pageserver
                .route_control_read_ready_count,
            root_pageserver_route_control_write_ready_count: pageserver
                .route_control_write_ready_count,
            root_pageserver_route_control_proof_checksum: pageserver
                .route_control_proof_checksum
                .clone(),
            root_pageserver_route_control_min_commit_index: pageserver
                .route_control_min_commit_index,
            root_pageserver_route_control_max_commit_index: pageserver
                .route_control_max_commit_index,
            root_pageserver_total_chunk_metadata_dirty_endpoint_count: pageserver
                .total_chunk_metadata_dirty_endpoint_count,
            root_pageserver_total_chunk_metadata_dirty_mutations: pageserver
                .total_chunk_metadata_dirty_mutations,
            root_pageserver_max_chunk_metadata_checkpoint_interval: pageserver
                .max_chunk_metadata_checkpoint_interval,
            root_pageserver_total_deferred_metadata_checkpoint_endpoint_count: pageserver
                .total_deferred_metadata_checkpoint_endpoint_count,
            root_pageserver_total_deferred_metadata_checkpoint_requests: pageserver
                .total_deferred_metadata_checkpoint_requests,
            root_pageserver_total_deferred_metadata_checkpoint_runs: pageserver
                .total_deferred_metadata_checkpoint_runs,
            root_pageserver_total_chunk_capacity_rejected_writes: pageserver
                .total_chunk_capacity_rejected_writes,
            root_pageserver_total_chunk_service_mode_rejected_mutations: pageserver
                .total_chunk_service_mode_rejected_mutations,
            root_pageserver_total_chunk_physical_write_count: pageserver
                .total_chunk_physical_write_count,
            root_pageserver_total_chunk_physical_write_bytes: pageserver
                .total_chunk_physical_write_bytes,
            root_pageserver_total_chunk_physical_discard_range_count: pageserver
                .total_chunk_physical_discard_range_count,
            root_pageserver_total_chunk_physical_discard_bytes: pageserver
                .total_chunk_physical_discard_bytes,
            root_pageserver_total_reclaimable_bytes: pageserver.total_reclaimable_bytes,
            root_pageserver_total_trailing_trim_bytes: pageserver.total_trailing_trim_bytes,
            root_pageserver_total_fragmented_chunk_count: pageserver.total_fragmented_chunk_count,
            root_pageserver_total_fragmented_discarded_bytes: pageserver
                .total_fragmented_discarded_bytes,
            root_pageserver_total_allocation_receipt_count: pageserver
                .total_allocation_receipt_count,
            root_pageserver_total_batch_allocation_receipt_count: pageserver
                .total_batch_allocation_receipt_count,
            root_pageserver_total_release_allocation_receipt_count: pageserver
                .total_release_allocation_receipt_count,
            root_pageserver_total_write_receipt_count: pageserver.total_write_receipt_count,
            root_pageserver_total_batch_write_receipt_count: pageserver
                .total_batch_write_receipt_count,
            root_pageserver_total_discard_receipt_count: pageserver.total_discard_receipt_count,
            root_pageserver_total_batch_discard_receipt_count: pageserver
                .total_batch_discard_receipt_count,
            root_pageserver_health_ok_count: pageserver.health_ok_count,
            root_pageserver_health_degraded_count: pageserver.health_degraded_count,
            root_pageserver_health_unavailable_count: pageserver.health_unavailable_count,
            root_pageserver_health_unknown_count: pageserver.health_unknown_count,
            root_pageserver_not_fault_tolerant_count: pageserver.not_fault_tolerant_count,
            root_pageserver_total_health_reason_count: pageserver.total_health_reason_count,
            root_pageserver_total_write_quorum: pageserver.total_write_quorum,
            root_pageserver_total_read_quorum: pageserver.total_read_quorum,
            root_pageserver_total_write_quorum_deficit: pageserver.total_write_quorum_deficit,
            root_pageserver_total_read_quorum_deficit: pageserver.total_read_quorum_deficit,
            root_pageserver_total_write_quorum_margin: pageserver.total_write_quorum_margin,
            root_pageserver_total_read_quorum_margin: pageserver.total_read_quorum_margin,
            root_pageserver_first_health_reasons: pageserver.first_health_reasons.clone(),
            root_pageserver_service_coverage_status: pageserver.status.clone(),
            root_active_chunkserver_count: chunkserver.active_chunkserver_count,
            root_writable_chunkserver_count: chunkserver.writable_chunkserver_count,
            root_placement_eligible_chunkserver_count: chunkserver
                .placement_eligible_chunkserver_count,
            root_placement_ineligible_chunkserver_count: chunkserver
                .placement_ineligible_chunkserver_count,
            root_chunkserver_control_proof_count: chunkserver.control_proof_count,
            root_chunkserver_control_read_ready_count: chunkserver.control_read_ready_count,
            root_chunkserver_control_write_ready_count: chunkserver.control_write_ready_count,
            root_chunkserver_control_proof_checksum: chunkserver.control_proof_checksum.clone(),
            root_chunkserver_control_min_raft_commit_index: chunkserver
                .control_min_raft_commit_index,
            root_chunkserver_control_max_raft_commit_index: chunkserver
                .control_max_raft_commit_index,
            root_chunkserver_total_discarded_range_count: chunkserver.total_discarded_range_count,
            root_chunkserver_total_reclaimable_bytes: chunkserver.total_reclaimable_bytes,
            root_chunkserver_total_trailing_trim_bytes: chunkserver.total_trailing_trim_bytes,
            root_chunkserver_total_fragmented_chunk_count: chunkserver.total_fragmented_chunk_count,
            root_chunkserver_total_fragmented_discarded_bytes: chunkserver
                .total_fragmented_discarded_bytes,
            root_reclaim_pressure_chunkserver_count: chunkserver.reclaim_pressure_chunkserver_count,
            root_chunkserver_coverage_status: chunkserver.status.clone(),
            root_chunkserver_health_ok_count: chunkserver.health_ok_count,
            root_chunkserver_health_degraded_count: chunkserver.health_degraded_count,
            root_chunkserver_health_unavailable_count: chunkserver.health_unavailable_count,
            root_chunkserver_health_unknown_count: chunkserver.health_unknown_count,
            root_chunkserver_not_fault_tolerant_count: chunkserver.not_fault_tolerant_count,
            root_chunkserver_total_health_reason_count: chunkserver.total_health_reason_count,
            root_chunkserver_first_health_reasons: chunkserver.first_health_reasons.clone(),
            control_checksum_format: "MOSRSROOTRAFTSTATUS7".to_string(),
            control_checksum: String::new(),
        };
        status.control_checksum = root_raft_status_checksum(&status);
        status
    }
}
