// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixMetaService {
    pub fn pageserver_service_coverage(&self) -> PlacementPageServerServiceCoverage {
        let mut coverage = PlacementPageServerServiceCoverage {
            status: "none".to_string(),
            ..PlacementPageServerServiceCoverage::default()
        };
        for node in self.nodes.values() {
            if node.service_kind != "blockserver"
                || !node.data_eligible
                || !node.online
                || node.draining
            {
                continue;
            }
            coverage.active_pageserver_count = coverage.active_pageserver_count.saturating_add(1);
            if node.page_health.ready {
                coverage.ready_pageserver_count =
                    coverage.ready_pageserver_count.saturating_add(1);
            }
            if node.page_health.read_ready {
                coverage.read_ready_pageserver_count =
                    coverage.read_ready_pageserver_count.saturating_add(1);
            }
            coverage.total_chunk_endpoint_count = coverage
                .total_chunk_endpoint_count
                .saturating_add(node.page_health.chunk_endpoint_count);
            coverage.total_writable_chunk_endpoint_count = coverage
                .total_writable_chunk_endpoint_count
                .saturating_add(node.page_health.writable_chunk_endpoint_count);
            coverage.total_healthy_chunk_endpoint_count = coverage
                .total_healthy_chunk_endpoint_count
                .saturating_add(node.page_health.healthy_chunk_endpoint_count);
            coverage.total_raft_writable_chunk_endpoint_count = coverage
                .total_raft_writable_chunk_endpoint_count
                .saturating_add(node.page_health.raft_writable_chunk_endpoint_count);
            coverage.total_raft_readable_chunk_endpoint_count = coverage
                .total_raft_readable_chunk_endpoint_count
                .saturating_add(node.page_health.raft_readable_chunk_endpoint_count);
            coverage.total_capacity_ready_writable_chunk_endpoint_count = coverage
                .total_capacity_ready_writable_chunk_endpoint_count
                .saturating_add(
                    node.page_health
                        .capacity_ready_writable_chunk_endpoint_count,
                );
            coverage.total_quarantined_chunk_replicas = coverage
                .total_quarantined_chunk_replicas
                .saturating_add(node.page_health.quarantined_chunk_replicas);
            coverage.total_chunk_capacity_bytes = coverage
                .total_chunk_capacity_bytes
                .saturating_add(node.page_health.chunk_capacity_bytes);
            coverage.total_chunk_capacity_free_bytes = coverage
                .total_chunk_capacity_free_bytes
                .saturating_add(node.page_health.chunk_capacity_free_bytes);
            coverage.total_chunk_metadata_dirty_endpoint_count = coverage
                .total_chunk_metadata_dirty_endpoint_count
                .saturating_add(node.page_health.chunk_metadata_dirty_endpoint_count);
            coverage.total_chunk_metadata_dirty_mutations = coverage
                .total_chunk_metadata_dirty_mutations
                .saturating_add(node.page_health.chunk_metadata_dirty_mutations);
            coverage.max_chunk_metadata_checkpoint_interval = coverage
                .max_chunk_metadata_checkpoint_interval
                .max(node.page_health.chunk_metadata_checkpoint_interval);
            coverage.total_deferred_metadata_checkpoint_endpoint_count = coverage
                .total_deferred_metadata_checkpoint_endpoint_count
                .saturating_add(
                    node.page_health
                        .deferred_metadata_checkpoint_endpoint_count,
                );
            coverage.total_deferred_metadata_checkpoint_requests = coverage
                .total_deferred_metadata_checkpoint_requests
                .saturating_add(node.page_health.deferred_metadata_checkpoint_requests);
            coverage.total_deferred_metadata_checkpoint_runs = coverage
                .total_deferred_metadata_checkpoint_runs
                .saturating_add(node.page_health.deferred_metadata_checkpoint_runs);
            coverage.total_chunk_capacity_rejected_writes = coverage
                .total_chunk_capacity_rejected_writes
                .saturating_add(node.page_health.chunk_capacity_rejected_writes);
            coverage.total_replica_service_mode_rejected_mutations = coverage
                .total_replica_service_mode_rejected_mutations
                .saturating_add(node.page_health.replica_service_mode_rejected_mutations);
            coverage.total_chunk_physical_write_count = coverage
                .total_chunk_physical_write_count
                .saturating_add(node.page_health.chunk_physical_write_count);
            coverage.total_chunk_physical_write_bytes = coverage
                .total_chunk_physical_write_bytes
                .saturating_add(node.page_health.chunk_physical_write_bytes);
            coverage.total_chunk_physical_discard_range_count = coverage
                .total_chunk_physical_discard_range_count
                .saturating_add(node.page_health.chunk_physical_discard_range_count);
            coverage.total_chunk_physical_discard_bytes = coverage
                .total_chunk_physical_discard_bytes
                .saturating_add(node.page_health.chunk_physical_discard_bytes);
            coverage.total_reclaimable_bytes = coverage
                .total_reclaimable_bytes
                .saturating_add(node.page_health.total_reclaimable_bytes);
            coverage.total_trailing_trim_bytes = coverage
                .total_trailing_trim_bytes
                .saturating_add(node.page_health.total_trailing_trim_bytes);
            coverage.total_fragmented_chunk_count = coverage
                .total_fragmented_chunk_count
                .saturating_add(node.page_health.total_fragmented_chunk_count);
            coverage.total_fragmented_discarded_bytes = coverage
                .total_fragmented_discarded_bytes
                .saturating_add(node.page_health.total_fragmented_discarded_bytes);
            coverage.total_allocation_receipt_count = coverage
                .total_allocation_receipt_count
                .saturating_add(node.page_health.allocation_receipt_count);
            coverage.total_batch_allocation_receipt_count = coverage
                .total_batch_allocation_receipt_count
                .saturating_add(node.page_health.batch_allocation_receipt_count);
            coverage.total_release_allocation_receipt_count = coverage
                .total_release_allocation_receipt_count
                .saturating_add(node.page_health.release_allocation_receipt_count);
            coverage.total_write_receipt_count = coverage
                .total_write_receipt_count
                .saturating_add(node.page_health.write_receipt_count);
            coverage.total_batch_write_receipt_count = coverage
                .total_batch_write_receipt_count
                .saturating_add(node.page_health.batch_write_receipt_count);
            coverage.total_discard_receipt_count = coverage
                .total_discard_receipt_count
                .saturating_add(node.page_health.discard_receipt_count);
            coverage.total_batch_discard_receipt_count = coverage
                .total_batch_discard_receipt_count
                .saturating_add(node.page_health.batch_discard_receipt_count);
            let health = root_pageserver_health_summary(&node.page_health);
            match health.status {
                "healthy" => coverage.health_ok_count = coverage.health_ok_count.saturating_add(1),
                "degraded" => {
                    coverage.health_degraded_count =
                        coverage.health_degraded_count.saturating_add(1)
                }
                "unavailable" => {
                    coverage.health_unavailable_count =
                        coverage.health_unavailable_count.saturating_add(1)
                }
                _ => {
                    coverage.health_unknown_count =
                        coverage.health_unknown_count.saturating_add(1)
                }
            }
            if !health.fault_tolerant {
                coverage.not_fault_tolerant_count =
                    coverage.not_fault_tolerant_count.saturating_add(1);
            }
            coverage.total_health_reason_count = coverage
                .total_health_reason_count
                .saturating_add(health.reason_count);
            coverage.total_write_quorum = coverage
                .total_write_quorum
                .saturating_add(health.write_quorum);
            coverage.total_read_quorum = coverage
                .total_read_quorum
                .saturating_add(health.read_quorum);
            coverage.total_write_quorum_deficit = coverage
                .total_write_quorum_deficit
                .saturating_add(health.write_quorum_deficit);
            coverage.total_read_quorum_deficit = coverage
                .total_read_quorum_deficit
                .saturating_add(health.read_quorum_deficit);
            coverage.total_write_quorum_margin = coverage
                .total_write_quorum_margin
                .saturating_add(health.write_quorum_margin);
            coverage.total_read_quorum_margin = coverage
                .total_read_quorum_margin
                .saturating_add(health.read_quorum_margin);
            if coverage.first_health_reasons.is_empty() && !health.reasons.is_empty() {
                coverage.first_health_reasons = format!("{}:{}", node.node_id, health.reasons);
            }
            if !node.page_health.route_control_checksum.is_empty() {
                coverage.route_control_proof_count =
                    coverage.route_control_proof_count.saturating_add(1);
                if node.page_health.read_ready {
                    coverage.route_control_read_ready_count =
                        coverage.route_control_read_ready_count.saturating_add(1);
                }
                if node.page_health.ready {
                    coverage.route_control_write_ready_count =
                        coverage.route_control_write_ready_count.saturating_add(1);
                }
                if node.page_health.route_control_commit_index != 0 {
                    coverage.route_control_min_commit_index =
                        if coverage.route_control_min_commit_index == 0 {
                            node.page_health.route_control_commit_index
                        } else {
                            coverage
                                .route_control_min_commit_index
                                .min(node.page_health.route_control_commit_index)
                        };
                    coverage.route_control_max_commit_index = coverage
                        .route_control_max_commit_index
                        .max(node.page_health.route_control_commit_index);
                }
            }
        }
        if coverage.route_control_proof_count != 0 {
            let mut proof_nodes: Vec<&PlacementNode> = self
                .nodes
                .values()
                .filter(|node| {
                    node.service_kind == "blockserver"
                        && node.data_eligible
                        && node.online
                        && !node.draining
                        && !node.page_health.route_control_checksum.is_empty()
                })
                .collect();
            proof_nodes.sort_by(|lhs, rhs| lhs.node_id.cmp(&rhs.node_id));
            let proof_material_capacity = encoded_string_len("MXOSROOT-BLOCK-ROUTE-CONTROL-PROOF1")
                + proof_nodes
                    .iter()
                    .map(|node| {
                        encoded_string_len(&node.node_id)
                            + encoded_string_len(&node.page_health.route_control_checksum)
                            + std::mem::size_of::<u64>() * 27
                    })
                    .sum::<usize>();
            let mut material = Vec::with_capacity(proof_material_capacity);
            append_string(&mut material, "MXOSROOT-BLOCK-ROUTE-CONTROL-PROOF1");
            for node in proof_nodes {
                append_string(&mut material, &node.node_id);
                append_string(&mut material, &node.page_health.route_control_checksum);
                append_u64(&mut material, node.page_health.route_control_commit_index);
                append_u64(&mut material, node.page_health.ready as u64);
                append_u64(&mut material, node.page_health.read_ready as u64);
                append_u64(&mut material, node.page_health.write_quorum);
                append_u64(&mut material, node.page_health.read_quorum);
                append_u64(&mut material, node.page_health.chunk_endpoint_count);
                append_u64(
                    &mut material,
                    node.page_health.writable_chunk_endpoint_count,
                );
                append_u64(
                    &mut material,
                    node.page_health.healthy_chunk_endpoint_count,
                );
                append_u64(
                    &mut material,
                    node.page_health.raft_writable_chunk_endpoint_count,
                );
                append_u64(
                    &mut material,
                    node.page_health.raft_readable_chunk_endpoint_count,
                );
                append_u64(
                    &mut material,
                    node.page_health
                        .capacity_ready_writable_chunk_endpoint_count,
                );
                append_u64(
                    &mut material,
                    node.page_health.chunk_metadata_dirty_endpoint_count,
                );
                append_u64(
                    &mut material,
                    node.page_health.chunk_metadata_dirty_mutations,
                );
                append_u64(
                    &mut material,
                    node.page_health.chunk_metadata_checkpoint_interval,
                );
                append_u64(
                    &mut material,
                    node.page_health
                        .deferred_metadata_checkpoint_endpoint_count,
                );
                append_u64(
                    &mut material,
                    node.page_health.deferred_metadata_checkpoint_requests,
                );
                append_u64(
                    &mut material,
                    node.page_health.deferred_metadata_checkpoint_runs,
                );
                append_u64(
                    &mut material,
                    node.page_health.chunk_capacity_rejected_writes,
                );
                append_u64(
                    &mut material,
                    node.page_health.replica_service_mode_rejected_mutations,
                );
                append_u64(&mut material, node.page_health.chunk_physical_write_count);
                append_u64(&mut material, node.page_health.chunk_physical_write_bytes);
                append_u64(
                    &mut material,
                    node.page_health.chunk_physical_discard_range_count,
                );
                append_u64(
                    &mut material,
                    node.page_health.chunk_physical_discard_bytes,
                );
                append_u64(&mut material, node.page_health.total_reclaimable_bytes);
                append_u64(&mut material, node.page_health.total_trailing_trim_bytes);
                append_u64(
                    &mut material,
                    node.page_health.total_fragmented_chunk_count,
                );
                append_u64(
                    &mut material,
                    node.page_health.total_fragmented_discarded_bytes,
                );
            }
            coverage.route_control_proof_checksum = etag(&material);
        }
        coverage.status = if coverage.active_pageserver_count == 0 {
            "none"
        } else if let Some(status) = control_proof_readiness_label(
            coverage.route_control_proof_count,
            coverage.route_control_read_ready_count,
            coverage.route_control_write_ready_count,
            "route_control_unready",
            "route_control_read_unready",
            "route_control_write_unready",
        ) {
            status
        } else {
            service_read_write_coverage_status(
                coverage.active_pageserver_count,
                coverage.read_ready_pageserver_count,
                coverage.ready_pageserver_count,
            )
        }
        .to_string();
        coverage
    }
}

struct PlacementPageServerHealthSummary {
    status: &'static str,
    fault_tolerant: bool,
    reason_count: u64,
    reasons: String,
    write_quorum: u64,
    read_quorum: u64,
    write_quorum_deficit: u64,
    read_quorum_deficit: u64,
    write_quorum_margin: u64,
    read_quorum_margin: u64,
}

fn root_pageserver_health_summary(health: &NodePageHealth) -> PlacementPageServerHealthSummary {
    let required_write = health.write_quorum.max(1);
    let required_read = health.read_quorum.max(1);
    let write_available = health
        .raft_writable_chunk_endpoint_count
        .min(health.capacity_ready_writable_chunk_endpoint_count);
    let read_available = health.raft_readable_chunk_endpoint_count;
    let write_quorum_deficit = required_write.saturating_sub(write_available);
    let read_quorum_deficit = required_read.saturating_sub(read_available);
    let write_quorum_margin = write_available.saturating_sub(required_write);
    let read_quorum_margin = read_available.saturating_sub(required_read);
    let mut reasons = Vec::new();
    if !health.ready {
        reasons.push("write_not_ready");
    }
    if !health.read_ready {
        reasons.push("read_not_ready");
    }
    if health.chunk_endpoint_count == 0 {
        reasons.push("no_chunk_endpoints");
    }
    if write_quorum_deficit != 0 {
        reasons.push("write_quorum_deficit");
    }
    if read_quorum_deficit != 0 {
        reasons.push("read_quorum_deficit");
    }
    if health.quarantined_chunk_replicas != 0 {
        reasons.push("quarantined_replicas");
    }
    if health.chunk_capacity_rejected_writes != 0 {
        reasons.push("capacity_rejected_writes");
    }
    if health.replica_service_mode_rejected_mutations != 0 {
        reasons.push("service_mode_rejected_mutations");
    }
    let fault_tolerant = health.ready
        && health.read_ready
        && write_quorum_deficit == 0
        && read_quorum_deficit == 0;
    let status = if fault_tolerant && health.quarantined_chunk_replicas == 0 {
        "healthy"
    } else if !health.ready && !health.read_ready {
        "unavailable"
    } else {
        "degraded"
    };
    PlacementPageServerHealthSummary {
        status,
        fault_tolerant,
        reason_count: reasons.len() as u64,
        reasons: reasons.join(";"),
        write_quorum: required_write,
        read_quorum: required_read,
        write_quorum_deficit,
        read_quorum_deficit,
        write_quorum_margin,
        read_quorum_margin,
    }
}
