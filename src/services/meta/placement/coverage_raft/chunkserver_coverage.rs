// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixMetaService {
    pub fn replicaserver_coverage(&self, min_free_bytes: u64) -> RootReplicaServerCoverage {
        let mut coverage = RootReplicaServerCoverage {
            min_free_bytes,
            status: "none".to_string(),
            ..RootReplicaServerCoverage::default()
        };
        for node in self.nodes.values() {
            if node.service_kind != "chunkserver" || !node.data_eligible || !node.online {
                continue;
            }
            coverage.active_replicaserver_count = coverage.active_replicaserver_count.saturating_add(1);
            if !node.chunk_health.control_proof_checksum.is_empty() {
                coverage.control_proof_count = coverage.control_proof_count.saturating_add(1);
                if node.chunk_health.control_read_ready {
                    coverage.control_read_ready_count =
                        coverage.control_read_ready_count.saturating_add(1);
                }
                if node.chunk_health.control_write_ready {
                    coverage.control_write_ready_count =
                        coverage.control_write_ready_count.saturating_add(1);
                }
                if coverage.control_min_raft_commit_index == 0
                    || node.chunk_health.raft_commit_index < coverage.control_min_raft_commit_index
                {
                    coverage.control_min_raft_commit_index = node.chunk_health.raft_commit_index;
                }
                coverage.control_max_raft_commit_index = coverage
                    .control_max_raft_commit_index
                    .max(node.chunk_health.raft_commit_index);
            }
            let reported_free = if node.chunk_health.capacity_free_bytes > 0 {
                node.chunk_health.capacity_free_bytes
            } else {
                node.capacity_bytes.saturating_sub(node.committed_bytes)
            };
            coverage.total_reported_free_bytes = coverage
                .total_reported_free_bytes
                .saturating_add(reported_free);
            coverage.total_discarded_range_count = coverage
                .total_discarded_range_count
                .saturating_add(node.chunk_health.discarded_range_count);
            coverage.total_reclaimable_bytes = coverage
                .total_reclaimable_bytes
                .saturating_add(node.chunk_health.reclaimable_bytes);
            coverage.total_trailing_trim_bytes = coverage
                .total_trailing_trim_bytes
                .saturating_add(node.chunk_health.trailing_trim_bytes);
            coverage.total_fragmented_chunk_count = coverage
                .total_fragmented_chunk_count
                .saturating_add(node.chunk_health.fragmented_chunk_count);
            coverage.total_fragmented_discarded_bytes = coverage
                .total_fragmented_discarded_bytes
                .saturating_add(node.chunk_health.fragmented_discarded_bytes);
            if node.chunk_health.reclaimable_bytes > 0
                || node.chunk_health.trailing_trim_bytes > 0
                || node.chunk_health.discarded_range_count > 0
                || node.chunk_health.fragmented_chunk_count > 0
                || node.chunk_health.fragmented_discarded_bytes > 0
            {
                coverage.reclaim_pressure_replicaserver_count = coverage
                    .reclaim_pressure_replicaserver_count
                    .saturating_add(1);
            }
            match node.chunk_health.service_health.as_str() {
                "ok" => coverage.health_ok_count = coverage.health_ok_count.saturating_add(1),
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
            if !node.chunk_health.service_fault_tolerant {
                coverage.not_fault_tolerant_count =
                    coverage.not_fault_tolerant_count.saturating_add(1);
            }
            coverage.total_health_reason_count = coverage
                .total_health_reason_count
                .saturating_add(node.chunk_health.service_health_reason_count);
            if coverage.first_health_reasons.is_empty()
                && !node.chunk_health.service_health_reasons.is_empty()
            {
                coverage.first_health_reasons = format!(
                    "{}:{}",
                    node.node_id, node.chunk_health.service_health_reasons
                );
            }
            let mode_read_only = node.chunk_health.service_mode == "read_only";
            let mode_draining = node.chunk_health.service_mode == "draining";
            if node.chunk_health.frozen {
                coverage.frozen_replicaserver_count =
                    coverage.frozen_replicaserver_count.saturating_add(1);
            }
            if mode_read_only || !node.chunk_health.accepting_writes {
                coverage.read_only_replicaserver_count =
                    coverage.read_only_replicaserver_count.saturating_add(1);
            }
            if !node.chunk_health.raft_writable {
                coverage.raft_unwritable_replicaserver_count =
                    coverage.raft_unwritable_replicaserver_count.saturating_add(1);
            }
            let has_control_proof = !node.chunk_health.control_proof_checksum.is_empty();
            let control_blocked = has_control_proof
                && (!node.chunk_health.control_read_ready
                    || !node.chunk_health.control_write_ready);
            let health_blocked = node.chunk_health.frozen
                || !node.chunk_health.accepting_writes
                || !node.chunk_health.raft_writable
                || control_blocked
                || mode_read_only
                || mode_draining;
            if node.draining || health_blocked {
                coverage.draining_replicaserver_count =
                    coverage.draining_replicaserver_count.saturating_add(1);
                coverage.placement_ineligible_replicaserver_count = coverage
                    .placement_ineligible_replicaserver_count
                    .saturating_add(1);
                continue;
            }
            if reported_free < min_free_bytes {
                coverage.capacity_floor_blocked_replicaserver_count = coverage
                    .capacity_floor_blocked_replicaserver_count
                    .saturating_add(1);
                coverage.placement_ineligible_replicaserver_count = coverage
                    .placement_ineligible_replicaserver_count
                    .saturating_add(1);
                continue;
            }
            coverage.writable_replicaserver_count =
                coverage.writable_replicaserver_count.saturating_add(1);
            coverage.placement_eligible_replicaserver_count = coverage
                .placement_eligible_replicaserver_count
                .saturating_add(1);
        }
        if coverage.control_proof_count > 0 {
            let mut proof_nodes: Vec<&RootNode> = self
                .nodes
                .values()
                .filter(|node| {
                    node.service_kind == "chunkserver"
                        && node.online
                        && node.data_eligible
                        && !node.chunk_health.control_proof_checksum.is_empty()
                })
                .collect();
            proof_nodes.sort_by(|lhs, rhs| lhs.node_id.cmp(&rhs.node_id));
            let proof_material_capacity = encoded_string_len("MXOSROOT-CHUNK-CONTROL-PROOF1")
                + proof_nodes
                    .iter()
                    .map(|node| {
                        encoded_string_len(&node.node_id)
                            + encoded_string_len(&node.chunk_health.control_proof_format)
                            + encoded_string_len(&node.chunk_health.control_proof_checksum)
                            + std::mem::size_of::<u64>() * 4
                    })
                    .sum::<usize>();
            let mut material = Vec::with_capacity(proof_material_capacity);
            append_string(&mut material, "MXOSROOT-CHUNK-CONTROL-PROOF1");
            for node in proof_nodes {
                append_string(&mut material, &node.node_id);
                append_string(&mut material, &node.chunk_health.control_proof_format);
                append_string(&mut material, &node.chunk_health.control_proof_checksum);
                append_u64(&mut material, node.chunk_health.control_proof_bytes);
                append_u64(&mut material, node.chunk_health.raft_commit_index);
                append_u64(&mut material, node.chunk_health.control_read_ready as u64);
                append_u64(&mut material, node.chunk_health.control_write_ready as u64);
            }
            coverage.control_proof_checksum = etag(&material);
        }
        coverage.status = if coverage.active_replicaserver_count == 0 {
            "none"
        } else if let Some(status) = control_proof_readiness_label(
            coverage.control_proof_count,
            coverage.control_read_ready_count,
            coverage.control_write_ready_count,
            "control_unready",
            "control_read_unready",
            "control_write_unready",
        ) {
            status
        } else {
            replicaserver_coverage_status(
                coverage.active_replicaserver_count,
                coverage.writable_replicaserver_count,
                coverage.draining_replicaserver_count,
                coverage.capacity_floor_blocked_replicaserver_count,
                coverage.frozen_replicaserver_count,
                coverage.read_only_replicaserver_count,
                coverage.raft_unwritable_replicaserver_count,
            )
        }
        .to_string();
        coverage
    }
}
