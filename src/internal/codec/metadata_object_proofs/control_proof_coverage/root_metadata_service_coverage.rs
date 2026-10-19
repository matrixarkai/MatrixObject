// SPDX-License-Identifier: Apache-2.0
pub(crate) fn compute_root_metadata_service_coverage(
    nodes: &[RootNode],
    metadata_quorum: u64,
) -> RootMetadataServiceCoverage {
    let mut coverage = RootMetadataServiceCoverage {
        active_metaserver_count: 0,
        metadata_value_bytes: 0,
        metadata_readable_metaserver_count: 0,
        metadata_writable_metaserver_count: 0,
        metadata_caught_up_metaserver_count: 0,
        metadata_apply_caught_up_metaserver_count: 0,
        metadata_control_apply_caught_up_metaserver_count: 0,
        metadata_read_fence_covered_metaserver_count: 0,
        metadata_quorum,
        metadata_min_last_applied_mutation_commit_index: 0,
        metadata_max_apply_commit_lag: 0,
        metadata_min_safe_read_fence: 0,
        metadata_max_read_fence_commit_lag: 0,
        metadata_max_read_fence_read_index_lag: 0,
        metadata_max_read_fence_applied_lag: 0,
        metadata_control_min_last_applied_mutation_commit_index: 0,
        metadata_control_max_last_applied_mutation_commit_index: 0,
        metadata_control_max_apply_commit_lag: 0,
        metadata_max_shard_topology_epoch: 0,
        metadata_control_proof_count: 0,
        metadata_control_read_ready_count: 0,
        metadata_control_write_ready_count: 0,
        metadata_external_coordinator_required_metaserver_count: 0,
        metadata_external_coordinator_endpoints_ready_metaserver_count: 0,
        metadata_external_coordinator_owner_sessions_ready_metaserver_count: 0,
        metadata_external_coordinator_ready_metaserver_count: 0,
        metadata_control_proof_checksum: String::new(),
        metadata_coordinator_backend_mismatch: false,
        metadata_coordinator_config_mismatch: false,
        metadata_read_quorum_available: false,
        metadata_write_quorum_available: false,
        metadata_caught_up_quorum_available: false,
        metadata_apply_quorum_available: false,
        metadata_read_fence_quorum_available: false,
        metadata_control_apply_quorum_available: false,
        metadata_shard_topology_mismatch: false,
        status: "none".to_string(),
    };
    let mut topology_checksum: Option<String> = None;
    let mut coordinator_backend: Option<String> = None;
    let mut coordinator_config_checksum: Option<String> = None;
    let mut min_last_applied_mutation_commit_index: Option<u64> = None;
    let mut min_control_last_applied_mutation_commit_index: Option<u64> = None;
    for node in nodes {
        if node.service_kind != "metaserver"
            || !node.metadata_eligible
            || !node.online
            || node.draining
        {
            continue;
        }
        coverage.active_metaserver_count = coverage.active_metaserver_count.saturating_add(1);
        coverage.metadata_value_bytes = coverage
            .metadata_value_bytes
            .saturating_add(node.metadata_value_bytes);
        let proof = &node.meta_health.control_proof;
        if !proof.state_checksum.is_empty() {
            coverage.metadata_control_proof_count =
                coverage.metadata_control_proof_count.saturating_add(1);
            if proof.read_ready {
                coverage.metadata_control_read_ready_count =
                    coverage.metadata_control_read_ready_count.saturating_add(1);
            }
            if proof.write_ready {
                coverage.metadata_control_write_ready_count = coverage
                    .metadata_control_write_ready_count
                    .saturating_add(1);
            }
            let backend = if proof.coordinator_backend.is_empty() {
                "embedded-raft"
            } else {
                proof.coordinator_backend.as_str()
            };
            if let Some(current) = &coordinator_backend {
                if current != backend {
                    coverage.metadata_coordinator_backend_mismatch = true;
                }
            } else {
                coordinator_backend = Some(backend.to_string());
            }
            if !proof.coordinator_config_checksum.is_empty() {
                if let Some(current) = &coordinator_config_checksum {
                    if current != &proof.coordinator_config_checksum {
                        coverage.metadata_coordinator_config_mismatch = true;
                    }
                } else {
                    coordinator_config_checksum = Some(proof.coordinator_config_checksum.clone());
                }
            }
        }
        if proof.external_coordinator_required {
            coverage.metadata_external_coordinator_required_metaserver_count = coverage
                .metadata_external_coordinator_required_metaserver_count
                .saturating_add(1);
        }
        if proof.external_coordinator_endpoints_ready {
            coverage.metadata_external_coordinator_endpoints_ready_metaserver_count = coverage
                .metadata_external_coordinator_endpoints_ready_metaserver_count
                .saturating_add(1);
        }
        if proof.external_coordinator_owner_sessions_ready {
            coverage.metadata_external_coordinator_owner_sessions_ready_metaserver_count = coverage
                .metadata_external_coordinator_owner_sessions_ready_metaserver_count
                .saturating_add(1);
        }
        if proof.external_coordinator_ready {
            coverage.metadata_external_coordinator_ready_metaserver_count = coverage
                .metadata_external_coordinator_ready_metaserver_count
                .saturating_add(1);
        }
        if proof.read_ready {
            coverage.metadata_readable_metaserver_count = coverage
                .metadata_readable_metaserver_count
                .saturating_add(1);
        }
        if proof.write_ready {
            coverage.metadata_writable_metaserver_count = coverage
                .metadata_writable_metaserver_count
                .saturating_add(1);
        }
        if proof.commit_bounds_valid
            && (proof.metadata_replication_quorum == 0
                || proof.metadata_replication_acks >= proof.metadata_replication_quorum)
        {
            coverage.metadata_caught_up_metaserver_count = coverage
                .metadata_caught_up_metaserver_count
                .saturating_add(1);
        }
        if proof.apply_caught_up {
            coverage.metadata_apply_caught_up_metaserver_count = coverage
                .metadata_apply_caught_up_metaserver_count
                .saturating_add(1);
            coverage.metadata_control_apply_caught_up_metaserver_count = coverage
                .metadata_control_apply_caught_up_metaserver_count
                .saturating_add(1);
        }
        if proof.read_fence_covered
            && proof.read_fence_commit_lag == 0
            && proof.read_fence_read_index_lag == 0
            && proof.read_fence_applied_lag == 0
        {
            coverage.metadata_read_fence_covered_metaserver_count = coverage
                .metadata_read_fence_covered_metaserver_count
                .saturating_add(1);
        }
        coverage.metadata_max_apply_commit_lag = coverage
            .metadata_max_apply_commit_lag
            .max(proof.apply_commit_lag);
        if proof.safe_read_fence != 0 {
            coverage.metadata_min_safe_read_fence = if coverage.metadata_min_safe_read_fence == 0 {
                proof.safe_read_fence
            } else {
                coverage
                    .metadata_min_safe_read_fence
                    .min(proof.safe_read_fence)
            };
        }
        coverage.metadata_max_read_fence_commit_lag = coverage
            .metadata_max_read_fence_commit_lag
            .max(proof.read_fence_commit_lag);
        coverage.metadata_max_read_fence_read_index_lag = coverage
            .metadata_max_read_fence_read_index_lag
            .max(proof.read_fence_read_index_lag);
        coverage.metadata_max_read_fence_applied_lag = coverage
            .metadata_max_read_fence_applied_lag
            .max(proof.read_fence_applied_lag);
        coverage.metadata_control_max_apply_commit_lag = coverage
            .metadata_control_max_apply_commit_lag
            .max(proof.apply_commit_lag);
        min_last_applied_mutation_commit_index = Some(
            min_last_applied_mutation_commit_index
                .map_or(proof.last_applied_mutation_commit_index, |current| {
                    current.min(proof.last_applied_mutation_commit_index)
                }),
        );
        min_control_last_applied_mutation_commit_index = Some(
            min_control_last_applied_mutation_commit_index
                .map_or(proof.last_applied_mutation_commit_index, |current| {
                    current.min(proof.last_applied_mutation_commit_index)
                }),
        );
        coverage.metadata_control_max_last_applied_mutation_commit_index = coverage
            .metadata_control_max_last_applied_mutation_commit_index
            .max(proof.last_applied_mutation_commit_index);
        coverage.metadata_max_shard_topology_epoch = coverage
            .metadata_max_shard_topology_epoch
            .max(node.meta_health.route_cache.topology_epoch)
            .max(proof.raft_membership_epoch);
        let checksum = if !node
            .meta_health
            .route_cache
            .shard_topology_checksum
            .is_empty()
        {
            &node.meta_health.route_cache.shard_topology_checksum
        } else {
            &proof.topology_checksum
        };
        if !checksum.is_empty() {
            if let Some(current) = &topology_checksum {
                if current != checksum {
                    coverage.metadata_shard_topology_mismatch = true;
                }
            } else {
                topology_checksum = Some(checksum.clone());
            }
        }
    }
    if coverage.metadata_control_proof_count != 0 {
        let mut proof_nodes: Vec<&RootNode> = nodes
            .iter()
            .filter(|node| {
                node.service_kind == "metaserver"
                    && node.metadata_eligible
                    && node.online
                    && !node.draining
                    && !node.meta_health.control_proof.state_checksum.is_empty()
            })
            .collect();
        proof_nodes.sort_by(|lhs, rhs| lhs.node_id.cmp(&rhs.node_id));
        let mut material = Vec::new();
        append_string(&mut material, "MXOSROOT-META-CONTROL-PROOF1");
        for node in proof_nodes {
            let proof = &node.meta_health.control_proof;
            append_string(&mut material, &node.node_id);
            append_string(&mut material, &proof.state_format);
            append_string(&mut material, &proof.state_checksum);
            append_u64(&mut material, proof.state_bytes);
            append_u64(&mut material, proof.read_ready as u64);
            append_u64(&mut material, proof.write_ready as u64);
            append_u64(&mut material, proof.commit_bounds_valid as u64);
            append_u64(&mut material, proof.apply_caught_up as u64);
            append_u64(&mut material, proof.last_applied_mutation_commit_index);
            append_u64(&mut material, proof.apply_commit_lag);
            append_u64(&mut material, proof.safe_read_fence);
            append_u64(&mut material, proof.read_fence_covered as u64);
            append_u64(&mut material, proof.read_fence_commit_lag);
            append_u64(&mut material, proof.read_fence_read_index_lag);
            append_u64(&mut material, proof.read_fence_applied_lag);
            append_string(&mut material, &proof.topology_checksum);
            append_string(&mut material, &proof.session_checksum);
            append_string(&mut material, &proof.coordinator_config_checksum);
            append_string(&mut material, &proof.coordinator_backend);
            append_u64(&mut material, proof.coordinator_config_revision);
            append_u64(&mut material, proof.external_coordinator_required as u64);
            append_u64(
                &mut material,
                proof.external_coordinator_endpoints_ready as u64,
            );
            append_u64(
                &mut material,
                proof.external_coordinator_owner_sessions_ready as u64,
            );
            append_u64(&mut material, proof.external_coordinator_ready as u64);
        }
        coverage.metadata_control_proof_checksum = etag(&material);
    }
    coverage.metadata_read_quorum_available =
        metadata_quorum > 0 && coverage.metadata_readable_metaserver_count >= metadata_quorum;
    coverage.metadata_write_quorum_available = metadata_quorum > 0
        && coverage.metadata_writable_metaserver_count >= 1
        && coverage.active_metaserver_count >= metadata_quorum;
    coverage.metadata_caught_up_quorum_available =
        metadata_quorum > 0 && coverage.metadata_caught_up_metaserver_count >= metadata_quorum;
    coverage.metadata_apply_quorum_available = metadata_quorum > 0
        && coverage.metadata_apply_caught_up_metaserver_count >= metadata_quorum;
    coverage.metadata_read_fence_quorum_available = metadata_quorum > 0
        && coverage.metadata_read_fence_covered_metaserver_count >= metadata_quorum;
    coverage.metadata_control_apply_quorum_available = metadata_quorum > 0
        && coverage.metadata_control_apply_caught_up_metaserver_count >= metadata_quorum;
    coverage.metadata_min_last_applied_mutation_commit_index =
        min_last_applied_mutation_commit_index.unwrap_or(0);
    coverage.metadata_control_min_last_applied_mutation_commit_index =
        min_control_last_applied_mutation_commit_index.unwrap_or(0);
    coverage.status = if coverage.active_metaserver_count == 0 {
        "none"
    } else if coverage.metadata_shard_topology_mismatch {
        "topology_mismatch"
    } else if coverage.metadata_coordinator_backend_mismatch {
        "coordinator_backend_mismatch"
    } else if coverage.metadata_coordinator_config_mismatch {
        "coordinator_config_mismatch"
    } else if let Some(status) = control_proof_readiness_label(
        coverage.metadata_control_proof_count,
        coverage.metadata_control_read_ready_count,
        coverage.metadata_control_write_ready_count,
        "control_unready",
        "control_read_unready",
        "control_write_unready",
    ) {
        status
    } else if let Some(status) = metadata_quorum_readiness_label(
        coverage.metadata_read_quorum_available,
        coverage.metadata_write_quorum_available,
        coverage.metadata_caught_up_quorum_available,
        coverage.metadata_apply_quorum_available,
        coverage.metadata_read_fence_quorum_available,
        coverage.metadata_control_apply_quorum_available,
        "read_quorum_unavailable",
        "write_quorum_unavailable",
        "caught_up_quorum_unavailable",
        "apply_quorum_unavailable",
        "read_fence_quorum_unavailable",
        "control_apply_quorum_unavailable",
    ) {
        status
    } else {
        "healthy"
    }
    .to_string();
    coverage
}
