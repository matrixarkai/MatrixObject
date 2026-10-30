// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl ObjectStoreDistributedReadiness {
    pub fn storage_fault_count(&self) -> u64 {
        self.batch_write_failures
            .saturating_add(self.batch_read_failures)
            .saturating_add(self.batch_discard_failures)
            .saturating_add(self.chunk_reclaim_failures)
            .saturating_add(self.storage_partial_reclaim_failures)
            .saturating_add(self.storage_local_replica_read_repair_failures)
            .saturating_add(self.storage_cumulative_failed_repairs)
            .saturating_add(self.storage_ec_reconstruction_failures)
            .saturating_add(self.storage_read_cache_corrupt_pages)
            .saturating_add(self.storage_quarantined_replicas)
    }

    pub fn storage_fault_status(&self) -> &'static str {
        if self.storage_fault_count() != 0 {
            "faulted"
        } else if self.storage_frozen_replicas != 0
            || self.storage_draining_replicas != 0
            || self.storage_capacity_pressure_replicas != 0
            || self.storage_trim_flow_limited_replicas != 0
            || self.storage_capacity_rejected_writes != 0
            || self.storage_service_mode_rejected_mutations != 0
            || self.storage_reclaim_pressure
            || self.storage_read_cache_pressure
        {
            "degraded"
        } else {
            "ok"
        }
    }

    pub fn storage_fault_reason(&self) -> String {
        let mut reasons = Vec::new();
        add_fault_count(&mut reasons, "batch_write_failures", self.batch_write_failures);
        add_fault_status(
            &mut reasons,
            "batch_write",
            &self.last_batch_write_failure_status,
        );
        add_fault_count(&mut reasons, "batch_read_failures", self.batch_read_failures);
        add_fault_status(
            &mut reasons,
            "batch_read",
            &self.last_batch_read_failure_status,
        );
        add_fault_count(
            &mut reasons,
            "batch_discard_failures",
            self.batch_discard_failures,
        );
        add_fault_status(
            &mut reasons,
            "batch_discard",
            &self.last_batch_discard_failure_status,
        );
        add_fault_count(&mut reasons, "chunk_reclaim_failures", self.chunk_reclaim_failures);
        add_fault_status(
            &mut reasons,
            "chunk_reclaim",
            &self.last_chunk_reclaim_failure_status,
        );
        add_fault_count(
            &mut reasons,
            "partial_reclaim_failures",
            self.storage_partial_reclaim_failures,
        );
        add_fault_status(
            &mut reasons,
            "partial_reclaim",
            &self.storage_last_partial_reclaim_failure_status,
        );
        add_fault_count(
            &mut reasons,
            "local_replica_read_repair_failures",
            self.storage_local_replica_read_repair_failures,
        );
        add_fault_status(
            &mut reasons,
            "local_replica_read_repair",
            fault_status(self.storage_local_replica_read_repair_failures),
        );
        add_fault_count(
            &mut reasons,
            "cumulative_failed_repairs",
            self.storage_cumulative_failed_repairs,
        );
        add_fault_count(
            &mut reasons,
            "ec_reconstruction_failures",
            self.storage_ec_reconstruction_failures,
        );
        add_fault_status(
            &mut reasons,
            "ec",
            fault_status(self.storage_ec_reconstruction_failures),
        );
        add_fault_count(
            &mut reasons,
            "read_cache_corrupt_pages",
            self.storage_read_cache_corrupt_pages,
        );
        add_fault_count(
            &mut reasons,
            "quarantined_replicas",
            self.storage_quarantined_replicas,
        );
        add_fault_count(&mut reasons, "frozen_replicas", self.storage_frozen_replicas);
        add_fault_count(
            &mut reasons,
            "draining_replicas",
            self.storage_draining_replicas,
        );
        add_fault_count(
            &mut reasons,
            "capacity_pressure_replicas",
            self.storage_capacity_pressure_replicas,
        );
        add_fault_count(
            &mut reasons,
            "trim_flow_limited_replicas",
            self.storage_trim_flow_limited_replicas,
        );
        add_fault_count(
            &mut reasons,
            "capacity_rejected_writes",
            self.storage_capacity_rejected_writes,
        );
        add_fault_status(
            &mut reasons,
            "capacity",
            fault_status(self.storage_capacity_rejected_writes),
        );
        add_fault_count(
            &mut reasons,
            "service_mode_rejected_mutations",
            self.storage_service_mode_rejected_mutations,
        );
        add_fault_status(
            &mut reasons,
            "mutation_admission",
            fault_status(self.storage_service_mode_rejected_mutations),
        );
        if self.storage_read_cache_pressure {
            reasons.push("read_cache_pressure".to_string());
        }
        if reasons.is_empty() {
            "ok".to_string()
        } else {
            reasons.join(";")
        }
    }

    pub fn storage_fault_reason_count(&self) -> u64 {
        let reason = self.storage_fault_reason();
        if reason.is_empty() || reason == "ok" {
            0
        } else {
            reason.split(';').filter(|entry| !entry.is_empty()).count() as u64
        }
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("reason", &self.reason),
            ("control_plane_mode", &self.control_plane_mode),
            ("root_status", &self.root_status),
            ("root_service_health", &self.root_service_health),
            (
                "placement_service_health_reasons",
                &self.placement_service_health_reasons,
            ),
            (
                "last_batch_write_failure_status",
                &self.last_batch_write_failure_status,
            ),
            (
                "last_batch_read_failure_status",
                &self.last_batch_read_failure_status,
            ),
            (
                "last_batch_discard_failure_status",
                &self.last_batch_discard_failure_status,
            ),
            (
                "last_chunk_reclaim_failure_status",
                &self.last_chunk_reclaim_failure_status,
            ),
            ("root_leader_node_id", &self.root_leader_node_id),
            (
                "metadata_state_fingerprint",
                &self.metadata_state_fingerprint,
            ),
            ("placement_state_fingerprint", &self.placement_state_fingerprint),
            (
                "placement_raft_status_checksum_format",
                &self.placement_raft_status_checksum_format,
            ),
            ("root_raft_status_checksum", &self.root_raft_status_checksum),
            ("page_state_fingerprint", &self.page_state_fingerprint),
            (
                "page_state_fingerprint_format",
                &self.page_state_fingerprint_format,
            ),
            ("chunk_state_fingerprint", &self.chunk_state_fingerprint),
            (
                "chunk_state_fingerprint_format",
                &self.chunk_state_fingerprint_format,
            ),
            (
                "metadata_coordinator_backend",
                &self.metadata_coordinator_backend,
            ),
            (
                "metadata_coordinator_policy_format",
                &self.metadata_coordinator_policy_format,
            ),
            (
                "metadata_coordinator_preferred_backend",
                &self.metadata_coordinator_preferred_backend,
            ),
            (
                "metadata_coordinator_external_dependency",
                &self.metadata_coordinator_external_dependency,
            ),
            (
                "metadata_coordinator_config_checksum",
                &self.metadata_coordinator_config_checksum,
            ),
            (
                "placement_metadata_control_proof_checksum",
                &self.placement_metadata_control_proof_checksum,
            ),
            (
                "placement_metadata_coverage_status",
                &self.placement_metadata_coverage_status,
            ),
            (
                "placement_pageserver_route_control_proof_checksum",
                &self.root_pageserver_route_control_proof_checksum,
            ),
            (
                "root_pageserver_first_health_reasons",
                &self.root_pageserver_first_health_reasons,
            ),
            (
                "placement_chunkserver_control_proof_checksum",
                &self.root_chunkserver_control_proof_checksum,
            ),
            (
                "root_chunkserver_first_health_reasons",
                &self.root_chunkserver_first_health_reasons,
            ),
            (
                "distributed_readiness_checksum",
                &self.distributed_readiness_checksum,
            ),
            ("control_checksum_format", &self.control_checksum_format),
            ("control_checksum", &self.control_checksum),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.clone());
        }
        for (name, value) in [
            ("ready", self.ready),
            ("metadata_ready", self.metadata_ready),
            ("page_ready", self.page_ready),
            ("placement_ready", self.placement_ready),
            ("placement_read_ready", self.placement_read_ready),
            (
                "placement_metadata_read_ready",
                self.placement_metadata_read_ready,
            ),
            (
                "placement_metadata_write_ready",
                self.placement_metadata_write_ready,
            ),
            (
                "root_pageserver_read_ready",
                self.root_pageserver_read_ready,
            ),
            (
                "root_pageserver_write_ready",
                self.root_pageserver_write_ready,
            ),
            (
                "root_chunkserver_read_ready",
                self.root_chunkserver_read_ready,
            ),
            (
                "root_chunkserver_write_ready",
                self.root_chunkserver_write_ready,
            ),
            (
                "placement_readiness_gate_read_ready",
                self.placement_readiness_gate_read_ready,
            ),
            (
                "placement_readiness_gate_write_ready",
                self.placement_readiness_gate_write_ready,
            ),
            (
                "placement_service_fault_tolerant",
                self.placement_service_fault_tolerant,
            ),
            ("placement_required", self.placement_required),
            ("placement_leader_ready", self.placement_leader_ready),
            (
                "metadata_replication_enabled",
                self.metadata_replication_enabled,
            ),
            ("metadata_read_ready", self.metadata_read_ready),
            ("metadata_write_ready", self.metadata_write_ready),
            ("metadata_caught_up", self.metadata_caught_up),
            ("metadata_quorum_ready", self.metadata_quorum_ready),
            (
                "metadata_management_pressure",
                self.metadata_management_pressure,
            ),
            (
                "metadata_retention_pressure",
                self.metadata_retention_pressure,
            ),
            (
                "metadata_coordinator_preferred_backend_active",
                self.metadata_coordinator_preferred_backend_active,
            ),
            (
                "metadata_coordinator_external_compatibility_mode",
                self.metadata_coordinator_external_compatibility_mode,
            ),
            (
                "metadata_coordinator_production_ready",
                self.metadata_coordinator_production_ready,
            ),
            (
                "metadata_external_coordinator_required",
                self.metadata_external_coordinator_required,
            ),
            (
                "metadata_external_coordinator_endpoints_ready",
                self.metadata_external_coordinator_endpoints_ready,
            ),
            (
                "metadata_external_coordinator_owner_sessions_ready",
                self.metadata_external_coordinator_owner_sessions_ready,
            ),
            (
                "metadata_external_coordinator_ready",
                self.metadata_external_coordinator_ready,
            ),
            (
                "placement_metadata_status_available",
                self.placement_metadata_status_available,
            ),
            (
                "placement_metadata_read_quorum_available",
                self.placement_metadata_read_quorum_available,
            ),
            (
                "placement_metadata_write_quorum_available",
                self.placement_metadata_write_quorum_available,
            ),
            (
                "placement_metadata_apply_quorum_available",
                self.placement_metadata_apply_quorum_available,
            ),
            (
                "placement_metadata_read_fence_quorum_available",
                self.placement_metadata_read_fence_quorum_available,
            ),
            (
                "placement_metadata_coordinator_backend_mismatch",
                self.placement_metadata_coordinator_backend_mismatch,
            ),
            (
                "placement_metadata_coordinator_config_mismatch",
                self.placement_metadata_coordinator_config_mismatch,
            ),
            (
                "placement_page_allocator_shards_complete",
                self.root_page_allocator_shards_complete,
            ),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        for (name, value) in [
            ("consensus_read_index", self.consensus_read_index),
            ("metadata_commit_index", self.metadata_commit_index),
            (
                "capacity_free_bytes",
                self.storage_total_capacity_free_bytes,
            ),
            (
                "fragmented_chunk_count",
                self.storage_total_fragmented_chunk_count,
            ),
            (
                "fragmented_discarded_bytes",
                self.storage_total_fragmented_discarded_bytes,
            ),
            (
                "capacity_pressure_replicas",
                self.storage_capacity_pressure_replicas,
            ),
            ("read_cache_entries", self.storage_read_cache_entries),
            (
                "metadata_replication_quorum",
                self.metadata_replication_quorum,
            ),
            ("metadata_replication_acks", self.metadata_replication_acks),
            (
                "metadata_coordinator_config_revision",
                self.metadata_coordinator_config_revision,
            ),
            (
                "metadata_coordinator_endpoint_count",
                self.metadata_coordinator_endpoint_count,
            ),
            (
                "metadata_coordinator_config_receipt_count",
                self.metadata_coordinator_config_receipt_count,
            ),
            (
                "metadata_active_coordination_count",
                self.metadata_active_coordination_count,
            ),
            (
                "metadata_expired_coordination_count",
                self.metadata_expired_coordination_count,
            ),
            (
                "metadata_open_transaction_count",
                self.metadata_open_transaction_count,
            ),
            ("placement_expected_nodes", self.placement_expected_nodes),
            (
                "placement_service_health_reason_count",
                self.placement_service_health_reason_count,
            ),
            ("placement_snapshot_fault_count", self.placement_snapshot_fault_count),
            (
                "placement_active_metaserver_count",
                self.placement_active_metaserver_count,
            ),
            ("placement_metadata_quorum", self.placement_metadata_quorum),
            (
                "placement_metadata_control_proof_count",
                self.placement_metadata_control_proof_count,
            ),
            (
                "placement_metadata_control_read_ready_count",
                self.placement_metadata_control_read_ready_count,
            ),
            (
                "placement_metadata_control_write_ready_count",
                self.placement_metadata_control_write_ready_count,
            ),
            (
                "placement_metadata_external_coordinator_required_metaserver_count",
                self.placement_metadata_external_coordinator_required_metaserver_count,
            ),
            (
                "placement_metadata_external_coordinator_endpoints_ready_metaserver_count",
                self.placement_metadata_external_coordinator_endpoints_ready_metaserver_count,
            ),
            (
                "placement_metadata_external_coordinator_owner_sessions_ready_metaserver_count",
                self.placement_metadata_external_coordinator_owner_sessions_ready_metaserver_count,
            ),
            (
                "placement_metadata_external_coordinator_ready_metaserver_count",
                self.placement_metadata_external_coordinator_ready_metaserver_count,
            ),
            (
                "placement_pageserver_route_control_proof_count",
                self.root_pageserver_route_control_proof_count,
            ),
            (
                "placement_pageserver_route_control_read_ready_count",
                self.root_pageserver_route_control_read_ready_count,
            ),
            (
                "placement_pageserver_route_control_write_ready_count",
                self.root_pageserver_route_control_write_ready_count,
            ),
            (
                "root_pageserver_health_ok_count",
                self.root_pageserver_health_ok_count,
            ),
            (
                "root_pageserver_health_degraded_count",
                self.root_pageserver_health_degraded_count,
            ),
            (
                "root_pageserver_health_unavailable_count",
                self.root_pageserver_health_unavailable_count,
            ),
            (
                "root_pageserver_health_unknown_count",
                self.root_pageserver_health_unknown_count,
            ),
            (
                "root_pageserver_not_fault_tolerant_count",
                self.root_pageserver_not_fault_tolerant_count,
            ),
            (
                "root_pageserver_total_health_reason_count",
                self.root_pageserver_total_health_reason_count,
            ),
            (
                "root_pageserver_total_write_quorum",
                self.root_pageserver_total_write_quorum,
            ),
            (
                "root_pageserver_total_read_quorum",
                self.root_pageserver_total_read_quorum,
            ),
            (
                "root_pageserver_total_write_quorum_deficit",
                self.root_pageserver_total_write_quorum_deficit,
            ),
            (
                "root_pageserver_total_read_quorum_deficit",
                self.root_pageserver_total_read_quorum_deficit,
            ),
            (
                "root_pageserver_total_write_quorum_margin",
                self.root_pageserver_total_write_quorum_margin,
            ),
            (
                "root_pageserver_total_read_quorum_margin",
                self.root_pageserver_total_read_quorum_margin,
            ),
            (
                "placement_chunkserver_control_proof_count",
                self.root_chunkserver_control_proof_count,
            ),
            (
                "placement_chunkserver_control_read_ready_count",
                self.root_chunkserver_control_read_ready_count,
            ),
            (
                "placement_chunkserver_control_write_ready_count",
                self.root_chunkserver_control_write_ready_count,
            ),
            (
                "root_chunkserver_health_ok_count",
                self.root_chunkserver_health_ok_count,
            ),
            (
                "root_chunkserver_health_degraded_count",
                self.root_chunkserver_health_degraded_count,
            ),
            (
                "root_chunkserver_health_unavailable_count",
                self.root_chunkserver_health_unavailable_count,
            ),
            (
                "root_chunkserver_health_unknown_count",
                self.root_chunkserver_health_unknown_count,
            ),
            (
                "root_chunkserver_not_fault_tolerant_count",
                self.root_chunkserver_not_fault_tolerant_count,
            ),
            (
                "root_chunkserver_total_health_reason_count",
                self.root_chunkserver_total_health_reason_count,
            ),
            ("batch_write_input_ranges", self.batch_write_input_ranges),
            ("batch_write_chunk_ranges", self.batch_write_chunk_ranges),
            ("batch_write_failures", self.batch_write_failures),
            (
                "chunk_batch_write_requests",
                self.chunk_batch_write_requests,
            ),
            ("chunk_batch_write_ranges", self.chunk_batch_write_ranges),
            (
                "chunk_batch_write_fallbacks",
                self.chunk_batch_write_fallbacks,
            ),
            (
                "fresh_chunk_batch_write_requests",
                self.fresh_chunk_batch_write_requests,
            ),
            (
                "fresh_chunk_batch_write_ranges",
                self.fresh_chunk_batch_write_ranges,
            ),
            (
                "fresh_chunk_batch_write_fallbacks",
                self.fresh_chunk_batch_write_fallbacks,
            ),
            ("batch_read_input_ranges", self.batch_read_input_ranges),
            ("batch_read_chunk_ranges", self.batch_read_chunk_ranges),
            ("batch_read_failures", self.batch_read_failures),
            (
                "primary_batch_read_requests",
                self.primary_batch_read_requests,
            ),
            (
                "primary_batch_read_chunk_ranges",
                self.primary_batch_read_chunk_ranges,
            ),
            (
                "primary_batch_read_fallbacks",
                self.primary_batch_read_fallbacks,
            ),
            (
                "primary_batch_read_repair_probe_limit",
                self.primary_batch_read_repair_probe_limit,
            ),
            (
                "batch_discard_input_ranges",
                self.batch_discard_input_ranges,
            ),
            (
                "batch_discard_merged_ranges",
                self.batch_discard_merged_ranges,
            ),
            (
                "batch_discard_chunk_ranges",
                self.batch_discard_chunk_ranges,
            ),
            ("batch_discard_failures", self.batch_discard_failures),
            (
                "chunk_batch_discard_requests",
                self.chunk_batch_discard_requests,
            ),
            (
                "chunk_batch_discard_ranges",
                self.chunk_batch_discard_ranges,
            ),
            (
                "chunk_batch_discard_fallbacks",
                self.chunk_batch_discard_fallbacks,
            ),
            (
                "discard_admission_requests",
                self.discard_admission_requests,
            ),
            ("discard_admission_ranges", self.discard_admission_ranges),
            ("chunk_reclaim_requests", self.chunk_reclaim_requests),
            (
                "chunk_reclaim_rewritten_chunks",
                self.chunk_reclaim_rewritten_chunks,
            ),
            ("chunk_reclaim_failures", self.chunk_reclaim_failures),
            (
                "storage_partial_reclaim_chunks",
                self.storage_partial_reclaim_chunks,
            ),
            (
                "storage_partial_reclaim_bytes",
                self.storage_partial_reclaim_bytes,
            ),
            (
                "storage_partial_reclaim_failures",
                self.storage_partial_reclaim_failures,
            ),
            (
                "storage_local_replica_read_repairs",
                self.storage_local_replica_read_repairs,
            ),
            (
                "storage_local_replica_read_repair_failures",
                self.storage_local_replica_read_repair_failures,
            ),
            (
                "storage_ec_reconstructed_reads",
                self.storage_ec_reconstructed_reads,
            ),
            (
                "storage_ec_reconstruction_failures",
                self.storage_ec_reconstruction_failures,
            ),
            ("storage_fault_count", self.storage_fault_count()),
            ("storage_fault_reason_count", self.storage_fault_reason_count()),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(
            format!("{field_prefix}storage_fault_reason"),
            self.storage_fault_reason(),
        );
        fields.insert(
            format!("{field_prefix}last_chunk_reclaim_failure_status"),
            self.last_chunk_reclaim_failure_status.clone(),
        );
        fields.insert(
            format!("{field_prefix}storage_last_partial_reclaim_failure_status"),
            self.storage_last_partial_reclaim_failure_status.clone(),
        );
        fields.insert(
            format!("{field_prefix}storage_last_local_replica_read_repair_failure_status"),
            if self.storage_local_replica_read_repair_failures == 0 {
                "ok".to_string()
            } else {
                "failure".to_string()
            },
        );
        fields.insert(
            format!("{field_prefix}storage_last_ec_reconstruction_failure_status"),
            if self.storage_ec_reconstruction_failures == 0 {
                "ok".to_string()
            } else {
                "failure".to_string()
            },
        );
        fields.insert(
            format!("{field_prefix}storage_fault_status"),
            self.storage_fault_status().to_string(),
        );
        fields.insert(
            format!("{field_prefix}read_cache_pressure"),
            self.storage_read_cache_pressure.to_string(),
        );
        fields
    }
}

fn add_fault_count(reasons: &mut Vec<String>, name: &str, value: u64) {
    if value != 0 {
        reasons.push(format!("{name}:{value}"));
    }
}

fn add_fault_status(reasons: &mut Vec<String>, name: &str, value: &str) {
    if !value.is_empty() && value != "ok" {
        reasons.push(format!("{name}:{}", normalize_fault_reason_detail(value)));
    }
}

fn fault_status(failures: u64) -> &'static str {
    if failures == 0 {
        "ok"
    } else {
        "failure"
    }
}

fn normalize_fault_reason_detail(detail: &str) -> String {
    detail
        .chars()
        .map(|ch| match ch {
            ';' | '\n' | '\r' | '\t' => '|',
            _ => ch,
        })
        .collect()
}

