// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    fn root_snapshot_fault_count(
        rootserver_required: bool,
        manifest_checkpoint_lag: u64,
        manifest_checkpoint_interval: u64,
        metadata_checkpoint_lag: u64,
        metadata_checkpoint_interval: u64,
    ) -> u64 {
        if !rootserver_required {
            return 0;
        }
        let manifest_overdue =
            manifest_checkpoint_interval != 0 && manifest_checkpoint_lag > manifest_checkpoint_interval;
        let metadata_overdue =
            metadata_checkpoint_interval != 0 && metadata_checkpoint_lag > metadata_checkpoint_interval;
        u64::from(manifest_overdue).saturating_add(u64::from(metadata_overdue))
    }

    pub fn distributed_readiness(
        &self,
        min_read_index: u64,
        now_ms: u64,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        expected_replica_count: usize,
        max_root_health_items: usize,
        max_root_health_payload_bytes: u64,
    ) -> Result<ObjectStoreDistributedReadiness, ObjectError> {
        let metadata = self.metadata_raft_readiness(min_read_index, now_ms);
        self.record_consensus_read_observation(&metadata);
        let page =
            self.page_storage_readiness(min_capacity_free_bytes, min_reclaimable_bytes, now_ms);
        let root_placement_health = self.audit_placement_health(
            None,
            max_root_health_items,
            expected_replica_count,
            min_capacity_free_bytes,
            true,
            max_root_health_payload_bytes,
        )?;
        let root_placement_repair = self.plan_unhealthy_placement_repairs(
            None,
            max_root_health_items,
            expected_replica_count,
            min_capacity_free_bytes,
            max_root_health_payload_bytes,
        )?;
        let placement_bytes_to_repair = root_placement_repair.bytes_to_repair;
        let placement_degraded_objects = root_placement_health.degraded_objects;
        let placement_truncated =
            root_placement_repair.has_more || root_placement_health.budget_limited;
        let storage_cache =
            DistributedStorageCacheSnapshot::capture(&page, &self.read_cache_stats());
        let read_cache_bytes = storage_cache.bytes;
        let storage_operations = DistributedStorageOperationsSnapshot::capture(&page);
        let storage_mutation = DistributedStorageMutationSnapshot::capture(&page);
        let object_footprint = DistributedObjectFootprintSnapshot::capture(self, &page);
        let storage_write_amplification_ppm = ratio_ppm(
            storage_mutation.physical_write_bytes,
            self.logical_written_bytes,
        );
        let metadata_replica_count = self.raft_voter_node_ids.len() as u64;
        let metadata_replication_enabled = metadata_replica_count > 1;
        let metadata_replication_quorum = raft_quorum(self.raft_voter_node_ids.len() as u64);
        let metadata_replication_acks = self
            .raft_healthy_peer_count
            .min(self.raft_voter_node_ids.len() as u64);
        let metadata_replication_failures =
            metadata_replica_count.saturating_sub(metadata_replication_acks);
        let metadata_replication_ready = metadata_replication_acks >= metadata_replication_quorum;
        let metadata_replication_last_status = if !metadata_replication_enabled {
            "single-node".to_string()
        } else if metadata_replication_ready {
            format!(
                "quorum available: {}/{}",
                metadata_replication_acks, metadata_replication_quorum
            )
        } else {
            format!(
                "quorum unavailable: {}/{}",
                metadata_replication_acks, metadata_replication_quorum
            )
        };
        let metadata_retention_pressure =
            metadata.change_log_pressure || metadata.mutation_receipt_pressure;
        let metadata_management_pressure =
            metadata.admission_pressure || metadata.qos_pressure || metadata_retention_pressure;
        let metadata_active_coordination_count = metadata
            .active_owner_sessions
            .saturating_add(metadata.active_namespace_leases);
        let metadata_expired_coordination_count = metadata
            .expired_owner_sessions
            .saturating_add(metadata.expired_namespace_leases);
        let root_placement_ready = root_placement_health.degraded_objects == 0;
        let DistributedPlacementReadinessSnapshot {
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
            root_replicaserver_coverage,
            root_replicaserver_ready,
            root_replicaserver_control_ready,
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
            root_replicaserver_read_ready,
            root_replicaserver_write_ready,
            root_readiness_gate_read_ready,
            root_readiness_gate_write_ready,
            root_leader_ready,
            root_read_ready,
            root_write_ready,
            root_ready,
        } = DistributedPlacementReadinessSnapshot::capture(
            self,
            metadata_replication_quorum,
            min_capacity_free_bytes,
            now_ms,
            root_placement_ready,
        );
        let metadata_state_fingerprint = metadata.metadata_state_fingerprint.clone();
        let page_state_fingerprint = page.page_state_fingerprint.clone();
        let page_state_fingerprint_format = page.page_state_fingerprint_format.clone();
        let page_state_fingerprint_bytes = page.page_state_fingerprint_bytes;
        let chunk_state_fingerprint_material =
            chunk_storage_readiness_fingerprint_material(&page.replica_readiness);
        let chunk_state_fingerprint = etag(&chunk_state_fingerprint_material);
        let chunk_state_fingerprint_format = CHUNK_STORAGE_READINESS_FINGERPRINT_FORMAT.to_string();
        let chunk_state_fingerprint_bytes = chunk_state_fingerprint_material.len() as u64;
        let DistributedMetadataReadinessSnapshot {
            metadata_route_cache,
            metadata_control_proof,
            metaserver_endpoint,
            metaserver_commit_last_status,
            metaserver_fence_shard_id,
            metaserver_fence_shard_owner_node_id,
            metaserver_fence_shard_epoch,
            metaserver_fence_lease_namespace,
            metaserver_fence_lease_owner,
            metaserver_fence_lease_epoch,
            metaserver_fence_lease_expire_time_ms,
            metaserver_fence_lease_ttl_ms,
            metaserver_route_cache_entries,
            metaserver_route_cache_hits,
            metaserver_route_cache_misses,
            metaserver_route_cache_invalidations,
            metaserver_route_cache_expired_invalidations,
            metaserver_route_cache_capacity,
            metaserver_route_cache_ttl_ms,
            metaserver_route_cache_capacity_evictions,
            metaserver_route_cache_lookup_count,
            metaserver_route_cache_hit_ratio_ppm,
            metaserver_route_cache_miss_ratio_ppm,
            metaserver_route_cache_remaining_capacity,
            metaserver_route_cache_pressure,
            metaserver_route_cache_fresh_entries,
            metaserver_route_cache_stale_entries,
            metaserver_route_cache_expired_entries,
            metaserver_route_cache_stale_topology_entries,
            metaserver_route_cache_stale_session_entries,
            metaserver_route_cache_fresh,
            metaserver_route_topology_epoch,
            metaserver_route_topology_checksum,
            metaserver_route_session_checksum,
            metadata_control_read_ready,
            metadata_control_write_ready,
            metadata_ready,
            metadata_coordinator_backend,
            metadata_coordinator_policy_format,
            metadata_coordinator_preferred_backend,
            metadata_coordinator_preferred_backend_active,
            metadata_coordinator_external_compatibility_mode,
            metadata_coordinator_production_ready,
            metadata_coordinator_external_dependency,
            metadata_coordinator_config_checksum,
            metadata_coordinator_config_revision,
            metadata_coordinator_endpoint_count,
            metadata_coordinator_config_receipt_count,
            metadata_external_coordinator_required,
            metadata_external_coordinator_endpoints_ready,
            metadata_external_coordinator_owner_sessions_ready,
            metadata_external_coordinator_ready,
        } = DistributedMetadataReadinessSnapshot::capture(
            self,
            &metadata,
            metadata_replication_quorum,
            metadata_replication_acks,
            metadata_replication_ready,
            now_ms,
        );
        let page_ready = page.ready;
        let ready = metadata_ready && page_ready && root_ready;
        let consensus_read_index = metadata.min_read_index;
        let consensus_read_refreshes = self.consensus_read_refreshes.get();
        let consensus_read_cache_hits = self.consensus_read_cache_hits.get();
        let consensus_read_last_status = self.consensus_read_last_status.borrow().clone();
        let metadata_commit_index = metadata.applied_commit_index;
        let object_manifest_checkpoint_lag = self.object_manifest_checkpoint_lag();
        let object_metadata_checkpoint_lag = self.object_metadata_checkpoint_lag();
        let object_manifest_checkpoint_interval = self.options.object_manifest_checkpoint_interval;
        let object_metadata_checkpoint_interval = self.options.object_metadata_checkpoint_interval;
        let object_deferred_manifest_checkpoints =
            object_manifest_checkpoint_lag != 0 || object_metadata_checkpoint_lag != 0;
        let object_checkpoint_pressure = object_deferred_manifest_checkpoints;
        let metadata_management_pressure = metadata_management_pressure
            || object_checkpoint_pressure
            || (rootserver_required && !root_metadata_external_coordinator_ready);
        let evaluation = DistributedReadinessEvaluation {
            ready,
            metadata_replication_ready,
            metadata_ready: metadata.ready,
            page_ready: page.ready,
            root_leader_ready,
            root_metadata_coverage: &root_metadata_coverage,
            root_metadata_external_coordinator_required,
            root_metadata_external_coordinator_endpoints_ready,
            root_metadata_external_coordinator_owner_sessions_ready,
            root_metadata_external_coordinator_ready,
            root_metadata_control_ready,
            root_metadata_coverage_ready,
            root_page_allocator_ready,
            root_pageserver_service_coverage: &root_pageserver_service_coverage,
            root_pageserver_service_ready,
            root_pageserver_route_control_ready,
            root_replicaserver_coverage: &root_replicaserver_coverage,
            root_replicaserver_ready,
            root_replicaserver_control_ready,
            rootserver_required,
            root_ready,
            root_leader_node_id: &root_leader_node_id,
            placement_degraded_objects,
        };
        let reason = evaluation.reason();
        let root_status = evaluation.root_status(&reason);
        let root_snapshot_fault_count = Self::root_snapshot_fault_count(
            rootserver_required,
            object_manifest_checkpoint_lag,
            object_manifest_checkpoint_interval,
            object_metadata_checkpoint_lag,
            object_metadata_checkpoint_interval,
        );
        let mut root_service_health_reasons = Vec::new();
        if rootserver_required && !root_leader_ready {
            root_service_health_reasons.push("leader_unavailable");
        }
        if root_snapshot_fault_count != 0 {
            root_service_health_reasons.push("snapshot_faults");
        }
        if !root_read_ready {
            root_service_health_reasons.push("linearizable_reads_unavailable");
        }
        if !root_metadata_coverage_ready {
            root_service_health_reasons.push("metadata_coverage_not_ready");
        }
        if !root_page_allocator_ready {
            root_service_health_reasons.push("block_allocator_not_ready");
        }
        if !root_pageserver_service_ready {
            root_service_health_reasons.push("blockserver_service_not_ready");
        }
        if !root_pageserver_route_control_ready {
            root_service_health_reasons.push("blockserver_route_control_not_ready");
        }
        if root_pageserver_service_coverage.not_fault_tolerant_count != 0 {
            root_service_health_reasons.push("blockserver_not_fault_tolerant");
        }
        if root_pageserver_service_coverage.health_unavailable_count != 0 {
            root_service_health_reasons.push("blockserver_health_unavailable");
        }
        if !root_replicaserver_ready {
            root_service_health_reasons.push("chunkserver_coverage_not_ready");
        }
        if !root_replicaserver_control_ready {
            root_service_health_reasons.push("chunkserver_control_not_ready");
        }
        if root_replicaserver_coverage.not_fault_tolerant_count != 0 {
            root_service_health_reasons.push("chunkserver_not_fault_tolerant");
        }
        if root_replicaserver_coverage.health_unavailable_count != 0 {
            root_service_health_reasons.push("chunkserver_health_unavailable");
        }
        let root_service_health_reason_count = root_service_health_reasons.len() as u64;
        let root_service_health_has_reasons = !root_service_health_reasons.is_empty();
        let root_service_health_reasons = root_service_health_reasons.join(";");
        let root_service_observed = rootserver_required || root_node_count != 0;
        let root_service_hard_unavailable = root_service_observed
            && (!root_read_ready
                || root_metadata_coverage.active_metaserver_count == 0
                || root_pageserver_service_coverage.active_pageserver_count == 0
                || root_replicaserver_coverage.active_replicaserver_count == 0
                || !root_metadata_read_ready
                || !root_pageserver_read_ready
                || !root_replicaserver_read_ready
                || root_pageserver_service_coverage.health_unavailable_count != 0
                || root_replicaserver_coverage.health_unavailable_count != 0);
        let root_service_health = if root_service_hard_unavailable {
            "unavailable"
        } else if root_service_health_has_reasons {
            "degraded"
        } else {
            "ok"
        }
        .to_string();
        let root_service_fault_tolerant = root_ready
            && root_read_ready
            && root_write_ready
            && root_snapshot_fault_count == 0
            && root_pageserver_service_coverage.not_fault_tolerant_count == 0
            && root_pageserver_service_coverage.health_unavailable_count == 0
            && root_replicaserver_coverage.not_fault_tolerant_count == 0
            && root_replicaserver_coverage.health_unavailable_count == 0;
        let mut report = ObjectStoreDistributedReadiness {
            ready,
            reason,
            metadata_control_proof,
            page,
            root_placement_health,
            metadata_ready,
            page_ready,
            root_ready,
            control_plane_mode,
            rootserver_required,
            root_status,
            root_leader_ready,
            root_read_ready,
            root_service_health,
            root_service_health_reason_count,
            root_service_health_reasons,
            root_service_fault_tolerant,
            root_snapshot_fault_count,
            root_leader_node_id,
            root_lease_epoch,
            root_lease_expire_time_ms,
            metadata_state_fingerprint,
            root_state_fingerprint,
            root_raft_status_checksum_format,
            root_raft_status_checksum,
            root_metadata_read_ready,
            root_metadata_write_ready,
            root_pageserver_read_ready,
            root_pageserver_write_ready,
            root_replicaserver_read_ready,
            root_replicaserver_write_ready,
            root_readiness_gate_read_ready,
            root_readiness_gate_write_ready,
            page_state_fingerprint,
            page_state_fingerprint_format,
            page_state_fingerprint_bytes,
            chunk_state_fingerprint,
            chunk_state_fingerprint_format,
            chunk_state_fingerprint_bytes,
            consensus_read_index,
            consensus_read_refreshes,
            consensus_read_cache_hits,
            consensus_read_last_status,
            metadata_commit_index,
            metadata_replication_enabled,
            metadata_replica_count,
            metadata_replication_quorum,
            metadata_replication_acks,
            metadata_replication_failures,
            metadata_replication_last_status,
            metadata_read_ready: metadata_control_read_ready,
            metadata_write_ready: metadata_control_write_ready,
            metadata_caught_up: metadata.caught_up,
            metadata_quorum_ready: metadata_replication_ready,
            metadata_management_pressure,
            metadata_retention_pressure,
            metadata_coordinator_backend,
            metadata_coordinator_policy_format,
            metadata_coordinator_preferred_backend,
            metadata_coordinator_preferred_backend_active,
            metadata_coordinator_external_compatibility_mode,
            metadata_coordinator_production_ready,
            metadata_coordinator_external_dependency,
            metadata_coordinator_config_checksum,
            metadata_coordinator_config_revision,
            metadata_coordinator_endpoint_count,
            metadata_coordinator_config_receipt_count,
            metadata_external_coordinator_required,
            metadata_external_coordinator_endpoints_ready,
            metadata_external_coordinator_owner_sessions_ready,
            metadata_external_coordinator_ready,
            metadata_active_coordination_count,
            metadata_expired_coordination_count,
            metadata_open_transaction_count: metadata.open_transactions,
            metadata_changefeed_retained_records: metadata.retained_change_records,
            metadata_change_checkpoint_lag: metadata.max_change_checkpoint_lag,
            metadata_snapshot_floor_commit_index: metadata.raft_snapshot_floor_commit_index,
            object_manifest_checkpoint_lag,
            object_metadata_checkpoint_lag,
            object_manifest_checkpoint_interval,
            object_metadata_checkpoint_interval,
            object_deferred_manifest_checkpoints,
            object_checkpoint_pressure,
            metadata,
            raft_membership_epoch: self.raft_membership_epoch,
            raft_membership_receipt_count: self.raft_membership_receipts.len() as u64,
            placement_bytes_to_repair,
            placement_degraded_objects,
            placement_truncated,
            root_expected_nodes,
            root_metadata_status_available,
            metadata_route_cache,
            metaserver_endpoint,
            metaserver_commit_last_status,
            metaserver_fence_shard_id,
            metaserver_fence_shard_owner_node_id,
            metaserver_fence_shard_epoch,
            metaserver_fence_lease_namespace,
            metaserver_fence_lease_owner,
            metaserver_fence_lease_epoch,
            metaserver_fence_lease_expire_time_ms,
            metaserver_fence_lease_ttl_ms,
            metaserver_route_cache_entries,
            metaserver_route_cache_hits,
            metaserver_route_cache_misses,
            metaserver_route_cache_invalidations,
            metaserver_route_cache_expired_invalidations,
            metaserver_route_cache_capacity,
            metaserver_route_cache_ttl_ms,
            metaserver_route_cache_capacity_evictions,
            metaserver_route_cache_lookup_count,
            metaserver_route_cache_hit_ratio_ppm,
            metaserver_route_cache_miss_ratio_ppm,
            metaserver_route_cache_remaining_capacity,
            metaserver_route_cache_pressure,
            metaserver_route_cache_fresh_entries,
            metaserver_route_cache_stale_entries,
            metaserver_route_cache_expired_entries,
            metaserver_route_cache_stale_topology_entries,
            metaserver_route_cache_stale_session_entries,
            metaserver_route_cache_fresh,
            metaserver_route_topology_epoch,
            metaserver_route_topology_checksum,
            metaserver_route_session_checksum,
            root_node_count,
            root_active_pageserver_count: root_page_allocator_coverage.active_pageserver_count,
            root_page_allocator_shard_count: root_page_allocator_coverage.shard_count,
            root_page_allocator_covered_shards: root_page_allocator_coverage.covered_shards,
            root_page_allocator_shards_complete: root_page_allocator_coverage.complete,
            root_page_allocator_shard_status: root_page_allocator_coverage.status,
            root_ready_pageserver_count: root_pageserver_service_coverage.ready_pageserver_count,
            root_read_ready_pageserver_count: root_pageserver_service_coverage
                .read_ready_pageserver_count,
            root_pageserver_total_chunk_endpoint_count: root_pageserver_service_coverage
                .total_chunk_endpoint_count,
            root_pageserver_total_writable_chunk_endpoint_count: root_pageserver_service_coverage
                .total_writable_chunk_endpoint_count,
            root_pageserver_total_healthy_chunk_endpoint_count: root_pageserver_service_coverage
                .total_healthy_chunk_endpoint_count,
            root_pageserver_total_raft_writable_chunk_endpoint_count:
                root_pageserver_service_coverage.total_raft_writable_chunk_endpoint_count,
            root_pageserver_total_raft_readable_chunk_endpoint_count:
                root_pageserver_service_coverage.total_raft_readable_chunk_endpoint_count,
            root_pageserver_total_capacity_ready_writable_chunk_endpoint_count:
                root_pageserver_service_coverage.total_capacity_ready_writable_chunk_endpoint_count,
            root_pageserver_total_quarantined_chunk_replicas: root_pageserver_service_coverage
                .total_quarantined_chunk_replicas,
            root_pageserver_total_chunk_capacity_bytes: root_pageserver_service_coverage
                .total_chunk_capacity_bytes,
            root_pageserver_total_chunk_capacity_free_bytes: root_pageserver_service_coverage
                .total_chunk_capacity_free_bytes,
            root_pageserver_total_chunk_metadata_dirty_endpoint_count:
                root_pageserver_service_coverage.total_chunk_metadata_dirty_endpoint_count,
            root_pageserver_total_chunk_metadata_dirty_mutations:
                root_pageserver_service_coverage.total_chunk_metadata_dirty_mutations,
            root_pageserver_max_chunk_metadata_checkpoint_interval:
                root_pageserver_service_coverage.max_chunk_metadata_checkpoint_interval,
            root_pageserver_total_deferred_metadata_checkpoint_endpoint_count:
                root_pageserver_service_coverage.total_deferred_metadata_checkpoint_endpoint_count,
            root_pageserver_total_deferred_metadata_checkpoint_requests:
                root_pageserver_service_coverage.total_deferred_metadata_checkpoint_requests,
            root_pageserver_total_deferred_metadata_checkpoint_runs:
                root_pageserver_service_coverage.total_deferred_metadata_checkpoint_runs,
            root_pageserver_total_chunk_capacity_rejected_writes:
                root_pageserver_service_coverage.total_chunk_capacity_rejected_writes,
            root_pageserver_total_replica_service_mode_rejected_mutations:
                root_pageserver_service_coverage.total_replica_service_mode_rejected_mutations,
            root_pageserver_total_chunk_physical_write_count: root_pageserver_service_coverage
                .total_chunk_physical_write_count,
            root_pageserver_total_chunk_physical_write_bytes: root_pageserver_service_coverage
                .total_chunk_physical_write_bytes,
            root_pageserver_total_chunk_physical_discard_range_count:
                root_pageserver_service_coverage.total_chunk_physical_discard_range_count,
            root_pageserver_total_chunk_physical_discard_bytes: root_pageserver_service_coverage
                .total_chunk_physical_discard_bytes,
            root_pageserver_total_reclaimable_bytes: root_pageserver_service_coverage
                .total_reclaimable_bytes,
            root_pageserver_total_trailing_trim_bytes: root_pageserver_service_coverage
                .total_trailing_trim_bytes,
            root_pageserver_total_fragmented_chunk_count: root_pageserver_service_coverage
                .total_fragmented_chunk_count,
            root_pageserver_total_fragmented_discarded_bytes: root_pageserver_service_coverage
                .total_fragmented_discarded_bytes,
            root_pageserver_total_allocation_receipt_count: root_pageserver_service_coverage
                .total_allocation_receipt_count,
            root_pageserver_total_batch_allocation_receipt_count:
                root_pageserver_service_coverage.total_batch_allocation_receipt_count,
            root_pageserver_total_release_allocation_receipt_count:
                root_pageserver_service_coverage.total_release_allocation_receipt_count,
            root_pageserver_total_write_receipt_count: root_pageserver_service_coverage
                .total_write_receipt_count,
            root_pageserver_total_batch_write_receipt_count: root_pageserver_service_coverage
                .total_batch_write_receipt_count,
            root_pageserver_total_discard_receipt_count: root_pageserver_service_coverage
                .total_discard_receipt_count,
            root_pageserver_total_batch_discard_receipt_count: root_pageserver_service_coverage
                .total_batch_discard_receipt_count,
            root_pageserver_health_ok_count: root_pageserver_service_coverage.health_ok_count,
            root_pageserver_health_degraded_count: root_pageserver_service_coverage
                .health_degraded_count,
            root_pageserver_health_unavailable_count: root_pageserver_service_coverage
                .health_unavailable_count,
            root_pageserver_health_unknown_count: root_pageserver_service_coverage
                .health_unknown_count,
            root_pageserver_not_fault_tolerant_count: root_pageserver_service_coverage
                .not_fault_tolerant_count,
            root_pageserver_total_health_reason_count: root_pageserver_service_coverage
                .total_health_reason_count,
            root_pageserver_total_write_quorum: root_pageserver_service_coverage
                .total_write_quorum,
            root_pageserver_total_read_quorum: root_pageserver_service_coverage
                .total_read_quorum,
            root_pageserver_total_write_quorum_deficit: root_pageserver_service_coverage
                .total_write_quorum_deficit,
            root_pageserver_total_read_quorum_deficit: root_pageserver_service_coverage
                .total_read_quorum_deficit,
            root_pageserver_total_write_quorum_margin: root_pageserver_service_coverage
                .total_write_quorum_margin,
            root_pageserver_total_read_quorum_margin: root_pageserver_service_coverage
                .total_read_quorum_margin,
            root_pageserver_first_health_reasons: root_pageserver_service_coverage
                .first_health_reasons,
            root_pageserver_route_control_proof_count: root_pageserver_service_coverage
                .route_control_proof_count,
            root_pageserver_route_control_read_ready_count: root_pageserver_service_coverage
                .route_control_read_ready_count,
            root_pageserver_route_control_write_ready_count: root_pageserver_service_coverage
                .route_control_write_ready_count,
            root_pageserver_route_control_proof_checksum: root_pageserver_service_coverage
                .route_control_proof_checksum,
            root_pageserver_route_control_min_commit_index: root_pageserver_service_coverage
                .route_control_min_commit_index,
            root_pageserver_route_control_max_commit_index: root_pageserver_service_coverage
                .route_control_max_commit_index,
            root_pageserver_service_coverage_status: root_pageserver_service_coverage.status,
            root_active_replicaserver_count: root_replicaserver_coverage.active_replicaserver_count,
            root_writable_replicaserver_count: root_replicaserver_coverage.writable_replicaserver_count,
            root_placement_eligible_replicaserver_count: root_replicaserver_coverage
                .placement_eligible_replicaserver_count,
            root_placement_ineligible_replicaserver_count: root_replicaserver_coverage
                .placement_ineligible_replicaserver_count,
            root_draining_replicaserver_count: root_replicaserver_coverage.draining_replicaserver_count,
            root_capacity_floor_blocked_replicaserver_count: root_replicaserver_coverage
                .capacity_floor_blocked_replicaserver_count,
            root_frozen_replicaserver_count: root_replicaserver_coverage.frozen_replicaserver_count,
            root_read_only_replicaserver_count: root_replicaserver_coverage.read_only_replicaserver_count,
            root_raft_unwritable_replicaserver_count: root_replicaserver_coverage
                .raft_unwritable_replicaserver_count,
            root_replicaserver_total_reported_free_bytes: root_replicaserver_coverage
                .total_reported_free_bytes,
            root_replicaserver_min_free_bytes: root_replicaserver_coverage.min_free_bytes,
            root_replicaserver_control_proof_count: root_replicaserver_coverage.control_proof_count,
            root_replicaserver_control_read_ready_count: root_replicaserver_coverage
                .control_read_ready_count,
            root_replicaserver_control_write_ready_count: root_replicaserver_coverage
                .control_write_ready_count,
            root_replicaserver_control_proof_checksum: root_replicaserver_coverage
                .control_proof_checksum,
            root_replicaserver_control_min_raft_commit_index: root_replicaserver_coverage
                .control_min_raft_commit_index,
            root_replicaserver_control_max_raft_commit_index: root_replicaserver_coverage
                .control_max_raft_commit_index,
            root_replicaserver_coverage_status: root_replicaserver_coverage.status,
            root_replicaserver_health_ok_count: root_replicaserver_coverage.health_ok_count,
            root_replicaserver_health_degraded_count: root_replicaserver_coverage.health_degraded_count,
            root_replicaserver_health_unavailable_count: root_replicaserver_coverage
                .health_unavailable_count,
            root_replicaserver_health_unknown_count: root_replicaserver_coverage.health_unknown_count,
            root_replicaserver_not_fault_tolerant_count: root_replicaserver_coverage
                .not_fault_tolerant_count,
            root_replicaserver_total_health_reason_count: root_replicaserver_coverage
                .total_health_reason_count,
            root_replicaserver_first_health_reasons: root_replicaserver_coverage.first_health_reasons,
            root_active_metaserver_count: root_metadata_coverage.active_metaserver_count,
            root_metadata_value_bytes: root_metadata_coverage.metadata_value_bytes,
            root_metadata_readable_metaserver_count: root_metadata_coverage
                .metadata_readable_metaserver_count,
            root_metadata_writable_metaserver_count: root_metadata_coverage
                .metadata_writable_metaserver_count,
            root_metadata_caught_up_metaserver_count: root_metadata_coverage
                .metadata_caught_up_metaserver_count,
            root_metadata_apply_caught_up_metaserver_count: root_metadata_coverage
                .metadata_apply_caught_up_metaserver_count,
            root_metadata_control_apply_caught_up_metaserver_count: root_metadata_coverage
                .metadata_control_apply_caught_up_metaserver_count,
            root_metadata_read_fence_covered_metaserver_count: root_metadata_coverage
                .metadata_read_fence_covered_metaserver_count,
            root_metadata_quorum: root_metadata_coverage.metadata_quorum,
            root_metadata_min_last_applied_mutation_commit_index: root_metadata_coverage
                .metadata_min_last_applied_mutation_commit_index,
            root_metadata_max_apply_commit_lag: root_metadata_coverage
                .metadata_max_apply_commit_lag,
            root_metadata_min_safe_read_fence: root_metadata_coverage.metadata_min_safe_read_fence,
            root_metadata_max_read_fence_commit_lag: root_metadata_coverage
                .metadata_max_read_fence_commit_lag,
            root_metadata_max_read_fence_read_index_lag: root_metadata_coverage
                .metadata_max_read_fence_read_index_lag,
            root_metadata_max_read_fence_applied_lag: root_metadata_coverage
                .metadata_max_read_fence_applied_lag,
            root_metadata_control_min_last_applied_mutation_commit_index: root_metadata_coverage
                .metadata_control_min_last_applied_mutation_commit_index,
            root_metadata_control_max_last_applied_mutation_commit_index: root_metadata_coverage
                .metadata_control_max_last_applied_mutation_commit_index,
            root_metadata_control_max_apply_commit_lag: root_metadata_coverage
                .metadata_control_max_apply_commit_lag,
            root_metadata_read_quorum_available: root_metadata_coverage
                .metadata_read_quorum_available,
            root_metadata_write_quorum_available: root_metadata_coverage
                .metadata_write_quorum_available,
            root_metadata_caught_up_quorum_available: root_metadata_coverage
                .metadata_caught_up_quorum_available,
            root_metadata_apply_quorum_available: root_metadata_coverage
                .metadata_apply_quorum_available,
            root_metadata_read_fence_quorum_available: root_metadata_coverage
                .metadata_read_fence_quorum_available,
            root_metadata_control_apply_quorum_available: root_metadata_coverage
                .metadata_control_apply_quorum_available,
            root_metadata_shard_topology_mismatch: root_metadata_coverage
                .metadata_shard_topology_mismatch,
            root_metadata_coordinator_backend_mismatch: root_metadata_coverage
                .metadata_coordinator_backend_mismatch,
            root_metadata_coordinator_config_mismatch: root_metadata_coverage
                .metadata_coordinator_config_mismatch,
            root_metadata_max_shard_topology_epoch: root_metadata_coverage
                .metadata_max_shard_topology_epoch,
            root_metadata_control_proof_count: root_metadata_coverage.metadata_control_proof_count,
            root_metadata_control_read_ready_count: root_metadata_coverage
                .metadata_control_read_ready_count,
            root_metadata_control_write_ready_count: root_metadata_coverage
                .metadata_control_write_ready_count,
            root_metadata_external_coordinator_required_metaserver_count: root_metadata_coverage
                .metadata_external_coordinator_required_metaserver_count,
            root_metadata_external_coordinator_endpoints_ready_metaserver_count:
                root_metadata_coverage
                    .metadata_external_coordinator_endpoints_ready_metaserver_count,
            root_metadata_external_coordinator_owner_sessions_ready_metaserver_count:
                root_metadata_coverage
                    .metadata_external_coordinator_owner_sessions_ready_metaserver_count,
            root_metadata_external_coordinator_ready_metaserver_count: root_metadata_coverage
                .metadata_external_coordinator_ready_metaserver_count,
            root_metadata_control_proof_checksum: root_metadata_coverage
                .metadata_control_proof_checksum,
            root_metadata_coverage_status: root_metadata_coverage.status,
            bucket_count: self.buckets.len() as u64,
            object_count: object_footprint.live_object_count,
            live_object_count: object_footprint.live_object_count,
            deleted_object_count: object_footprint.deleted_object_count,
            chunk_count: self.chunks.len() as u64,
            page_count: object_footprint.page_count,
            live_bytes: object_footprint.live_bytes,
            stale_bytes: object_footprint.stale_bytes,
            pending_discard_bytes: object_footprint.pending_discard_bytes,
            discarded_bytes: object_footprint.discarded_bytes,
            physical_bytes: object_footprint.physical_bytes,
            live_bytes_ppm: object_footprint.live_bytes_ppm,
            stale_bytes_ppm: object_footprint.stale_bytes_ppm,
            pending_discard_bytes_ppm: object_footprint.pending_discard_bytes_ppm,
            discarded_bytes_ppm: object_footprint.discarded_bytes_ppm,
            logical_write_amplification_ppm: object_footprint.logical_write_amplification_ppm,
            storage_write_amplification_ppm,
            space_amplification_ppm: object_footprint.space_amplification_ppm,
            discard_backlog_ppm: object_footprint.discard_backlog_ppm,
            discard_candidate_bytes: object_footprint.discard_candidate_bytes,
            discard_candidate_chunks: object_footprint.discard_candidate_chunks,
            pending_discard_range_count: object_footprint.pending_discard_range_count,
            merged_discard_range_count: object_footprint.merged_discard_range_count,
            fragmented_range_count: object_footprint.fragmented_range_count,
            compaction_candidate_chunks: object_footprint.compaction_candidate_chunks,
            compaction_candidate_bytes: object_footprint.compaction_candidate_bytes,
            compaction_candidate_range_count: object_footprint.compaction_candidate_range_count,
            compaction_recommended: object_footprint.compaction_recommended,
            read_cache_bytes,
            logical_written_bytes: self.logical_written_bytes,
            physical_written_bytes: self.physical_written_bytes,
            chunk_read_cache_max_bytes_per_replica: self.chunk_read_cache_max_bytes_per_replica,
            chunk_read_cache_max_cacheable_read_bytes_per_replica: self
                .chunk_read_cache_max_cacheable_read_bytes_per_replica,
            storage_volume_count: self.options.volume_count,
            storage_replica_count: self.options.replica_count as u64,
            storage_replica_fallback_reads: storage_operations.replica_fallback_reads,
            storage_local_replica_read_repairs: storage_operations.local_replica_read_repairs,
            storage_local_replica_read_repair_failures: storage_operations
                .local_replica_read_repair_failures,
            storage_read_cache_bytes: storage_cache.bytes,
            storage_read_cache_entries: storage_cache.entries,
            storage_read_cache_max_bytes: storage_cache.max_bytes,
            storage_read_cache_max_cacheable_read_bytes: storage_cache.max_cacheable_read_bytes,
            storage_read_cache_hits: storage_cache.hits,
            storage_read_cache_misses: storage_cache.misses,
            storage_read_cache_evictions: storage_cache.evictions,
            storage_read_cache_pressure: storage_cache.pressure,
            storage_read_cache_corrupt_pages: storage_cache.corrupt_pages,
            storage_read_cache_corrupt_bytes: storage_cache.corrupt_bytes,
            storage_read_cache_invalidations: storage_cache.invalidations,
            storage_read_cache_invalidated_entries: storage_cache.invalidated_entries,
            storage_read_cache_invalidated_bytes: storage_cache.invalidated_bytes,
            storage_read_cache_clears: storage_cache.clears,
            storage_read_cache_cleared_entries: storage_cache.cleared_entries,
            storage_read_cache_cleared_bytes: storage_cache.cleared_bytes,
            storage_read_cache_direct_fill_pages: storage_cache.direct_fill_pages,
            storage_read_cache_direct_fill_bytes: storage_cache.direct_fill_bytes,
            storage_read_cache_compressed_fill_pages: storage_cache.compressed_fill_pages,
            storage_read_cache_compressed_fill_bytes: storage_cache.compressed_fill_bytes,
            storage_read_cache_ec_fill_pages: storage_cache.ec_fill_pages,
            storage_read_cache_ec_fill_bytes: storage_cache.ec_fill_bytes,
            storage_read_cache_uncacheable_read_pages: storage_cache.uncacheable_read_pages,
            storage_read_cache_uncacheable_read_bytes: storage_cache.uncacheable_read_bytes,
            storage_capacity_rejected_writes: storage_mutation.capacity_rejected_writes,
            storage_service_mode_rejected_mutations: storage_mutation
                .service_mode_rejected_mutations,
            storage_physical_read_count: storage_operations.physical_read_count,
            storage_coalesced_read_count: storage_operations.coalesced_read_count,
            storage_physical_write_count: storage_mutation.physical_write_count,
            storage_physical_write_bytes: storage_mutation.physical_write_bytes,
            storage_physical_discard_ranges: storage_mutation.physical_discard_ranges,
            storage_physical_discard_bytes: storage_mutation.physical_discard_bytes,
            storage_allocation_receipt_count: storage_mutation.allocation_receipt_count,
            storage_batch_allocation_receipt_count: storage_mutation.batch_allocation_receipt_count,
            storage_release_allocation_receipt_count: storage_mutation
                .release_allocation_receipt_count,
            storage_write_receipt_count: storage_mutation.write_receipt_count,
            storage_discard_receipt_count: storage_mutation.discard_receipt_count,
            storage_batch_write_receipt_count: storage_mutation.batch_write_receipt_count,
            storage_batch_discard_receipt_count: storage_mutation.batch_discard_receipt_count,
            batch_write_input_ranges: 0,
            batch_write_chunk_ranges: 0,
            batch_write_failures: 0,
            last_batch_write_failure_status: "ok".to_string(),
            chunk_batch_write_requests: 0,
            chunk_batch_write_ranges: 0,
            chunk_batch_write_fallbacks: 0,
            fresh_chunk_batch_write_requests: 0,
            fresh_chunk_batch_write_ranges: 0,
            fresh_chunk_batch_write_fallbacks: 0,
            batch_read_input_ranges: 0,
            batch_read_chunk_ranges: 0,
            batch_read_failures: 0,
            last_batch_read_failure_status: "ok".to_string(),
            primary_batch_read_requests: 0,
            primary_batch_read_chunk_ranges: 0,
            primary_batch_read_fallbacks: 0,
            primary_batch_read_repair_probe_limit: 0,
            batch_discard_input_ranges: 0,
            batch_discard_merged_ranges: 0,
            batch_discard_chunk_ranges: 0,
            batch_discard_failures: 0,
            last_batch_discard_failure_status: "ok".to_string(),
            chunk_batch_discard_requests: 0,
            chunk_batch_discard_ranges: 0,
            chunk_batch_discard_fallbacks: 0,
            discard_admission_requests: 0,
            discard_admission_ranges: 0,
            chunk_reclaim_requests: 0,
            chunk_reclaim_rewritten_chunks: 0,
            chunk_reclaim_failures: 0,
            last_chunk_reclaim_failure_status: "ok".to_string(),
            storage_cumulative_repair_checked_chunks: storage_operations
                .cumulative_repair_checked_chunks,
            storage_cumulative_repair_degraded_chunks: storage_operations
                .cumulative_repair_degraded_chunks,
            storage_cumulative_repaired_replicas: storage_operations.cumulative_repaired_replicas,
            storage_cumulative_failed_repairs: storage_operations.cumulative_failed_repairs,
            storage_compressed_chunk_count: self.chunk_service.compressed_chunk_count(),
            storage_compression_saved_bytes: self.chunk_service.compression_saved_bytes(),
            storage_ec_reconstructed_reads: self.chunk_service.ec_reconstructed_reads(),
            storage_ec_reconstruction_failures: self.chunk_service.ec_reconstruction_failures(),
            storage_partial_reclaim_chunks: storage_operations.partial_reclaim_chunks,
            storage_partial_reclaim_bytes: storage_operations.partial_reclaim_bytes,
            storage_partial_reclaim_failures: storage_operations.partial_reclaim_failures,
            storage_last_partial_reclaim_failure_status: storage_operations
                .last_partial_reclaim_failure_status
                .clone(),
            storage_readable_replicas: storage_operations.readable_replicas,
            storage_writable_replicas: storage_operations.writable_replicas,
            storage_active_replicas: storage_operations.active_replicas,
            storage_quarantined_replicas: storage_operations.quarantined_replicas,
            storage_frozen_replicas: storage_operations.frozen_replicas,
            storage_draining_replicas: storage_operations.draining_replicas,
            storage_capacity_pressure_replicas: storage_operations.capacity_pressure_replicas,
            storage_trim_flow_limited_replicas: storage_operations.trim_flow_limited_replicas,
            storage_trim_flow_available_ops: storage_operations.trim_flow_available_ops,
            storage_trim_flow_throttled_ops: storage_operations.trim_flow_throttled_ops,
            storage_total_capacity_bytes: storage_operations.total_capacity_bytes,
            storage_total_capacity_free_bytes: storage_operations.total_capacity_free_bytes,
            storage_total_reclaimable_bytes: storage_operations.total_reclaimable_bytes,
            storage_total_fragmented_chunk_count: storage_operations.total_fragmented_chunk_count,
            storage_total_fragmented_discarded_bytes: storage_operations
                .total_fragmented_discarded_bytes,
            storage_total_trailing_trim_bytes: storage_operations.total_trailing_trim_bytes,
            storage_page_read_parallelism: storage_operations.page_read_parallelism,
            storage_page_write_parallelism: storage_operations.page_write_parallelism,
            storage_page_batch_read_parallelism: storage_operations.page_batch_read_parallelism,
            storage_page_batch_write_parallelism: storage_operations.page_batch_write_parallelism,
            storage_page_batch_discard_parallelism: storage_operations
                .page_batch_discard_parallelism,
            storage_page_repair_parallelism: storage_operations.page_repair_parallelism,
            storage_page_chunk_fanout_parallelism: storage_operations
                .replica_fanout_parallelism,
            storage_effective_chunk_fanout_parallelism: storage_operations
                .effective_chunk_fanout_parallelism,
            storage_local_replica_store_parallelism: storage_operations
                .local_replica_store_parallelism,
            storage_effective_local_replica_store_parallelism: storage_operations
                .effective_local_replica_store_parallelism,
            storage_reclaim_pressure: storage_operations.reclaim_pressure,
            distributed_readiness_checksum: String::new(),
            control_checksum_format: "MOSRSOBJECTREADY77".to_string(),
            control_checksum: String::new(),
        };
        report.control_checksum = objectstore_distributed_readiness_checksum(&report);
        report.distributed_readiness_checksum = report.control_checksum.clone();
        Ok(report)
    }
}
