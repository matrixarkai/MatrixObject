// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
pub(crate) fn append_root_meta_health(out: &mut Vec<u8>, health: &RootMetaHealth) {
    let proof = &health.control_proof;
    append_string(out, &proof.state_format);
    append_u64(out, proof.state_bytes);
    append_string(out, &proof.state_checksum);
    append_u64(out, proof.read_ready as u64);
    append_u64(out, proof.write_ready as u64);
    append_u64(out, proof.commit_bounds_valid as u64);
    append_u64(out, proof.admission_pressure as u64);
    append_u64(out, proof.qos_pressure as u64);
    append_string(out, &proof.topology_checksum);
    append_string(out, &proof.session_checksum);
    append_string(out, &proof.state_fingerprint);
    append_u64(out, proof.active_owner_sessions);
    append_u64(out, proof.expired_owner_sessions);
    append_u64(out, proof.writable_shards);
    append_u64(out, proof.draining_shards);
    append_u64(out, proof.stale_owner_shards);
    append_u64(out, proof.raft_membership_epoch);
    append_u64(out, proof.raft_membership_receipt_count);
    append_u64(out, proof.metadata_replication_quorum);
    append_u64(out, proof.metadata_replication_acks);
    append_u64(out, proof.retained_mutation_receipts);
    append_u64(out, proof.change_log_pressure as u64);
    append_u64(out, proof.mutation_receipt_pressure as u64);
    append_u64(out, proof.max_change_checkpoint_lag);
    append_u64(out, proof.remaining_open_transactions);
    append_u64(out, proof.remaining_namespace_leases);
    append_u64(out, proof.remaining_coordinator_sessions);
    append_u64(out, proof.remaining_shards);
    append_u64(out, proof.open_transaction_admission_pressure as u64);
    append_u64(out, proof.namespace_lease_admission_pressure as u64);
    append_u64(out, proof.coordinator_session_admission_pressure as u64);
    append_u64(out, proof.shard_admission_pressure as u64);
    append_string(out, &proof.coordinator_config_checksum);
    let effective_coordinator_backend = if proof.coordinator_backend.is_empty() {
        if proof.external_coordinator_required {
            "external"
        } else {
            "embedded-raft"
        }
    } else {
        &proof.coordinator_backend
    };
    append_string(out, effective_coordinator_backend);
    append_u64(out, proof.coordinator_config_revision);
    append_u64(out, proof.coordinator_config_endpoint_count);
    append_u64(out, proof.coordinator_config_receipt_count);
    append_u64(out, proof.external_coordinator_required as u64);
    append_u64(out, proof.external_coordinator_endpoints_ready as u64);
    append_u64(out, proof.external_coordinator_owner_sessions_ready as u64);
    append_u64(out, proof.external_coordinator_ready as u64);
    append_u64(out, proof.last_applied_mutation_commit_index);
    append_u64(out, proof.last_applied_mutation_time_ms);
    append_u64(out, proof.last_applied_mutation_age_ms);
    append_u64(out, proof.apply_commit_lag);
    append_u64(out, proof.apply_caught_up as u64);
    append_u64(out, proof.requested_read_fence);
    append_u64(out, proof.safe_read_fence);
    append_u64(out, proof.read_fence_covered as u64);
    append_u64(out, proof.read_fence_commit_lag);
    append_u64(out, proof.read_fence_read_index_lag);
    append_u64(out, proof.read_fence_applied_lag);

    let cache = &health.route_cache;
    append_u64(out, cache.entries);
    append_u64(out, cache.capacity);
    append_u64(out, cache.ttl_ms);
    append_u64(out, cache.hits);
    append_u64(out, cache.misses);
    append_u64(out, cache.invalidations);
    append_u64(out, cache.expired_invalidations);
    append_u64(out, cache.capacity_evictions);
    append_u64(out, cache.pressure as u64);
    append_u64(out, cache.topology_epoch);
    append_string(out, &cache.topology_checksum);
    append_string(out, &cache.shard_topology_checksum);
    append_string(out, &cache.session_checksum);
    append_u64(out, cache.fresh_entries);
    append_u64(out, cache.expired_entries);
    append_u64(out, cache.stale_topology_entries);
    append_u64(out, cache.stale_session_entries);
    append_u64(out, cache.stale_entries);
    append_u64(out, cache.fresh as u64);

    append_u64(out, health.raft_membership_receipt_retention_limit);
    append_u64(
        out,
        health.raft_membership_receipt_retention_coupled_to_mutation_receipts as u64,
    );
}

pub(crate) fn append_root_chunk_health(
    out: &mut Vec<u8>,
    health: &RootChunkHealth,
    include_control: bool,
    include_reclaim: bool,
    include_service_health: bool,
) {
    append_u64(out, health.accepting_writes as u64);
    append_u64(out, health.frozen as u64);
    append_u64(out, health.raft_writable as u64);
    append_u64(out, health.capacity_free_bytes);
    append_string(out, &health.service_mode);
    if include_reclaim {
        append_u64(out, health.discarded_range_count);
        append_u64(out, health.reclaimable_bytes);
        append_u64(out, health.trailing_trim_bytes);
        append_u64(out, health.fragmented_chunk_count);
        append_u64(out, health.fragmented_discarded_bytes);
    }
    if include_control {
        append_string(out, &health.control_proof_format);
        append_string(out, &health.control_proof_checksum);
        append_u64(out, health.control_proof_bytes);
        append_u64(out, health.raft_commit_index);
        append_u64(out, health.control_read_ready as u64);
        append_u64(out, health.control_write_ready as u64);
    }
    if include_service_health {
        append_string(out, &health.service_health);
        append_u64(out, health.service_health_reason_count);
        append_string(out, &health.service_health_reasons);
        append_u64(out, health.service_fault_tolerant as u64);
    }
}

pub(crate) fn read_root_chunk_health(
    cursor: &mut SnapshotByteCursor<'_>,
    has_control: bool,
    has_reclaim: bool,
    has_reclaim_discarded_bytes: bool,
    has_service_health: bool,
) -> Result<RootChunkHealth, ObjectError> {
    let mut health = RootChunkHealth {
        accepting_writes: cursor.read_u64()? != 0,
        frozen: cursor.read_u64()? != 0,
        raft_writable: cursor.read_u64()? != 0,
        capacity_free_bytes: cursor.read_u64()?,
        service_mode: cursor.read_string()?,
        ..RootChunkHealth::default()
    };
    if has_reclaim {
        health.discarded_range_count = cursor.read_u64()?;
        health.reclaimable_bytes = cursor.read_u64()?;
        health.trailing_trim_bytes = cursor.read_u64()?;
        health.fragmented_chunk_count = cursor.read_u64()?;
        if has_reclaim_discarded_bytes {
            health.fragmented_discarded_bytes = cursor.read_u64()?;
        }
    }
    if has_control {
        health.control_proof_format = cursor.read_string()?;
        health.control_proof_checksum = cursor.read_string()?;
        health.control_proof_bytes = cursor.read_u64()?;
        health.raft_commit_index = cursor.read_u64()?;
        health.control_read_ready = cursor.read_u64()? != 0;
        health.control_write_ready = cursor.read_u64()? != 0;
    }
    if has_service_health {
        health.service_health = cursor.read_string()?;
        health.service_health_reason_count = cursor.read_u64()?;
        health.service_health_reasons = cursor.read_string()?;
        health.service_fault_tolerant = cursor.read_u64()? != 0;
        if health.service_health.is_empty() {
            health.service_health = "unknown".to_string();
        }
    }
    Ok(health)
}

pub(crate) fn read_root_meta_health(
    cursor: &mut SnapshotByteCursor<'_>,
    has_control2_meta_health: bool,
    has_control3_meta_health: bool,
    has_control4_meta_health: bool,
    has_control5_meta_health: bool,
    has_control6_meta_health: bool,
    has_control7_meta_health: bool,
    has_control8_meta_health: bool,
    has_control9_meta_health: bool,
    has_control10_meta_health: bool,
) -> Result<RootMetaHealth, ObjectError> {
    let mut control_proof = MetadataControlProof {
        state_format: cursor.read_string()?,
        state_bytes: cursor.read_u64()?,
        state_checksum: cursor.read_string()?,
        read_ready: cursor.read_u64()? != 0,
        write_ready: cursor.read_u64()? != 0,
        commit_bounds_valid: cursor.read_u64()? != 0,
        admission_pressure: cursor.read_u64()? != 0,
        qos_pressure: cursor.read_u64()? != 0,
        topology_checksum: cursor.read_string()?,
        session_checksum: cursor.read_string()?,
        state_fingerprint: cursor.read_string()?,
        active_owner_sessions: cursor.read_u64()?,
        expired_owner_sessions: cursor.read_u64()?,
        writable_shards: cursor.read_u64()?,
        draining_shards: cursor.read_u64()?,
        stale_owner_shards: cursor.read_u64()?,
        raft_membership_epoch: cursor.read_u64()?,
        raft_membership_receipt_count: cursor.read_u64()?,
        metadata_replication_quorum: cursor.read_u64()?,
        metadata_replication_acks: cursor.read_u64()?,
        ..MetadataControlProof::default()
    };
    if has_control2_meta_health {
        control_proof.retained_mutation_receipts = cursor.read_u64()?;
        control_proof.change_log_pressure = cursor.read_u64()? != 0;
        control_proof.mutation_receipt_pressure = cursor.read_u64()? != 0;
        control_proof.max_change_checkpoint_lag = cursor.read_u64()?;
        if has_control8_meta_health {
            control_proof.remaining_open_transactions = cursor.read_u64()?;
            control_proof.remaining_namespace_leases = cursor.read_u64()?;
            control_proof.remaining_coordinator_sessions = cursor.read_u64()?;
            control_proof.remaining_shards = cursor.read_u64()?;
            control_proof.open_transaction_admission_pressure = cursor.read_u64()? != 0;
            control_proof.namespace_lease_admission_pressure = cursor.read_u64()? != 0;
            control_proof.coordinator_session_admission_pressure = cursor.read_u64()? != 0;
            control_proof.shard_admission_pressure = cursor.read_u64()? != 0;
        }
    }
    if has_control3_meta_health {
        control_proof.coordinator_config_checksum = cursor.read_string()?;
        if has_control10_meta_health {
            control_proof.coordinator_backend = cursor.read_string()?;
        }
        control_proof.coordinator_config_revision = cursor.read_u64()?;
        control_proof.coordinator_config_endpoint_count = cursor.read_u64()?;
        if has_control4_meta_health {
            control_proof.coordinator_config_receipt_count = cursor.read_u64()?;
        }
        if has_control9_meta_health {
            control_proof.external_coordinator_required = cursor.read_u64()? != 0;
            control_proof.external_coordinator_endpoints_ready = cursor.read_u64()? != 0;
            control_proof.external_coordinator_owner_sessions_ready = cursor.read_u64()? != 0;
            control_proof.external_coordinator_ready = cursor.read_u64()? != 0;
        }
        if control_proof.coordinator_backend.is_empty() {
            control_proof.coordinator_backend = if control_proof.external_coordinator_required {
                "external".to_string()
            } else {
                "embedded-raft".to_string()
            };
        }
    }
    if has_control5_meta_health {
        control_proof.last_applied_mutation_commit_index = cursor.read_u64()?;
        control_proof.last_applied_mutation_time_ms = cursor.read_u64()?;
        control_proof.last_applied_mutation_age_ms = cursor.read_u64()?;
        control_proof.apply_commit_lag = cursor.read_u64()?;
        control_proof.apply_caught_up = cursor.read_u64()? != 0;
    }
    if has_control6_meta_health {
        if has_control7_meta_health {
            control_proof.requested_read_fence = cursor.read_u64()?;
        }
        control_proof.safe_read_fence = cursor.read_u64()?;
        control_proof.read_fence_covered = cursor.read_u64()? != 0;
        control_proof.read_fence_commit_lag = cursor.read_u64()?;
        control_proof.read_fence_read_index_lag = cursor.read_u64()?;
        control_proof.read_fence_applied_lag = cursor.read_u64()?;
    } else if has_control5_meta_health {
        control_proof.requested_read_fence = control_proof.last_applied_mutation_commit_index;
        control_proof.safe_read_fence = control_proof.last_applied_mutation_commit_index;
        control_proof.read_fence_covered =
            control_proof.read_ready && control_proof.apply_caught_up;
    }
    Ok(RootMetaHealth {
        control_proof,
        route_cache: MetadataRouteCacheStatus {
            entries: cursor.read_u64()?,
            capacity: cursor.read_u64()?,
            ttl_ms: cursor.read_u64()?,
            hits: cursor.read_u64()?,
            misses: cursor.read_u64()?,
            invalidations: cursor.read_u64()?,
            expired_invalidations: cursor.read_u64()?,
            capacity_evictions: cursor.read_u64()?,
            pressure: cursor.read_u64()? != 0,
            topology_epoch: cursor.read_u64()?,
            topology_checksum: cursor.read_string()?,
            shard_topology_checksum: cursor.read_string()?,
            session_checksum: cursor.read_string()?,
            fresh_entries: cursor.read_u64()?,
            expired_entries: cursor.read_u64()?,
            stale_topology_entries: cursor.read_u64()?,
            stale_session_entries: cursor.read_u64()?,
            stale_entries: cursor.read_u64()?,
            fresh: cursor.read_u64()? != 0,
        },
        raft_membership_receipt_retention_limit: cursor.read_u64()?,
        raft_membership_receipt_retention_coupled_to_mutation_receipts: cursor.read_u64()? != 0,
    })
}

/// The bytes one node's page health takes in a root snapshot.
///
/// # WHY THIS IS A HELPER NOW
///
/// `append_root_meta_health` and `append_root_chunk_health` have always been helpers; page
/// health was thirty-eight `append_*` calls written out inside `RootSnapshot::append_to_bytes`.
/// That asymmetry is why putting roster nodes anywhere else -- a `MetaSnapshot`, a wire
/// message -- meant copying thirty-eight calls, which is thirty-eight chances to drop a field
/// where nothing would notice.
///
/// The bytes are unchanged, and that is pinned rather than asserted: see
/// `the_root_snapshot_encoding_is_byte_for_byte_what_it_was`, whose length and digest were
/// captured from the encoder BEFORE this helper existed.
///
/// NOT SHARED WITH THE CHECKSUM MATERIAL, which encodes the same fields in a different order
/// behind version flags. Reordering the material would change every stored checksum, so those
/// two encodings stay separate and are held together by a guard instead -- see
/// `every_page_health_field_the_snapshot_carries_is_covered_by_its_checksum`.
pub(crate) fn append_root_page_health(out: &mut Vec<u8>, health: &RootPageHealth) {
    append_u64(out, health.ready as u64);
    append_u64(out, health.read_ready as u64);
    append_string(out, &health.route_control_checksum);
    append_u64(out, health.route_control_commit_index);
    append_u64(out, health.chunk_endpoint_count);
    append_u64(out, health.write_quorum);
    append_u64(out, health.read_quorum);
    append_u64(out, health.writable_chunk_endpoint_count);
    append_u64(out, health.healthy_chunk_endpoint_count);
    append_u64(
        out,
        health.raft_writable_chunk_endpoint_count,
    );
    append_u64(
        out,
        health.raft_readable_chunk_endpoint_count,
    );
    append_u64(
        out,
        health
            .capacity_ready_writable_chunk_endpoint_count,
    );
    append_u64(out, health.quarantined_chunk_replicas);
    append_u64(out, health.chunk_capacity_bytes);
    append_u64(out, health.chunk_capacity_free_bytes);
    append_u64(
        out,
        health.chunk_metadata_dirty_endpoint_count,
    );
    append_u64(out, health.chunk_metadata_dirty_mutations);
    append_u64(
        out,
        health.chunk_metadata_checkpoint_interval,
    );
    append_u64(
        out,
        health
            .deferred_metadata_checkpoint_endpoint_count,
    );
    append_u64(
        out,
        health.deferred_metadata_checkpoint_requests,
    );
    append_u64(
        out,
        health.deferred_metadata_checkpoint_runs,
    );
    append_u64(out, health.chunk_capacity_rejected_writes);
    append_u64(
        out,
        health.replica_service_mode_rejected_mutations,
    );
    append_u64(out, health.chunk_physical_write_count);
    append_u64(out, health.chunk_physical_write_bytes);
    append_u64(
        out,
        health.chunk_physical_discard_range_count,
    );
    append_u64(out, health.chunk_physical_discard_bytes);
    append_u64(out, health.total_reclaimable_bytes);
    append_u64(out, health.total_trailing_trim_bytes);
    append_u64(out, health.total_fragmented_chunk_count);
    append_u64(out, health.total_fragmented_discarded_bytes);
    append_u64(out, health.allocation_receipt_count);
    append_u64(out, health.batch_allocation_receipt_count);
    append_u64(out, health.release_allocation_receipt_count);
    append_u64(out, health.write_receipt_count);
    append_u64(out, health.batch_write_receipt_count);
    append_u64(out, health.discard_receipt_count);
    append_u64(out, health.batch_discard_receipt_count);
}
