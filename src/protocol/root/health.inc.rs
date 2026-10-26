// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeMetaHealth {
    pub control_proof: MetadataControlProof,
    pub route_cache: MetadataRouteCacheStatus,
    pub raft_membership_receipt_retention_limit: u64,
    pub raft_membership_receipt_retention_coupled_to_mutation_receipts: bool,
}

impl Default for NodeMetaHealth {
    fn default() -> Self {
        Self {
            control_proof: MetadataControlProof::default(),
            route_cache: MetadataRouteCacheStatus::default(),
            raft_membership_receipt_retention_limit: DEFAULT_META_RETENTION_LIMIT as u64,
            raft_membership_receipt_retention_coupled_to_mutation_receipts: true,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NodePageHealth {
    pub ready: bool,
    pub read_ready: bool,
    pub route_control_checksum: String,
    pub route_control_commit_index: u64,
    pub chunk_endpoint_count: u64,
    pub write_quorum: u64,
    pub read_quorum: u64,
    pub writable_chunk_endpoint_count: u64,
    pub healthy_chunk_endpoint_count: u64,
    pub raft_writable_chunk_endpoint_count: u64,
    pub raft_readable_chunk_endpoint_count: u64,
    pub capacity_ready_writable_chunk_endpoint_count: u64,
    pub quarantined_chunk_replicas: u64,
    pub chunk_capacity_bytes: u64,
    pub chunk_capacity_free_bytes: u64,
    pub chunk_metadata_dirty_endpoint_count: u64,
    pub chunk_metadata_dirty_mutations: u64,
    pub chunk_metadata_checkpoint_interval: u64,
    pub deferred_metadata_checkpoint_endpoint_count: u64,
    pub deferred_metadata_checkpoint_requests: u64,
    pub deferred_metadata_checkpoint_runs: u64,
    pub chunk_capacity_rejected_writes: u64,
    pub replica_service_mode_rejected_mutations: u64,
    pub chunk_physical_write_count: u64,
    pub chunk_physical_write_bytes: u64,
    pub chunk_physical_discard_range_count: u64,
    pub chunk_physical_discard_bytes: u64,
    pub total_reclaimable_bytes: u64,
    pub total_trailing_trim_bytes: u64,
    pub total_fragmented_chunk_count: u64,
    pub total_fragmented_discarded_bytes: u64,
    pub allocation_receipt_count: u64,
    pub batch_allocation_receipt_count: u64,
    pub release_allocation_receipt_count: u64,
    pub write_receipt_count: u64,
    pub batch_write_receipt_count: u64,
    pub discard_receipt_count: u64,
    pub batch_discard_receipt_count: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeChunkHealth {
    pub accepting_writes: bool,
    pub frozen: bool,
    pub raft_writable: bool,
    pub capacity_free_bytes: u64,
    pub discarded_range_count: u64,
    pub reclaimable_bytes: u64,
    pub trailing_trim_bytes: u64,
    pub fragmented_chunk_count: u64,
    pub fragmented_discarded_bytes: u64,
    pub service_mode: String,
    pub control_proof_format: String,
    pub control_proof_checksum: String,
    pub control_proof_bytes: u64,
    pub raft_commit_index: u64,
    pub control_read_ready: bool,
    pub control_write_ready: bool,
    pub service_health: String,
    pub service_health_reason_count: u64,
    pub service_health_reasons: String,
    pub service_fault_tolerant: bool,
}

impl Default for NodeChunkHealth {
    fn default() -> Self {
        Self {
            accepting_writes: true,
            frozen: false,
            raft_writable: true,
            capacity_free_bytes: 0,
            discarded_range_count: 0,
            reclaimable_bytes: 0,
            trailing_trim_bytes: 0,
            fragmented_chunk_count: 0,
            fragmented_discarded_bytes: 0,
            service_mode: String::new(),
            control_proof_format: String::new(),
            control_proof_checksum: String::new(),
            control_proof_bytes: 0,
            raft_commit_index: 0,
            control_read_ready: false,
            control_write_ready: false,
            service_health: "unknown".to_string(),
            service_health_reason_count: 0,
            service_health_reasons: String::new(),
            service_fault_tolerant: false,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PlacementMetadataServiceCoverage {
    pub(crate) active_metaserver_count: u64,
    pub(crate) metadata_value_bytes: u64,
    pub(crate) metadata_readable_metaserver_count: u64,
    pub(crate) metadata_writable_metaserver_count: u64,
    pub(crate) metadata_caught_up_metaserver_count: u64,
    pub(crate) metadata_apply_caught_up_metaserver_count: u64,
    pub(crate) metadata_control_apply_caught_up_metaserver_count: u64,
    pub(crate) metadata_read_fence_covered_metaserver_count: u64,
    pub(crate) metadata_quorum: u64,
    pub(crate) metadata_min_last_applied_mutation_commit_index: u64,
    pub(crate) metadata_max_apply_commit_lag: u64,
    pub(crate) metadata_min_safe_read_fence: u64,
    pub(crate) metadata_max_read_fence_commit_lag: u64,
    pub(crate) metadata_max_read_fence_read_index_lag: u64,
    pub(crate) metadata_max_read_fence_applied_lag: u64,
    pub(crate) metadata_control_min_last_applied_mutation_commit_index: u64,
    pub(crate) metadata_control_max_last_applied_mutation_commit_index: u64,
    pub(crate) metadata_control_max_apply_commit_lag: u64,
    pub(crate) metadata_max_shard_topology_epoch: u64,
    pub(crate) metadata_control_proof_count: u64,
    pub(crate) metadata_control_read_ready_count: u64,
    pub(crate) metadata_control_write_ready_count: u64,
    pub(crate) metadata_external_coordinator_required_metaserver_count: u64,
    pub(crate) metadata_external_coordinator_endpoints_ready_metaserver_count: u64,
    pub(crate) metadata_external_coordinator_owner_sessions_ready_metaserver_count: u64,
    pub(crate) metadata_external_coordinator_ready_metaserver_count: u64,
    pub(crate) metadata_control_proof_checksum: String,
    pub(crate) metadata_coordinator_backend_mismatch: bool,
    pub(crate) metadata_coordinator_config_mismatch: bool,
    pub(crate) metadata_read_quorum_available: bool,
    pub(crate) metadata_write_quorum_available: bool,
    pub(crate) metadata_caught_up_quorum_available: bool,
    pub(crate) metadata_apply_quorum_available: bool,
    pub(crate) metadata_read_fence_quorum_available: bool,
    pub(crate) metadata_control_apply_quorum_available: bool,
    pub(crate) metadata_shard_topology_mismatch: bool,
    pub(crate) status: String,
}

pub(crate) fn control_proof_readiness_label<'a>(
    proof_count: u64,
    read_ready_count: u64,
    write_ready_count: u64,
    unready_label: &'a str,
    read_unready_label: &'a str,
    write_unready_label: &'a str,
) -> Option<&'a str> {
    if proof_count == 0 || (read_ready_count != 0 && write_ready_count != 0) {
        None
    } else if read_ready_count == 0 && write_ready_count == 0 {
        Some(unready_label)
    } else if read_ready_count == 0 {
        Some(read_unready_label)
    } else {
        Some(write_unready_label)
    }
}

pub(crate) fn metadata_quorum_readiness_label<'a>(
    read_quorum_available: bool,
    write_quorum_available: bool,
    caught_up_quorum_available: bool,
    apply_quorum_available: bool,
    read_fence_quorum_available: bool,
    control_apply_quorum_available: bool,
    read_quorum_label: &'a str,
    write_quorum_label: &'a str,
    caught_up_quorum_label: &'a str,
    apply_quorum_label: &'a str,
    read_fence_quorum_label: &'a str,
    control_apply_quorum_label: &'a str,
) -> Option<&'a str> {
    if read_quorum_available
        && write_quorum_available
        && caught_up_quorum_available
        && apply_quorum_available
        && read_fence_quorum_available
        && control_apply_quorum_available
    {
        None
    } else if !read_quorum_available {
        Some(read_quorum_label)
    } else if !write_quorum_available {
        Some(write_quorum_label)
    } else if !caught_up_quorum_available {
        Some(caught_up_quorum_label)
    } else if !apply_quorum_available {
        Some(apply_quorum_label)
    } else if !read_fence_quorum_available {
        Some(read_fence_quorum_label)
    } else {
        Some(control_apply_quorum_label)
    }
}

pub(crate) fn service_read_write_coverage_status(
    active_count: u64,
    read_ready_count: u64,
    write_ready_count: u64,
) -> &'static str {
    if active_count == 0 {
        "none"
    } else if read_ready_count == active_count && write_ready_count == active_count {
        "healthy"
    } else if read_ready_count == 0 && write_ready_count == 0 {
        "unready"
    } else if read_ready_count == 0 {
        "read_unavailable"
    } else if write_ready_count == 0 {
        "write_unavailable"
    } else {
        "degraded"
    }
}

pub(crate) fn service_read_write_readiness_label<'a>(
    read_ready_count: u64,
    write_ready_count: u64,
    unready_label: &'a str,
    read_unavailable_label: &'a str,
    write_unavailable_label: &'a str,
) -> &'a str {
    if read_ready_count == 0 && write_ready_count == 0 {
        unready_label
    } else if read_ready_count == 0 {
        read_unavailable_label
    } else {
        write_unavailable_label
    }
}

pub(crate) fn chunkserver_coverage_status(
    active_count: u64,
    writable_count: u64,
    draining_count: u64,
    capacity_blocked_count: u64,
    frozen_count: u64,
    read_only_count: u64,
    raft_unwritable_count: u64,
) -> &'static str {
    if active_count == 0 {
        "none"
    } else if writable_count == 0 {
        if frozen_count != 0 {
            "frozen_unwritable"
        } else if read_only_count != 0 {
            "read_only_unwritable"
        } else if raft_unwritable_count != 0 {
            "raft_unwritable"
        } else if capacity_blocked_count != 0 {
            "capacity_floor_unavailable"
        } else if draining_count != 0 {
            "draining_unwritable"
        } else {
            "no_writable"
        }
    } else if frozen_count != 0 {
        "frozen_degraded"
    } else if read_only_count != 0 {
        "read_only_degraded"
    } else if raft_unwritable_count != 0 {
        "raft_degraded"
    } else if capacity_blocked_count != 0 {
        "capacity_floor_degraded"
    } else if draining_count != 0 {
        "draining_degraded"
    } else {
        "healthy"
    }
}

pub(crate) fn chunkserver_readiness_label<'a>(
    writable_count: u64,
    draining_count: u64,
    capacity_blocked_count: u64,
    frozen_count: u64,
    read_only_count: u64,
    raft_unwritable_count: u64,
    unwritable_label: &'a str,
    frozen_unwritable_label: &'a str,
    read_only_unwritable_label: &'a str,
    raft_unwritable_label: &'a str,
    capacity_floor_unavailable_label: &'a str,
    draining_unwritable_label: &'a str,
) -> &'a str {
    if writable_count == 0 {
        if frozen_count != 0 {
            frozen_unwritable_label
        } else if read_only_count != 0 {
            read_only_unwritable_label
        } else if raft_unwritable_count != 0 {
            raft_unwritable_label
        } else if capacity_blocked_count != 0 {
            capacity_floor_unavailable_label
        } else if draining_count != 0 {
            draining_unwritable_label
        } else {
            unwritable_label
        }
    } else {
        unwritable_label
    }
}

