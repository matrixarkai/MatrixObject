// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RootReadProof {
    pub proof_format: String,
    pub operation: String,
    pub requested_root_read_index: u64,
    pub requested_root_commit_index: u64,
    pub root_applied_index: u64,
    pub root_read_index_lag: u64,
    pub root_raft_term: u64,
    pub root_state_fingerprint: String,
    pub proof_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProvenRootRead<T> {
    pub value: T,
    pub proof: RootReadProof,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedRootPlacementMigrationEnvelope {
    pub object_id: ObjectId,
    pub version: u64,
    pub length: u64,
    pub source_node_id: String,
    pub target_node_id: String,
    pub before_node_ids: Vec<String>,
    pub after_node_ids: Vec<String>,
    pub reason: String,
    pub expected_plan_checksum: String,
    pub min_free_bytes: u64,
    pub before_state_fingerprint: String,
    pub root_read_index: u64,
    pub root_raft_term: u64,
    pub root_read_proof_checksum: String,
    pub request_checksum: String,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub raft_entry_checksum: String,
    pub envelope_format: String,
    pub envelope_bytes: u64,
    pub envelope_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProvenPreparedRootPlacementMigrationEnvelope {
    pub envelope: PreparedRootPlacementMigrationEnvelope,
    pub proof: RootReadProof,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommittedRootPlacementMigrationEnvelope {
    pub object_id: ObjectId,
    pub version: u64,
    pub length: u64,
    pub source_node_id: String,
    pub target_node_id: String,
    pub before_node_ids: Vec<String>,
    pub after_node_ids: Vec<String>,
    pub reason: String,
    pub expected_plan_checksum: String,
    pub min_free_bytes: u64,
    pub request_checksum: String,
    pub prepared_commit_index: u64,
    pub raft_entry_checksum: String,
    pub committed_raft_index: u64,
    pub metadata_version: u64,
    pub metadata_length: u64,
    pub metadata_etag: String,
    pub metadata_content_type: String,
    pub metadata_update_time_ms: u64,
    pub envelope_format: String,
    pub envelope_bytes: u64,
    pub envelope_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommittedRootPlacementMigration {
    pub metadata: ObjectMetadata,
    pub envelope: CommittedRootPlacementMigrationEnvelope,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedRootPlacementRepairEnvelope {
    pub repair_kind: String,
    pub marker: Option<ObjectId>,
    pub max_objects: u64,
    pub expected_replica_count: u64,
    pub min_free_bytes: u64,
    pub max_payload_bytes: u64,
    pub has_more: bool,
    pub next_marker: Option<ObjectId>,
    pub scanned_objects: u64,
    pub repair_objects: u64,
    pub repaired_objects: u64,
    pub skipped_objects: u64,
    pub failed_repairs: u64,
    pub bytes_to_repair: u64,
    pub bytes_repaired: u64,
    pub repair_needed: bool,
    pub repair_can_apply: bool,
    pub repair_reason: String,
    pub planned_item_count: u64,
    pub first_object: Option<ObjectId>,
    pub first_source_node_id: String,
    pub first_target_node_id: String,
    pub before_state_fingerprint: String,
    pub request_checksum: String,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub raft_entry_checksum: String,
    pub envelope_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProvenPreparedRootPlacementRepairEnvelope {
    pub envelope: PreparedRootPlacementRepairEnvelope,
    pub proof: RootReadProof,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedRootCapacityRebalanceEnvelope {
    pub max_objects: u64,
    pub min_free_bytes: u64,
    pub pressure_node_count: u64,
    pub has_more: bool,
    pub scanned_objects: u64,
    pub rebalance_objects: u64,
    pub rebalanced_objects: u64,
    pub skipped_objects: u64,
    pub failed_rebalances: u64,
    pub bytes_to_rebalance: u64,
    pub bytes_rebalanced: u64,
    pub rebalance_needed: bool,
    pub rebalance_can_apply: bool,
    pub rebalance_reason: String,
    pub planned_item_count: u64,
    pub first_object: Option<ObjectId>,
    pub first_pressure_node_id: String,
    pub first_target_node_id: String,
    pub before_state_fingerprint: String,
    pub request_checksum: String,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub raft_entry_checksum: String,
    pub envelope_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProvenPreparedRootCapacityRebalanceEnvelope {
    pub envelope: PreparedRootCapacityRebalanceEnvelope,
    pub proof: RootReadProof,
}

