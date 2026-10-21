// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlacementPlan {
    pub object_id: ObjectId,
    pub length: u64,
    pub node_ids: Vec<String>,
    pub skipped_draining: u64,
    pub skipped_offline: u64,
    pub skipped_capacity: u64,
    pub plan_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlacementMigrationPlan {
    pub object_id: ObjectId,
    pub version: u64,
    pub length: u64,
    pub source_node_id: String,
    pub target_node_id: String,
    pub before_node_ids: Vec<String>,
    pub after_node_ids: Vec<String>,
    pub reason: String,
    pub plan_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RootPlacementRepairItem {
    pub plan: PlacementMigrationPlan,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RootPlacementRepairReport {
    pub marker: Option<ObjectId>,
    pub next_marker: Option<ObjectId>,
    pub has_more: bool,
    pub scanned_objects: u64,
    pub repair_objects: u64,
    pub repaired_objects: u64,
    pub skipped_objects: u64,
    pub failed_repairs: u64,
    pub bytes_to_repair: u64,
    pub bytes_repaired: u64,
    pub items: Vec<RootPlacementRepairItem>,
    pub errors: Vec<String>,
    pub repair_needed: bool,
    pub repair_can_apply: bool,
    pub repair_reason: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RootPlacementRefreshReport {
    pub marker: Option<ObjectId>,
    pub next_marker: Option<ObjectId>,
    pub has_more: bool,
    pub scanned_objects: u64,
    pub refreshed_objects: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RootPlacementRefreshRunReport {
    pub report: RootPlacementRefreshReport,
    pub metadata: ObjectMetadata,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RootCapacityRebalanceItem {
    pub plan: PlacementMigrationPlan,
    pub pressure_node_id: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RootCapacityRebalanceReport {
    pub scanned_objects: u64,
    pub pressure_node_count: u64,
    pub rebalance_objects: u64,
    pub rebalanced_objects: u64,
    pub has_more: bool,
    pub skipped_objects: u64,
    pub failed_rebalances: u64,
    pub bytes_to_rebalance: u64,
    pub bytes_rebalanced: u64,
    pub items: Vec<RootCapacityRebalanceItem>,
    pub errors: Vec<String>,
    pub rebalance_needed: bool,
    pub rebalance_can_apply: bool,
    pub rebalance_reason: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedRootCapacityRebalance {
    pub max_objects: usize,
    pub min_free_bytes: u64,
    pub report: RootCapacityRebalanceReport,
    pub before_state_fingerprint: String,
    pub request_checksum: String,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub raft_entry_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedRootPlacementRepair {
    pub max_objects: usize,
    pub report: RootPlacementRepairReport,
    pub before_state_fingerprint: String,
    pub request_checksum: String,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub raft_entry_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedRootUnhealthyPlacementRepair {
    pub marker: Option<ObjectId>,
    pub max_objects: usize,
    pub expected_replica_count: usize,
    pub min_free_bytes: u64,
    pub max_payload_bytes: u64,
    pub report: RootPlacementRepairReport,
    pub before_state_fingerprint: String,
    pub request_checksum: String,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub raft_entry_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedRootPlacementMigration {
    pub plan: PlacementMigrationPlan,
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
}

macro_rules! impl_prepared_root_raft_entry_checksum_guard {
    ($ty:ty, $label:literal) => {
        impl $ty {
            pub fn verify_expected_raft_entry_checksum(
                &self,
                expected_raft_entry_checksum: impl AsRef<str>,
            ) -> Result<(), ObjectError> {
                let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
                if expected_raft_entry_checksum.is_empty() {
                    return Ok(());
                }
                if self.raft_entry_checksum != expected_raft_entry_checksum {
                    return Err(ObjectError::PreconditionFailed(format!(
                        "prepared root {} raft entry checksum mismatch: expected {} got {}",
                        $label, expected_raft_entry_checksum, self.raft_entry_checksum
                    )));
                }
                Ok(())
            }
        }
    };
}

impl_prepared_root_raft_entry_checksum_guard!(
    PreparedRootPlacementMigration,
    "placement migration"
);
impl_prepared_root_raft_entry_checksum_guard!(PreparedRootPlacementRepair, "placement repair");
impl_prepared_root_raft_entry_checksum_guard!(
    PreparedRootUnhealthyPlacementRepair,
    "unhealthy placement repair"
);
impl_prepared_root_raft_entry_checksum_guard!(PreparedRootCapacityRebalance, "capacity rebalance");

