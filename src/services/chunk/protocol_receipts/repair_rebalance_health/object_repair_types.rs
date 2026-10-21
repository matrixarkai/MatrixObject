// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectReplicaRepairItem {
    pub object: ObjectMetadata,
    pub estimated_chunks: u64,
    pub estimated_bytes: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ObjectReplicaRepairPlan {
    pub checked_objects: u64,
    pub repair_objects: u64,
    pub estimated_chunks: u64,
    pub bytes_to_repair: u64,
    pub marker: Option<ObjectId>,
    pub next_marker: Option<ObjectId>,
    pub truncated: bool,
    pub repair_needed: bool,
    pub repair_can_apply: bool,
    pub repair_reason: String,
    pub items: Vec<ObjectReplicaRepairItem>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ObjectReplicaRepairRunReport {
    pub plan: ObjectReplicaRepairPlan,
    pub attempted_repair_objects: u64,
    pub skipped_repair_objects: u64,
    pub repaired_objects: u64,
    pub failed_repair_objects: u64,
    pub bytes_repaired: u64,
    pub stats: RepairStats,
    pub first_error: Option<String>,
    pub repaired: Vec<ObjectMetadata>,
    pub route_control_proof: ChunkRouteControlProof,
    pub expected_route_control_checksum: String,
    pub expected_plan_checksum: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ObjectReplicaDecommissionReport {
    pub plan: RootPlacementRepairReport,
    pub attempted_repair_objects: u64,
    pub skipped_repair_objects: u64,
    pub repaired_objects: u64,
    pub failed_repair_objects: u64,
    pub attempted_migration_objects: u64,
    pub skipped_migration_objects: u64,
    pub migrated_objects: u64,
    pub failed_migration_objects: u64,
    pub bytes_repaired: u64,
    pub bytes_migrated: u64,
    pub stats: RepairStats,
    pub first_error: Option<String>,
    pub migrated: Vec<ObjectPlacement>,
    pub route_control_proof: ChunkRouteControlProof,
    pub expected_route_control_checksum: String,
    pub expected_plan_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectCapacityRebalanceItem {
    pub object: ObjectMetadata,
    pub pressure_node_id: String,
    pub plan: PlacementMigrationPlan,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ObjectCapacityRebalanceReport {
    pub checked_objects: u64,
    pub rebalance_objects: u64,
    pub bytes_to_rebalance: u64,
    pub pressure_node_count: u64,
    pub min_free_bytes: u64,
    pub marker: Option<ObjectId>,
    pub next_marker: Option<ObjectId>,
    pub truncated: bool,
    pub rebalance_needed: bool,
    pub rebalance_can_apply: bool,
    pub rebalance_reason: String,
    pub items: Vec<ObjectCapacityRebalanceItem>,
    pub errors: Vec<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ObjectCapacityRebalanceRunReport {
    pub plan: ObjectCapacityRebalanceReport,
    pub attempted_migration_objects: u64,
    pub skipped_migration_objects: u64,
    pub migrated_objects: u64,
    pub failed_migration_objects: u64,
    pub bytes_migrated: u64,
    pub first_error: Option<String>,
    pub migrated: Vec<ObjectPlacement>,
    pub route_control_proof: ChunkRouteControlProof,
    pub expected_route_control_checksum: String,
    pub expected_plan_checksum: String,
}

