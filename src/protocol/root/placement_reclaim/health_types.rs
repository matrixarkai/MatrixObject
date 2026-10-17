#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RootPlacementHealthItem {
    pub placement: ObjectPlacement,
    pub missing_node_ids: Vec<String>,
    pub offline_node_ids: Vec<String>,
    pub draining_node_ids: Vec<String>,
    pub capacity_pressure_node_ids: Vec<String>,
    pub under_replicated: bool,
    pub healthy: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RootPlacementHealthPage {
    pub items: Vec<RootPlacementHealthItem>,
    pub marker: Option<ObjectId>,
    pub next_marker: Option<ObjectId>,
    pub expected_replica_count: usize,
    pub min_free_bytes: u64,
    pub degraded_only: bool,
    pub scanned_objects: u64,
    pub skipped_objects: u64,
    pub healthy_objects: u64,
    pub degraded_objects: u64,
    pub under_replicated_objects: u64,
    pub missing_node_references: u64,
    pub offline_node_references: u64,
    pub draining_node_references: u64,
    pub capacity_pressure_references: u64,
    pub payload_bytes: u64,
    pub budget_limited: bool,
    pub has_more: bool,
}

