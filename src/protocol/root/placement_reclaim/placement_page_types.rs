#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RootPlacementPage {
    pub placements: Vec<ObjectPlacement>,
    pub marker: Option<ObjectId>,
    pub next_marker: Option<ObjectId>,
    pub node_id_filter: Option<String>,
    pub scanned_objects: u64,
    pub skipped_objects: u64,
    pub payload_bytes: u64,
    pub has_more: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RootNodeUsage {
    pub node: RootNode,
    pub registered: bool,
    pub object_count: u64,
    pub referenced_bytes: u64,
    pub free_bytes: u64,
    pub reported_free_bytes: u64,
    pub committed_vs_reported_delta_bytes: u64,
    pub reclaimable_bytes: u64,
    pub overcommitted_bytes: u64,
    pub capacity_pressure: bool,
    pub reclaim_pressure: bool,
    pub placement_eligible: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RootNodeUsagePage {
    pub nodes: Vec<RootNodeUsage>,
    pub marker: Option<String>,
    pub next_marker: Option<String>,
    pub service_kind_filter: Option<String>,
    pub draining_filter: Option<bool>,
    pub online_filter: Option<bool>,
    pub capacity_pressure_filter: Option<bool>,
    pub reclaim_pressure_filter: Option<bool>,
    pub placement_eligible_filter: Option<bool>,
    pub min_free_bytes: u64,
    pub min_reclaimable_bytes: u64,
    pub total_node_count: u64,
    pub registered_node_count: u64,
    pub referenced_node_count: u64,
    pub scanned_nodes: u64,
    pub skipped_nodes: u64,
    pub unregistered_referenced_nodes: u64,
    pub total_referenced_object_bytes: u64,
    pub total_replicated_object_bytes: u64,
    pub reclaim_pressure_nodes: u64,
    pub total_capacity_bytes: u64,
    pub total_used_bytes: u64,
    pub total_committed_bytes: u64,
    pub total_free_bytes: u64,
    pub total_reported_free_bytes: u64,
    pub total_reclaimable_bytes: u64,
    pub total_overcommitted_bytes: u64,
    pub has_more: bool,
}

