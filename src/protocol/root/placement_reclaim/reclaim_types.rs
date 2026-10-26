// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlacementReclaimNodePlan {
    pub usage: PlacementNodeUsage,
    pub reclaim_score: u64,
    pub reclaim_reason: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PlacementReclaimNodePlanPage {
    pub plans: Vec<PlacementReclaimNodePlan>,
    pub marker: Option<String>,
    pub next_marker: Option<String>,
    pub service_kind_filter: Option<String>,
    pub only_online: bool,
    pub include_draining: bool,
    pub min_reclaimable_bytes: u64,
    pub min_overcommitted_bytes: u64,
    pub min_free_bytes: u64,
    pub min_page_physical_discard_bytes: u64,
    pub min_page_physical_discard_ranges: u64,
    pub scanned_nodes: u64,
    pub candidate_nodes: u64,
    pub skipped_nodes: u64,
    pub total_reclaim_score: u64,
    pub total_reclaimable_bytes: u64,
    pub total_overcommitted_bytes: u64,
    pub total_capacity_pressure_bytes: u64,
    pub has_more: bool,
    pub reclaim_needed: bool,
    pub reclaim_can_apply: bool,
    pub reclaim_reason: String,
}

