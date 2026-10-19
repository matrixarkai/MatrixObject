// SPDX-License-Identifier: Apache-2.0
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GcReport {
    pub chunk_count: u64,
    pub logical_bytes: u64,
    pub live_bytes: u64,
    pub stale_bytes: u64,
    pub discarded_bytes: u64,
    pub logical_write_amplification_ppm: u64,
    pub space_amplification_ppm: u64,
    pub candidate_count: u64,
    pub discard_candidate_bytes: u64,
    pub min_discard_bytes: u64,
    pub selected_discard_bytes: u64,
    pub selected_discard_range_count: u64,
    pub budget_skipped_discard_range_count: u64,
    pub max_discard_bytes: u64,
    pub discard_budget_limited: bool,
    pub pending_discard_range_count: u64,
    pub merged_discard_range_count: u64,
    pub fragmented_range_count: u64,
    pub compaction_candidate_count: u64,
    pub compaction_candidate_bytes: u64,
    pub compaction_candidate_range_count: u64,
    pub discard_candidates: Vec<ObjectGcDiscardCandidate>,
    pub route_control_proof: ChunkRouteControlProof,
    pub expected_route_control_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectGcDiscardCandidate {
    pub chunk_id: ChunkId,
    pub range: DataRange,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FullCompactionPlan {
    pub min_stale_bytes: u64,
    pub candidate_count: u64,
    pub candidate_stale_bytes: u64,
    pub candidate_live_bytes: u64,
    pub candidate_logical_bytes: u64,
    pub candidate_chunk_ids: Vec<ChunkId>,
    pub route_control_proof: ChunkRouteControlProof,
    pub expected_route_control_checksum: String,
    pub object_gc_report_proof_checksum: String,
}
