// SPDX-License-Identifier: Apache-2.0
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReplicaPartialReclaimReport {
    pub scanned_chunks: u64,
    pub candidate_chunks: u64,
    pub reclaimed_chunks: u64,
    pub skipped_chunks: u64,
    pub reclaimed_discard_bytes: u64,
    pub rewritten_bytes: u64,
    pub trimmed_bytes: u64,
    pub skipped_rewrite_bytes: u64,
    pub remaining_discard_bytes: u64,
    pub budget_limited: bool,
    pub trim_flow_limited: bool,
    pub requested_trim_bytes: u64,
    pub effective_trim_bytes: u64,
    pub trim_flow_available_bytes: u64,
    pub trim_flow_throttled_ops: u64,
    pub trim_flow_throttled_bytes: u64,
    pub partial_reclaim_failures: u64,
    pub last_partial_reclaim_failure_status: String,
}

impl ReplicaPartialReclaimReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("scanned_chunks", self.scanned_chunks),
            ("candidate_chunks", self.candidate_chunks),
            ("reclaimed_chunks", self.reclaimed_chunks),
            ("skipped_chunks", self.skipped_chunks),
            ("reclaimed_discard_bytes", self.reclaimed_discard_bytes),
            ("rewritten_bytes", self.rewritten_bytes),
            ("trimmed_bytes", self.trimmed_bytes),
            ("skipped_rewrite_bytes", self.skipped_rewrite_bytes),
            ("remaining_discard_bytes", self.remaining_discard_bytes),
            ("requested_trim_bytes", self.requested_trim_bytes),
            ("effective_trim_bytes", self.effective_trim_bytes),
            ("trim_flow_available_bytes", self.trim_flow_available_bytes),
            ("trim_flow_throttled_ops", self.trim_flow_throttled_ops),
            ("trim_flow_throttled_bytes", self.trim_flow_throttled_bytes),
            ("partial_reclaim_failures", self.partial_reclaim_failures),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(
            format!("{field_prefix}budget_limited"),
            self.budget_limited.to_string(),
        );
        fields.insert(
            format!("{field_prefix}trim_flow_limited"),
            self.trim_flow_limited.to_string(),
        );
        fields.insert(
            format!("{field_prefix}last_partial_reclaim_failure_status"),
            if self.last_partial_reclaim_failure_status.is_empty() {
                "ok".to_string()
            } else {
                self.last_partial_reclaim_failure_status.clone()
            },
        );
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaReclaimCandidate {
    pub page_id: PageId,
    pub logical_length: u64,
    pub discarded_bytes: u64,
    pub discarded_range_count: u64,
    pub discard_ratio_ppm: u64,
    pub rewrite_bytes: u64,
    pub trim_bytes: u64,
    pub reclaim_score: u64,
    pub reason: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReplicaReclaimPlan {
    pub marker: Option<PageId>,
    pub next_marker: Option<PageId>,
    pub continuation_safe: bool,
    pub next_marker_advanced: bool,
    pub scanned_chunks: u64,
    pub candidate_chunks: u64,
    pub scheduled_chunks: u64,
    pub skipped_chunks: u64,
    pub skipped_by_budget_chunks: u64,
    pub skipped_by_budget_bytes: u64,
    pub skipped_by_trim_budget_chunks: u64,
    pub skipped_by_trim_budget_bytes: u64,
    pub skipped_by_fragmentation_chunks: u64,
    pub skipped_by_fragmentation_discarded_bytes: u64,
    pub skipped_by_fragmentation_ranges: u64,
    pub candidate_discarded_bytes: u64,
    pub scheduled_rewrite_bytes: u64,
    pub scheduled_trim_ops: u64,
    pub scheduled_trim_bytes: u64,
    pub remaining_discard_bytes: u64,
    pub budget_limited: bool,
    pub truncated: bool,
    pub trim_flow_limited: bool,
    pub requested_trim_bytes: u64,
    pub effective_trim_bytes: u64,
    pub trim_flow_available_bytes: u64,
    pub trim_flow_throttled_ops: u64,
    pub trim_flow_throttled_bytes: u64,
    pub candidates: Vec<ReplicaReclaimCandidate>,
    pub first_budget_skipped_candidate: Option<ReplicaReclaimCandidate>,
    pub first_trim_budget_skipped_candidate: Option<ReplicaReclaimCandidate>,
    pub reclaim_needed: bool,
    pub reclaim_can_apply: bool,
    pub reclaim_reason: String,
    pub plan_checksum: String,
    pub expected_plan_checksum: String,
    pub actual_plan_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkReplicaReclaimCandidate {
    pub replica_index: usize,
    pub candidate: ReplicaReclaimCandidate,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ChunkReplicaReclaimPlan {
    pub continuation_safe: bool,
    pub next_marker_advanced: bool,
    pub scanned_replicas: u64,
    pub skipped_replicas: u64,
    pub frozen_replicas: u64,
    pub quarantined_replicas: u64,
    pub scanned_chunks: u64,
    pub candidate_chunks: u64,
    pub scheduled_chunks: u64,
    pub skipped_chunks: u64,
    pub skipped_by_budget_chunks: u64,
    pub skipped_by_budget_bytes: u64,
    pub skipped_by_trim_budget_chunks: u64,
    pub skipped_by_trim_budget_bytes: u64,
    pub skipped_by_fragmentation_chunks: u64,
    pub skipped_by_fragmentation_discarded_bytes: u64,
    pub skipped_by_fragmentation_ranges: u64,
    pub candidate_discarded_bytes: u64,
    pub scheduled_rewrite_bytes: u64,
    pub scheduled_trim_ops: u64,
    pub scheduled_trim_bytes: u64,
    pub remaining_discard_bytes: u64,
    pub budget_limited: bool,
    pub truncated: bool,
    pub global_reclaim_budget_enforced: bool,
    pub global_reclaim_budget_basis: String,
    pub candidates: Vec<ChunkReplicaReclaimCandidate>,
    pub first_budget_skipped_candidate: Option<ChunkReplicaReclaimCandidate>,
    pub first_trim_budget_skipped_candidate: Option<ChunkReplicaReclaimCandidate>,
    pub reclaim_needed: bool,
    pub reclaim_can_apply: bool,
    pub reclaim_reason: String,
    pub plan_checksum: String,
    pub expected_plan_checksum: String,
    pub actual_plan_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedChunkReplicaReclaim {
    pub replica_index: usize,
    pub max_chunks: usize,
    pub max_rewrite_bytes: u64,
    pub min_discarded_bytes: u64,
    pub min_discard_ratio_ppm: u64,
    pub max_discard_ranges: usize,
    pub max_trim_bytes: u64,
    pub now_ms: u64,
    pub before_replica_service_fingerprint: String,
    pub after_replica_service_fingerprint: String,
    pub plan: ReplicaReclaimPlan,
    pub report: ReplicaPartialReclaimReport,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub route_control_proof: ChunkRouteControlProof,
    pub expected_route_control_checksum: String,
    pub raft_entry_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedChunkReclaimApplyReport {
    pub mutation_id: String,
    pub mutation_request_checksum: String,
    pub mutation_commit_index: u64,
    pub next_log_index: u64,
    pub committed: bool,
    pub prepared: bool,
    pub external_raft_required: bool,
    pub deduped: bool,
    pub replica_index: usize,
    pub report: ReplicaPartialReclaimReport,
    pub metadata: ObjectMetadata,
}

impl PreparedChunkReclaimApplyReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .report
            .to_control_fields(&format!("{field_prefix}report."));
        fields.insert(
            format!("{field_prefix}mutation_id"),
            self.mutation_id.clone(),
        );
        fields.insert(
            format!("{field_prefix}mutation_request_checksum"),
            self.mutation_request_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}mutation_commit_index"),
            self.mutation_commit_index.to_string(),
        );
        fields.insert(
            format!("{field_prefix}next_log_index"),
            self.next_log_index.to_string(),
        );
        fields.insert(
            format!("{field_prefix}committed"),
            self.committed.to_string(),
        );
        fields.insert(format!("{field_prefix}prepared"), self.prepared.to_string());
        fields.insert(
            format!("{field_prefix}external_raft_required"),
            self.external_raft_required.to_string(),
        );
        fields.insert(format!("{field_prefix}deduped"), self.deduped.to_string());
        fields.insert(
            format!("{field_prefix}replica_index"),
            self.replica_index.to_string(),
        );
        fields.extend(
            self.metadata
                .to_control_fields(&format!("{field_prefix}metadata.")),
        );
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedChunkReclaimEnvelope {
    pub envelope_format: String,
    pub replica_index: usize,
    pub max_chunks: u64,
    pub max_rewrite_bytes: u64,
    pub min_discarded_bytes: u64,
    pub min_discard_ratio_ppm: u64,
    pub max_discard_ranges: u64,
    pub max_trim_bytes: u64,
    pub now_ms: u64,
    pub plan_checksum: String,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub before_replica_service_fingerprint: String,
    pub after_replica_service_fingerprint: String,
    pub route_control_proof_format: String,
    pub route_control_proof_checksum: String,
    pub route_control_proof_bytes: u64,
    pub route_control_commit_index: u64,
    pub route_control_next_log_index: u64,
    pub expected_route_control_checksum: String,
    pub raft_entry_checksum: String,
    pub candidate_chunks: u64,
    pub candidate_discarded_bytes: u64,
    pub scheduled_reclaim_chunks: u64,
    pub scheduled_rewrite_bytes: u64,
    pub scheduled_trim_ops: u64,
    pub scheduled_trim_bytes: u64,
    pub reclaimed_chunks: u64,
    pub reclaimed_discard_bytes: u64,
    pub rewritten_bytes: u64,
    pub trimmed_bytes: u64,
    pub budget_limited: bool,
    pub trim_flow_limited: bool,
    pub truncated: bool,
    pub continuation_safe: bool,
    pub next_marker_advanced: bool,
    pub first_reclaim_chunk_id: Option<PageId>,
    pub first_reclaim_discarded_bytes: u64,
    pub first_reclaim_discard_ratio_ppm: u64,
    pub first_reclaim_trim_bytes: u64,
    pub envelope_checksum: String,
}

