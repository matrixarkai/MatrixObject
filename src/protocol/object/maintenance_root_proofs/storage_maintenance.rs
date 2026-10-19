#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedStorageMaintenanceEnvelope {
    pub envelope_format: String,
    pub max_actions: u64,
    pub min_capacity_free_bytes: u64,
    pub min_reclaimable_bytes: u64,
    pub max_reclaim_chunks: u64,
    pub max_rewrite_bytes: u64,
    pub min_discarded_bytes: u64,
    pub min_discard_ratio_ppm: u64,
    pub max_discard_ranges: u64,
    pub max_trim_bytes: u64,
    pub now_ms: u64,
    pub plan_checksum: String,
    pub page_state_fingerprint: String,
    pub route_control_proof_format: String,
    pub route_control_proof_checksum: String,
    pub route_control_proof_bytes: u64,
    pub route_control_commit_index: u64,
    pub route_control_next_log_index: u64,
    pub selected_action_count: u64,
    pub planned_repair_actions: u64,
    pub planned_reclaim_chunks: u64,
    pub scheduled_reclaim_chunks: u64,
    pub scheduled_rewrite_bytes: u64,
    pub scheduled_trim_ops: u64,
    pub scheduled_trim_bytes: u64,
    pub candidate_discarded_bytes: u64,
    pub planned_slab_discards: u64,
    pub planned_slab_discard_bytes: u64,
    pub planned_slab_compaction_fallbacks: u64,
    pub budget_limited: bool,
    pub trim_flow_limited: bool,
    pub truncated: bool,
    pub reclaim_continuation_safe: bool,
    pub reclaim_next_marker_advanced: bool,
    pub capacity_pressure: bool,
    pub reclaim_pressure: bool,
    pub trim_flow_pressure: bool,
    pub quorum_pressure: bool,
    pub first_action: String,
    pub first_action_reason: String,
    pub first_replica_index: Option<usize>,
    pub first_chunk_id: Option<PageId>,
    pub first_estimated_bytes: u64,
    pub envelope_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProvenPreparedStorageMaintenanceEnvelope {
    pub envelope: PreparedStorageMaintenanceEnvelope,
    pub proof: StorageReadProof,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageMaintenanceApplyReport {
    pub mutation_id: String,
    pub mutation_request_checksum: String,
    pub mutation_commit_index: u64,
    pub next_log_index: u64,
    pub committed: bool,
    pub prepared: bool,
    pub external_raft_required: bool,
    pub deduped: bool,
    pub metadata: ObjectMetadata,
    pub proven_envelope: ProvenPreparedStorageMaintenanceEnvelope,
}

impl StorageMaintenanceApplyReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .proven_envelope
            .to_control_fields(&format!("{field_prefix}proven."));
        fields.insert(
            format!("{field_prefix}mutation_id"),
            self.mutation_id.clone(),
        );
        fields.insert(
            format!("{field_prefix}mutation_request_checksum"),
            self.mutation_request_checksum.clone(),
        );
        for (name, value) in [
            ("mutation_commit_index", self.mutation_commit_index),
            ("next_log_index", self.next_log_index),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        for (name, value) in [
            ("committed", self.committed),
            ("prepared", self.prepared),
            ("external_raft_required", self.external_raft_required),
            ("deduped", self.deduped),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.extend(
            self.metadata
                .to_control_fields(&format!("{field_prefix}metadata.")),
        );
        fields
    }
}

impl PreparedStorageMaintenanceEnvelope {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("max_actions", self.max_actions),
            ("min_capacity_free_bytes", self.min_capacity_free_bytes),
            ("min_reclaimable_bytes", self.min_reclaimable_bytes),
            ("max_reclaim_chunks", self.max_reclaim_chunks),
            ("max_rewrite_bytes", self.max_rewrite_bytes),
            ("min_discarded_bytes", self.min_discarded_bytes),
            ("min_discard_ratio_ppm", self.min_discard_ratio_ppm),
            ("max_discard_ranges", self.max_discard_ranges),
            ("max_trim_bytes", self.max_trim_bytes),
            ("now_ms", self.now_ms),
            ("route_control_proof_bytes", self.route_control_proof_bytes),
            (
                "route_control_commit_index",
                self.route_control_commit_index,
            ),
            (
                "route_control_next_log_index",
                self.route_control_next_log_index,
            ),
            ("selected_action_count", self.selected_action_count),
            ("planned_repair_actions", self.planned_repair_actions),
            ("planned_reclaim_chunks", self.planned_reclaim_chunks),
            ("scheduled_reclaim_chunks", self.scheduled_reclaim_chunks),
            ("scheduled_rewrite_bytes", self.scheduled_rewrite_bytes),
            ("scheduled_trim_ops", self.scheduled_trim_ops),
            ("scheduled_trim_bytes", self.scheduled_trim_bytes),
            ("candidate_discarded_bytes", self.candidate_discarded_bytes),
            ("planned_segment_discards", self.planned_slab_discards),
            (
                "planned_segment_discard_bytes",
                self.planned_slab_discard_bytes,
            ),
            (
                "planned_segment_compaction_fallbacks",
                self.planned_slab_compaction_fallbacks,
            ),
            ("first_estimated_bytes", self.first_estimated_bytes),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        for (name, value) in [
            ("budget_limited", self.budget_limited),
            ("trim_flow_limited", self.trim_flow_limited),
            ("truncated", self.truncated),
            ("reclaim_continuation_safe", self.reclaim_continuation_safe),
            (
                "reclaim_next_marker_advanced",
                self.reclaim_next_marker_advanced,
            ),
            ("capacity_pressure", self.capacity_pressure),
            ("reclaim_pressure", self.reclaim_pressure),
            ("trim_flow_pressure", self.trim_flow_pressure),
            ("quorum_pressure", self.quorum_pressure),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        for (name, value) in [
            ("envelope_format", &self.envelope_format),
            ("plan_checksum", &self.plan_checksum),
            ("block_state_fingerprint", &self.page_state_fingerprint),
            (
                "route_control_proof_format",
                &self.route_control_proof_format,
            ),
            (
                "route_control_proof_checksum",
                &self.route_control_proof_checksum,
            ),
            ("first_action", &self.first_action),
            ("first_action_reason", &self.first_action_reason),
            ("envelope_checksum", &self.envelope_checksum),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.clone());
        }
        fields.insert(
            format!("{field_prefix}first_replica_index_present"),
            self.first_replica_index.is_some().to_string(),
        );
        if let Some(index) = self.first_replica_index {
            fields.insert(
                format!("{field_prefix}first_replica_index"),
                index.to_string(),
            );
        }
        fields.insert(
            format!("{field_prefix}first_chunk_present"),
            self.first_chunk_id.is_some().to_string(),
        );
        if let Some(page_id) = &self.first_chunk_id {
            fields.extend(page_id.to_control_fields(&format!("{field_prefix}first_chunk.")));
        }
        fields
    }
}

impl ProvenPreparedStorageMaintenanceEnvelope {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .envelope
            .to_control_fields(&format!("{field_prefix}envelope."));
        fields.extend(
            self.proof
                .to_control_fields(&format!("{field_prefix}storage_read_proof.")),
        );
        fields
    }
}

