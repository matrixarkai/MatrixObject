// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ObjectRangeAdmission {
    pub bucket: String,
    pub key: String,
    pub can_accept: bool,
    pub object_length: u64,
    pub requested_offset: u64,
    pub requested_length: u64,
    pub selected_bytes: u64,
    pub extent_count: u64,
    pub mapped_chunk_ranges: u64,
    pub payload_bytes: u64,
    pub max_payload_bytes: u64,
    pub max_chunk_read_count: u64,
    pub max_discard_bytes: u64,
    pub budget_limited: bool,
    pub min_ready_replica_count: u64,
    pub required_quorum: u64,
    pub readable_chunk_count: u64,
    pub failed_chunk_count: u64,
    pub storage_active_replicas: u64,
    pub storage_readable_replicas: u64,
    pub storage_writable_replicas: u64,
    pub storage_quarantined_replicas: u64,
    pub storage_frozen_replicas: u64,
    pub storage_draining_replicas: u64,
    pub storage_capacity_pressure_replicas: u64,
    pub storage_trim_flow_limited_replicas: u64,
    pub storage_total_capacity_bytes: u64,
    pub storage_total_capacity_free_bytes: u64,
    pub storage_total_reclaimable_bytes: u64,
    pub storage_total_fragmented_chunk_count: u64,
    pub storage_total_fragmented_discarded_bytes: u64,
    pub storage_total_trailing_trim_bytes: u64,
    pub storage_reclaim_pressure: bool,
    pub storage_read_cache_bytes: u64,
    pub storage_read_cache_entries: u64,
    pub storage_read_cache_max_bytes: u64,
    pub storage_read_cache_max_cacheable_read_bytes: u64,
    pub storage_read_cache_hits: u64,
    pub storage_read_cache_misses: u64,
    pub storage_read_cache_evictions: u64,
    pub storage_read_cache_pressure: bool,
    pub storage_admission_reason: String,
    pub storage_admission_proof_format: String,
    pub storage_admission_proof_checksum: String,
    pub storage_read_quorum_limited: bool,
    pub storage_write_quorum_limited: bool,
    pub storage_discard_budget_limited: bool,
    pub selected_discard_bytes: u64,
    pub skipped_discard_bytes: u64,
    pub read_stats: ChunkReadStats,
    pub partial_status: String,
    pub route_control_proof: ChunkRouteControlProof,
    pub expected_route_control_checksum: String,
}

impl ObjectRangeAdmission {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(format!("{field_prefix}bucket"), self.bucket.clone());
        fields.insert(format!("{field_prefix}key"), self.key.clone());
        fields.insert(
            format!("{field_prefix}can_accept"),
            self.can_accept.to_string(),
        );
        for (name, value) in [
            ("object_length", self.object_length),
            ("requested_offset", self.requested_offset),
            ("requested_length", self.requested_length),
            ("selected_bytes", self.selected_bytes),
            ("extent_count", self.extent_count),
            ("mapped_chunk_ranges", self.mapped_chunk_ranges),
            ("payload_bytes", self.payload_bytes),
            ("max_payload_bytes", self.max_payload_bytes),
            ("max_chunk_read_count", self.max_chunk_read_count),
            ("max_discard_bytes", self.max_discard_bytes),
            ("min_ready_replica_count", self.min_ready_replica_count),
            ("required_quorum", self.required_quorum),
            ("readable_chunk_count", self.readable_chunk_count),
            ("failed_chunk_count", self.failed_chunk_count),
            ("storage_active_replicas", self.storage_active_replicas),
            ("storage_readable_replicas", self.storage_readable_replicas),
            ("storage_writable_replicas", self.storage_writable_replicas),
            (
                "storage_quarantined_replicas",
                self.storage_quarantined_replicas,
            ),
            ("storage_frozen_replicas", self.storage_frozen_replicas),
            ("storage_draining_replicas", self.storage_draining_replicas),
            (
                "storage_capacity_pressure_replicas",
                self.storage_capacity_pressure_replicas,
            ),
            (
                "storage_trim_flow_limited_replicas",
                self.storage_trim_flow_limited_replicas,
            ),
            (
                "storage_total_capacity_bytes",
                self.storage_total_capacity_bytes,
            ),
            (
                "storage_total_capacity_free_bytes",
                self.storage_total_capacity_free_bytes,
            ),
            (
                "storage_total_reclaimable_bytes",
                self.storage_total_reclaimable_bytes,
            ),
            (
                "storage_total_fragmented_chunk_count",
                self.storage_total_fragmented_chunk_count,
            ),
            (
                "storage_total_fragmented_discarded_bytes",
                self.storage_total_fragmented_discarded_bytes,
            ),
            (
                "storage_total_trailing_trim_bytes",
                self.storage_total_trailing_trim_bytes,
            ),
            ("storage_read_cache_bytes", self.storage_read_cache_bytes),
            (
                "storage_read_cache_entries",
                self.storage_read_cache_entries,
            ),
            (
                "storage_read_cache_max_bytes",
                self.storage_read_cache_max_bytes,
            ),
            (
                "storage_read_cache_max_cacheable_read_bytes",
                self.storage_read_cache_max_cacheable_read_bytes,
            ),
            ("storage_read_cache_hits", self.storage_read_cache_hits),
            ("storage_read_cache_misses", self.storage_read_cache_misses),
            (
                "storage_read_cache_evictions",
                self.storage_read_cache_evictions,
            ),
            ("selected_discard_bytes", self.selected_discard_bytes),
            ("skipped_discard_bytes", self.skipped_discard_bytes),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        for (name, value) in [
            ("budget_limited", self.budget_limited),
            ("storage_reclaim_pressure", self.storage_reclaim_pressure),
            (
                "storage_read_cache_pressure",
                self.storage_read_cache_pressure,
            ),
            (
                "storage_read_quorum_limited",
                self.storage_read_quorum_limited,
            ),
            (
                "storage_write_quorum_limited",
                self.storage_write_quorum_limited,
            ),
            (
                "storage_discard_budget_limited",
                self.storage_discard_budget_limited,
            ),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(
            format!("{field_prefix}storage_admission_reason"),
            self.storage_admission_reason.clone(),
        );
        fields.insert(
            format!("{field_prefix}storage_admission_proof_format"),
            self.storage_admission_proof_format.clone(),
        );
        fields.insert(
            format!("{field_prefix}storage_admission_proof_checksum"),
            self.storage_admission_proof_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}partial_status"),
            self.partial_status.clone(),
        );
        fields.insert(
            format!("{field_prefix}expected_route_control_checksum"),
            self.expected_route_control_checksum.clone(),
        );
        fields.extend(
            self.read_stats
                .to_control_fields(&format!("{field_prefix}read_stats.")),
        );
        fields.extend(
            self.route_control_proof
                .to_control_fields(&format!("{field_prefix}route_control.")),
        );
        fields
    }
}

