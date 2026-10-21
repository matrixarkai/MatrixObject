// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedChunkCapacityConfig {
    pub replica_index: usize,
    pub capacity_bytes: u64,
    pub before_page_service_fingerprint: String,
    pub after_page_service_fingerprint: String,
    pub chunk_capacity_prepared_aggregate_checksum: String,
    pub chunk_prepared_replica_count: u64,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub route_control_proof: ChunkRouteControlProof,
    pub expected_route_control_checksum: String,
    pub raft_entry_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedReplicaServiceModeConfig {
    pub replica_index: usize,
    pub accepting_writes: Option<bool>,
    pub draining: Option<bool>,
    pub frozen: Option<bool>,
    pub before_page_service_fingerprint: String,
    pub after_page_service_fingerprint: String,
    pub replica_service_mode_prepared_aggregate_checksum: String,
    pub chunk_prepared_replica_count: u64,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub route_control_proof: ChunkRouteControlProof,
    pub expected_route_control_checksum: String,
    pub raft_entry_checksum: String,
}

impl PreparedChunkCapacityConfig {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}replica_index"),
            self.replica_index.to_string(),
        );
        fields.insert(
            format!("{field_prefix}capacity_bytes"),
            self.capacity_bytes.to_string(),
        );
        fields.insert(
            format!("{field_prefix}chunk_capacity_prepared_aggregate_checksum"),
            self.chunk_capacity_prepared_aggregate_checksum.clone(),
        );
        append_prepared_chunk_page_fingerprint_fields(
            &mut fields,
            field_prefix,
            &self.before_page_service_fingerprint,
            &self.after_page_service_fingerprint,
        );
        append_prepared_chunk_common_fields(
            &mut fields,
            field_prefix,
            self.chunk_prepared_replica_count,
            self.expected_previous_commit_index,
            self.prepared_commit_index,
            &self.expected_route_control_checksum,
            &self.raft_entry_checksum,
            &self.route_control_proof,
        );
        fields
    }
}

impl PreparedReplicaServiceModeConfig {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}replica_index"),
            self.replica_index.to_string(),
        );
        if let Some(accepting_writes) = self.accepting_writes {
            fields.insert(
                format!("{field_prefix}accepting_writes"),
                accepting_writes.to_string(),
            );
        }
        if let Some(draining) = self.draining {
            fields.insert(format!("{field_prefix}draining"), draining.to_string());
        }
        if let Some(frozen) = self.frozen {
            fields.insert(format!("{field_prefix}frozen"), frozen.to_string());
        }
        fields.insert(
            format!("{field_prefix}chunk_service_mode_prepared_aggregate_checksum"),
            self.replica_service_mode_prepared_aggregate_checksum.clone(),
        );
        append_prepared_chunk_page_fingerprint_fields(
            &mut fields,
            field_prefix,
            &self.before_page_service_fingerprint,
            &self.after_page_service_fingerprint,
        );
        append_prepared_chunk_common_fields(
            &mut fields,
            field_prefix,
            self.chunk_prepared_replica_count,
            self.expected_previous_commit_index,
            self.prepared_commit_index,
            &self.expected_route_control_checksum,
            &self.raft_entry_checksum,
            &self.route_control_proof,
        );
        fields
    }
}

