// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProvenPreparedChunkReclaimEnvelope {
    pub envelope: PreparedChunkReclaimEnvelope,
    pub proof: StorageReadProof,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedChunkMutationQosConfig {
    pub target_replica_index: Option<usize>,
    pub config: ReplicaMutationQosConfig,
    pub chunk_service: MatrixChunkService,
    pub chunk_mutation_qos_prepared_aggregate_checksum: String,
    pub chunk_prepared_replica_count: u64,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub route_control_proof: ChunkRouteControlProof,
    pub expected_route_control_checksum: String,
    pub raft_entry_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedChunkTrimFlowConfig {
    pub target_replica_index: Option<usize>,
    pub rate_bytes_per_sec: u64,
    pub burst_bytes: u64,
    pub rate_ops_per_sec: u64,
    pub burst_ops: u64,
    pub now_ms: u64,
    pub before_page_service_fingerprint: String,
    pub after_page_service_fingerprint: String,
    pub chunk_trim_flow_prepared_aggregate_checksum: String,
    pub chunk_prepared_replica_count: u64,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub route_control_proof: ChunkRouteControlProof,
    pub expected_route_control_checksum: String,
    pub raft_entry_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedChunkReadCacheConfig {
    pub target_replica_index: Option<usize>,
    pub max_bytes_per_replica: u64,
    pub max_cacheable_read_bytes_per_replica: u64,
    pub before_page_service_fingerprint: String,
    pub after_page_service_fingerprint: String,
    pub chunk_read_cache_prepared_aggregate_checksum: String,
    pub chunk_prepared_replica_count: u64,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub route_control_proof: ChunkRouteControlProof,
    pub expected_route_control_checksum: String,
    pub raft_entry_checksum: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PreparedChunkReceiptRetentionConfig {
    pub target_replica_index: Option<usize>,
    pub retention: usize,
    pub trimmed_receipts: u64,
    pub chunk_prepared_replica_count: u64,
    pub before_page_service_fingerprint: String,
    pub after_page_service_fingerprint: String,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub route_control_proof: ChunkRouteControlProof,
    pub expected_route_control_checksum: String,
    pub raft_entry_checksum: String,
}

impl PreparedChunkReceiptRetentionConfig {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}target_replica_index_present"),
            self.target_replica_index.is_some().to_string(),
        );
        if let Some(replica_index) = self.target_replica_index {
            fields.insert(
                format!("{field_prefix}target_replica_index"),
                replica_index.to_string(),
            );
        }
        for (name, value) in [
            ("retention", self.retention as u64),
            ("trimmed_receipts", self.trimmed_receipts),
            (
                "chunk_prepared_replica_count",
                self.chunk_prepared_replica_count,
            ),
            (
                "expected_previous_commit_index",
                self.expected_previous_commit_index,
            ),
            ("prepared_commit_index", self.prepared_commit_index),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        for (name, value) in [
            (
                "before_page_service_fingerprint",
                &self.before_page_service_fingerprint,
            ),
            (
                "after_page_service_fingerprint",
                &self.after_page_service_fingerprint,
            ),
            (
                "expected_route_control_checksum",
                &self.expected_route_control_checksum,
            ),
            ("raft_entry_checksum", &self.raft_entry_checksum),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.clone());
        }
        fields.extend(
            self.route_control_proof
                .to_control_fields(&format!("{field_prefix}route_control.")),
        );
        fields
    }
}

