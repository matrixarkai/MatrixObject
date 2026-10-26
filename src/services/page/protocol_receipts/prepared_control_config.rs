// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedPageRouteQosConfig {
    pub config: ChunkRouteQosConfig,
    pub before_page_service_fingerprint: String,
    pub after_page_service_fingerprint: String,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub route_control_proof: ChunkRouteControlProof,
    pub expected_route_control_checksum: String,
    pub raft_entry_checksum: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PreparedPageReceiptRetentionConfig {
    pub retention: usize,
    pub trimmed_receipts: u64,
    pub before_page_service_fingerprint: String,
    pub after_page_service_fingerprint: String,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub route_control_proof: ChunkRouteControlProof,
    pub expected_route_control_checksum: String,
    pub raft_entry_checksum: String,
}

impl PreparedPageReceiptRetentionConfig {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("retention", self.retention as u64),
            ("trimmed_receipts", self.trimmed_receipts),
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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PreparedPageQuorumConfig {
    pub write_quorum: usize,
    pub read_quorum: usize,
    pub before_page_service_fingerprint: String,
    pub after_page_service_fingerprint: String,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub route_control_proof: ChunkRouteControlProof,
    pub expected_route_control_checksum: String,
    pub raft_entry_checksum: String,
}
