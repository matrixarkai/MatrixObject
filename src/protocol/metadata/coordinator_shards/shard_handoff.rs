// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedMetadataShardRecordHandoff {
    pub source_node_id: String,
    pub shard: MetadataShard,
    pub marker: Option<String>,
    pub next_marker: Option<String>,
    pub limit: usize,
    pub max_payload_bytes: u64,
    pub include_deleted: bool,
    pub records: Vec<MetadataRecord>,
    pub scanned_records: u64,
    pub payload_bytes: u64,
    pub truncated: bool,
    pub source_commit_index: u64,
    pub source_read_index: u64,
    pub source_shard_topology_epoch: u64,
    pub source_shard_topology_checksum: String,
    pub handoff_token: String,
    pub handoff_batch_checksum: String,
    pub apply_mutation_id: String,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub raft_entry_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataShardRecordHandoffApplyReport {
    pub mutation_id: String,
    pub mutation_request_checksum: String,
    pub mutation_commit_index: u64,
    pub next_log_index: u64,
    pub committed: bool,
    pub prepared: bool,
    pub external_raft_required: bool,
    pub deduped: bool,
    pub source_node_id: String,
    pub handoff_token: String,
    pub handoff_batch_checksum: String,
    pub raft_entry_checksum: String,
    pub source_commit_index: u64,
    pub source_read_index: u64,
    pub source_shard_topology_epoch: u64,
    pub source_shard_topology_checksum: String,
    pub record_count: u64,
    pub scanned_records: u64,
    pub payload_bytes: u64,
    pub truncated: bool,
    pub marker: Option<String>,
    pub next_marker: Option<String>,
    pub shard: MetadataShard,
    pub receipt: MetadataBatchReceipt,
    pub metadata: ObjectMetadata,
}

impl MetadataShardRecordHandoffApplyReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .shard
            .to_control_fields(&format!("{field_prefix}shard."));
        for (name, value) in [
            ("mutation_commit_index", self.mutation_commit_index),
            ("next_log_index", self.next_log_index),
            ("source_commit_index", self.source_commit_index),
            ("source_read_index", self.source_read_index),
            (
                "source_shard_topology_epoch",
                self.source_shard_topology_epoch,
            ),
            ("record_count", self.record_count),
            ("scanned_records", self.scanned_records),
            ("payload_bytes", self.payload_bytes),
            (
                "receipt_first_commit_index",
                self.receipt.first_commit_index,
            ),
            ("receipt_last_commit_index", self.receipt.last_commit_index),
            ("receipt_record_count", self.receipt.records.len() as u64),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        let receipt_proof_checksum = self.receipt.proof_checksum();
        for (name, value) in [
            ("mutation_id", self.mutation_id.as_str()),
            (
                "mutation_request_checksum",
                self.mutation_request_checksum.as_str(),
            ),
            ("source_node_id", self.source_node_id.as_str()),
            ("handoff_token", self.handoff_token.as_str()),
            (
                "handoff_batch_checksum",
                self.handoff_batch_checksum.as_str(),
            ),
            ("raft_entry_checksum", self.raft_entry_checksum.as_str()),
            (
                "source_shard_topology_checksum",
                self.source_shard_topology_checksum.as_str(),
            ),
            ("receipt_proof_checksum", receipt_proof_checksum.as_str()),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
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
            format!("{field_prefix}truncated"),
            self.truncated.to_string(),
        );
        fields.insert(
            format!("{field_prefix}marker"),
            self.marker.clone().unwrap_or_default(),
        );
        fields.insert(
            format!("{field_prefix}next_marker"),
            self.next_marker.clone().unwrap_or_default(),
        );
        fields.insert(
            format!("{field_prefix}receipt_expected_owner"),
            self.receipt.expected_owner.clone(),
        );
        fields.insert(
            format!("{field_prefix}receipt_expected_epoch"),
            self.receipt.expected_epoch.to_string(),
        );
        fields.extend(
            self.metadata
                .to_control_fields(&format!("{field_prefix}metadata.")),
        );
        fields
    }
}
