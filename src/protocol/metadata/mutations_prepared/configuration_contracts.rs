// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedMetadataTransaction {
    pub receipt: MetadataTransactionReceipt,
    pub metadata_control_proof_format: String,
    pub metadata_control_proof_checksum: String,
    pub metadata_control_proof_bytes: u64,
    pub metadata_control_proof_now_ms: u64,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub raft_entry_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedMetadataTransactionGc {
    pub now_ms: u64,
    pub max_transactions: usize,
    pub min_age_ms: u64,
    pub report: MetadataTransactionGcReport,
    pub removed_transactions: Vec<MetadataTransaction>,
    pub request_checksum: String,
    pub metadata_control_proof_format: String,
    pub metadata_control_proof_checksum: String,
    pub metadata_control_proof_bytes: u64,
    pub metadata_control_proof_now_ms: u64,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub raft_entry_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedMetadataChangeCheckpoint {
    pub checkpoint: MetadataChangeCheckpoint,
    pub idempotency_key: String,
    pub request_checksum: String,
    pub metadata_control_proof_format: String,
    pub metadata_control_proof_checksum: String,
    pub metadata_control_proof_bytes: u64,
    pub metadata_control_proof_now_ms: u64,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub raft_entry_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedMetadataChangefeedCompaction {
    pub prefix: String,
    pub max_changes: usize,
    pub report: MetadataChangefeedCompactionReport,
    pub compacted: Vec<MetadataChange>,
    pub request_checksum: String,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub raft_entry_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedMetadataTombstoneCompaction {
    pub prefix: String,
    pub after_key: Option<String>,
    pub max_records: usize,
    pub max_compact_bytes: u64,
    pub report: MetadataTombstoneCompactionReport,
    pub compacted: Vec<MetadataRecord>,
    pub request_checksum: String,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub raft_entry_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedMetadataPrefixDelete {
    pub prefix: String,
    pub max_records: usize,
    pub mutations: Vec<MetadataMutation>,
    pub images: Vec<MetadataRecord>,
    pub lease_owner: Option<String>,
    pub lease_epoch: Option<u64>,
    pub now_ms: u64,
    pub idempotency_key: String,
    pub request_checksum: String,
    pub metadata_control_proof_format: String,
    pub metadata_control_proof_checksum: String,
    pub metadata_control_proof_bytes: u64,
    pub metadata_control_proof_now_ms: u64,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub raft_entry_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedMetadataSnapshotCompaction {
    pub mutation_id: String,
    pub snapshot_commit_index: u64,
    pub max_change_records: usize,
    pub max_batch_receipts: usize,
    pub report: MetadataSnapshotCompactionReport,
    pub before_state_fingerprint: String,
    pub snapshot: MetaSnapshot,
    pub request_checksum: String,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub raft_entry_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedMetadataRetentionConfig {
    pub change_log_retention_limit: usize,
    pub mutation_receipt_retention_limit: usize,
    pub report: MetadataRetentionConfigReport,
    pub before_state_fingerprint: String,
    pub metadata_control_proof_format: String,
    pub metadata_control_proof_checksum: String,
    pub metadata_control_proof_bytes: u64,
    pub metadata_control_proof_now_ms: u64,
    pub request_checksum: String,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub raft_entry_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedMetadataAdmissionPolicy {
    pub policy: MetadataAdmissionPolicy,
    pub before_state_fingerprint: String,
    pub metadata_control_proof_format: String,
    pub metadata_control_proof_checksum: String,
    pub metadata_control_proof_bytes: u64,
    pub metadata_control_proof_now_ms: u64,
    pub request_checksum: String,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub raft_entry_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedMetadataQosConfig {
    pub config: MetadataQosConfig,
    pub before_state_fingerprint: String,
    pub metadata_control_proof_format: String,
    pub metadata_control_proof_checksum: String,
    pub metadata_control_proof_bytes: u64,
    pub metadata_control_proof_now_ms: u64,
    pub request_checksum: String,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub raft_entry_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataRouteCacheConfig {
    pub entries: usize,
    pub ttl_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedMetadataRouteCacheConfig {
    pub config: MetadataRouteCacheConfig,
    pub before_state_fingerprint: String,
    pub after_state_fingerprint: String,
    pub metadata_control_proof_format: String,
    pub metadata_control_proof_checksum: String,
    pub metadata_control_proof_bytes: u64,
    pub metadata_control_proof_now_ms: u64,
    pub request_checksum: String,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub raft_entry_checksum: String,
}

pub(crate) fn append_prepared_metadata_common_fields(
    fields: &mut BTreeMap<String, String>,
    field_prefix: &str,
    metadata_control_proof_checksum: &str,
    metadata_control_proof_now_ms: u64,
    request_checksum: &str,
    expected_previous_commit_index: u64,
    prepared_commit_index: u64,
    raft_entry_checksum: &str,
) {
    for (name, value) in [
        (
            "metadata_control_proof_now_ms",
            metadata_control_proof_now_ms,
        ),
        (
            "expected_previous_commit_index",
            expected_previous_commit_index,
        ),
        ("prepared_commit_index", prepared_commit_index),
    ] {
        fields.insert(format!("{field_prefix}{name}"), value.to_string());
    }
    for (name, value) in [
        (
            "metadata_control_proof_checksum",
            metadata_control_proof_checksum,
        ),
        ("request_checksum", request_checksum),
        ("raft_entry_checksum", raft_entry_checksum),
    ] {
        fields.insert(format!("{field_prefix}{name}"), value.to_string());
    }
}

pub(crate) fn append_prepared_metadata_control_proof_shape_fields(
    fields: &mut BTreeMap<String, String>,
    field_prefix: &str,
    metadata_control_proof_format: &str,
    metadata_control_proof_bytes: u64,
) {
    fields.insert(
        format!("{field_prefix}metadata_control_proof_format"),
        metadata_control_proof_format.to_string(),
    );
    fields.insert(
        format!("{field_prefix}metadata_control_proof_bytes"),
        metadata_control_proof_bytes.to_string(),
    );
}

