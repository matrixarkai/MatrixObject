// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetadataTransactionGcReport {
    pub scanned_transactions: u64,
    pub compacted_transactions: u64,
    pub skipped_transactions: u64,
    pub removed_transaction_ids: Vec<String>,
}

impl MetadataTransactionGcReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("scanned_transactions", self.scanned_transactions),
            ("compacted_transactions", self.compacted_transactions),
            ("skipped_transactions", self.skipped_transactions),
            (
                "removed_transaction_count",
                self.removed_transaction_ids.len() as u64,
            ),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        for (index, transaction_id) in self.removed_transaction_ids.iter().enumerate() {
            fields.insert(
                format!("{field_prefix}removed_transaction.{index}"),
                transaction_id.clone(),
            );
        }
        fields
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetadataChangefeedCompactionReport {
    pub safe_sequence: u64,
    pub scanned_changes: u64,
    pub compacted_changes: u64,
    pub retained_changes: u64,
    pub remaining_changes: u64,
}

impl MetadataChangefeedCompactionReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("safe_sequence", self.safe_sequence),
            ("scanned_changes", self.scanned_changes),
            ("compacted_changes", self.compacted_changes),
            ("retained_changes", self.retained_changes),
            ("remaining_changes", self.remaining_changes),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetadataTombstoneCompactionReport {
    pub safe_sequence: u64,
    pub scanned_records: u64,
    pub compactable_records: u64,
    pub compacted_records: u64,
    pub skipped_records: u64,
    pub compacted_bytes: u64,
    pub budget_limited: bool,
    pub truncated: bool,
    pub next_key_marker: Option<String>,
    pub compacted_tombstone_count: u64,
    pub last_tombstone_compaction_commit_index: u64,
}

impl MetadataTombstoneCompactionReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("safe_sequence", self.safe_sequence),
            ("scanned_records", self.scanned_records),
            ("compactable_records", self.compactable_records),
            ("compacted_records", self.compacted_records),
            ("skipped_records", self.skipped_records),
            ("compacted_bytes", self.compacted_bytes),
            ("compacted_tombstone_count", self.compacted_tombstone_count),
            (
                "last_tombstone_compaction_commit_index",
                self.last_tombstone_compaction_commit_index,
            ),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(
            format!("{field_prefix}budget_limited"),
            self.budget_limited.to_string(),
        );
        fields.insert(
            format!("{field_prefix}truncated"),
            self.truncated.to_string(),
        );
        fields.insert(
            format!("{field_prefix}next_key_marker"),
            self.next_key_marker.clone().unwrap_or_default(),
        );
        fields
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetadataSnapshotCompactionReport {
    pub raft_snapshot_floor_commit_index: u64,
    pub compacted_change_records: u64,
    pub compacted_batch_receipts: u64,
    pub compacted_lease_receipts: u64,
    pub compacted_coordinator_config_receipts: u64,
    pub compacted_coordinator_session_receipts: u64,
    pub compacted_shard_receipts: u64,
    pub compacted_transaction_receipts: u64,
    pub raft_snapshot_compacted_change_count: u64,
    pub raft_snapshot_compacted_batch_receipt_count: u64,
    pub remaining_changes: u64,
    pub remaining_batch_receipts: u64,
    pub remaining_lease_receipts: u64,
    pub remaining_coordinator_config_receipts: u64,
    pub remaining_coordinator_session_receipts: u64,
    pub remaining_shard_receipts: u64,
    pub remaining_transaction_receipts: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetadataSnapshotCompactionApplyReport {
    pub mutation_id: String,
    pub mutation_request_checksum: String,
    pub mutation_commit_index: u64,
    pub next_log_index: u64,
    pub committed: bool,
    pub prepared: bool,
    pub external_raft_required: bool,
    pub deduped: bool,
    pub metadata: Option<ObjectMetadata>,
    pub compaction: MetadataSnapshotCompactionReport,
}

impl MetadataSnapshotCompactionApplyReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .compaction
            .to_control_fields(&format!("{field_prefix}compaction."));
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
        if let Some(metadata) = &self.metadata {
            fields.extend(metadata.to_control_fields(&format!("{field_prefix}metadata.")));
        }
        fields
    }
}

impl MetadataSnapshotCompactionReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            (
                "raft_snapshot_floor_commit_index",
                self.raft_snapshot_floor_commit_index,
            ),
            ("compacted_change_records", self.compacted_change_records),
            ("compacted_batch_receipts", self.compacted_batch_receipts),
            ("compacted_lease_receipts", self.compacted_lease_receipts),
            (
                "compacted_coordinator_config_receipts",
                self.compacted_coordinator_config_receipts,
            ),
            (
                "compacted_coordinator_session_receipts",
                self.compacted_coordinator_session_receipts,
            ),
            ("compacted_shard_receipts", self.compacted_shard_receipts),
            (
                "compacted_transaction_receipts",
                self.compacted_transaction_receipts,
            ),
            (
                "raft_snapshot_compacted_change_count",
                self.raft_snapshot_compacted_change_count,
            ),
            (
                "raft_snapshot_compacted_batch_receipt_count",
                self.raft_snapshot_compacted_batch_receipt_count,
            ),
            ("remaining_changes", self.remaining_changes),
            ("remaining_batch_receipts", self.remaining_batch_receipts),
            ("remaining_lease_receipts", self.remaining_lease_receipts),
            (
                "remaining_coordinator_config_receipts",
                self.remaining_coordinator_config_receipts,
            ),
            (
                "remaining_coordinator_session_receipts",
                self.remaining_coordinator_session_receipts,
            ),
            ("remaining_shard_receipts", self.remaining_shard_receipts),
            (
                "remaining_transaction_receipts",
                self.remaining_transaction_receipts,
            ),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields
    }
}

