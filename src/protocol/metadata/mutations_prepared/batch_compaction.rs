// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedMetadataBatch {
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

impl PreparedMetadataBatch {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}mutation_count"),
            self.mutations.len().to_string(),
        );
        fields.insert(
            format!("{field_prefix}image_count"),
            self.images.len().to_string(),
        );
        fields.insert(
            format!("{field_prefix}lease_owner"),
            self.lease_owner.clone().unwrap_or_default(),
        );
        fields.insert(
            format!("{field_prefix}lease_epoch"),
            self.lease_epoch.unwrap_or_default().to_string(),
        );
        fields.insert(format!("{field_prefix}now_ms"), self.now_ms.to_string());
        fields.insert(
            format!("{field_prefix}idempotency_key"),
            self.idempotency_key.clone(),
        );
        fields.insert(
            format!("{field_prefix}request_checksum"),
            self.request_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}metadata_control_proof_format"),
            self.metadata_control_proof_format.clone(),
        );
        fields.insert(
            format!("{field_prefix}metadata_control_proof_checksum"),
            self.metadata_control_proof_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}metadata_control_proof_bytes"),
            self.metadata_control_proof_bytes.to_string(),
        );
        fields.insert(
            format!("{field_prefix}metadata_control_proof_now_ms"),
            self.metadata_control_proof_now_ms.to_string(),
        );
        fields.insert(
            format!("{field_prefix}expected_previous_commit_index"),
            self.expected_previous_commit_index.to_string(),
        );
        fields.insert(
            format!("{field_prefix}prepared_commit_index"),
            self.prepared_commit_index.to_string(),
        );
        fields.insert(
            format!("{field_prefix}raft_entry_checksum"),
            self.raft_entry_checksum.clone(),
        );
        fields
    }
}

impl PreparedMetadataChangeCheckpoint {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .checkpoint
            .to_control_fields(&format!("{field_prefix}checkpoint."));
        fields.extend(self.checkpoint.to_control_fields(field_prefix));
        fields.insert(
            format!("{field_prefix}idempotency_key"),
            self.idempotency_key.clone(),
        );
        fields.insert(
            format!("{field_prefix}request_checksum"),
            self.request_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}metadata_control_proof_format"),
            self.metadata_control_proof_format.clone(),
        );
        fields.insert(
            format!("{field_prefix}metadata_control_proof_checksum"),
            self.metadata_control_proof_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}metadata_control_proof_bytes"),
            self.metadata_control_proof_bytes.to_string(),
        );
        fields.insert(
            format!("{field_prefix}metadata_control_proof_now_ms"),
            self.metadata_control_proof_now_ms.to_string(),
        );
        fields.insert(
            format!("{field_prefix}expected_previous_commit_index"),
            self.expected_previous_commit_index.to_string(),
        );
        fields.insert(
            format!("{field_prefix}prepared_commit_index"),
            self.prepared_commit_index.to_string(),
        );
        fields.insert(
            format!("{field_prefix}raft_entry_checksum"),
            self.raft_entry_checksum.clone(),
        );
        fields
    }
}

impl PreparedMetadataPrefixDelete {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(format!("{field_prefix}prefix"), self.prefix.clone());
        fields.insert(
            format!("{field_prefix}max_records"),
            self.max_records.to_string(),
        );
        fields.insert(
            format!("{field_prefix}mutation_count"),
            self.mutations.len().to_string(),
        );
        fields.insert(
            format!("{field_prefix}image_count"),
            self.images.len().to_string(),
        );
        fields.insert(
            format!("{field_prefix}lease_owner"),
            self.lease_owner.clone().unwrap_or_default(),
        );
        fields.insert(
            format!("{field_prefix}lease_epoch"),
            self.lease_epoch.unwrap_or_default().to_string(),
        );
        fields.insert(format!("{field_prefix}now_ms"), self.now_ms.to_string());
        fields.insert(
            format!("{field_prefix}idempotency_key"),
            self.idempotency_key.clone(),
        );
        fields.insert(
            format!("{field_prefix}request_checksum"),
            self.request_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}metadata_control_proof_format"),
            self.metadata_control_proof_format.clone(),
        );
        fields.insert(
            format!("{field_prefix}metadata_control_proof_checksum"),
            self.metadata_control_proof_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}metadata_control_proof_bytes"),
            self.metadata_control_proof_bytes.to_string(),
        );
        fields.insert(
            format!("{field_prefix}metadata_control_proof_now_ms"),
            self.metadata_control_proof_now_ms.to_string(),
        );
        fields.insert(
            format!("{field_prefix}expected_previous_commit_index"),
            self.expected_previous_commit_index.to_string(),
        );
        fields.insert(
            format!("{field_prefix}prepared_commit_index"),
            self.prepared_commit_index.to_string(),
        );
        fields.insert(
            format!("{field_prefix}raft_entry_checksum"),
            self.raft_entry_checksum.clone(),
        );
        for (index, mutation) in self.mutations.iter().enumerate() {
            fields.extend(mutation.to_control_fields(&format!("{field_prefix}mutation.{index}.")));
        }
        for (index, image) in self.images.iter().enumerate() {
            fields.extend(image.to_control_fields(&format!("{field_prefix}image.{index}.")));
        }
        fields
    }
}

impl PreparedMetadataChangefeedCompaction {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .report
            .to_control_fields(&format!("{field_prefix}report."));
        fields.insert(format!("{field_prefix}prefix"), self.prefix.clone());
        fields.insert(
            format!("{field_prefix}max_changes"),
            self.max_changes.to_string(),
        );
        fields.insert(
            format!("{field_prefix}compacted_count"),
            self.compacted.len().to_string(),
        );
        fields.insert(
            format!("{field_prefix}request_checksum"),
            self.request_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}expected_previous_commit_index"),
            self.expected_previous_commit_index.to_string(),
        );
        fields.insert(
            format!("{field_prefix}prepared_commit_index"),
            self.prepared_commit_index.to_string(),
        );
        fields.insert(
            format!("{field_prefix}raft_entry_checksum"),
            self.raft_entry_checksum.clone(),
        );
        for (index, change) in self.compacted.iter().enumerate() {
            fields.extend(
                change.to_control_fields(&format!("{field_prefix}compacted_change.{index}.")),
            );
        }
        fields
    }
}

impl PreparedMetadataTombstoneCompaction {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .report
            .to_control_fields(&format!("{field_prefix}report."));
        fields.insert(format!("{field_prefix}prefix"), self.prefix.clone());
        fields.insert(
            format!("{field_prefix}after_key"),
            self.after_key.clone().unwrap_or_default(),
        );
        fields.insert(
            format!("{field_prefix}max_records"),
            self.max_records.to_string(),
        );
        fields.insert(
            format!("{field_prefix}max_compact_bytes"),
            self.max_compact_bytes.to_string(),
        );
        fields.insert(
            format!("{field_prefix}compacted_count"),
            self.compacted.len().to_string(),
        );
        fields.insert(
            format!("{field_prefix}request_checksum"),
            self.request_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}expected_previous_commit_index"),
            self.expected_previous_commit_index.to_string(),
        );
        fields.insert(
            format!("{field_prefix}prepared_commit_index"),
            self.prepared_commit_index.to_string(),
        );
        fields.insert(
            format!("{field_prefix}raft_entry_checksum"),
            self.raft_entry_checksum.clone(),
        );
        for (index, record) in self.compacted.iter().enumerate() {
            fields.extend(
                record.to_control_fields(&format!("{field_prefix}compacted_record.{index}.")),
            );
        }
        fields
    }
}

impl PreparedMetadataSnapshotCompaction {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .report
            .to_control_fields(&format!("{field_prefix}report."));
        fields.insert(
            format!("{field_prefix}mutation_id"),
            self.mutation_id.clone(),
        );
        fields.insert(
            format!("{field_prefix}snapshot_commit_index"),
            self.snapshot_commit_index.to_string(),
        );
        fields.insert(
            format!("{field_prefix}max_change_records"),
            self.max_change_records.to_string(),
        );
        fields.insert(
            format!("{field_prefix}max_batch_receipts"),
            self.max_batch_receipts.to_string(),
        );
        fields.insert(
            format!("{field_prefix}before_state_fingerprint"),
            self.before_state_fingerprint.clone(),
        );
        fields.insert(
            format!("{field_prefix}snapshot_checksum"),
            self.snapshot.checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}snapshot_next_commit_index"),
            self.snapshot.next_commit_index.to_string(),
        );
        fields.insert(
            format!("{field_prefix}snapshot_floor_commit_index"),
            self.snapshot.raft_snapshot_floor_commit_index.to_string(),
        );
        fields.insert(
            format!("{field_prefix}request_checksum"),
            self.request_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}expected_previous_commit_index"),
            self.expected_previous_commit_index.to_string(),
        );
        fields.insert(
            format!("{field_prefix}prepared_commit_index"),
            self.prepared_commit_index.to_string(),
        );
        fields.insert(
            format!("{field_prefix}raft_entry_checksum"),
            self.raft_entry_checksum.clone(),
        );
        fields
    }
}

