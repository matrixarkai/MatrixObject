// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataChangeCheckpoint {
    pub consumer_id: String,
    pub prefix: String,
    pub after_change_sequence: u64,
    pub after_commit_index: u64,
    pub update_time_ms: u64,
    pub owner: String,
    pub note: String,
}

impl MetadataChangeCheckpoint {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}consumer_id"),
            self.consumer_id.clone(),
        );
        fields.insert(format!("{field_prefix}prefix"), self.prefix.clone());
        fields.insert(
            format!("{field_prefix}after_change_sequence"),
            self.after_change_sequence.to_string(),
        );
        fields.insert(
            format!("{field_prefix}after_commit_index"),
            self.after_commit_index.to_string(),
        );
        fields.insert(
            format!("{field_prefix}update_time_ms"),
            self.update_time_ms.to_string(),
        );
        fields.insert(format!("{field_prefix}owner"), self.owner.clone());
        fields.insert(format!("{field_prefix}note"), self.note.clone());
        fields
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetadataChangeCheckpointPage {
    pub checkpoints: Vec<MetadataChangeCheckpoint>,
    pub prefix: String,
    pub marker: Option<String>,
    pub next_marker: Option<String>,
    pub checkpoint_count: u64,
    pub matched_count: u64,
    pub scanned_count: u64,
    pub total_checkpoint_count: u64,
    pub payload_bytes: u64,
    pub max_payload_bytes: u64,
    pub budget_limited: bool,
    pub truncated: bool,
}

impl MetadataChangeCheckpointPage {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(format!("{field_prefix}prefix"), self.prefix.clone());
        append_optional_string_field(&mut fields, field_prefix, "marker", &self.marker);
        append_optional_string_field(&mut fields, field_prefix, "next_marker", &self.next_marker);
        for (name, value) in [
            ("checkpoint_count", self.checkpoint_count),
            ("matched_count", self.matched_count),
            ("scanned_count", self.scanned_count),
            ("total_checkpoint_count", self.total_checkpoint_count),
            ("payload_bytes", self.payload_bytes),
            ("max_payload_bytes", self.max_payload_bytes),
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
        for (index, checkpoint) in self.checkpoints.iter().enumerate() {
            fields.extend(
                checkpoint.to_control_fields(&format!("{field_prefix}checkpoint.{index}.")),
            );
        }
        fields
    }
}
