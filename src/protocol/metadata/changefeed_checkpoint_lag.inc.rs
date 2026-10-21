// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataChangeCheckpointLagItem {
    pub checkpoint: MetadataChangeCheckpoint,
    pub change_lag: u64,
    pub commit_lag: u64,
    pub behind_target: bool,
    pub retained_window_compacted: bool,
    pub first_retained_change_sequence: u64,
    pub first_retained_commit_index: u64,
    pub target_change_sequence: u64,
    pub target_commit_index: u64,
}

impl MetadataChangeCheckpointLagItem {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .checkpoint
            .to_control_fields(&format!("{field_prefix}checkpoint."));
        for (name, value) in [
            ("change_lag", self.change_lag),
            ("commit_lag", self.commit_lag),
            (
                "first_retained_change_sequence",
                self.first_retained_change_sequence,
            ),
            (
                "first_retained_commit_index",
                self.first_retained_commit_index,
            ),
            ("target_change_sequence", self.target_change_sequence),
            ("target_commit_index", self.target_commit_index),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(
            format!("{field_prefix}behind_target"),
            self.behind_target.to_string(),
        );
        fields.insert(
            format!("{field_prefix}retained_window_compacted"),
            self.retained_window_compacted.to_string(),
        );
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataChangeCheckpointLagPlan {
    pub items: Vec<MetadataChangeCheckpointLagItem>,
    pub prefix: String,
    pub marker: Option<String>,
    pub next_marker: Option<String>,
    pub scanned_checkpoints: u64,
    pub matched_checkpoints: u64,
    pub checkpoint_count: u64,
    pub target_change_sequence: u64,
    pub target_commit_index: u64,
    pub first_retained_change_sequence: u64,
    pub last_retained_change_sequence: u64,
    pub first_retained_commit_index: u64,
    pub last_retained_commit_index: u64,
    pub compacted_change_count: u64,
    pub max_change_lag: u64,
    pub max_commit_lag: u64,
    pub retained_window_compacted_count: u64,
    pub payload_bytes: u64,
    pub max_payload_bytes: u64,
    pub budget_limited: bool,
    pub truncated: bool,
    pub lag_plan_needed: bool,
    pub lag_plan_can_apply: bool,
    pub lag_plan_reason: String,
    pub plan_checksum: String,
    pub expected_plan_checksum: String,
}

impl MetadataChangeCheckpointLagPlan {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(format!("{field_prefix}prefix"), self.prefix.clone());
        append_optional_string_field(&mut fields, field_prefix, "marker", &self.marker);
        append_optional_string_field(&mut fields, field_prefix, "next_marker", &self.next_marker);
        for (name, value) in [
            ("item_count", self.items.len() as u64),
            ("scanned_checkpoints", self.scanned_checkpoints),
            ("matched_checkpoints", self.matched_checkpoints),
            ("checkpoint_count", self.checkpoint_count),
            ("target_change_sequence", self.target_change_sequence),
            ("target_commit_index", self.target_commit_index),
            (
                "first_retained_change_sequence",
                self.first_retained_change_sequence,
            ),
            (
                "last_retained_change_sequence",
                self.last_retained_change_sequence,
            ),
            (
                "first_retained_commit_index",
                self.first_retained_commit_index,
            ),
            (
                "last_retained_commit_index",
                self.last_retained_commit_index,
            ),
            ("compacted_change_count", self.compacted_change_count),
            ("max_change_lag", self.max_change_lag),
            ("max_commit_lag", self.max_commit_lag),
            (
                "retained_window_compacted_count",
                self.retained_window_compacted_count,
            ),
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
        fields.insert(
            format!("{field_prefix}lag_plan_needed"),
            self.lag_plan_needed.to_string(),
        );
        fields.insert(
            format!("{field_prefix}lag_plan_can_apply"),
            self.lag_plan_can_apply.to_string(),
        );
        fields.insert(
            format!("{field_prefix}lag_plan_reason"),
            self.lag_plan_reason.clone(),
        );
        fields.insert(
            format!("{field_prefix}plan_checksum"),
            self.plan_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}expected_plan_checksum"),
            self.expected_plan_checksum.clone(),
        );
        for (index, item) in self.items.iter().enumerate() {
            fields.extend(item.to_control_fields(&format!("{field_prefix}item.{index}.")));
        }
        fields
    }
}
