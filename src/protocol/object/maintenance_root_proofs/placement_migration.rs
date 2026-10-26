// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl PreparedPlacementMigrationEnvelope {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .object_id
            .to_control_fields(&format!("{field_prefix}object."));
        for (name, value) in [
            ("version", self.version),
            ("length", self.length),
            ("min_free_bytes", self.min_free_bytes),
            ("placement_read_index", self.placement_read_index),
            ("placement_raft_term", self.placement_raft_term),
            (
                "expected_previous_commit_index",
                self.expected_previous_commit_index,
            ),
            ("prepared_commit_index", self.prepared_commit_index),
            ("before_node_count", self.before_node_ids.len() as u64),
            ("after_node_count", self.after_node_ids.len() as u64),
            ("envelope_bytes", self.envelope_bytes),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        for (name, value) in [
            ("source_node_id", &self.source_node_id),
            ("target_node_id", &self.target_node_id),
            ("reason", &self.reason),
            ("expected_plan_checksum", &self.expected_plan_checksum),
            ("before_state_fingerprint", &self.before_state_fingerprint),
            ("root_read_proof_checksum", &self.root_read_proof_checksum),
            ("request_checksum", &self.request_checksum),
            ("raft_entry_checksum", &self.raft_entry_checksum),
            ("envelope_format", &self.envelope_format),
            ("envelope_checksum", &self.envelope_checksum),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.clone());
        }
        for (index, node_id) in self.before_node_ids.iter().enumerate() {
            fields.insert(
                format!("{field_prefix}before_node.{index}"),
                node_id.clone(),
            );
        }
        for (index, node_id) in self.after_node_ids.iter().enumerate() {
            fields.insert(format!("{field_prefix}after_node.{index}"), node_id.clone());
        }
        fields
    }
}

impl ProvenPreparedPlacementMigrationEnvelope {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .envelope
            .to_control_fields(&format!("{field_prefix}envelope."));
        fields.extend(
            self.proof
                .to_control_fields(&format!("{field_prefix}root_read.")),
        );
        fields
    }
}

impl CommittedPlacementMigrationEnvelope {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .object_id
            .to_control_fields(&format!("{field_prefix}object."));
        for (name, value) in [
            ("version", self.version),
            ("length", self.length),
            ("min_free_bytes", self.min_free_bytes),
            ("prepared_commit_index", self.prepared_commit_index),
            ("committed_raft_index", self.committed_raft_index),
            ("metadata_version", self.metadata_version),
            ("metadata_length", self.metadata_length),
            ("metadata_update_time_ms", self.metadata_update_time_ms),
            ("before_node_count", self.before_node_ids.len() as u64),
            ("after_node_count", self.after_node_ids.len() as u64),
            ("envelope_bytes", self.envelope_bytes),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        for (name, value) in [
            ("source_node_id", &self.source_node_id),
            ("target_node_id", &self.target_node_id),
            ("reason", &self.reason),
            ("expected_plan_checksum", &self.expected_plan_checksum),
            ("request_checksum", &self.request_checksum),
            ("raft_entry_checksum", &self.raft_entry_checksum),
            ("metadata_etag", &self.metadata_etag),
            ("metadata_content_type", &self.metadata_content_type),
            ("envelope_format", &self.envelope_format),
            ("envelope_checksum", &self.envelope_checksum),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.clone());
        }
        for (index, node_id) in self.before_node_ids.iter().enumerate() {
            fields.insert(
                format!("{field_prefix}before_node.{index}"),
                node_id.clone(),
            );
        }
        for (index, node_id) in self.after_node_ids.iter().enumerate() {
            fields.insert(format!("{field_prefix}after_node.{index}"), node_id.clone());
        }
        fields
    }
}

impl CommittedPlacementMigration {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .metadata
            .to_control_fields(&format!("{field_prefix}metadata."));
        fields.extend(
            self.envelope
                .to_control_fields(&format!("{field_prefix}envelope.")),
        );
        fields
    }
}

