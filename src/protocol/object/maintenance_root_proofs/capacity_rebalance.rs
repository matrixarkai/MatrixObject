// SPDX-License-Identifier: Apache-2.0
impl PreparedRootCapacityRebalanceEnvelope {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("max_objects", self.max_objects),
            ("min_free_bytes", self.min_free_bytes),
            ("pressure_node_count", self.pressure_node_count),
            ("scanned_objects", self.scanned_objects),
            ("rebalance_objects", self.rebalance_objects),
            ("rebalanced_objects", self.rebalanced_objects),
            ("skipped_objects", self.skipped_objects),
            ("failed_rebalances", self.failed_rebalances),
            ("bytes_to_rebalance", self.bytes_to_rebalance),
            ("bytes_rebalanced", self.bytes_rebalanced),
            ("planned_item_count", self.planned_item_count),
            (
                "expected_previous_commit_index",
                self.expected_previous_commit_index,
            ),
            ("prepared_commit_index", self.prepared_commit_index),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        for (name, value) in [
            ("has_more", self.has_more),
            ("rebalance_needed", self.rebalance_needed),
            ("rebalance_can_apply", self.rebalance_can_apply),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(
            format!("{field_prefix}rebalance_reason"),
            self.rebalance_reason.clone(),
        );
        fields.insert(
            format!("{field_prefix}first_pressure_node_id"),
            self.first_pressure_node_id.clone(),
        );
        fields.insert(
            format!("{field_prefix}first_target_node_id"),
            self.first_target_node_id.clone(),
        );
        fields.insert(
            format!("{field_prefix}before_state_fingerprint"),
            self.before_state_fingerprint.clone(),
        );
        fields.insert(
            format!("{field_prefix}request_checksum"),
            self.request_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}raft_entry_checksum"),
            self.raft_entry_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}envelope_checksum"),
            self.envelope_checksum.clone(),
        );
        if let Some(first_object) = &self.first_object {
            fields.extend(first_object.to_control_fields(&format!("{field_prefix}first_object.")));
        }
        fields
    }
}

impl ProvenPreparedRootCapacityRebalanceEnvelope {
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

