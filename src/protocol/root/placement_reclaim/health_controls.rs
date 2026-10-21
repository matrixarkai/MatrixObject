// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl RootPlacementHealthItem {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .placement
            .to_control_fields(&format!("{field_prefix}placement."));
        for (name, ids) in [
            ("missing_node", &self.missing_node_ids),
            ("offline_node", &self.offline_node_ids),
            ("draining_node", &self.draining_node_ids),
            ("capacity_pressure_node", &self.capacity_pressure_node_ids),
        ] {
            fields.insert(format!("{field_prefix}{name}_count"), ids.len().to_string());
            for (index, node_id) in ids.iter().enumerate() {
                fields.insert(format!("{field_prefix}{name}.{index}"), node_id.clone());
            }
        }
        fields.insert(
            format!("{field_prefix}under_replicated"),
            self.under_replicated.to_string(),
        );
        fields.insert(format!("{field_prefix}healthy"), self.healthy.to_string());
        fields
    }
}

impl RootPlacementHealthPage {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}marker_present"),
            self.marker.is_some().to_string(),
        );
        if let Some(marker) = &self.marker {
            fields.extend(marker.to_control_fields(&format!("{field_prefix}marker.")));
        }
        fields.insert(
            format!("{field_prefix}next_marker_present"),
            self.next_marker.is_some().to_string(),
        );
        if let Some(next_marker) = &self.next_marker {
            fields.extend(next_marker.to_control_fields(&format!("{field_prefix}next_marker.")));
        }
        for (name, value) in [
            ("degraded_only", self.degraded_only),
            ("budget_limited", self.budget_limited),
            ("has_more", self.has_more),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        for (name, value) in [
            ("item_count", self.items.len() as u64),
            ("expected_replica_count", self.expected_replica_count as u64),
            ("min_free_bytes", self.min_free_bytes),
            ("scanned_objects", self.scanned_objects),
            ("skipped_objects", self.skipped_objects),
            ("healthy_objects", self.healthy_objects),
            ("degraded_objects", self.degraded_objects),
            ("under_replicated_objects", self.under_replicated_objects),
            ("missing_node_references", self.missing_node_references),
            ("offline_node_references", self.offline_node_references),
            ("draining_node_references", self.draining_node_references),
            (
                "capacity_pressure_references",
                self.capacity_pressure_references,
            ),
            ("payload_bytes", self.payload_bytes),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        for (index, item) in self.items.iter().enumerate() {
            fields.extend(item.to_control_fields(&format!("{field_prefix}item.{index}.")));
        }
        fields
    }
}

