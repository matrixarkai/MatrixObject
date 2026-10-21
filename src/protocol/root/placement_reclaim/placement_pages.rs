// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl RootPlacementPage {
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
        append_optional_string_field(
            &mut fields,
            field_prefix,
            "node_id_filter",
            &self.node_id_filter,
        );
        for (name, value) in [
            ("placement_count", self.placements.len() as u64),
            ("scanned_objects", self.scanned_objects),
            ("skipped_objects", self.skipped_objects),
            ("payload_bytes", self.payload_bytes),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(format!("{field_prefix}has_more"), self.has_more.to_string());
        for (index, placement) in self.placements.iter().enumerate() {
            fields
                .extend(placement.to_control_fields(&format!("{field_prefix}placement.{index}.")));
        }
        fields
    }
}

impl RootNodeUsage {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self.node.to_control_fields(&format!("{field_prefix}node."));
        for (name, value) in [
            ("registered", self.registered),
            ("capacity_pressure", self.capacity_pressure),
            ("reclaim_pressure", self.reclaim_pressure),
            ("placement_eligible", self.placement_eligible),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        for (name, value) in [
            ("object_count", self.object_count),
            ("referenced_bytes", self.referenced_bytes),
            ("free_bytes", self.free_bytes),
            ("reported_free_bytes", self.reported_free_bytes),
            (
                "committed_vs_reported_delta_bytes",
                self.committed_vs_reported_delta_bytes,
            ),
            ("reclaimable_bytes", self.reclaimable_bytes),
            ("overcommitted_bytes", self.overcommitted_bytes),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields
    }
}

impl RootNodeUsagePage {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        append_optional_string_field(&mut fields, field_prefix, "marker", &self.marker);
        append_optional_string_field(&mut fields, field_prefix, "next_marker", &self.next_marker);
        append_optional_string_field(
            &mut fields,
            field_prefix,
            "service_kind_filter",
            &self.service_kind_filter,
        );
        for (name, value) in [
            ("draining_filter", self.draining_filter),
            ("online_filter", self.online_filter),
            ("capacity_pressure_filter", self.capacity_pressure_filter),
            ("reclaim_pressure_filter", self.reclaim_pressure_filter),
            ("placement_eligible_filter", self.placement_eligible_filter),
        ] {
            if let Some(value) = value {
                fields.insert(format!("{field_prefix}{name}"), value.to_string());
            }
        }
        for (name, value) in [
            ("node_count", self.nodes.len() as u64),
            ("min_free_bytes", self.min_free_bytes),
            ("min_reclaimable_bytes", self.min_reclaimable_bytes),
            ("total_node_count", self.total_node_count),
            ("registered_node_count", self.registered_node_count),
            ("referenced_node_count", self.referenced_node_count),
            ("scanned_nodes", self.scanned_nodes),
            ("skipped_nodes", self.skipped_nodes),
            (
                "unregistered_referenced_nodes",
                self.unregistered_referenced_nodes,
            ),
            (
                "total_referenced_object_bytes",
                self.total_referenced_object_bytes,
            ),
            (
                "total_replicated_object_bytes",
                self.total_replicated_object_bytes,
            ),
            ("reclaim_pressure_nodes", self.reclaim_pressure_nodes),
            ("total_capacity_bytes", self.total_capacity_bytes),
            ("total_used_bytes", self.total_used_bytes),
            ("total_committed_bytes", self.total_committed_bytes),
            ("total_free_bytes", self.total_free_bytes),
            ("total_reported_free_bytes", self.total_reported_free_bytes),
            ("total_reclaimable_bytes", self.total_reclaimable_bytes),
            ("total_overcommitted_bytes", self.total_overcommitted_bytes),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(format!("{field_prefix}has_more"), self.has_more.to_string());
        for (index, node) in self.nodes.iter().enumerate() {
            fields.extend(node.to_control_fields(&format!("{field_prefix}node.{index}.")));
        }
        fields
    }
}

