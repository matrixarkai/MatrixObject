impl PreparedRootPlacementRepairEnvelope {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("max_objects", self.max_objects),
            ("expected_replica_count", self.expected_replica_count),
            ("min_free_bytes", self.min_free_bytes),
            ("max_payload_bytes", self.max_payload_bytes),
            ("scanned_objects", self.scanned_objects),
            ("repair_objects", self.repair_objects),
            ("repaired_objects", self.repaired_objects),
            ("skipped_objects", self.skipped_objects),
            ("failed_repairs", self.failed_repairs),
            ("bytes_to_repair", self.bytes_to_repair),
            ("bytes_repaired", self.bytes_repaired),
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
            ("repair_needed", self.repair_needed),
            ("repair_can_apply", self.repair_can_apply),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(
            format!("{field_prefix}repair_kind"),
            self.repair_kind.clone(),
        );
        fields.insert(
            format!("{field_prefix}repair_reason"),
            self.repair_reason.clone(),
        );
        fields.insert(
            format!("{field_prefix}first_source_node_id"),
            self.first_source_node_id.clone(),
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
        if let Some(marker) = &self.marker {
            fields.extend(marker.to_control_fields(&format!("{field_prefix}marker.")));
        }
        if let Some(next_marker) = &self.next_marker {
            fields.extend(next_marker.to_control_fields(&format!("{field_prefix}next_marker.")));
        }
        if let Some(first_object) = &self.first_object {
            fields.extend(first_object.to_control_fields(&format!("{field_prefix}first_object.")));
        }
        fields
    }
}

impl ProvenPreparedRootPlacementRepairEnvelope {
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

