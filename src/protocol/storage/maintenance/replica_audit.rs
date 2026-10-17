impl PageReplicaAuditPage {
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
            ("item_count", self.items.len() as u64),
            ("scanned_chunks", self.scanned_chunks),
            ("healthy_chunks", self.healthy_chunks),
            ("degraded_chunks", self.degraded_chunks),
            ("missing_replicas", self.missing_replicas),
            ("corrupt_replicas", self.corrupt_replicas),
            ("divergent_replicas", self.divergent_replicas),
            ("quarantined_replicas", self.quarantined_replicas),
            ("frozen_replicas", self.frozen_replicas),
            ("payload_bytes", self.payload_bytes),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        for (name, value) in [
            ("budget_limited", self.budget_limited),
            ("has_more", self.has_more),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        for (index, item) in self.items.iter().enumerate() {
            fields.extend(
                item.chunk_id
                    .to_control_fields(&format!("{field_prefix}item.{index}.chunk.")),
            );
            fields.insert(
                format!("{field_prefix}item.{index}.replica_index"),
                item.replica_index.to_string(),
            );
            fields.insert(
                format!("{field_prefix}item.{index}.status"),
                item.status.clone(),
            );
            for (name, value) in [
                ("logical_length", item.logical_length),
                ("write_sequence", item.write_sequence),
                ("discarded_range_count", item.discarded_range_count),
            ] {
                fields.insert(
                    format!("{field_prefix}item.{index}.{name}"),
                    value.to_string(),
                );
            }
            fields.insert(
                format!("{field_prefix}item.{index}.checksum"),
                item.checksum.clone(),
            );
            fields.insert(
                format!("{field_prefix}item.{index}.frozen"),
                item.frozen.to_string(),
            );
            fields.insert(
                format!("{field_prefix}item.{index}.quarantined"),
                item.quarantined.to_string(),
            );
        }
        fields
    }
}

impl PageReplicaAuditRepairReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .audit
            .to_control_fields(&format!("{field_prefix}audit."));
        for (name, value) in [
            ("candidate_chunks", self.candidate_chunks),
            ("repaired_chunks", self.repaired_chunks),
            ("unrepaired_chunks", self.unrepaired_chunks),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(
            format!("{field_prefix}repair_needed"),
            self.repair_needed.to_string(),
        );
        fields.insert(
            format!("{field_prefix}repair_can_apply"),
            self.repair_can_apply.to_string(),
        );
        fields.insert(
            format!("{field_prefix}repair_reason"),
            self.repair_reason.clone(),
        );
        fields.extend(
            self.stats
                .to_control_fields(&format!("{field_prefix}stats.")),
        );
        append_route_control_fields(
            &self.route_control_proof,
            &self.expected_route_control_checksum,
            field_prefix,
            &mut fields,
        );
        fields
    }
}

