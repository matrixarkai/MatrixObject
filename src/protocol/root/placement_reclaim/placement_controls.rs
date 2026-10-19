// SPDX-License-Identifier: Apache-2.0
impl PlacementPlan {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .object_id
            .to_control_fields(&format!("{field_prefix}object."));
        for (name, value) in [
            ("length", self.length),
            ("node_count", self.node_ids.len() as u64),
            ("skipped_draining", self.skipped_draining),
            ("skipped_offline", self.skipped_offline),
            ("skipped_capacity", self.skipped_capacity),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        for (index, node_id) in self.node_ids.iter().enumerate() {
            fields.insert(format!("{field_prefix}node.{index}"), node_id.clone());
        }
        fields.insert(
            format!("{field_prefix}plan_checksum"),
            self.plan_checksum.clone(),
        );
        fields
    }
}

impl PlacementMigrationPlan {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .object_id
            .to_control_fields(&format!("{field_prefix}object."));
        for (name, value) in [
            ("version", self.version),
            ("length", self.length),
            ("before_node_count", self.before_node_ids.len() as u64),
            ("after_node_count", self.after_node_ids.len() as u64),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        for (name, value) in [
            ("source_node_id", &self.source_node_id),
            ("target_node_id", &self.target_node_id),
            ("reason", &self.reason),
            ("plan_checksum", &self.plan_checksum),
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

impl RootPlacementRepairItem {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        self.plan.to_control_fields(&format!("{field_prefix}plan."))
    }
}

impl RootPlacementRepairReport {
    pub fn plan_checksum(&self) -> String {
        let mut material = Vec::new();
        append_string(&mut material, "root_placement_repair_report.v1");
        append_optional_object_id(&mut material, self.marker.as_ref());
        append_optional_object_id(&mut material, self.next_marker.as_ref());
        append_string(&mut material, &self.has_more.to_string());
        append_u64(&mut material, self.scanned_objects);
        append_u64(&mut material, self.repair_objects);
        append_u64(&mut material, self.repaired_objects);
        append_u64(&mut material, self.skipped_objects);
        append_u64(&mut material, self.failed_repairs);
        append_u64(&mut material, self.bytes_to_repair);
        append_u64(&mut material, self.bytes_repaired);
        append_string(&mut material, &self.repair_needed.to_string());
        append_string(&mut material, &self.repair_can_apply.to_string());
        append_string(&mut material, &self.repair_reason);
        append_u64(&mut material, self.items.len() as u64);
        for item in &self.items {
            append_string(&mut material, &item.plan.plan_checksum);
        }
        append_string_list(&mut material, &self.errors);
        etag(&material)
    }

    pub fn verify_expected_plan_checksum(
        &self,
        expected_plan_checksum: &str,
    ) -> Result<(), ObjectError> {
        if expected_plan_checksum.is_empty() {
            return Ok(());
        }
        let actual = self.plan_checksum();
        if actual == expected_plan_checksum {
            return Ok(());
        }
        Err(ObjectError::PreconditionFailed(format!(
            "root placement repair plan checksum mismatch: expected {} got {}",
            expected_plan_checksum, actual
        )))
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(format!("{field_prefix}plan_checksum"), self.plan_checksum());
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
            ("scanned_objects", self.scanned_objects),
            ("repair_objects", self.repair_objects),
            ("repaired_objects", self.repaired_objects),
            ("skipped_objects", self.skipped_objects),
            ("failed_repairs", self.failed_repairs),
            ("bytes_to_repair", self.bytes_to_repair),
            ("bytes_repaired", self.bytes_repaired),
            ("item_count", self.items.len() as u64),
            ("error_count", self.errors.len() as u64),
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
            format!("{field_prefix}repair_reason"),
            self.repair_reason.clone(),
        );
        for (index, item) in self.items.iter().enumerate() {
            fields.extend(item.to_control_fields(&format!("{field_prefix}item.{index}.")));
        }
        for (index, error) in self.errors.iter().enumerate() {
            fields.insert(format!("{field_prefix}error.{index}"), error.clone());
        }
        fields
    }
}

impl RootPlacementRefreshReport {
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
            ("scanned_objects", self.scanned_objects),
            ("refreshed_objects", self.refreshed_objects),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(format!("{field_prefix}has_more"), self.has_more.to_string());
        fields
    }
}

impl RootPlacementRefreshRunReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .report
            .to_control_fields(&format!("{field_prefix}report."));
        fields.extend(
            self.metadata
                .to_control_fields(&format!("{field_prefix}metadata.")),
        );
        fields
    }
}

impl RootCapacityRebalanceItem {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self.plan.to_control_fields(&format!("{field_prefix}plan."));
        fields.insert(
            format!("{field_prefix}pressure_node_id"),
            self.pressure_node_id.clone(),
        );
        fields
    }
}

impl RootCapacityRebalanceReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("scanned_objects", self.scanned_objects),
            ("pressure_node_count", self.pressure_node_count),
            ("rebalance_objects", self.rebalance_objects),
            ("rebalanced_objects", self.rebalanced_objects),
            ("skipped_objects", self.skipped_objects),
            ("failed_rebalances", self.failed_rebalances),
            ("bytes_to_rebalance", self.bytes_to_rebalance),
            ("bytes_rebalanced", self.bytes_rebalanced),
            ("item_count", self.items.len() as u64),
            ("error_count", self.errors.len() as u64),
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
        for (index, item) in self.items.iter().enumerate() {
            fields.extend(item.to_control_fields(&format!("{field_prefix}item.{index}.")));
        }
        for (index, error) in self.errors.iter().enumerate() {
            fields.insert(format!("{field_prefix}error.{index}"), error.clone());
        }
        fields
    }
}

