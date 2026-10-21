// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl ObjectCapacityRebalanceItem {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .object
            .to_control_fields(&format!("{field_prefix}object."));
        fields.insert(
            format!("{field_prefix}pressure_node_id"),
            self.pressure_node_id.clone(),
        );
        fields.extend(self.plan.to_control_fields(&format!("{field_prefix}plan.")));
        fields
    }
}

impl ObjectCapacityRebalanceReport {
    pub fn plan_checksum(&self) -> String {
        let error_bytes = self
            .errors
            .iter()
            .map(|error| encoded_string_len(error))
            .sum::<usize>();
        let mut material = Vec::with_capacity(
            encoded_string_len("object_capacity_rebalance_plan.v1")
                + std::mem::size_of::<u64>() * 8
                + encoded_string_len(&self.truncated.to_string())
                + encoded_string_len(&self.rebalance_needed.to_string())
                + encoded_string_len(&self.rebalance_can_apply.to_string())
                + encoded_string_len(&self.rebalance_reason)
                + self.items.len().saturating_mul(224)
                + error_bytes
                + 256,
        );
        append_string(&mut material, "object_capacity_rebalance_plan.v1");
        append_u64(&mut material, self.checked_objects);
        append_u64(&mut material, self.rebalance_objects);
        append_u64(&mut material, self.bytes_to_rebalance);
        append_u64(&mut material, self.pressure_node_count);
        append_u64(&mut material, self.min_free_bytes);
        append_string(&mut material, &self.truncated.to_string());
        append_string(&mut material, &self.rebalance_needed.to_string());
        append_string(&mut material, &self.rebalance_can_apply.to_string());
        append_string(&mut material, &self.rebalance_reason);
        append_optional_object_id(&mut material, self.marker.as_ref());
        append_optional_object_id(&mut material, self.next_marker.as_ref());
        append_u64(&mut material, self.items.len() as u64);
        for item in &self.items {
            append_object_metadata_identity(&mut material, &item.object);
            append_string(&mut material, &item.pressure_node_id);
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
            "object capacity rebalance plan checksum mismatch: expected {} got {}",
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
            ("checked_objects", self.checked_objects),
            ("rebalance_objects", self.rebalance_objects),
            ("bytes_to_rebalance", self.bytes_to_rebalance),
            ("pressure_node_count", self.pressure_node_count),
            ("min_free_bytes", self.min_free_bytes),
            ("item_count", self.items.len() as u64),
            ("error_count", self.errors.len() as u64),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        for (name, value) in [
            ("truncated", self.truncated),
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

impl ObjectCapacityRebalanceRunReport {
    pub fn verify_expected_plan_checksum(
        &self,
        expected_plan_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        let expected_plan_checksum = expected_plan_checksum.as_ref();
        self.plan
            .verify_expected_plan_checksum(expected_plan_checksum)?;
        if !self.expected_plan_checksum.is_empty()
            && self.expected_plan_checksum != self.plan.plan_checksum()
        {
            return Err(ObjectError::PreconditionFailed(format!(
                "object capacity rebalance run expected plan checksum mismatch: expected {} got {}",
                self.expected_plan_checksum,
                self.plan.plan_checksum()
            )));
        }
        Ok(())
    }

    pub fn verify_expected_route_control_checksum(
        &self,
        expected_route_control_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        let expected_route_control_checksum = expected_route_control_checksum.as_ref();
        if !self.expected_route_control_checksum.is_empty()
            && self.expected_route_control_checksum != self.route_control_proof.state_checksum
        {
            return Err(ObjectError::PreconditionFailed(format!(
                "object capacity rebalance run expected route control checksum mismatch: expected {} got {}",
                self.expected_route_control_checksum, self.route_control_proof.state_checksum
            )));
        }
        if expected_route_control_checksum.is_empty()
            || self.route_control_proof.state_checksum == expected_route_control_checksum
        {
            return Ok(());
        }
        Err(ObjectError::PreconditionFailed(format!(
            "object capacity rebalance route control checksum mismatch: expected {} got {}",
            expected_route_control_checksum, self.route_control_proof.state_checksum
        )))
    }

    pub fn verify_expected_fences(
        &self,
        expected_plan_checksum: impl AsRef<str>,
        expected_route_control_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        self.verify_expected_plan_checksum(expected_plan_checksum)?;
        self.verify_expected_route_control_checksum(expected_route_control_checksum)
    }

    pub fn require_expected_fences(
        self,
        expected_plan_checksum: impl AsRef<str>,
        expected_route_control_checksum: impl AsRef<str>,
    ) -> Result<Self, ObjectError> {
        self.verify_expected_fences(expected_plan_checksum, expected_route_control_checksum)?;
        Ok(self)
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self.plan.to_control_fields(&format!("{field_prefix}plan."));
        for (name, value) in [
            (
                "attempted_migration_objects",
                self.attempted_migration_objects,
            ),
            ("skipped_migration_objects", self.skipped_migration_objects),
            ("migrated_objects", self.migrated_objects),
            ("failed_migration_objects", self.failed_migration_objects),
            ("bytes_migrated", self.bytes_migrated),
            ("migrated_count", self.migrated.len() as u64),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        append_optional_string_field(&mut fields, field_prefix, "first_error", &self.first_error);
        append_route_control_fields(
            &self.route_control_proof,
            &self.expected_route_control_checksum,
            field_prefix,
            &mut fields,
        );
        fields.insert(
            format!("{field_prefix}expected_plan_checksum"),
            self.expected_plan_checksum.clone(),
        );
        for (index, placement) in self.migrated.iter().enumerate() {
            fields.extend(placement.to_control_fields(&format!("{field_prefix}migrated.{index}.")));
        }
        fields
    }
}

impl ProvenMetadataRead<ObjectCapacityRebalanceRunReport> {
    pub fn verify_expected_capacity_rebalance_fences(
        &self,
        expected_plan_checksum: impl AsRef<str>,
        expected_route_control_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        self.value
            .verify_expected_fences(expected_plan_checksum, expected_route_control_checksum)
    }

    pub fn require_expected_capacity_rebalance_fences(
        self,
        expected_plan_checksum: impl AsRef<str>,
        expected_route_control_checksum: impl AsRef<str>,
    ) -> Result<Self, ObjectError> {
        self.verify_expected_capacity_rebalance_fences(
            expected_plan_checksum,
            expected_route_control_checksum,
        )?;
        Ok(self)
    }
}

