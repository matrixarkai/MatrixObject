// SPDX-License-Identifier: Apache-2.0
impl ObjectReplicaRepairItem {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .object
            .to_control_fields(&format!("{field_prefix}object."));
        fields.insert(
            format!("{field_prefix}estimated_chunks"),
            self.estimated_chunks.to_string(),
        );
        fields.insert(
            format!("{field_prefix}estimated_bytes"),
            self.estimated_bytes.to_string(),
        );
        fields
    }
}

impl ObjectReplicaRepairPlan {
    pub fn plan_checksum(&self) -> String {
        let mut material = Vec::with_capacity(
            encoded_string_len("object_replica_repair_plan.v1")
                + std::mem::size_of::<u64>() * 7
                + encoded_string_len(&self.truncated.to_string())
                + encoded_string_len(&self.repair_needed.to_string())
                + encoded_string_len(&self.repair_can_apply.to_string())
                + encoded_string_len(&self.repair_reason)
                + self.items.len().saturating_mul(192)
                + 256,
        );
        append_string(&mut material, "object_replica_repair_plan.v1");
        append_u64(&mut material, self.checked_objects);
        append_u64(&mut material, self.repair_objects);
        append_u64(&mut material, self.estimated_chunks);
        append_u64(&mut material, self.bytes_to_repair);
        append_string(&mut material, &self.truncated.to_string());
        append_string(&mut material, &self.repair_needed.to_string());
        append_string(&mut material, &self.repair_can_apply.to_string());
        append_string(&mut material, &self.repair_reason);
        append_optional_object_id(&mut material, self.marker.as_ref());
        append_optional_object_id(&mut material, self.next_marker.as_ref());
        append_u64(&mut material, self.items.len() as u64);
        for item in &self.items {
            append_object_metadata_identity(&mut material, &item.object);
            append_u64(&mut material, item.estimated_chunks);
            append_u64(&mut material, item.estimated_bytes);
        }
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
            "object replica repair plan checksum mismatch: expected {} got {}",
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
            ("repair_objects", self.repair_objects),
            ("estimated_chunks", self.estimated_chunks),
            ("bytes_to_repair", self.bytes_to_repair),
            ("item_count", self.items.len() as u64),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        for (name, value) in [
            ("truncated", self.truncated),
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
        fields
    }
}

impl ObjectReplicaRepairRunReport {
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
                "object replica repair run expected plan checksum mismatch: expected {} got {}",
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
                "object replica repair run expected route control checksum mismatch: expected {} got {}",
                self.expected_route_control_checksum, self.route_control_proof.state_checksum
            )));
        }
        if expected_route_control_checksum.is_empty() {
            return Ok(());
        }
        if self.route_control_proof.state_checksum == expected_route_control_checksum {
            return Ok(());
        }
        Err(ObjectError::PreconditionFailed(format!(
            "object replica repair route control checksum mismatch: expected {} got {}",
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
            ("attempted_repair_objects", self.attempted_repair_objects),
            ("skipped_repair_objects", self.skipped_repair_objects),
            ("repaired_objects", self.repaired_objects),
            ("failed_repair_objects", self.failed_repair_objects),
            ("bytes_repaired", self.bytes_repaired),
            ("repaired_count", self.repaired.len() as u64),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        append_optional_string_field(&mut fields, field_prefix, "first_error", &self.first_error);
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
        fields.insert(
            format!("{field_prefix}expected_plan_checksum"),
            self.expected_plan_checksum.clone(),
        );
        for (index, metadata) in self.repaired.iter().enumerate() {
            fields.extend(metadata.to_control_fields(&format!("{field_prefix}repaired.{index}.")));
        }
        fields
    }
}

