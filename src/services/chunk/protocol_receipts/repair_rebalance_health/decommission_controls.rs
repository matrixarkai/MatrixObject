// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl ProvenMetadataRead<ObjectReplicaRepairRunReport> {
    pub fn verify_expected_repair_fences(
        &self,
        expected_plan_checksum: impl AsRef<str>,
        expected_route_control_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        self.value
            .verify_expected_fences(expected_plan_checksum, expected_route_control_checksum)
    }

    pub fn require_expected_repair_fences(
        self,
        expected_plan_checksum: impl AsRef<str>,
        expected_route_control_checksum: impl AsRef<str>,
    ) -> Result<Self, ObjectError> {
        self.verify_expected_repair_fences(
            expected_plan_checksum,
            expected_route_control_checksum,
        )?;
        Ok(self)
    }
}

impl ObjectReplicaDecommissionReport {
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
                "object replica decommission expected plan checksum mismatch: expected {} got {}",
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
                "object replica decommission expected route control checksum mismatch: expected {} got {}",
                self.expected_route_control_checksum, self.route_control_proof.state_checksum
            )));
        }
        if expected_route_control_checksum.is_empty()
            || self.route_control_proof.state_checksum == expected_route_control_checksum
        {
            return Ok(());
        }
        Err(ObjectError::PreconditionFailed(format!(
            "object replica decommission route control checksum mismatch: expected {} got {}",
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
            (
                "attempted_migration_objects",
                self.attempted_migration_objects,
            ),
            ("skipped_migration_objects", self.skipped_migration_objects),
            ("migrated_objects", self.migrated_objects),
            ("failed_migration_objects", self.failed_migration_objects),
            ("bytes_repaired", self.bytes_repaired),
            ("bytes_migrated", self.bytes_migrated),
            ("migrated_count", self.migrated.len() as u64),
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
        for (index, placement) in self.migrated.iter().enumerate() {
            fields.extend(placement.to_control_fields(&format!("{field_prefix}migrated.{index}.")));
        }
        fields
    }
}

impl ProvenMetadataRead<ObjectReplicaDecommissionReport> {
    pub fn verify_expected_decommission_fences(
        &self,
        expected_plan_checksum: impl AsRef<str>,
        expected_route_control_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        self.value
            .verify_expected_fences(expected_plan_checksum, expected_route_control_checksum)
    }

    pub fn require_expected_decommission_fences(
        self,
        expected_plan_checksum: impl AsRef<str>,
        expected_route_control_checksum: impl AsRef<str>,
    ) -> Result<Self, ObjectError> {
        self.verify_expected_decommission_fences(
            expected_plan_checksum,
            expected_route_control_checksum,
        )?;
        Ok(self)
    }
}

