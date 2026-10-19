// SPDX-License-Identifier: Apache-2.0
impl RootServiceBootstrapReport {
    pub fn report_checksum_format(&self) -> &'static str {
        ROOT_SERVICE_BOOTSTRAP_REPORT_FORMAT
    }

    pub fn report_checksum(&self) -> String {
        root_service_bootstrap_report_checksum(self)
    }

    pub fn verify_expected_report_checksum(
        &self,
        expected_report_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        let expected_report_checksum = expected_report_checksum.as_ref();
        if expected_report_checksum.is_empty() {
            return Ok(());
        }
        let actual = self.report_checksum();
        if actual == expected_report_checksum {
            return Ok(());
        }
        Err(ObjectError::PreconditionFailed(format!(
            "rootserver service bootstrap report checksum mismatch: expected {} got {}",
            expected_report_checksum, actual
        )))
    }

    pub fn require_report_checksum(
        self,
        expected_report_checksum: impl AsRef<str>,
    ) -> Result<Self, ObjectError> {
        self.verify_expected_report_checksum(expected_report_checksum)?;
        Ok(self)
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}report_checksum_format"),
            self.report_checksum_format().to_string(),
        );
        fields.insert(
            format!("{field_prefix}report_checksum"),
            self.report_checksum(),
        );
        fields.insert(
            format!("{field_prefix}catalog_bootstrap_present"),
            self.catalog_report.is_some().to_string(),
        );
        fields.insert(
            format!("{field_prefix}before_control_checksum"),
            self.before_status.control_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}after_control_checksum"),
            self.after_status.control_checksum.clone(),
        );
        fields.extend(
            self.before_status
                .to_control_fields(&format!("{field_prefix}before.")),
        );
        fields.extend(
            self.after_status
                .to_control_fields(&format!("{field_prefix}after.")),
        );
        if let Some(report) = &self.catalog_report {
            fields.extend(report.to_control_fields(&format!("{field_prefix}catalog.")));
        }
        fields
    }
}

