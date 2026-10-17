impl RootCatalogBootstrapReport {
    pub fn report_checksum_format(&self) -> &'static str {
        ROOT_CATALOG_BOOTSTRAP_REPORT_FORMAT
    }

    pub fn report_checksum(&self) -> String {
        root_catalog_bootstrap_report_checksum(self)
    }

    pub fn verify_expected_report_checksum(
        &self,
        expected_report_checksum: &str,
    ) -> Result<(), ObjectError> {
        if expected_report_checksum.is_empty() || self.report_checksum() == expected_report_checksum
        {
            return Ok(());
        }
        Err(ObjectError::PreconditionFailed(format!(
            "root catalog bootstrap report checksum mismatch: expected {} got {}",
            expected_report_checksum,
            self.report_checksum()
        )))
    }

    pub fn require_report_checksum(
        self,
        expected_report_checksum: &str,
    ) -> Result<Self, ObjectError> {
        self.verify_expected_report_checksum(expected_report_checksum)?;
        Ok(self)
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("target_commit_index", self.target_commit_index),
            ("target_read_index", self.target_read_index),
            ("max_steps", self.max_steps),
            ("applied_steps", self.applied_steps),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(
            format!("{field_prefix}heartbeat_node_id"),
            self.heartbeat_node_id.clone(),
        );
        fields.insert(
            format!("{field_prefix}last_metadata_present"),
            self.last_metadata.is_some().to_string(),
        );
        fields.insert(
            format!("{field_prefix}report_checksum_format"),
            ROOT_CATALOG_BOOTSTRAP_REPORT_FORMAT.to_string(),
        );
        fields.insert(
            format!("{field_prefix}report_checksum"),
            self.report_checksum(),
        );
        fields.extend(
            self.before_status
                .to_control_fields(&format!("{field_prefix}before.")),
        );
        fields.extend(
            self.after_status
                .to_control_fields(&format!("{field_prefix}after.")),
        );
        if let Some(metadata) = &self.last_metadata {
            fields.extend(metadata.to_control_fields(&format!("{field_prefix}metadata.")));
        }
        fields
    }
}

