// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataCoordinatorBootstrapReport {
    pub config: MetadataCoordinatorConfig,
    pub before_readiness: MetadataRaftReadiness,
    pub after_readiness: MetadataRaftReadiness,
    pub applied_config: MetadataCoordinatorConfig,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataRaftBootstrapReport {
    pub mutation_id: String,
    pub target_voter_node_ids: Vec<String>,
    pub target_healthy_peer_count: u64,
    pub before_readiness: MetadataRaftReadiness,
    pub after_readiness: MetadataRaftReadiness,
    pub membership_receipt: Option<RaftMembershipReceipt>,
    pub last_metadata: Option<ObjectMetadata>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataServiceBootstrapReport {
    pub raft_report: Option<MetadataRaftBootstrapReport>,
    pub coordinator_report: Option<MetadataCoordinatorBootstrapReport>,
    pub before_readiness: MetadataRaftReadiness,
    pub after_readiness: MetadataRaftReadiness,
}

impl MetadataServiceBootstrapReport {
    pub fn report_checksum_format(&self) -> &'static str {
        META_SERVICE_BOOTSTRAP_REPORT_FORMAT
    }

    pub fn report_checksum(&self) -> String {
        meta_service_bootstrap_report_checksum(self)
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
            "metadata service bootstrap report checksum mismatch: expected {} got {}",
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
            format!("{field_prefix}raft_bootstrap_present"),
            self.raft_report.is_some().to_string(),
        );
        fields.insert(
            format!("{field_prefix}coordinator_bootstrap_present"),
            self.coordinator_report.is_some().to_string(),
        );
        fields.insert(
            format!("{field_prefix}before_metadata_control_proof_checksum"),
            self.before_readiness
                .metadata_control_proof_checksum
                .clone(),
        );
        fields.insert(
            format!("{field_prefix}after_metadata_control_proof_checksum"),
            self.after_readiness.metadata_control_proof_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}before_ready"),
            self.before_readiness.ready.to_string(),
        );
        fields.insert(
            format!("{field_prefix}after_ready"),
            self.after_readiness.ready.to_string(),
        );
        if let Some(report) = &self.raft_report {
            fields.extend(report.to_control_fields(&format!("{field_prefix}raft.")));
        }
        if let Some(report) = &self.coordinator_report {
            fields.extend(report.to_control_fields(&format!("{field_prefix}coordinator.")));
        }
        fields
    }
}

impl MetadataRaftBootstrapReport {
    pub fn report_checksum_format(&self) -> &'static str {
        META_RAFT_BOOTSTRAP_REPORT_FORMAT
    }

    pub fn report_checksum(&self) -> String {
        meta_raft_bootstrap_report_checksum(self)
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
            "metadata raft bootstrap report checksum mismatch: expected {} got {}",
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
            format!("{field_prefix}mutation_id"),
            self.mutation_id.clone(),
        );
        fields.insert(
            format!("{field_prefix}target_voter_count"),
            self.target_voter_node_ids.len().to_string(),
        );
        for (index, node_id) in self.target_voter_node_ids.iter().enumerate() {
            fields.insert(
                format!("{field_prefix}target_voter.{index}"),
                node_id.clone(),
            );
        }
        fields.insert(
            format!("{field_prefix}target_voter_checksum"),
            raft_voter_checksum(&self.target_voter_node_ids),
        );
        fields.insert(
            format!("{field_prefix}target_healthy_peer_count"),
            self.target_healthy_peer_count.to_string(),
        );
        for (name, value) in [
            (
                "before_raft_log_commit_index",
                self.before_readiness.raft_log_commit_index,
            ),
            (
                "before_applied_commit_index",
                self.before_readiness.applied_commit_index,
            ),
            (
                "before_raft_membership_receipt_count",
                self.before_readiness.raft_membership_receipt_count,
            ),
            (
                "after_raft_log_commit_index",
                self.after_readiness.raft_log_commit_index,
            ),
            (
                "after_applied_commit_index",
                self.after_readiness.applied_commit_index,
            ),
            (
                "after_raft_membership_receipt_count",
                self.after_readiness.raft_membership_receipt_count,
            ),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(
            format!("{field_prefix}before_metadata_control_proof_checksum"),
            self.before_readiness
                .metadata_control_proof_checksum
                .clone(),
        );
        fields.insert(
            format!("{field_prefix}after_metadata_control_proof_checksum"),
            self.after_readiness.metadata_control_proof_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}membership_receipt_present"),
            self.membership_receipt.is_some().to_string(),
        );
        fields.insert(
            format!("{field_prefix}last_metadata_present"),
            self.last_metadata.is_some().to_string(),
        );
        if let Some(receipt) = &self.membership_receipt {
            fields.extend(receipt.to_control_fields(&format!("{field_prefix}receipt.")));
        }
        if let Some(metadata) = &self.last_metadata {
            fields.extend(metadata.to_control_fields(&format!("{field_prefix}metadata.")));
        }
        fields
    }
}

impl MetadataCoordinatorBootstrapReport {
    pub fn report_checksum_format(&self) -> &'static str {
        META_COORDINATOR_BOOTSTRAP_REPORT_FORMAT
    }

    pub fn report_checksum(&self) -> String {
        meta_coordinator_bootstrap_report_checksum(self)
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
            "metadata coordinator bootstrap report checksum mismatch: expected {} got {}",
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
        fields.extend(
            self.config
                .to_control_fields(&format!("{field_prefix}config.")),
        );
        fields.extend(
            self.applied_config
                .to_control_fields(&format!("{field_prefix}applied_config.")),
        );
        for (name, value) in [
            (
                "before_raft_log_commit_index",
                self.before_readiness.raft_log_commit_index,
            ),
            (
                "before_applied_commit_index",
                self.before_readiness.applied_commit_index,
            ),
            (
                "before_last_applied_metadata_mutation_commit_index",
                self.before_readiness
                    .last_applied_metadata_mutation_commit_index,
            ),
            (
                "after_raft_log_commit_index",
                self.after_readiness.raft_log_commit_index,
            ),
            (
                "after_applied_commit_index",
                self.after_readiness.applied_commit_index,
            ),
            (
                "after_last_applied_metadata_mutation_commit_index",
                self.after_readiness
                    .last_applied_metadata_mutation_commit_index,
            ),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        for (name, value) in [
            ("before_ready", self.before_readiness.ready),
            ("before_readable", self.before_readiness.readable),
            ("before_writable", self.before_readiness.writable),
            ("after_ready", self.after_readiness.ready),
            ("after_readable", self.after_readiness.readable),
            ("after_writable", self.after_readiness.writable),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(
            format!("{field_prefix}before_reason"),
            self.before_readiness.reason.clone(),
        );
        fields.insert(
            format!("{field_prefix}after_reason"),
            self.after_readiness.reason.clone(),
        );
        fields.insert(
            format!("{field_prefix}before_metadata_control_proof_format"),
            self.before_readiness.metadata_control_proof_format.clone(),
        );
        fields.insert(
            format!("{field_prefix}before_metadata_control_proof_checksum"),
            self.before_readiness
                .metadata_control_proof_checksum
                .clone(),
        );
        fields.insert(
            format!("{field_prefix}after_metadata_control_proof_format"),
            self.after_readiness.metadata_control_proof_format.clone(),
        );
        fields.insert(
            format!("{field_prefix}after_metadata_control_proof_checksum"),
            self.after_readiness.metadata_control_proof_checksum.clone(),
        );
        fields
    }
}
