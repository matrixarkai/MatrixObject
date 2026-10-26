// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReplicaMutationControlBootstrapReport {
    pub replica_index: usize,
    pub target_commit_index: u64,
    pub max_steps: u64,
    pub applied_steps: u64,
    pub before_proof: ReplicaServiceControlProof,
    pub after_proof: ReplicaServiceControlProof,
    pub last_metadata: Option<ObjectMetadata>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReplicaServiceBootstrapReport {
    pub replica_index: usize,
    pub mutation_report: Option<ReplicaMutationControlBootstrapReport>,
    pub before_readiness: ReplicaStorageReadiness,
    pub after_readiness: ReplicaStorageReadiness,
}

impl ReplicaServiceBootstrapReport {
    pub fn report_checksum_format(&self) -> &'static str {
        REPLICA_SERVICE_BOOTSTRAP_REPORT_FORMAT
    }

    pub fn report_checksum(&self) -> String {
        replica_service_bootstrap_report_checksum(self)
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
            "chunkserver service bootstrap report checksum mismatch: expected {} got {}",
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
            format!("{field_prefix}replica_index"),
            self.replica_index.to_string(),
        );
        fields.insert(
            format!("{field_prefix}mutation_bootstrap_present"),
            self.mutation_report.is_some().to_string(),
        );
        fields.insert(
            format!("{field_prefix}before_ready"),
            self.before_readiness.ready.to_string(),
        );
        fields.insert(
            format!("{field_prefix}after_ready"),
            self.after_readiness.ready.to_string(),
        );
        fields.insert(
            format!("{field_prefix}before_chunkserver_control_proof_checksum"),
            self.before_readiness
                .chunkserver_control_proof_checksum
                .clone(),
        );
        fields.insert(
            format!("{field_prefix}after_chunkserver_control_proof_checksum"),
            self.after_readiness
                .chunkserver_control_proof_checksum
                .clone(),
        );
        fields.extend(
            self.before_readiness
                .to_control_fields(&format!("{field_prefix}before.")),
        );
        fields.extend(
            self.after_readiness
                .to_control_fields(&format!("{field_prefix}after.")),
        );
        if let Some(report) = &self.mutation_report {
            fields.extend(report.to_control_fields(&format!("{field_prefix}mutation.")));
        }
        fields
    }
}

impl ReplicaMutationControlBootstrapReport {
    pub fn report_checksum_format(&self) -> &'static str {
        CHUNK_MUTATION_BOOTSTRAP_REPORT_FORMAT
    }

    pub fn report_checksum(&self) -> String {
        chunk_mutation_bootstrap_report_checksum(self)
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
            "chunk mutation-control bootstrap report checksum mismatch: expected {} got {}",
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
            ("replica_index", self.replica_index as u64),
            ("target_commit_index", self.target_commit_index),
            ("max_steps", self.max_steps),
            ("applied_steps", self.applied_steps),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(
            format!("{field_prefix}last_metadata_present"),
            self.last_metadata.is_some().to_string(),
        );
        fields.insert(
            format!("{field_prefix}report_checksum_format"),
            CHUNK_MUTATION_BOOTSTRAP_REPORT_FORMAT.to_string(),
        );
        fields.insert(
            format!("{field_prefix}report_checksum"),
            self.report_checksum(),
        );
        fields.extend(
            self.before_proof
                .to_control_fields(&format!("{field_prefix}before.")),
        );
        fields.extend(
            self.after_proof
                .to_control_fields(&format!("{field_prefix}after.")),
        );
        if let Some(metadata) = &self.last_metadata {
            fields.extend(metadata.to_control_fields(&format!("{field_prefix}metadata.")));
        }
        fields
    }
}

