// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl StorageSnapshotProofStreamReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("format", &self.format),
            ("aggregate_checksum", &self.aggregate_checksum),
            ("source_control_kind", &self.source_control_kind),
            ("source_control_checksum", &self.source_control_checksum),
            ("full_snapshot_checksum", &self.full_snapshot_checksum),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.clone());
        }
        for (name, value) in [
            ("proof_count", self.proof_count),
            ("source_read_index", self.source_read_index),
            ("source_commit_index", self.source_commit_index),
            ("total_payload_bytes", self.total_payload_bytes),
            ("first_offset", self.first_offset),
            ("last_end_offset", self.last_end_offset),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields
    }

    pub fn verify_against(
        &self,
        actual: &StorageSnapshotProofStreamReport,
        expected_source_control_kind: &str,
        expected_source_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<(), ObjectError> {
        verify_storage_snapshot_proof_stream_report(
            self,
            actual,
            expected_source_control_kind,
            expected_source_control_checksum,
            min_read_index,
            min_commit_index,
        )
    }

    pub fn require_min_fence(
        &self,
        expected_source_control_kind: &str,
        expected_source_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<(), ObjectError> {
        self.verify_against(
            self,
            expected_source_control_kind,
            expected_source_control_checksum,
            min_read_index,
            min_commit_index,
        )
    }
}

impl StorageSnapshotInstallSourceProofReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("format", &self.format),
            ("source_control_kind", &self.source_control_kind),
            (
                "expected_snapshot_read_proof_checksum",
                &self.expected_snapshot_read_proof_checksum,
            ),
            (
                "expected_source_control_checksum",
                &self.expected_source_control_checksum,
            ),
            ("envelope_checksum", &self.envelope_checksum),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.clone());
        }
        for (name, value) in [
            ("source_read_index", self.source_read_index),
            ("source_commit_index", self.source_commit_index),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields
    }

    pub fn verify_against(
        &self,
        actual: &StorageSnapshotInstallSourceProofReport,
        expected_source_control_kind: &str,
    ) -> Result<(), ObjectError> {
        verify_storage_snapshot_install_source_proof_report(
            self,
            actual,
            expected_source_control_kind,
        )
    }

    pub fn require_source_control_kind(
        &self,
        expected_source_control_kind: &str,
    ) -> Result<(), ObjectError> {
        self.verify_against(self, expected_source_control_kind)
    }
}

impl MetadataSnapshotProofStreamReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("format", &self.format),
            ("aggregate_checksum", &self.aggregate_checksum),
            ("full_snapshot_checksum", &self.full_snapshot_checksum),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.clone());
        }
        for (name, value) in [
            ("proof_count", self.proof_count),
            ("requested_read_index", self.requested_read_index),
            ("consensus_read_index", self.consensus_read_index),
            ("metadata_commit_index", self.metadata_commit_index),
            ("total_payload_bytes", self.total_payload_bytes),
            ("first_offset", self.first_offset),
            ("last_end_offset", self.last_end_offset),
            (
                "snapshot_last_applied_index",
                self.snapshot_last_applied_index,
            ),
            ("snapshot_term", self.snapshot_term),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields
    }

    pub fn verify_against(
        &self,
        actual: &MetadataSnapshotProofStreamReport,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<(), ObjectError> {
        verify_metadata_snapshot_proof_stream_report(self, actual, min_read_index, min_commit_index)
    }

    pub fn require_min_fence(
        &self,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<(), ObjectError> {
        self.verify_against(self, min_read_index, min_commit_index)
    }
}

impl MetadataSnapshotInstallSourceProofReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("format", &self.format),
            (
                "expected_snapshot_read_proof_checksum",
                &self.expected_snapshot_read_proof_checksum,
            ),
            ("full_snapshot_checksum", &self.full_snapshot_checksum),
            ("envelope_checksum", &self.envelope_checksum),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.clone());
        }
        for (name, value) in [
            ("source_read_index", self.source_read_index),
            ("source_commit_index", self.source_commit_index),
            ("source_applied_index", self.source_applied_index),
            ("source_term", self.source_term),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields
    }

    pub fn verify_against(
        &self,
        actual: &MetadataSnapshotInstallSourceProofReport,
    ) -> Result<(), ObjectError> {
        verify_metadata_snapshot_install_source_proof_report(self, actual)
    }

    pub fn require_valid_envelope(&self) -> Result<(), ObjectError> {
        self.verify_against(self)
    }
}

impl MetaServerSnapshotProofStreamReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("format", &self.format),
            ("aggregate_checksum", &self.aggregate_checksum),
            ("full_snapshot_checksum", &self.full_snapshot_checksum),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.clone());
        }
        for (name, value) in [
            ("proof_count", self.proof_count),
            ("requested_read_index", self.requested_read_index),
            ("consensus_read_index", self.consensus_read_index),
            ("metadata_commit_index", self.metadata_commit_index),
            ("total_payload_bytes", self.total_payload_bytes),
            ("first_offset", self.first_offset),
            ("last_end_offset", self.last_end_offset),
            (
                "snapshot_last_applied_index",
                self.snapshot_last_applied_index,
            ),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields
    }

    pub fn verify_against(
        &self,
        actual: &MetaServerSnapshotProofStreamReport,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<(), ObjectError> {
        verify_metaserver_snapshot_proof_stream_report(
            self,
            actual,
            min_read_index,
            min_commit_index,
        )
    }

    pub fn require_min_fence(
        &self,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<(), ObjectError> {
        self.verify_against(self, min_read_index, min_commit_index)
    }
}

impl RootSnapshotProofStreamReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("format", &self.format),
            ("aggregate_checksum", &self.aggregate_checksum),
            ("operation", &self.operation),
            ("root_state_fingerprint", &self.root_state_fingerprint),
            ("full_snapshot_checksum", &self.full_snapshot_checksum),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.clone());
        }
        for (name, value) in [
            ("proof_count", self.proof_count),
            ("requested_root_read_index", self.requested_root_read_index),
            (
                "requested_root_commit_index",
                self.requested_root_commit_index,
            ),
            ("root_applied_index", self.root_applied_index),
            ("root_raft_term", self.root_raft_term),
            ("total_payload_bytes", self.total_payload_bytes),
            ("first_offset", self.first_offset),
            ("last_end_offset", self.last_end_offset),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields
    }

    pub fn verify_against(
        &self,
        actual: &RootSnapshotProofStreamReport,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<(), ObjectError> {
        verify_root_snapshot_proof_stream_report(self, actual, min_read_index, min_commit_index)
    }

    pub fn require_min_fence(
        &self,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<(), ObjectError> {
        self.verify_against(self, min_read_index, min_commit_index)
    }
}

impl ServiceSnapshotInstallSourceProofReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("format", &self.format),
            ("service", &self.service),
            (
                "expected_snapshot_read_proof_checksum",
                &self.expected_snapshot_read_proof_checksum,
            ),
            ("source_state_fingerprint", &self.source_state_fingerprint),
            ("envelope_checksum", &self.envelope_checksum),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.clone());
        }
        for (name, value) in [
            ("source_read_index", self.source_read_index),
            ("source_commit_index", self.source_commit_index),
            ("source_applied_index", self.source_applied_index),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields
    }

    pub fn verify_against(
        &self,
        actual: &ServiceSnapshotInstallSourceProofReport,
    ) -> Result<(), ObjectError> {
        verify_service_snapshot_install_source_proof_report(self, actual)
    }

    pub fn require_valid_envelope(&self) -> Result<(), ObjectError> {
        self.verify_against(self)
    }
}

