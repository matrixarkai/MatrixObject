// SPDX-License-Identifier: Apache-2.0
impl MetadataQosConfig {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("window_ms", self.window_ms),
            ("max_write_ops_per_window", self.max_write_ops_per_window),
            (
                "max_write_bytes_per_window",
                self.max_write_bytes_per_window,
            ),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields
    }
}

impl MetadataRouteCacheConfig {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(format!("{field_prefix}entries"), self.entries.to_string());
        fields.insert(format!("{field_prefix}ttl_ms"), self.ttl_ms.to_string());
        fields
    }
}

impl PreparedMetadataCoordinatorConfig {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .config
            .to_control_fields(&format!("{field_prefix}config."));
        fields.extend(self.config.to_control_fields(field_prefix));
        fields.insert(
            format!("{field_prefix}idempotency_key"),
            self.idempotency_key.clone(),
        );
        append_prepared_metadata_common_fields(
            &mut fields,
            field_prefix,
            &self.metadata_control_proof_checksum,
            self.metadata_control_proof_now_ms,
            &self.request_checksum,
            self.expected_previous_commit_index,
            self.prepared_commit_index,
            &self.raft_entry_checksum,
        );
        append_prepared_metadata_control_proof_shape_fields(
            &mut fields,
            field_prefix,
            &self.metadata_control_proof_format,
            self.metadata_control_proof_bytes,
        );
        fields
    }
}

impl PreparedMetadataRetentionConfig {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}change_log_retention_limit"),
            self.change_log_retention_limit.to_string(),
        );
        fields.insert(
            format!("{field_prefix}mutation_receipt_retention_limit"),
            self.mutation_receipt_retention_limit.to_string(),
        );
        fields.extend(
            self.report
                .to_control_fields(&format!("{field_prefix}report.")),
        );
        fields.insert(
            format!("{field_prefix}before_state_fingerprint"),
            self.before_state_fingerprint.clone(),
        );
        append_prepared_metadata_common_fields(
            &mut fields,
            field_prefix,
            &self.metadata_control_proof_checksum,
            self.metadata_control_proof_now_ms,
            &self.request_checksum,
            self.expected_previous_commit_index,
            self.prepared_commit_index,
            &self.raft_entry_checksum,
        );
        append_prepared_metadata_control_proof_shape_fields(
            &mut fields,
            field_prefix,
            &self.metadata_control_proof_format,
            self.metadata_control_proof_bytes,
        );
        fields
    }
}

impl PreparedMetadataAdmissionPolicy {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .policy
            .to_control_fields(&format!("{field_prefix}policy."));
        fields.insert(
            format!("{field_prefix}before_state_fingerprint"),
            self.before_state_fingerprint.clone(),
        );
        append_prepared_metadata_common_fields(
            &mut fields,
            field_prefix,
            &self.metadata_control_proof_checksum,
            self.metadata_control_proof_now_ms,
            &self.request_checksum,
            self.expected_previous_commit_index,
            self.prepared_commit_index,
            &self.raft_entry_checksum,
        );
        append_prepared_metadata_control_proof_shape_fields(
            &mut fields,
            field_prefix,
            &self.metadata_control_proof_format,
            self.metadata_control_proof_bytes,
        );
        fields
    }
}

impl PreparedMetadataQosConfig {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .config
            .to_control_fields(&format!("{field_prefix}config."));
        fields.insert(
            format!("{field_prefix}before_state_fingerprint"),
            self.before_state_fingerprint.clone(),
        );
        append_prepared_metadata_common_fields(
            &mut fields,
            field_prefix,
            &self.metadata_control_proof_checksum,
            self.metadata_control_proof_now_ms,
            &self.request_checksum,
            self.expected_previous_commit_index,
            self.prepared_commit_index,
            &self.raft_entry_checksum,
        );
        append_prepared_metadata_control_proof_shape_fields(
            &mut fields,
            field_prefix,
            &self.metadata_control_proof_format,
            self.metadata_control_proof_bytes,
        );
        fields
    }
}

impl PreparedMetadataRouteCacheConfig {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .config
            .to_control_fields(&format!("{field_prefix}config."));
        fields.insert(
            format!("{field_prefix}before_state_fingerprint"),
            self.before_state_fingerprint.clone(),
        );
        fields.insert(
            format!("{field_prefix}after_state_fingerprint"),
            self.after_state_fingerprint.clone(),
        );
        append_prepared_metadata_common_fields(
            &mut fields,
            field_prefix,
            &self.metadata_control_proof_checksum,
            self.metadata_control_proof_now_ms,
            &self.request_checksum,
            self.expected_previous_commit_index,
            self.prepared_commit_index,
            &self.raft_entry_checksum,
        );
        append_prepared_metadata_control_proof_shape_fields(
            &mut fields,
            field_prefix,
            &self.metadata_control_proof_format,
            self.metadata_control_proof_bytes,
        );
        fields
    }
}

macro_rules! impl_proven_prepared_metadata_config_control_fields {
    ($ty:ty) => {
        impl ProvenMetadataRead<$ty> {
            pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
                let mut fields = self.value.to_control_fields(field_prefix);
                fields.extend(
                    self.proof
                        .to_control_fields(&format!("{field_prefix}metadata_read_proof.")),
                );
                fields
            }
        }
    };
}

impl_proven_prepared_metadata_config_control_fields!(PreparedMetadataCoordinatorConfig);
impl_proven_prepared_metadata_config_control_fields!(PreparedMetadataRetentionConfig);
impl_proven_prepared_metadata_config_control_fields!(PreparedMetadataAdmissionPolicy);
impl_proven_prepared_metadata_config_control_fields!(PreparedMetadataQosConfig);
impl_proven_prepared_metadata_config_control_fields!(PreparedMetadataRouteCacheConfig);
impl_proven_prepared_metadata_config_control_fields!(PreparedMetadataChangeCheckpoint);
impl_proven_prepared_metadata_config_control_fields!(PreparedMetadataPrefixDelete);
impl_proven_prepared_metadata_config_control_fields!(PreparedMetadataChangefeedCompaction);
impl_proven_prepared_metadata_config_control_fields!(PreparedMetadataTombstoneCompaction);
impl_proven_prepared_metadata_config_control_fields!(PreparedMetadataSnapshotCompaction);

