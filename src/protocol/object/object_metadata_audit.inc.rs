// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectMetadataAuditItem {
    pub object_id: ObjectId,
    pub issue: String,
    pub projection: Option<MetadataRecord>,
    pub metadata: Option<ObjectMetadata>,
}

impl ObjectMetadataAuditItem {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self.object_id.to_control_fields(field_prefix);
        fields.insert(format!("{field_prefix}issue"), self.issue.clone());
        fields.insert(
            format!("{field_prefix}projection_present"),
            self.projection.is_some().to_string(),
        );
        fields.insert(
            format!("{field_prefix}metadata_present"),
            self.metadata.is_some().to_string(),
        );
        if let Some(projection) = &self.projection {
            fields.extend(projection.to_control_fields(&format!("{field_prefix}projection.")));
        }
        if let Some(metadata) = &self.metadata {
            fields.extend(metadata.to_control_fields(&format!("{field_prefix}metadata.")));
        }
        fields
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ObjectMetadataAudit {
    pub bucket: String,
    pub prefix: String,
    pub marker: Option<String>,
    pub items: Vec<ObjectMetadataAuditItem>,
    pub scanned_projection_records: u64,
    pub scanned_objects: u64,
    pub audited_objects: u64,
    pub issue_count: u64,
    pub missing_projection_count: u64,
    pub missing_object_count: u64,
    pub deleted_projection_count: u64,
    pub stale_projection_count: u64,
    pub empty_projection_count: u64,
    pub payload_bytes: u64,
    pub max_payload_bytes: u64,
    pub next_marker: Option<String>,
    pub has_more: bool,
    pub readiness: MetadataRaftReadiness,
    pub expected_metadata_shard_topology_epoch: Option<u64>,
    pub expected_metadata_shard_topology_checksum: String,
}

impl ObjectMetadataAudit {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(format!("{field_prefix}bucket"), self.bucket.clone());
        fields.insert(format!("{field_prefix}prefix"), self.prefix.clone());
        if let Some(marker) = &self.marker {
            fields.insert(format!("{field_prefix}marker"), marker.clone());
        }
        if let Some(next_marker) = &self.next_marker {
            fields.insert(format!("{field_prefix}next_marker"), next_marker.clone());
        }
        for (name, value) in [
            (
                "scanned_projection_records",
                self.scanned_projection_records,
            ),
            ("scanned_objects", self.scanned_objects),
            ("audited_objects", self.audited_objects),
            ("issue_count", self.issue_count),
            ("missing_projection_count", self.missing_projection_count),
            ("missing_object_count", self.missing_object_count),
            ("deleted_projection_count", self.deleted_projection_count),
            ("stale_projection_count", self.stale_projection_count),
            ("empty_projection_count", self.empty_projection_count),
            ("payload_bytes", self.payload_bytes),
            ("max_payload_bytes", self.max_payload_bytes),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(format!("{field_prefix}has_more"), self.has_more.to_string());
        fields.insert(
            format!("{field_prefix}expected_metadata_shard_topology_epoch_present"),
            self.expected_metadata_shard_topology_epoch
                .is_some()
                .to_string(),
        );
        if let Some(expected_epoch) = self.expected_metadata_shard_topology_epoch {
            fields.insert(
                format!("{field_prefix}expected_metadata_shard_topology_epoch"),
                expected_epoch.to_string(),
            );
        }
        fields.insert(
            format!("{field_prefix}expected_metadata_shard_topology_checksum"),
            self.expected_metadata_shard_topology_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}readiness_requested_read_index"),
            self.readiness.min_read_index.to_string(),
        );
        fields.insert(
            format!("{field_prefix}readiness_consensus_read_index"),
            self.readiness.safe_read_fence.to_string(),
        );
        fields.insert(
            format!("{field_prefix}readiness_metadata_commit_index"),
            self.readiness.applied_commit_index.to_string(),
        );
        fields.insert(
            format!("{field_prefix}readiness_metadata_control_proof_checksum"),
            self.readiness.metadata_control_proof_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}readiness_reason"),
            self.readiness.reason.clone(),
        );
        for (index, item) in self.items.iter().enumerate() {
            fields.extend(item.to_control_fields(&format!("{field_prefix}item.{index}.")));
        }
        fields
    }
}

