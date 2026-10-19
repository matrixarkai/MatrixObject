// SPDX-License-Identifier: Apache-2.0
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataShardSplit {
    pub left: MetadataShard,
    pub right: MetadataShard,
}

impl MetadataShardSplit {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self.left.to_control_fields(&format!("{field_prefix}left."));
        fields.extend(
            self.right
                .to_control_fields(&format!("{field_prefix}right.")),
        );
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataShardSplitAction {
    pub source_shard_id: String,
    pub split_key: String,
    pub left_shard_id: String,
    pub right_shard_id: String,
    pub owner_node_id: String,
    pub expected_epoch: u64,
    pub new_epoch: u64,
    pub record_count: u64,
    pub payload_bytes: u64,
    pub left_record_count: u64,
    pub right_record_count: u64,
    pub reason: String,
}

impl MetadataShardSplitAction {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("source_shard_id", self.source_shard_id.as_str()),
            ("split_key", self.split_key.as_str()),
            ("left_shard_id", self.left_shard_id.as_str()),
            ("right_shard_id", self.right_shard_id.as_str()),
            ("owner_node_id", self.owner_node_id.as_str()),
            ("reason", self.reason.as_str()),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        for (name, value) in [
            ("expected_epoch", self.expected_epoch),
            ("new_epoch", self.new_epoch),
            ("record_count", self.record_count),
            ("payload_bytes", self.payload_bytes),
            ("left_record_count", self.left_record_count),
            ("right_record_count", self.right_record_count),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetadataShardSplitReport {
    pub scanned_shards: u64,
    pub candidate_shards: u64,
    pub planned_shards: u64,
    pub split_shards: u64,
    pub skipped_shards: u64,
    pub failed_shards: u64,
    pub scanned_records: u64,
    pub candidate_records: u64,
    pub candidate_payload_bytes: u64,
    pub actions: Vec<MetadataShardSplitAction>,
    pub errors: Vec<String>,
    pub shard_plan_needed: bool,
    pub shard_plan_can_apply: bool,
    pub shard_plan_reason: String,
    pub plan_checksum: String,
    pub expected_plan_checksum: String,
    pub metadata_shard_topology_epoch: u64,
    pub metadata_shard_topology_checksum: String,
    pub expected_metadata_shard_topology_epoch: Option<u64>,
    pub expected_metadata_shard_topology_checksum: String,
}

impl MetadataShardSplitReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = metadata_shard_plan_common_control_fields(
            self.scanned_shards,
            self.candidate_shards,
            self.planned_shards,
            self.skipped_shards,
            self.failed_shards,
            self.shard_plan_needed,
            self.shard_plan_can_apply,
            &self.shard_plan_reason,
            &self.errors,
            field_prefix,
        );
        fields.insert(
            format!("{field_prefix}split_shards"),
            self.split_shards.to_string(),
        );
        fields.insert(
            format!("{field_prefix}scanned_records"),
            self.scanned_records.to_string(),
        );
        fields.insert(
            format!("{field_prefix}candidate_records"),
            self.candidate_records.to_string(),
        );
        fields.insert(
            format!("{field_prefix}candidate_payload_bytes"),
            self.candidate_payload_bytes.to_string(),
        );
        fields.insert(
            format!("{field_prefix}plan_checksum"),
            self.plan_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}expected_plan_checksum"),
            self.expected_plan_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}metadata_shard_topology_epoch"),
            self.metadata_shard_topology_epoch.to_string(),
        );
        fields.insert(
            format!("{field_prefix}metadata_shard_topology_checksum"),
            self.metadata_shard_topology_checksum.clone(),
        );
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
            format!("{field_prefix}action_count"),
            self.actions.len().to_string(),
        );
        for (index, action) in self.actions.iter().enumerate() {
            fields.extend(action.to_control_fields(&format!("{field_prefix}action.{index}.")));
        }
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataShardMigrationAction {
    pub shard_id: String,
    pub current_owner_node_id: String,
    pub target_owner_node_id: String,
    pub expected_epoch: u64,
    pub new_epoch: u64,
    pub draining: bool,
    pub reason: String,
}

impl MetadataShardMigrationAction {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(format!("{field_prefix}shard_id"), self.shard_id.clone());
        fields.insert(
            format!("{field_prefix}current_owner_node_id"),
            self.current_owner_node_id.clone(),
        );
        fields.insert(
            format!("{field_prefix}target_owner_node_id"),
            self.target_owner_node_id.clone(),
        );
        fields.insert(
            format!("{field_prefix}expected_epoch"),
            self.expected_epoch.to_string(),
        );
        fields.insert(
            format!("{field_prefix}new_epoch"),
            self.new_epoch.to_string(),
        );
        fields.insert(format!("{field_prefix}draining"), self.draining.to_string());
        fields.insert(format!("{field_prefix}reason"), self.reason.clone());
        fields
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetadataShardMigrationReport {
    pub scanned_shards: u64,
    pub candidate_shards: u64,
    pub planned_shards: u64,
    pub migrated_shards: u64,
    pub skipped_shards: u64,
    pub failed_shards: u64,
    pub actions: Vec<MetadataShardMigrationAction>,
    pub errors: Vec<String>,
    pub shard_plan_needed: bool,
    pub shard_plan_can_apply: bool,
    pub shard_plan_reason: String,
    pub plan_checksum: String,
    pub expected_plan_checksum: String,
    pub metadata_shard_topology_epoch: u64,
    pub metadata_shard_topology_checksum: String,
    pub metadata_coordinator_session_checksum: String,
    pub expected_metadata_shard_topology_epoch: Option<u64>,
    pub expected_metadata_shard_topology_checksum: String,
    pub expected_metadata_coordinator_session_checksum: String,
}

impl MetadataShardMigrationReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = metadata_shard_plan_common_control_fields(
            self.scanned_shards,
            self.candidate_shards,
            self.planned_shards,
            self.skipped_shards,
            self.failed_shards,
            self.shard_plan_needed,
            self.shard_plan_can_apply,
            &self.shard_plan_reason,
            &self.errors,
            field_prefix,
        );
        fields.insert(
            format!("{field_prefix}migrated_shards"),
            self.migrated_shards.to_string(),
        );
        fields.insert(
            format!("{field_prefix}plan_checksum"),
            self.plan_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}expected_plan_checksum"),
            self.expected_plan_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}metadata_shard_topology_epoch"),
            self.metadata_shard_topology_epoch.to_string(),
        );
        fields.insert(
            format!("{field_prefix}metadata_shard_topology_checksum"),
            self.metadata_shard_topology_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}metadata_coordinator_session_checksum"),
            self.metadata_coordinator_session_checksum.clone(),
        );
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
            format!("{field_prefix}expected_metadata_coordinator_session_checksum"),
            self.expected_metadata_coordinator_session_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}action_count"),
            self.actions.len().to_string(),
        );
        for (index, action) in self.actions.iter().enumerate() {
            fields.extend(action.to_control_fields(&format!("{field_prefix}action.{index}.")));
        }
        fields
    }
}

pub(crate) fn metadata_shard_plan_common_control_fields(
    scanned_shards: u64,
    candidate_shards: u64,
    planned_shards: u64,
    skipped_shards: u64,
    failed_shards: u64,
    shard_plan_needed: bool,
    shard_plan_can_apply: bool,
    shard_plan_reason: &str,
    errors: &[String],
    field_prefix: &str,
) -> BTreeMap<String, String> {
    let mut fields = BTreeMap::new();
    for (name, value) in [
        ("scanned_shards", scanned_shards),
        ("candidate_shards", candidate_shards),
        ("planned_shards", planned_shards),
        ("skipped_shards", skipped_shards),
        ("failed_shards", failed_shards),
        ("error_count", errors.len() as u64),
    ] {
        fields.insert(format!("{field_prefix}{name}"), value.to_string());
    }
    fields.insert(
        format!("{field_prefix}shard_plan_needed"),
        shard_plan_needed.to_string(),
    );
    fields.insert(
        format!("{field_prefix}shard_plan_can_apply"),
        shard_plan_can_apply.to_string(),
    );
    fields.insert(
        format!("{field_prefix}shard_plan_reason"),
        shard_plan_reason.to_string(),
    );
    for (index, error) in errors.iter().enumerate() {
        fields.insert(format!("{field_prefix}error.{index}"), error.clone());
    }
    fields
}
