// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectPlacement {
    pub object_id: ObjectId,
    pub version: u64,
    pub length: u64,
    pub node_ids: Vec<String>,
    pub update_time_ms: u64,
}

impl ObjectPlacement {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}bucket"),
            self.object_id.bucket.to_string(),
        );
        fields.insert(format!("{field_prefix}key"), self.object_id.key.to_string());
        fields.insert(format!("{field_prefix}version"), self.version.to_string().into());
        fields.insert(format!("{field_prefix}length"), self.length.to_string().into());
        fields.insert(
            format!("{field_prefix}node_count"),
            self.node_ids.len().to_string().into(),
        );
        for (index, node_id) in self.node_ids.iter().enumerate() {
            fields.insert(format!("{field_prefix}node.{index}"), node_id.clone().into());
        }
        fields.insert(
            format!("{field_prefix}update_time_ms"),
            self.update_time_ms.to_string().into(),
        );
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RootLeaderLease {
    pub leader_node_id: String,
    pub epoch: u64,
    pub acquire_time_ms: u64,
    pub expire_time_ms: u64,
    pub ttl_ms: u64,
}

impl RootLeaderLease {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}leader_node_id"),
            self.leader_node_id.clone(),
        );
        fields.insert(format!("{field_prefix}lease_epoch"), self.epoch.to_string());
        fields.insert(
            format!("{field_prefix}lease_acquire_time_ms"),
            self.acquire_time_ms.to_string(),
        );
        fields.insert(
            format!("{field_prefix}lease_expire_time_ms"),
            self.expire_time_ms.to_string(),
        );
        fields.insert(
            format!("{field_prefix}lease_ttl_ms"),
            self.ttl_ms.to_string(),
        );
        fields
    }
}

