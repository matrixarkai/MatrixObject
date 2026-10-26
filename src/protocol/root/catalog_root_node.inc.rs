// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlacementNode {
    pub node_id: String,
    pub endpoint: String,
    pub service_kind: String,
    pub failure_domain: String,
    pub metadata_eligible: bool,
    pub data_eligible: bool,
    pub gateway_eligible: bool,
    pub metadata_value_bytes: u64,
    pub capacity_bytes: u64,
    pub used_bytes: u64,
    pub committed_bytes: u64,
    pub draining: bool,
    pub online: bool,
    pub last_heartbeat_ms: u64,
    pub page_allocator_shard_index: u64,
    pub page_allocator_shard_count: u64,
    pub meta_health: NodeMetaHealth,
    pub page_health: NodePageHealth,
    pub chunk_health: NodeChunkHealth,
}

impl PlacementNode {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("node_id", &self.node_id),
            ("endpoint", &self.endpoint),
            ("service_kind", &self.service_kind),
            ("failure_domain", &self.failure_domain),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.clone());
        }
        for (name, value) in [
            ("metadata_eligible", self.metadata_eligible),
            ("data_eligible", self.data_eligible),
            ("gateway_eligible", self.gateway_eligible),
            ("draining", self.draining),
            ("online", self.online),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        for (name, value) in [
            ("metadata_value_bytes", self.metadata_value_bytes),
            ("capacity_bytes", self.capacity_bytes),
            ("used_bytes", self.used_bytes),
            ("committed_bytes", self.committed_bytes),
            ("last_heartbeat_ms", self.last_heartbeat_ms),
            (
                "page_allocator_shard_index",
                self.page_allocator_shard_index,
            ),
            (
                "page_allocator_shard_count",
                self.page_allocator_shard_count,
            ),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields
    }
}

