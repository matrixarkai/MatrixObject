// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataShardRoute {
    pub shard: MetadataShard,
    pub active_owner_session: Option<CoordinatorSession>,
    pub preferred_endpoint: String,
    pub fallback_endpoints: Vec<String>,
    pub contains_prefix_start: bool,
    pub owner_serving: bool,
}

impl MetadataShardRoute {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .shard
            .to_control_fields(&format!("{field_prefix}shard."));
        fields.insert(
            format!("{field_prefix}active_owner_session_present"),
            self.active_owner_session.is_some().to_string(),
        );
        if let Some(session) = &self.active_owner_session {
            fields.extend(session.to_control_fields(&format!("{field_prefix}owner_session.")));
        }
        fields.insert(
            format!("{field_prefix}preferred_endpoint"),
            self.preferred_endpoint.clone(),
        );
        fields.insert(
            format!("{field_prefix}fallback_endpoint_count"),
            self.fallback_endpoints.len().to_string(),
        );
        for (index, endpoint) in self.fallback_endpoints.iter().enumerate() {
            fields.insert(
                format!("{field_prefix}fallback_endpoint.{index}"),
                endpoint.clone(),
            );
        }
        fields.insert(
            format!("{field_prefix}contains_prefix_start"),
            self.contains_prefix_start.to_string(),
        );
        fields.insert(
            format!("{field_prefix}owner_serving"),
            self.owner_serving.to_string(),
        );
        fields
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetadataShardRoutePage {
    pub routes: Vec<MetadataShardRoute>,
    pub prefix: String,
    pub next_shard_marker: Option<String>,
    pub scanned_shards: u64,
    pub skipped_shards: u64,
    pub payload_bytes: u64,
    pub max_payload_bytes: u64,
    pub metadata_shard_topology_epoch: u64,
    pub metadata_shard_topology_checksum: String,
    pub metadata_coordinator_session_checksum: String,
    pub expected_metadata_shard_topology_epoch: Option<u64>,
    pub expected_metadata_shard_topology_checksum: String,
    pub expected_metadata_coordinator_session_checksum: String,
    pub budget_limited: bool,
    pub has_more: bool,
}

impl MetadataShardRoutePage {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(format!("{field_prefix}prefix"), self.prefix.clone());
        append_optional_string_field(
            &mut fields,
            field_prefix,
            "next_shard_marker",
            &self.next_shard_marker,
        );
        for (name, value) in [
            ("route_count", self.routes.len() as u64),
            ("scanned_shards", self.scanned_shards),
            ("skipped_shards", self.skipped_shards),
            ("payload_bytes", self.payload_bytes),
            ("max_payload_bytes", self.max_payload_bytes),
            (
                "metadata_shard_topology_epoch",
                self.metadata_shard_topology_epoch,
            ),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
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
            format!("{field_prefix}budget_limited"),
            self.budget_limited.to_string(),
        );
        fields.insert(format!("{field_prefix}has_more"), self.has_more.to_string());
        fields.insert(
            format!("{field_prefix}truncated"),
            self.has_more.to_string(),
        );
        for (index, route) in self.routes.iter().enumerate() {
            fields.extend(route.to_control_fields(&format!("{field_prefix}route.{index}.")));
        }
        fields
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetadataShardRouteBatch {
    pub routes: Vec<MetadataShardRoute>,
    pub key_count: u64,
    pub missing_count: u64,
    pub draining_count: u64,
    pub owner_inactive_count: u64,
    pub payload_bytes: u64,
    pub max_payload_bytes: u64,
    pub metadata_shard_topology_epoch: u64,
    pub metadata_shard_topology_checksum: String,
    pub metadata_coordinator_session_checksum: String,
    pub expected_metadata_shard_topology_epoch: Option<u64>,
    pub expected_metadata_shard_topology_checksum: String,
    pub expected_metadata_coordinator_session_checksum: String,
    pub route_cache_entries: u64,
    pub route_cache_hits: u64,
    pub route_cache_misses: u64,
    pub route_cache_invalidations: u64,
    pub route_cache_expired_invalidations: u64,
}

impl MetadataShardRouteBatch {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("route_count", self.routes.len() as u64),
            ("key_count", self.key_count),
            ("missing_count", self.missing_count),
            ("draining_count", self.draining_count),
            ("owner_inactive_count", self.owner_inactive_count),
            ("payload_bytes", self.payload_bytes),
            ("max_payload_bytes", self.max_payload_bytes),
            (
                "metadata_shard_topology_epoch",
                self.metadata_shard_topology_epoch,
            ),
            ("route_cache_entries", self.route_cache_entries),
            ("route_cache_hits", self.route_cache_hits),
            ("route_cache_misses", self.route_cache_misses),
            ("route_cache_invalidations", self.route_cache_invalidations),
            (
                "route_cache_expired_invalidations",
                self.route_cache_expired_invalidations,
            ),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
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
        for (index, route) in self.routes.iter().enumerate() {
            fields.extend(route.to_control_fields(&format!("{field_prefix}route.{index}.")));
        }
        fields
    }
}
