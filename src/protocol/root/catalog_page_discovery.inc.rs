// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CatalogPageEndpointDiscovery {
    pub catalog_source: String,
    pub catalog_endpoint: String,
    pub catalog_route_checksum_format: String,
    pub catalog_route_checksum: String,
    pub endpoints: Vec<String>,
    pub readiness_policy: String,
    pub skipped_not_ready: u64,
    pub skipped_write_not_ready: u64,
    pub skipped_read_not_ready: u64,
    pub active_pageserver_count: u64,
    pub ready_pageserver_count: u64,
    pub read_ready_pageserver_count: u64,
    pub allocator_shard_count: u64,
    pub allocator_covered_shards: u64,
    pub allocator_status: String,
}

impl CatalogPageEndpointDiscovery {
    pub fn endpoint_count(&self) -> u64 {
        self.endpoints.len() as u64
    }

    pub fn to_control_fields(&self) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert("catalog_source".to_string(), self.catalog_source.clone());
        fields.insert(
            "catalog_endpoint".to_string(),
            self.catalog_endpoint.clone(),
        );
        fields.insert(
            "catalog_route_checksum_format".to_string(),
            self.catalog_route_checksum_format.clone(),
        );
        fields.insert(
            "catalog_route_checksum".to_string(),
            self.catalog_route_checksum.clone(),
        );
        fields.insert(
            "endpoint_count".to_string(),
            self.endpoint_count().to_string(),
        );
        for (idx, endpoint) in self.endpoints.iter().enumerate() {
            fields.insert(format!("endpoint.{idx}"), endpoint.clone());
        }
        fields.insert(
            "readiness_policy".to_string(),
            self.readiness_policy.clone(),
        );
        fields.insert(
            "skipped_not_ready".to_string(),
            self.skipped_not_ready.to_string(),
        );
        fields.insert(
            "skipped_write_not_ready".to_string(),
            self.skipped_write_not_ready.to_string(),
        );
        fields.insert(
            "skipped_read_not_ready".to_string(),
            self.skipped_read_not_ready.to_string(),
        );
        fields.insert(
            "active_pageserver_count".to_string(),
            self.active_pageserver_count.to_string(),
        );
        fields.insert(
            "ready_pageserver_count".to_string(),
            self.ready_pageserver_count.to_string(),
        );
        fields.insert(
            "read_ready_pageserver_count".to_string(),
            self.read_ready_pageserver_count.to_string(),
        );
        fields.insert(
            "allocator_shard_count".to_string(),
            self.allocator_shard_count.to_string(),
        );
        fields.insert(
            "allocator_covered_shards".to_string(),
            self.allocator_covered_shards.to_string(),
        );
        fields.insert(
            "allocator_status".to_string(),
            self.allocator_status.clone(),
        );
        fields
    }
}

