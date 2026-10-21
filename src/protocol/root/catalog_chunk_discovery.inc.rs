// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RootCatalogChunkEndpointDiscovery {
    pub catalog_source: String,
    pub catalog_endpoint: String,
    pub catalog_route_checksum_format: String,
    pub catalog_route_checksum: String,
    pub endpoints: Vec<String>,
    pub disabled_endpoint_indexes: BTreeSet<usize>,
    pub min_free_bytes: u64,
    pub active_replicaserver_count: u64,
    pub writable_replicaserver_count: u64,
    pub draining_replicaserver_count: u64,
    pub capacity_floor_blocked_replicaserver_count: u64,
    pub frozen_replicaserver_count: u64,
    pub read_only_replicaserver_count: u64,
    pub raft_unwritable_replicaserver_count: u64,
    pub coverage_status: String,
}

impl RootCatalogChunkEndpointDiscovery {
    pub fn endpoint_count(&self) -> u64 {
        self.endpoints.len() as u64
    }

    pub fn disabled_endpoint_count(&self) -> u64 {
        self.disabled_endpoint_indexes.len() as u64
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
            "disabled_endpoint_count".to_string(),
            self.disabled_endpoint_count().to_string(),
        );
        for (idx, endpoint_index) in self.disabled_endpoint_indexes.iter().enumerate() {
            fields.insert(
                format!("disabled_endpoint_index.{idx}"),
                endpoint_index.to_string(),
            );
        }
        fields.insert(
            "min_free_bytes".to_string(),
            self.min_free_bytes.to_string(),
        );
        fields.insert(
            "active_chunkserver_count".to_string(),
            self.active_replicaserver_count.to_string(),
        );
        fields.insert(
            "writable_chunkserver_count".to_string(),
            self.writable_replicaserver_count.to_string(),
        );
        fields.insert(
            "draining_chunkserver_count".to_string(),
            self.draining_replicaserver_count.to_string(),
        );
        fields.insert(
            "capacity_floor_blocked_chunkserver_count".to_string(),
            self.capacity_floor_blocked_replicaserver_count.to_string(),
        );
        fields.insert(
            "frozen_chunkserver_count".to_string(),
            self.frozen_replicaserver_count.to_string(),
        );
        fields.insert(
            "read_only_chunkserver_count".to_string(),
            self.read_only_replicaserver_count.to_string(),
        );
        fields.insert(
            "raft_unwritable_chunkserver_count".to_string(),
            self.raft_unwritable_replicaserver_count.to_string(),
        );
        fields.insert("coverage_status".to_string(), self.coverage_status.clone());
        fields
    }
}

