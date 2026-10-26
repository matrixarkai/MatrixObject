// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CatalogObjectStoreEndpointDiscovery {
    pub catalog_source: String,
    pub catalog_endpoint: String,
    pub catalog_route_checksum_format: String,
    pub catalog_route_checksum: String,
    pub readiness_policy: String,
    pub endpoints: Vec<String>,
    pub skipped_not_gateway_eligible: u64,
    pub skipped_not_online: u64,
    pub skipped_draining: u64,
    pub skipped_empty_endpoint: u64,
    pub skipped_metadata_unreadable: u64,
    pub skipped_metadata_unwritable: u64,
    pub skipped_page_unready: u64,
    pub skipped_page_write_unready: u64,
    pub active_objectstore_count: u64,
    pub online_objectstore_count: u64,
    pub ready_gateway_count: u64,
    pub read_ready_gateway_count: u64,
    pub write_ready_gateway_count: u64,
}

impl CatalogObjectStoreEndpointDiscovery {
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
            "readiness_policy".to_string(),
            self.readiness_policy.clone(),
        );
        fields.insert(
            "endpoint_count".to_string(),
            self.endpoint_count().to_string(),
        );
        for (idx, endpoint) in self.endpoints.iter().enumerate() {
            fields.insert(format!("endpoint.{idx}"), endpoint.clone());
        }
        fields.insert(
            "skipped_not_gateway_eligible".to_string(),
            self.skipped_not_gateway_eligible.to_string(),
        );
        fields.insert(
            "skipped_not_online".to_string(),
            self.skipped_not_online.to_string(),
        );
        fields.insert(
            "skipped_draining".to_string(),
            self.skipped_draining.to_string(),
        );
        fields.insert(
            "skipped_empty_endpoint".to_string(),
            self.skipped_empty_endpoint.to_string(),
        );
        fields.insert(
            "skipped_metadata_unreadable".to_string(),
            self.skipped_metadata_unreadable.to_string(),
        );
        fields.insert(
            "skipped_metadata_unwritable".to_string(),
            self.skipped_metadata_unwritable.to_string(),
        );
        fields.insert(
            "skipped_block_unready".to_string(),
            self.skipped_page_unready.to_string(),
        );
        fields.insert(
            "skipped_block_write_unready".to_string(),
            self.skipped_page_write_unready.to_string(),
        );
        fields.insert(
            "active_objectstore_count".to_string(),
            self.active_objectstore_count.to_string(),
        );
        fields.insert(
            "online_objectstore_count".to_string(),
            self.online_objectstore_count.to_string(),
        );
        fields.insert(
            "ready_gateway_count".to_string(),
            self.ready_gateway_count.to_string(),
        );
        fields.insert(
            "read_ready_gateway_count".to_string(),
            self.read_ready_gateway_count.to_string(),
        );
        fields.insert(
            "write_ready_gateway_count".to_string(),
            self.write_ready_gateway_count.to_string(),
        );
        fields
    }
}

