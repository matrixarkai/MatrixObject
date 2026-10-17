#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RootNodePage {
    pub nodes: Vec<RootNode>,
    pub marker: Option<String>,
    pub next_marker: Option<String>,
    pub service_kind_filter: Option<String>,
    pub draining_filter: Option<bool>,
    pub online_filter: Option<bool>,
    pub metadata_eligible_filter: Option<bool>,
    pub data_eligible_filter: Option<bool>,
    pub gateway_eligible_filter: Option<bool>,
    pub scanned_nodes: u64,
    pub matched_nodes: u64,
    pub skipped_nodes: u64,
    pub skipped_service_kind_nodes: u64,
    pub skipped_draining_nodes: u64,
    pub skipped_online_nodes: u64,
    pub skipped_metadata_eligible_nodes: u64,
    pub skipped_data_eligible_nodes: u64,
    pub skipped_gateway_eligible_nodes: u64,
    pub total_node_count: u64,
    pub total_capacity_bytes: u64,
    pub total_used_bytes: u64,
    pub total_committed_bytes: u64,
    pub has_more: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RootCatalogMetaServerEndpointDiscovery {
    pub catalog_source: String,
    pub catalog_endpoint: String,
    pub endpoints: Vec<String>,
    pub skipped_not_metadata_eligible: u64,
    pub skipped_not_online: u64,
    pub skipped_draining: u64,
    pub skipped_not_ready: u64,
    pub skipped_read_not_ready: u64,
    pub skipped_write_not_ready: u64,
    pub skipped_not_caught_up: u64,
    pub skipped_read_fence_not_ready: u64,
    pub skipped_empty_endpoint: u64,
    pub active_metaserver_count: u64,
    pub readable_metaserver_count: u64,
    pub writable_metaserver_count: u64,
    pub caught_up_metaserver_count: u64,
    pub read_fence_covered_metaserver_count: u64,
    pub metadata_quorum: u64,
    pub metadata_coverage_status: String,
    pub metadata_read_quorum_available: bool,
    pub metadata_write_quorum_available: bool,
    pub metadata_caught_up_quorum_available: bool,
    pub metadata_read_fence_quorum_available: bool,
    pub metadata_min_safe_read_fence: u64,
    pub metadata_max_apply_commit_lag: u64,
}

impl RootCatalogMetaServerEndpointDiscovery {
    pub fn endpoint_count(&self) -> u64 {
        self.endpoints.len() as u64
    }

    pub fn selected_endpoint(&self) -> Option<&str> {
        self.endpoints.first().map(String::as_str)
    }

    pub fn to_control_fields(&self) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert("catalog_source".to_string(), self.catalog_source.clone());
        fields.insert(
            "catalog_endpoint".to_string(),
            self.catalog_endpoint.clone(),
        );
        fields.insert(
            "endpoint_count".to_string(),
            self.endpoint_count().to_string(),
        );
        for (idx, endpoint) in self.endpoints.iter().enumerate() {
            fields.insert(format!("endpoint.{idx}"), endpoint.clone());
        }
        if let Some(endpoint) = self.selected_endpoint() {
            fields.insert("selected_endpoint".to_string(), endpoint.to_string());
        }
        fields.insert(
            "skipped_not_metadata_eligible".to_string(),
            self.skipped_not_metadata_eligible.to_string(),
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
            "skipped_not_ready".to_string(),
            self.skipped_not_ready.to_string(),
        );
        fields.insert(
            "skipped_read_not_ready".to_string(),
            self.skipped_read_not_ready.to_string(),
        );
        fields.insert(
            "skipped_write_not_ready".to_string(),
            self.skipped_write_not_ready.to_string(),
        );
        fields.insert(
            "skipped_not_caught_up".to_string(),
            self.skipped_not_caught_up.to_string(),
        );
        fields.insert(
            "skipped_read_fence_not_ready".to_string(),
            self.skipped_read_fence_not_ready.to_string(),
        );
        fields.insert(
            "skipped_empty_endpoint".to_string(),
            self.skipped_empty_endpoint.to_string(),
        );
        fields.insert(
            "active_metaserver_count".to_string(),
            self.active_metaserver_count.to_string(),
        );
        fields.insert(
            "readable_metaserver_count".to_string(),
            self.readable_metaserver_count.to_string(),
        );
        fields.insert(
            "writable_metaserver_count".to_string(),
            self.writable_metaserver_count.to_string(),
        );
        fields.insert(
            "caught_up_metaserver_count".to_string(),
            self.caught_up_metaserver_count.to_string(),
        );
        fields.insert(
            "read_fence_covered_metaserver_count".to_string(),
            self.read_fence_covered_metaserver_count.to_string(),
        );
        fields.insert(
            "metadata_quorum".to_string(),
            self.metadata_quorum.to_string(),
        );
        fields.insert(
            "metadata_coverage_status".to_string(),
            self.metadata_coverage_status.clone(),
        );
        fields.insert(
            "metadata_read_quorum_available".to_string(),
            self.metadata_read_quorum_available.to_string(),
        );
        fields.insert(
            "metadata_write_quorum_available".to_string(),
            self.metadata_write_quorum_available.to_string(),
        );
        fields.insert(
            "metadata_caught_up_quorum_available".to_string(),
            self.metadata_caught_up_quorum_available.to_string(),
        );
        fields.insert(
            "metadata_read_fence_quorum_available".to_string(),
            self.metadata_read_fence_quorum_available.to_string(),
        );
        fields.insert(
            "metadata_min_safe_read_fence".to_string(),
            self.metadata_min_safe_read_fence.to_string(),
        );
        fields.insert(
            "metadata_max_apply_commit_lag".to_string(),
            self.metadata_max_apply_commit_lag.to_string(),
        );
        fields
    }
}

