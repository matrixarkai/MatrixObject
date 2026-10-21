// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixMetaService {
    pub fn list_nodes(&self) -> Vec<RootNode> {
        self.nodes.values().cloned().collect()
    }

    pub fn placement_coordinator_config(&self) -> MetadataCoordinatorConfig {
        self.placement_coordinator_config.clone()
    }

    pub fn configure_placement_coordinator_config(
        &mut self,
        mut config: MetadataCoordinatorConfig,
    ) -> Result<MetadataCoordinatorConfig, ObjectError> {
        config.endpoints.sort();
        config.endpoints.dedup();
        validate_metadata_coordinator_config(&config)?;
        if config.revision <= self.placement_coordinator_config.revision {
            config.revision = self.placement_coordinator_config.revision.saturating_add(1);
        }
        self.placement_coordinator_config = config.clone();
        self.advance_commit_index();
        Ok(config)
    }

    pub fn list_nodes_page(
        &self,
        marker: Option<&str>,
        limit: usize,
        service_kind_filter: Option<&str>,
        draining_filter: Option<bool>,
        online_filter: Option<bool>,
        metadata_eligible_filter: Option<bool>,
        data_eligible_filter: Option<bool>,
        gateway_eligible_filter: Option<bool>,
    ) -> Result<RootNodePage, ObjectError> {
        if let Some(marker) = marker {
            validate_node_id(marker)?;
        }
        if let Some(service_kind) = service_kind_filter {
            validate_service_kind(service_kind)?;
        }
        if limit == 0 {
            return Err(ObjectError::InvalidArgument(
                "root node page limit must be positive".to_string(),
            ));
        }
        let mut page = RootNodePage {
            marker: marker.map(str::to_string),
            service_kind_filter: service_kind_filter.map(str::to_string),
            draining_filter,
            online_filter,
            metadata_eligible_filter,
            data_eligible_filter,
            gateway_eligible_filter,
            total_node_count: self.nodes.len() as u64,
            ..RootNodePage::default()
        };
        for (node_id, node) in &self.nodes {
            if marker.map_or(false, |marker| node_id.as_str() <= marker) {
                continue;
            }
            page.scanned_nodes = page.scanned_nodes.saturating_add(1);
            if service_kind_filter.map_or(false, |service_kind| node.service_kind != service_kind) {
                page.skipped_service_kind_nodes = page.skipped_service_kind_nodes.saturating_add(1);
                page.skipped_nodes = page.skipped_nodes.saturating_add(1);
                continue;
            }
            if draining_filter.map_or(false, |draining| node.draining != draining) {
                page.skipped_draining_nodes = page.skipped_draining_nodes.saturating_add(1);
                page.skipped_nodes = page.skipped_nodes.saturating_add(1);
                continue;
            }
            if online_filter.map_or(false, |online| node.online != online) {
                page.skipped_online_nodes = page.skipped_online_nodes.saturating_add(1);
                page.skipped_nodes = page.skipped_nodes.saturating_add(1);
                continue;
            }
            if metadata_eligible_filter.map_or(false, |eligible| node.metadata_eligible != eligible)
            {
                page.skipped_metadata_eligible_nodes =
                    page.skipped_metadata_eligible_nodes.saturating_add(1);
                page.skipped_nodes = page.skipped_nodes.saturating_add(1);
                continue;
            }
            if data_eligible_filter.map_or(false, |eligible| node.data_eligible != eligible) {
                page.skipped_data_eligible_nodes =
                    page.skipped_data_eligible_nodes.saturating_add(1);
                page.skipped_nodes = page.skipped_nodes.saturating_add(1);
                continue;
            }
            if gateway_eligible_filter.map_or(false, |eligible| node.gateway_eligible != eligible) {
                page.skipped_gateway_eligible_nodes =
                    page.skipped_gateway_eligible_nodes.saturating_add(1);
                page.skipped_nodes = page.skipped_nodes.saturating_add(1);
                continue;
            }
            page.matched_nodes = page.matched_nodes.saturating_add(1);
            if page.nodes.len() >= limit {
                page.has_more = true;
                page.next_marker = page.nodes.last().map(|last| last.node_id.clone());
                break;
            }
            page.total_capacity_bytes = page
                .total_capacity_bytes
                .saturating_add(node.capacity_bytes);
            page.total_used_bytes = page.total_used_bytes.saturating_add(node.used_bytes);
            page.total_committed_bytes = page
                .total_committed_bytes
                .saturating_add(node.committed_bytes);
            page.nodes.push(node.clone());
        }
        if !page.has_more {
            page.next_marker = None;
        }
        Ok(page)
    }
}
