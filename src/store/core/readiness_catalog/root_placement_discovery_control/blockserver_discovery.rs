impl MatrixObjectStore {
    pub fn discover_pageserver_endpoints_from_catalog(
        &self,
        readiness_policy: &str,
        require_complete_allocator_shards: bool,
    ) -> Result<RootCatalogPageEndpointDiscovery, ObjectError> {
        self.discover_pageserver_endpoints_from_catalog_with_route_fence(
            readiness_policy,
            require_complete_allocator_shards,
            None,
        )
    }

    pub fn discover_pageserver_endpoints_from_catalog_with_route_fence(
        &self,
        readiness_policy: &str,
        require_complete_allocator_shards: bool,
        expected_catalog_route_checksum: Option<&str>,
    ) -> Result<RootCatalogPageEndpointDiscovery, ObjectError> {
        if !matches!(readiness_policy, "write" | "read" | "online") {
            return Err(ObjectError::InvalidArgument(
                "blockserver discovery readiness must be write, read, or online".to_string(),
            ));
        }
        let endpoint_set = self.select_catalog_endpoint_set(None, None)?;
        if let Some(expected) = expected_catalog_route_checksum {
            endpoint_set.require_catalog_route_checksum(expected)?;
        }
        let coverage = self.root_service.page_allocator_shard_coverage();
        let service_coverage = self.root_service.pageserver_service_coverage();
        if coverage.status == "none" {
            return Err(ObjectError::PreconditionFailed(
                "catalog has no active blockserver allocator shards".to_string(),
            ));
        }
        if coverage.status != "complete"
            && (require_complete_allocator_shards
                || matches!(
                    coverage.status.as_str(),
                    "invalid_shard" | "duplicate_shard" | "mixed_shard_count"
                ))
        {
            return Err(ObjectError::Corruption(format!(
                "catalog block allocator shards are not safe for discovery: {} {}/{}",
                coverage.status, coverage.covered_shards, coverage.shard_count
            )));
        }

        let mut endpoints = Vec::new();
        let mut seen = BTreeSet::new();
        let mut allocator_shards = BTreeSet::new();
        let mut skipped_not_ready = 0u64;
        let mut skipped_write_not_ready = 0u64;
        let mut skipped_read_not_ready = 0u64;
        for node in self.root_service.list_nodes() {
            if node.service_kind != "blockserver"
                || !node.data_eligible
                || !node.online
                || node.draining
            {
                continue;
            }
            if readiness_policy == "write" && !node.page_health.ready {
                skipped_not_ready = skipped_not_ready.saturating_add(1);
                skipped_write_not_ready = skipped_write_not_ready.saturating_add(1);
                continue;
            }
            if readiness_policy == "read" && !node.page_health.read_ready {
                skipped_not_ready = skipped_not_ready.saturating_add(1);
                skipped_read_not_ready = skipped_read_not_ready.saturating_add(1);
                continue;
            }
            if node.endpoint.is_empty() {
                continue;
            }
            if node.page_allocator_shard_count == 0
                || node.page_allocator_shard_index >= node.page_allocator_shard_count
            {
                return Err(ObjectError::Corruption(format!(
                    "catalog blockserver node has invalid allocator shard: {}",
                    node.node_id
                )));
            }
            if !allocator_shards.insert((
                node.page_allocator_shard_count,
                node.page_allocator_shard_index,
            )) {
                return Err(ObjectError::Corruption(format!(
                    "catalog blockserver allocator shard is duplicated: {}/{}",
                    node.page_allocator_shard_index, node.page_allocator_shard_count
                )));
            }
            if seen.insert(node.endpoint.clone()) {
                endpoints.push(node.endpoint);
            }
        }
        endpoints.sort();
        if endpoints.is_empty() {
            return Err(ObjectError::PreconditionFailed(format!(
                "catalog has no eligible blockserver endpoints for readiness={readiness_policy}"
            )));
        }
        Ok(RootCatalogPageEndpointDiscovery {
            catalog_source: endpoint_set
                .selected
                .as_ref()
                .map(|route| route.catalog_source.clone())
                .unwrap_or_else(|| endpoint_set.effective_mode.clone()),
            catalog_endpoint: endpoint_set
                .selected
                .as_ref()
                .map(|route| route.catalog_endpoint.clone())
                .unwrap_or_default(),
            catalog_route_checksum_format: endpoint_set.catalog_route_checksum_format,
            catalog_route_checksum: endpoint_set.catalog_route_checksum,
            endpoints,
            readiness_policy: readiness_policy.to_string(),
            skipped_not_ready,
            skipped_write_not_ready,
            skipped_read_not_ready,
            active_pageserver_count: coverage.active_pageserver_count,
            ready_pageserver_count: service_coverage.ready_pageserver_count,
            read_ready_pageserver_count: service_coverage.read_ready_pageserver_count,
            allocator_shard_count: coverage.shard_count,
            allocator_covered_shards: coverage.covered_shards,
            allocator_status: coverage.status,
        })
    }

}
