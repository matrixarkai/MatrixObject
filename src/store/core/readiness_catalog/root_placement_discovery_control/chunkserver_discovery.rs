impl MatrixObjectStore {
    pub fn discover_replicaserver_endpoints_from_catalog(
        &self,
        min_free_bytes: u64,
    ) -> Result<RootCatalogChunkEndpointDiscovery, ObjectError> {
        self.discover_replicaserver_endpoints_from_catalog_with_route_fence(min_free_bytes, None)
    }

    pub fn discover_replicaserver_endpoints_from_catalog_with_route_fence(
        &self,
        min_free_bytes: u64,
        expected_catalog_route_checksum: Option<&str>,
    ) -> Result<RootCatalogChunkEndpointDiscovery, ObjectError> {
        let endpoint_set = self.select_catalog_endpoint_set(None, None)?;
        if let Some(expected) = expected_catalog_route_checksum {
            endpoint_set.require_catalog_route_checksum(expected)?;
        }
        let coverage = self.meta_service.replicaserver_coverage(min_free_bytes);
        if coverage.status == "none" {
            return Err(ObjectError::PreconditionFailed(
                "catalog has no active chunkserver endpoints".to_string(),
            ));
        }
        if coverage.status == "no_writable" || coverage.writable_replicaserver_count == 0 {
            return Err(ObjectError::PreconditionFailed(format!(
                "catalog has no writable chunkserver endpoints: status={} writable={} draining={} capacity_floor_blocked={} frozen={} read_only={} raft_unwritable={}",
                coverage.status,
                coverage.writable_replicaserver_count,
                coverage.draining_replicaserver_count,
                coverage.capacity_floor_blocked_replicaserver_count,
                coverage.frozen_replicaserver_count,
                coverage.read_only_replicaserver_count,
                coverage.raft_unwritable_replicaserver_count
            )));
        }

        let mut endpoints = Vec::new();
        let mut disabled_endpoint_indexes = BTreeSet::new();
        let mut seen = BTreeSet::new();
        for node in self.meta_service.list_nodes() {
            if node.service_kind != "chunkserver" || !node.online || !node.data_eligible {
                continue;
            }
            if node.endpoint.is_empty() {
                continue;
            }
            if !seen.insert(node.endpoint.clone()) {
                continue;
            }
            let reported_free = if node.chunk_health.capacity_free_bytes > 0 {
                node.chunk_health.capacity_free_bytes
            } else {
                node.capacity_bytes.saturating_sub(node.committed_bytes)
            };
            let has_control_proof = !node.chunk_health.control_proof_checksum.is_empty();
            let control_blocked = has_control_proof
                && (!node.chunk_health.control_read_ready
                    || !node.chunk_health.control_write_ready);
            let disabled = node.draining
                || node.chunk_health.frozen
                || !node.chunk_health.accepting_writes
                || !node.chunk_health.raft_writable
                || control_blocked
                || node.chunk_health.service_mode == "read_only"
                || node.chunk_health.service_mode == "draining"
                || reported_free < min_free_bytes;
            let index = endpoints.len();
            endpoints.push(node.endpoint);
            if disabled {
                disabled_endpoint_indexes.insert(index);
            }
        }
        if endpoints.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "catalog has no online data-eligible chunkserver endpoints".to_string(),
            ));
        }
        let writable = endpoints
            .len()
            .saturating_sub(disabled_endpoint_indexes.len()) as u64;
        if writable != coverage.writable_replicaserver_count {
            return Err(ObjectError::Corruption(format!(
                "catalog chunkserver coverage does not match discovered routes: writable={writable} root_writable={}",
                coverage.writable_replicaserver_count
            )));
        }
        Ok(RootCatalogChunkEndpointDiscovery {
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
            disabled_endpoint_indexes,
            min_free_bytes,
            active_replicaserver_count: coverage.active_replicaserver_count,
            writable_replicaserver_count: coverage.writable_replicaserver_count,
            draining_replicaserver_count: coverage.draining_replicaserver_count,
            capacity_floor_blocked_replicaserver_count: coverage
                .capacity_floor_blocked_replicaserver_count,
            frozen_replicaserver_count: coverage.frozen_replicaserver_count,
            read_only_replicaserver_count: coverage.read_only_replicaserver_count,
            raft_unwritable_replicaserver_count: coverage.raft_unwritable_replicaserver_count,
            coverage_status: coverage.status,
        })
    }

}
