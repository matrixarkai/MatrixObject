// SPDX-License-Identifier: Apache-2.0
impl MatrixObjectStore {
    pub fn discover_metaserver_endpoints_from_catalog(
        &self,
    ) -> Result<RootCatalogMetaServerEndpointDiscovery, ObjectError> {
        let nodes = self.meta_service.list_nodes();
        let metadata_quorum = raft_quorum(self.raft_voter_node_ids.len() as u64);
        let coverage = compute_root_metadata_service_coverage(&nodes, metadata_quorum);
        if coverage.active_metaserver_count == 0 {
            return Err(ObjectError::PreconditionFailed(
                "catalog has no active metaserver endpoints".to_string(),
            ));
        }
        if coverage.status != "healthy"
            || !coverage.metadata_read_quorum_available
            || !coverage.metadata_write_quorum_available
            || !coverage.metadata_caught_up_quorum_available
            || !coverage.metadata_read_fence_quorum_available
        {
            return Err(ObjectError::PreconditionFailed(format!(
                "catalog metaserver coverage is not healthy: {}",
                coverage.status
            )));
        }

        let mut discovery = RootCatalogMetaServerEndpointDiscovery {
            catalog_source: self.effective_control_plane_mode().to_string(),
            catalog_endpoint: EMBEDDED_ROOT_CATALOG_ENDPOINT.to_string(),
            active_metaserver_count: coverage.active_metaserver_count,
            readable_metaserver_count: coverage.metadata_readable_metaserver_count,
            writable_metaserver_count: coverage.metadata_writable_metaserver_count,
            caught_up_metaserver_count: coverage.metadata_caught_up_metaserver_count,
            read_fence_covered_metaserver_count: coverage
                .metadata_read_fence_covered_metaserver_count,
            metadata_quorum: coverage.metadata_quorum,
            metadata_coverage_status: coverage.status,
            metadata_read_quorum_available: coverage.metadata_read_quorum_available,
            metadata_write_quorum_available: coverage.metadata_write_quorum_available,
            metadata_caught_up_quorum_available: coverage.metadata_caught_up_quorum_available,
            metadata_read_fence_quorum_available: coverage.metadata_read_fence_quorum_available,
            metadata_min_safe_read_fence: coverage.metadata_min_safe_read_fence,
            metadata_max_apply_commit_lag: coverage.metadata_max_apply_commit_lag,
            ..RootCatalogMetaServerEndpointDiscovery::default()
        };
        let mut seen = BTreeSet::new();
        for node in nodes {
            if node.service_kind != "metaserver" {
                continue;
            }
            if !node.metadata_eligible {
                discovery.skipped_not_metadata_eligible =
                    discovery.skipped_not_metadata_eligible.saturating_add(1);
                continue;
            }
            if !node.online {
                discovery.skipped_not_online = discovery.skipped_not_online.saturating_add(1);
                continue;
            }
            if node.draining {
                discovery.skipped_draining = discovery.skipped_draining.saturating_add(1);
                continue;
            }
            let proof = &node.meta_health.control_proof;
            let caught_up = proof.commit_bounds_valid
                && (proof.metadata_replication_quorum == 0
                    || proof.metadata_replication_acks >= proof.metadata_replication_quorum);
            let read_fence_ready = proof.read_fence_covered
                && proof.read_fence_commit_lag == 0
                && proof.read_fence_read_index_lag == 0
                && proof.read_fence_applied_lag == 0;
            if !proof.read_ready || !proof.write_ready || !caught_up || !read_fence_ready {
                discovery.skipped_not_ready = discovery.skipped_not_ready.saturating_add(1);
                if !proof.read_ready {
                    discovery.skipped_read_not_ready =
                        discovery.skipped_read_not_ready.saturating_add(1);
                }
                if !proof.write_ready {
                    discovery.skipped_write_not_ready =
                        discovery.skipped_write_not_ready.saturating_add(1);
                }
                if !caught_up {
                    discovery.skipped_not_caught_up =
                        discovery.skipped_not_caught_up.saturating_add(1);
                }
                if !read_fence_ready {
                    discovery.skipped_read_fence_not_ready =
                        discovery.skipped_read_fence_not_ready.saturating_add(1);
                }
                continue;
            }
            if node.endpoint.is_empty() {
                discovery.skipped_empty_endpoint =
                    discovery.skipped_empty_endpoint.saturating_add(1);
                continue;
            }
            if seen.insert(node.endpoint.clone()) {
                discovery.endpoints.push(node.endpoint);
            }
        }
        discovery.endpoints.sort();
        if discovery.endpoints.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "catalog has no eligible metaserver endpoints".to_string(),
            ));
        }
        Ok(discovery)
    }

}
