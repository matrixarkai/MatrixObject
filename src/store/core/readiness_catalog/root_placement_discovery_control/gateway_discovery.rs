// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn discover_objectstore_gateway_endpoints_from_catalog(
        &self,
    ) -> Result<CatalogObjectStoreEndpointDiscovery, ObjectError> {
        self.discover_objectstore_gateway_endpoints_from_catalog_with_policy("read")
    }

    pub fn discover_objectstore_gateway_endpoints_from_catalog_with_policy(
        &self,
        readiness_policy: &str,
    ) -> Result<CatalogObjectStoreEndpointDiscovery, ObjectError> {
        self.discover_objectstore_gateway_endpoints_from_catalog_with_policy_and_route_fence(
            readiness_policy,
            None,
        )
    }

    pub fn discover_objectstore_gateway_endpoints_from_catalog_with_policy_and_route_fence(
        &self,
        readiness_policy: &str,
        expected_catalog_route_checksum: Option<&str>,
    ) -> Result<CatalogObjectStoreEndpointDiscovery, ObjectError> {
        if !matches!(readiness_policy, "read" | "write" | "online") {
            return Err(ObjectError::InvalidArgument(
                "objectstore gateway discovery readiness must be read, write, or online"
                    .to_string(),
            ));
        }
        let endpoint_set = self.select_catalog_endpoint_set(None, None)?;
        if let Some(expected) = expected_catalog_route_checksum {
            endpoint_set.require_catalog_route_checksum(expected)?;
        }
        let mut discovery = CatalogObjectStoreEndpointDiscovery::default();
        discovery.catalog_source = endpoint_set
            .selected
            .as_ref()
            .map(|route| route.catalog_source.clone())
            .unwrap_or_else(|| endpoint_set.effective_mode.clone());
        discovery.catalog_endpoint = endpoint_set
            .selected
            .as_ref()
            .map(|route| route.catalog_endpoint.clone())
            .unwrap_or_default();
        discovery.catalog_route_checksum_format = endpoint_set.catalog_route_checksum_format;
        discovery.catalog_route_checksum = endpoint_set.catalog_route_checksum;
        discovery.readiness_policy = readiness_policy.to_string();
        let mut seen = BTreeSet::new();
        for node in self.meta_service.list_nodes() {
            if node.service_kind != "objectstore" {
                continue;
            }
            discovery.active_objectstore_count =
                discovery.active_objectstore_count.saturating_add(1);
            if !node.gateway_eligible {
                discovery.skipped_not_gateway_eligible =
                    discovery.skipped_not_gateway_eligible.saturating_add(1);
                continue;
            }
            if !node.online {
                discovery.skipped_not_online = discovery.skipped_not_online.saturating_add(1);
                continue;
            }
            discovery.online_objectstore_count =
                discovery.online_objectstore_count.saturating_add(1);
            if node.draining {
                discovery.skipped_draining = discovery.skipped_draining.saturating_add(1);
                continue;
            }
            let read_ready =
                node.meta_health.control_proof.read_ready && node.page_health.read_ready;
            let write_ready = node.meta_health.control_proof.write_ready && node.page_health.ready;
            if !node.meta_health.control_proof.read_ready {
                discovery.skipped_metadata_unreadable =
                    discovery.skipped_metadata_unreadable.saturating_add(1);
            }
            if !node.page_health.read_ready {
                discovery.skipped_page_unready = discovery.skipped_page_unready.saturating_add(1);
            }
            if !node.meta_health.control_proof.write_ready {
                discovery.skipped_metadata_unwritable =
                    discovery.skipped_metadata_unwritable.saturating_add(1);
            }
            if !node.page_health.ready {
                discovery.skipped_page_write_unready =
                    discovery.skipped_page_write_unready.saturating_add(1);
            }
            if node.endpoint.is_empty() {
                discovery.skipped_empty_endpoint =
                    discovery.skipped_empty_endpoint.saturating_add(1);
                continue;
            }
            let include_endpoint = readiness_policy == "online"
                || (readiness_policy == "read" && read_ready)
                || (readiness_policy == "write" && write_ready);
            if !include_endpoint {
                continue;
            }
            if seen.insert(node.endpoint.clone()) {
                discovery.endpoints.push(node.endpoint);
                discovery.ready_gateway_count = discovery.ready_gateway_count.saturating_add(1);
                if read_ready {
                    discovery.read_ready_gateway_count =
                        discovery.read_ready_gateway_count.saturating_add(1);
                }
                if write_ready {
                    discovery.write_ready_gateway_count =
                        discovery.write_ready_gateway_count.saturating_add(1);
                }
            }
        }
        discovery.endpoints.sort();
        if discovery.active_objectstore_count == 0 {
            return Err(ObjectError::PreconditionFailed(
                "catalog has no objectstore gateway nodes".to_string(),
            ));
        }
        if discovery.endpoints.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "catalog has no eligible objectstore gateway endpoints".to_string(),
            ));
        }
        Ok(discovery)
    }

}
