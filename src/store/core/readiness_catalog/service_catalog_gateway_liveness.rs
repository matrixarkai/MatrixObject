// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn register_local_objectstore_gateway_in_catalog(
        &mut self,
        node_id: &str,
        endpoint: &str,
        failure_domain: &str,
        capacity_bytes: u64,
        used_bytes: u64,
        draining: bool,
        now_ms: u64,
        catalog_source: &str,
    ) -> Result<CatalogObjectStoreRegistration, ObjectError> {
        validate_node_id(node_id)?;
        if endpoint.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "objectstore catalog endpoint must be set".to_string(),
            ));
        }
        if failure_domain.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "objectstore catalog failure domain must be set".to_string(),
            ));
        }
        if catalog_source.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "objectstore catalog source must be set".to_string(),
            ));
        }
        let effective_capacity = if capacity_bytes == 0 {
            u64::MAX
        } else {
            capacity_bytes
        };
        if used_bytes > effective_capacity {
            return Err(ObjectError::InvalidArgument(
                "objectstore catalog used bytes exceeds capacity".to_string(),
            ));
        }
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let node = PlacementNode {
            node_id: node_id.to_string(),
            endpoint: endpoint.to_string(),
            service_kind: "objectstore".to_string(),
            failure_domain: failure_domain.to_string(),
            metadata_eligible: false,
            data_eligible: false,
            gateway_eligible: true,
            metadata_value_bytes: 0,
            capacity_bytes: effective_capacity,
            used_bytes,
            committed_bytes: 0,
            draining,
            online: true,
            last_heartbeat_ms: now_ms,
            page_allocator_shard_index: 0,
            page_allocator_shard_count: 1,
            meta_health: NodeMetaHealth::default(),
            page_health: NodePageHealth::default(),
            chunk_health: NodeChunkHealth::default(),
        };
        let node = self.register_root_node(node)?;
        if self.options.persistent_snapshot_flush_on_commit {
            if let Err(err) = self.flush_persistent_snapshot() {
                if let Some(rollback) = rollback {
                    self.restore_object_mutation_rollback(rollback);
                }
                return Err(err);
            }
        }
        if let Err(err) = self.append_objectstore_gateway_registration_journal_record(
            node_id,
            endpoint,
            failure_domain,
            effective_capacity,
            used_bytes,
            draining,
            now_ms,
            catalog_source,
        ) {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(CatalogObjectStoreRegistration {
            node,
            catalog_source: catalog_source.to_string(),
            catalog_endpoint: EMBEDDED_METASERVER_CATALOG_ENDPOINT.to_string(),
            catalog_endpoint_count: 1,
        })
    }

    pub fn register_local_objectstore_gateway_at_catalog(
        &mut self,
        node_id: &str,
        endpoint: &str,
        failure_domain: &str,
        capacity_bytes: u64,
        used_bytes: u64,
        draining: bool,
        now_ms: u64,
        catalog_source: &str,
        catalog_endpoint: &str,
        catalog_endpoint_count: u64,
    ) -> Result<CatalogObjectStoreRegistration, ObjectError> {
        self.register_local_objectstore_gateway_in_catalog(
            node_id,
            endpoint,
            failure_domain,
            capacity_bytes,
            used_bytes,
            draining,
            now_ms,
            catalog_source,
        )?
        .with_catalog_route(catalog_source, catalog_endpoint, catalog_endpoint_count)
    }

    pub fn register_local_objectstore_gateway_at_catalog_fenced(
        &mut self,
        node_id: &str,
        endpoint: &str,
        failure_domain: &str,
        capacity_bytes: u64,
        used_bytes: u64,
        draining: bool,
        now_ms: u64,
        catalog_source: &str,
        catalog_endpoint: &str,
        catalog_endpoint_count: u64,
        expected_catalog_route_checksum: &str,
    ) -> Result<CatalogObjectStoreRegistration, ObjectError> {
        let registration = self.register_local_objectstore_gateway_at_catalog(
            node_id,
            endpoint,
            failure_domain,
            capacity_bytes,
            used_bytes,
            draining,
            now_ms,
            catalog_source,
            catalog_endpoint,
            catalog_endpoint_count,
        )?;
        registration.require_catalog_route_checksum(expected_catalog_route_checksum)?;
        Ok(registration)
    }

    pub fn heartbeat_local_objectstore_gateway_in_catalog(
        &mut self,
        node_id: &str,
        capacity_bytes: u64,
        used_bytes: u64,
        draining: bool,
        online: bool,
        now_ms: u64,
        catalog_source: &str,
    ) -> Result<CatalogObjectStoreRegistration, ObjectError> {
        validate_node_id(node_id)?;
        if catalog_source.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "objectstore catalog source must be set".to_string(),
            ));
        }
        let effective_capacity = if capacity_bytes == 0 {
            u64::MAX
        } else {
            capacity_bytes
        };
        if used_bytes > effective_capacity {
            return Err(ObjectError::InvalidArgument(
                "objectstore catalog used bytes exceeds capacity".to_string(),
            ));
        }
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let node = self.heartbeat_root_node(
            node_id,
            used_bytes,
            effective_capacity,
            draining,
            online,
            now_ms,
        )?;
        if node.service_kind != "objectstore" || !node.gateway_eligible {
            return Err(ObjectError::PreconditionFailed(
                "objectstore catalog heartbeat requires registered gateway objectstore node"
                    .to_string(),
            ));
        }
        if self.options.persistent_snapshot_flush_on_commit {
            if let Err(err) = self.flush_persistent_snapshot() {
                if let Some(rollback) = rollback {
                    self.restore_object_mutation_rollback(rollback);
                }
                return Err(err);
            }
        }
        if let Err(err) = self.append_objectstore_gateway_heartbeat_journal_record(
            node_id,
            effective_capacity,
            used_bytes,
            draining,
            online,
            now_ms,
            catalog_source,
        ) {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(CatalogObjectStoreRegistration {
            node,
            catalog_source: catalog_source.to_string(),
            catalog_endpoint: EMBEDDED_METASERVER_CATALOG_ENDPOINT.to_string(),
            catalog_endpoint_count: 1,
        })
    }

    pub fn heartbeat_local_objectstore_gateway_at_catalog(
        &mut self,
        node_id: &str,
        capacity_bytes: u64,
        used_bytes: u64,
        draining: bool,
        online: bool,
        now_ms: u64,
        catalog_source: &str,
        catalog_endpoint: &str,
        catalog_endpoint_count: u64,
    ) -> Result<CatalogObjectStoreRegistration, ObjectError> {
        self.heartbeat_local_objectstore_gateway_in_catalog(
            node_id,
            capacity_bytes,
            used_bytes,
            draining,
            online,
            now_ms,
            catalog_source,
        )?
        .with_catalog_route(catalog_source, catalog_endpoint, catalog_endpoint_count)
    }

    pub fn heartbeat_local_objectstore_gateway_at_catalog_fenced(
        &mut self,
        node_id: &str,
        capacity_bytes: u64,
        used_bytes: u64,
        draining: bool,
        online: bool,
        now_ms: u64,
        catalog_source: &str,
        catalog_endpoint: &str,
        catalog_endpoint_count: u64,
        expected_catalog_route_checksum: &str,
    ) -> Result<CatalogObjectStoreRegistration, ObjectError> {
        let registration = self.heartbeat_local_objectstore_gateway_at_catalog(
            node_id,
            capacity_bytes,
            used_bytes,
            draining,
            online,
            now_ms,
            catalog_source,
            catalog_endpoint,
            catalog_endpoint_count,
        )?;
        registration.require_catalog_route_checksum(expected_catalog_route_checksum)?;
        Ok(registration)
    }

    pub fn offline_local_objectstore_gateway_in_catalog(
        &mut self,
        node_id: &str,
        now_ms: u64,
        catalog_source: &str,
    ) -> Result<CatalogObjectStoreRegistration, ObjectError> {
        validate_node_id(node_id)?;
        if catalog_source.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "objectstore catalog source must be set".to_string(),
            ));
        }
        let existing = self
            .meta_service
            .nodes
            .get(node_id)
            .ok_or_else(|| ObjectError::NotFound("root node missing".to_string()))?
            .clone();
        if existing.service_kind != "objectstore" || !existing.gateway_eligible {
            return Err(ObjectError::PreconditionFailed(
                "objectstore catalog offline heartbeat requires registered gateway objectstore node"
                    .to_string(),
            ));
        }
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let node = self.heartbeat_root_node(
            node_id,
            existing.used_bytes,
            existing.capacity_bytes,
            true,
            false,
            now_ms,
        )?;
        if self.options.persistent_snapshot_flush_on_commit {
            if let Err(err) = self.flush_persistent_snapshot() {
                if let Some(rollback) = rollback {
                    self.restore_object_mutation_rollback(rollback);
                }
                return Err(err);
            }
        }
        if let Err(err) =
            self.append_objectstore_gateway_offline_journal_record(node_id, now_ms, catalog_source)
        {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(CatalogObjectStoreRegistration {
            node,
            catalog_source: catalog_source.to_string(),
            catalog_endpoint: EMBEDDED_METASERVER_CATALOG_ENDPOINT.to_string(),
            catalog_endpoint_count: 1,
        })
    }

    pub fn offline_local_objectstore_gateway_at_catalog(
        &mut self,
        node_id: &str,
        now_ms: u64,
        catalog_source: &str,
        catalog_endpoint: &str,
        catalog_endpoint_count: u64,
    ) -> Result<CatalogObjectStoreRegistration, ObjectError> {
        self.offline_local_objectstore_gateway_in_catalog(node_id, now_ms, catalog_source)?
            .with_catalog_route(catalog_source, catalog_endpoint, catalog_endpoint_count)
    }

    pub fn offline_local_objectstore_gateway_at_catalog_fenced(
        &mut self,
        node_id: &str,
        now_ms: u64,
        catalog_source: &str,
        catalog_endpoint: &str,
        catalog_endpoint_count: u64,
        expected_catalog_route_checksum: &str,
    ) -> Result<CatalogObjectStoreRegistration, ObjectError> {
        let registration = self.offline_local_objectstore_gateway_at_catalog(
            node_id,
            now_ms,
            catalog_source,
            catalog_endpoint,
            catalog_endpoint_count,
        )?;
        registration.require_catalog_route_checksum(expected_catalog_route_checksum)?;
        Ok(registration)
    }

}
