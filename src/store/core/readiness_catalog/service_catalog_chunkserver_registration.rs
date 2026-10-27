// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn register_local_chunkserver_in_catalog(
        &mut self,
        node_id: &str,
        endpoint: &str,
        failure_domain: &str,
        replica_index: usize,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        now_ms: u64,
        catalog_source: &str,
    ) -> Result<CatalogChunkRegistration, ObjectError> {
        validate_node_id(node_id)?;
        if endpoint.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "chunkserver catalog endpoint must be set".to_string(),
            ));
        }
        if failure_domain.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "chunkserver catalog failure domain must be set".to_string(),
            ));
        }
        if catalog_source.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "chunkserver catalog source must be set".to_string(),
            ));
        }
        let readiness = self.local_chunk_storage_readiness(
            replica_index,
            min_capacity_free_bytes,
            min_reclaimable_bytes,
            now_ms,
        )?;
        let chunk_health = placement_chunk_health_from_readiness(&readiness);
        let capacity_bytes = catalog_capacity_bytes(&readiness);
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let node = PlacementNode {
            node_id: node_id.to_string(),
            endpoint: endpoint.to_string(),
            service_kind: "chunkserver".to_string(),
            failure_domain: failure_domain.to_string(),
            metadata_eligible: false,
            data_eligible: true,
            gateway_eligible: false,
            serves_page_tier: false,
            metadata_value_bytes: 0,
            capacity_bytes,
            used_bytes: readiness.physical_bytes,
            committed_bytes: 0,
            draining: catalog_chunk_draining(&readiness),
            online: true,
            last_heartbeat_ms: now_ms,
            page_allocator_shard_index: 0,
            page_allocator_shard_count: 1,
            meta_health: NodeMetaHealth::default(),
            page_health: NodePageHealth::default(),
            chunk_health,
        };
        let node = self.register_placement_node(node)?;
        if self.options.persistent_snapshot_flush_on_commit {
            if let Err(err) = self.flush_persistent_snapshot() {
                if let Some(rollback) = rollback {
                    self.restore_object_mutation_rollback(rollback);
                }
                return Err(err);
            }
        }
        if let Err(err) = self.append_chunkserver_catalog_registration_journal_record(
            node_id,
            endpoint,
            failure_domain,
            replica_index,
            min_capacity_free_bytes,
            min_reclaimable_bytes,
            now_ms,
            catalog_source,
        ) {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(CatalogChunkRegistration {
            node,
            readiness,
            replica_index,
            catalog_source: catalog_source.to_string(),
            catalog_endpoint: EMBEDDED_PLACEMENT_CATALOG_ENDPOINT.to_string(),
            catalog_endpoint_count: 1,
        })
    }

    pub fn register_local_chunkserver_at_catalog(
        &mut self,
        node_id: &str,
        endpoint: &str,
        failure_domain: &str,
        replica_index: usize,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        now_ms: u64,
        catalog_source: &str,
        catalog_endpoint: &str,
        catalog_endpoint_count: u64,
    ) -> Result<CatalogChunkRegistration, ObjectError> {
        self.register_local_chunkserver_in_catalog(
            node_id,
            endpoint,
            failure_domain,
            replica_index,
            min_capacity_free_bytes,
            min_reclaimable_bytes,
            now_ms,
            catalog_source,
        )?
        .with_catalog_route(catalog_source, catalog_endpoint, catalog_endpoint_count)
    }

    pub fn register_local_chunkserver_at_catalog_fenced(
        &mut self,
        node_id: &str,
        endpoint: &str,
        failure_domain: &str,
        replica_index: usize,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        now_ms: u64,
        catalog_source: &str,
        catalog_endpoint: &str,
        catalog_endpoint_count: u64,
        expected_catalog_route_checksum: &str,
    ) -> Result<CatalogChunkRegistration, ObjectError> {
        let registration = self.register_local_chunkserver_at_catalog(
            node_id,
            endpoint,
            failure_domain,
            replica_index,
            min_capacity_free_bytes,
            min_reclaimable_bytes,
            now_ms,
            catalog_source,
            catalog_endpoint,
            catalog_endpoint_count,
        )?;
        registration.require_catalog_route_checksum(expected_catalog_route_checksum)?;
        Ok(registration)
    }
}
