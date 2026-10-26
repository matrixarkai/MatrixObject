// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn heartbeat_local_pageserver_in_catalog(
        &mut self,
        node_id: &str,
        allocator_shard_index: u64,
        allocator_shard_count: u64,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        now_ms: u64,
        catalog_source: &str,
        online: bool,
    ) -> Result<CatalogPageRegistration, ObjectError> {
        validate_node_id(node_id)?;
        if catalog_source.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "blockserver catalog source must be set".to_string(),
            ));
        }
        if allocator_shard_count == 0 || allocator_shard_index >= allocator_shard_count {
            return Err(ObjectError::InvalidArgument(
                "blockserver allocator shard must satisfy index < count and count > 0".to_string(),
            ));
        }
        let readiness =
            self.page_storage_readiness(min_capacity_free_bytes, min_reclaimable_bytes, now_ms);
        let capacity_bytes = catalog_page_capacity_bytes(&readiness);
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        self.heartbeat_placement_node(
            node_id,
            readiness.total_physical_bytes,
            capacity_bytes,
            catalog_page_draining(&readiness),
            online,
            now_ms,
        )?;
        let health = placement_page_health_from_readiness(&readiness);
        let node = self.heartbeat_placement_page_health(node_id, health, now_ms)?;
        if node.page_allocator_shard_index != allocator_shard_index
            || node.page_allocator_shard_count != allocator_shard_count
        {
            return Err(ObjectError::PreconditionFailed(
                "blockserver catalog heartbeat shard fence does not match registered node"
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
        if let Err(err) = self.append_pageserver_catalog_heartbeat_journal_record(
            node_id,
            allocator_shard_index,
            allocator_shard_count,
            min_capacity_free_bytes,
            min_reclaimable_bytes,
            now_ms,
            catalog_source,
            online,
        ) {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(CatalogPageRegistration {
            node,
            readiness,
            allocator_shard_index,
            allocator_shard_count,
            catalog_source: catalog_source.to_string(),
            catalog_endpoint: EMBEDDED_PLACEMENT_CATALOG_ENDPOINT.to_string(),
            catalog_endpoint_count: 1,
        })
    }

    pub fn heartbeat_local_pageserver_at_catalog(
        &mut self,
        node_id: &str,
        allocator_shard_index: u64,
        allocator_shard_count: u64,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        now_ms: u64,
        catalog_source: &str,
        online: bool,
        catalog_endpoint: &str,
        catalog_endpoint_count: u64,
    ) -> Result<CatalogPageRegistration, ObjectError> {
        self.heartbeat_local_pageserver_in_catalog(
            node_id,
            allocator_shard_index,
            allocator_shard_count,
            min_capacity_free_bytes,
            min_reclaimable_bytes,
            now_ms,
            catalog_source,
            online,
        )?
        .with_catalog_route(catalog_source, catalog_endpoint, catalog_endpoint_count)
    }

    pub fn heartbeat_local_pageserver_at_catalog_fenced(
        &mut self,
        node_id: &str,
        allocator_shard_index: u64,
        allocator_shard_count: u64,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        now_ms: u64,
        catalog_source: &str,
        online: bool,
        catalog_endpoint: &str,
        catalog_endpoint_count: u64,
        expected_catalog_route_checksum: &str,
    ) -> Result<CatalogPageRegistration, ObjectError> {
        let registration = self.heartbeat_local_pageserver_at_catalog(
            node_id,
            allocator_shard_index,
            allocator_shard_count,
            min_capacity_free_bytes,
            min_reclaimable_bytes,
            now_ms,
            catalog_source,
            online,
            catalog_endpoint,
            catalog_endpoint_count,
        )?;
        registration.require_catalog_route_checksum(expected_catalog_route_checksum)?;
        Ok(registration)
    }
}
