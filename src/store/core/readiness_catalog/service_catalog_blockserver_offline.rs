impl MatrixObjectStore {
    pub fn offline_local_pageserver_in_catalog(
        &mut self,
        node_id: &str,
        allocator_shard_index: u64,
        allocator_shard_count: u64,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        now_ms: u64,
        catalog_source: &str,
    ) -> Result<RootCatalogPageRegistration, ObjectError> {
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
        let existing = self
            .meta_service
            .nodes
            .get(node_id)
            .ok_or_else(|| ObjectError::NotFound("root node missing".to_string()))?
            .clone();
        if existing.service_kind != "blockserver" || !existing.gateway_eligible {
            return Err(ObjectError::PreconditionFailed(
                "blockserver catalog offline heartbeat requires registered blockserver node"
                    .to_string(),
            ));
        }
        if existing.page_allocator_shard_index != allocator_shard_index
            || existing.page_allocator_shard_count != allocator_shard_count
        {
            return Err(ObjectError::PreconditionFailed(
                "blockserver catalog offline heartbeat shard fence does not match registered node"
                    .to_string(),
            ));
        }
        let readiness =
            self.page_storage_readiness(min_capacity_free_bytes, min_reclaimable_bytes, now_ms);
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        self.heartbeat_root_node(
            node_id,
            existing.used_bytes,
            existing.capacity_bytes,
            true,
            false,
            now_ms,
        )?;
        let mut health = root_page_health_from_readiness(&readiness);
        health.ready = false;
        health.read_ready = false;
        let node = self.heartbeat_root_page_health(node_id, health, now_ms)?;
        if self.options.persistent_snapshot_flush_on_commit {
            if let Err(err) = self.flush_persistent_snapshot() {
                if let Some(rollback) = rollback {
                    self.restore_object_mutation_rollback(rollback);
                }
                return Err(err);
            }
        }
        if let Err(err) = self.append_pageserver_catalog_offline_journal_record(
            node_id,
            allocator_shard_index,
            allocator_shard_count,
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
        Ok(RootCatalogPageRegistration {
            node,
            readiness,
            allocator_shard_index,
            allocator_shard_count,
            catalog_source: catalog_source.to_string(),
            catalog_endpoint: EMBEDDED_ROOT_CATALOG_ENDPOINT.to_string(),
            catalog_endpoint_count: 1,
        })
    }

    pub fn offline_local_pageserver_at_catalog(
        &mut self,
        node_id: &str,
        allocator_shard_index: u64,
        allocator_shard_count: u64,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        now_ms: u64,
        catalog_source: &str,
        catalog_endpoint: &str,
        catalog_endpoint_count: u64,
    ) -> Result<RootCatalogPageRegistration, ObjectError> {
        self.offline_local_pageserver_in_catalog(
            node_id,
            allocator_shard_index,
            allocator_shard_count,
            min_capacity_free_bytes,
            min_reclaimable_bytes,
            now_ms,
            catalog_source,
        )?
        .with_catalog_route(catalog_source, catalog_endpoint, catalog_endpoint_count)
    }

    pub fn offline_local_pageserver_at_catalog_fenced(
        &mut self,
        node_id: &str,
        allocator_shard_index: u64,
        allocator_shard_count: u64,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        now_ms: u64,
        catalog_source: &str,
        catalog_endpoint: &str,
        catalog_endpoint_count: u64,
        expected_catalog_route_checksum: &str,
    ) -> Result<RootCatalogPageRegistration, ObjectError> {
        let registration = self.offline_local_pageserver_at_catalog(
            node_id,
            allocator_shard_index,
            allocator_shard_count,
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
