// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn heartbeat_local_chunkserver_in_catalog(
        &mut self,
        node_id: &str,
        replica_index: usize,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        now_ms: u64,
        catalog_source: &str,
        online: bool,
    ) -> Result<CatalogChunkRegistration, ObjectError> {
        validate_node_id(node_id)?;
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
        let capacity_bytes = catalog_capacity_bytes(&readiness);
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        self.heartbeat_placement_node(
            node_id,
            readiness.physical_bytes,
            capacity_bytes,
            catalog_chunk_draining(&readiness),
            online,
            now_ms,
        )?;
        let health = placement_chunk_health_from_readiness(&readiness);
        let node = self.heartbeat_placement_chunk_health(node_id, health, now_ms)?;
        if self.options.persistent_snapshot_flush_on_commit {
            if let Err(err) = self.flush_persistent_snapshot() {
                if let Some(rollback) = rollback {
                    self.restore_object_mutation_rollback(rollback);
                }
                return Err(err);
            }
        }
        if let Err(err) = self.append_chunkserver_catalog_heartbeat_journal_record(
            node_id,
            replica_index,
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
        Ok(CatalogChunkRegistration {
            node,
            readiness,
            replica_index,
            catalog_source: catalog_source.to_string(),
            catalog_endpoint: EMBEDDED_PLACEMENT_CATALOG_ENDPOINT.to_string(),
            catalog_endpoint_count: 1,
        })
    }

    pub fn heartbeat_local_chunkserver_at_catalog(
        &mut self,
        node_id: &str,
        replica_index: usize,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        now_ms: u64,
        catalog_source: &str,
        online: bool,
        catalog_endpoint: &str,
        catalog_endpoint_count: u64,
    ) -> Result<CatalogChunkRegistration, ObjectError> {
        self.heartbeat_local_chunkserver_in_catalog(
            node_id,
            replica_index,
            min_capacity_free_bytes,
            min_reclaimable_bytes,
            now_ms,
            catalog_source,
            online,
        )?
        .with_catalog_route(catalog_source, catalog_endpoint, catalog_endpoint_count)
    }

    pub fn heartbeat_local_chunkserver_at_catalog_fenced(
        &mut self,
        node_id: &str,
        replica_index: usize,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        now_ms: u64,
        catalog_source: &str,
        online: bool,
        catalog_endpoint: &str,
        catalog_endpoint_count: u64,
        expected_catalog_route_checksum: &str,
    ) -> Result<CatalogChunkRegistration, ObjectError> {
        let registration = self.heartbeat_local_chunkserver_at_catalog(
            node_id,
            replica_index,
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
