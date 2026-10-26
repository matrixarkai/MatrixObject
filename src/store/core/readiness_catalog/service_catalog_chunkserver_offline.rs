// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn offline_local_chunkserver_in_catalog(
        &mut self,
        node_id: &str,
        replica_index: usize,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        now_ms: u64,
        catalog_source: &str,
    ) -> Result<CatalogChunkRegistration, ObjectError> {
        validate_node_id(node_id)?;
        if catalog_source.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "chunkserver catalog source must be set".to_string(),
            ));
        }
        let existing = self
            .meta_service
            .nodes
            .get(node_id)
            .ok_or_else(|| ObjectError::NotFound("root node missing".to_string()))?
            .clone();
        if existing.service_kind != "chunkserver" || !existing.data_eligible {
            return Err(ObjectError::PreconditionFailed(
                "chunkserver catalog offline heartbeat requires registered chunkserver node"
                    .to_string(),
            ));
        }
        let readiness = self.local_chunk_storage_readiness(
            replica_index,
            min_capacity_free_bytes,
            min_reclaimable_bytes,
            now_ms,
        )?;
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        self.heartbeat_placement_node(
            node_id,
            existing.used_bytes,
            existing.capacity_bytes,
            true,
            false,
            now_ms,
        )?;
        let mut health = placement_chunk_health_from_readiness(&readiness);
        health.accepting_writes = false;
        health.raft_writable = false;
        health.service_mode = "offline".to_string();
        health.control_read_ready = false;
        health.control_write_ready = false;
        let node = self.heartbeat_placement_chunk_health(node_id, health, now_ms)?;
        if self.options.persistent_snapshot_flush_on_commit {
            if let Err(err) = self.flush_persistent_snapshot() {
                if let Some(rollback) = rollback {
                    self.restore_object_mutation_rollback(rollback);
                }
                return Err(err);
            }
        }
        if let Err(err) = self.append_chunkserver_catalog_offline_journal_record(
            node_id,
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

    pub fn offline_local_chunkserver_at_catalog(
        &mut self,
        node_id: &str,
        replica_index: usize,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        now_ms: u64,
        catalog_source: &str,
        catalog_endpoint: &str,
        catalog_endpoint_count: u64,
    ) -> Result<CatalogChunkRegistration, ObjectError> {
        self.offline_local_chunkserver_in_catalog(
            node_id,
            replica_index,
            min_capacity_free_bytes,
            min_reclaimable_bytes,
            now_ms,
            catalog_source,
        )?
        .with_catalog_route(catalog_source, catalog_endpoint, catalog_endpoint_count)
    }

    pub fn offline_local_chunkserver_at_catalog_fenced(
        &mut self,
        node_id: &str,
        replica_index: usize,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        now_ms: u64,
        catalog_source: &str,
        catalog_endpoint: &str,
        catalog_endpoint_count: u64,
        expected_catalog_route_checksum: &str,
    ) -> Result<CatalogChunkRegistration, ObjectError> {
        let registration = self.offline_local_chunkserver_at_catalog(
            node_id,
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
