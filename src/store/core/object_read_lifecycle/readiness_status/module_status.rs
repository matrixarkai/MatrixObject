// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn module_status(
        &self,
        min_read_index: u64,
        now_ms: u64,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        expected_replica_count: usize,
        max_root_health_items: usize,
        max_root_health_payload_bytes: u64,
    ) -> Result<ObjectStoreModuleStatus, ObjectError> {
        let readiness = self.distributed_readiness(
            min_read_index,
            now_ms,
            min_capacity_free_bytes,
            min_reclaimable_bytes,
            expected_replica_count,
            max_root_health_items,
            max_root_health_payload_bytes,
        )?;
        let health = ModuleHealthLabels::derive(&readiness, min_capacity_free_bytes);
        let metadata_replication_proof = metadata_replication_proof_report(&readiness);
        let configured_control_plane_mode =
            self.configured_control_plane_mode().as_str().to_string();
        let catalog_endpoint_set = self.select_catalog_endpoint_set(None, None)?;
        let control_plane_mode = if catalog_endpoint_set.selected.is_some()
            || catalog_endpoint_set.effective_mode != "none"
        {
            catalog_endpoint_set.effective_mode.clone()
        } else {
            readiness.control_plane_mode.clone()
        };
        let catalog_source = catalog_endpoint_set
            .selected
            .as_ref()
            .map(|selection| selection.catalog_source.clone())
            .unwrap_or_else(|| control_plane_mode.clone());
        let catalog_endpoint = catalog_endpoint_set
            .selected
            .as_ref()
            .map(|selection| selection.catalog_endpoint.clone())
            .unwrap_or_else(|| EMBEDDED_ROOT_CATALOG_ENDPOINT.to_string());
        Ok(build_objectstore_module_status(
            readiness,
            health,
            metadata_replication_proof,
            configured_control_plane_mode,
            control_plane_mode,
            catalog_source,
            catalog_endpoint,
            catalog_endpoint_set.metaserver_endpoint_configured,
            catalog_endpoint_set.rootserver_endpoint_configured,
            catalog_endpoint_set.rootserver_required,
        ))
    }
}
