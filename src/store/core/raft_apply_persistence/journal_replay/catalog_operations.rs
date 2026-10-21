// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    fn apply_persistent_journal_catalog_operation(
        &mut self,
        op: u64,
        mut cursor: &mut SnapshotByteCursor<'_>,
    ) -> Result<bool, ObjectError> {
        match op {
            25 => {
                let node_id = cursor.read_string()?;
                let endpoint = cursor.read_string()?;
                let failure_domain = cursor.read_string()?;
                let capacity_bytes = cursor.read_u64()?;
                let used_bytes = cursor.read_u64()?;
                let draining = cursor.read_bool()?;
                let now_ms = cursor.read_u64()?;
                let catalog_source = cursor.read_string()?;
                self.register_local_objectstore_gateway_in_catalog(
                    &node_id,
                    &endpoint,
                    &failure_domain,
                    capacity_bytes,
                    used_bytes,
                    draining,
                    now_ms,
                    &catalog_source,
                )?;
            }
            26 => {
                let node_id = cursor.read_string()?;
                let capacity_bytes = cursor.read_u64()?;
                let used_bytes = cursor.read_u64()?;
                let draining = cursor.read_bool()?;
                let online = cursor.read_bool()?;
                let now_ms = cursor.read_u64()?;
                let catalog_source = cursor.read_string()?;
                self.heartbeat_local_objectstore_gateway_in_catalog(
                    &node_id,
                    capacity_bytes,
                    used_bytes,
                    draining,
                    online,
                    now_ms,
                    &catalog_source,
                )?;
            }
            27 => {
                let node_id = cursor.read_string()?;
                let now_ms = cursor.read_u64()?;
                let catalog_source = cursor.read_string()?;
                self.offline_local_objectstore_gateway_in_catalog(
                    &node_id,
                    now_ms,
                    &catalog_source,
                )?;
            }
            28 => {
                let node_id = cursor.read_string()?;
                let endpoint = cursor.read_string()?;
                let failure_domain = cursor.read_string()?;
                let allocator_shard_index = cursor.read_u64()?;
                let allocator_shard_count = cursor.read_u64()?;
                let min_capacity_free_bytes = cursor.read_u64()?;
                let min_reclaimable_bytes = cursor.read_u64()?;
                let now_ms = cursor.read_u64()?;
                let catalog_source = cursor.read_string()?;
                self.register_local_pageserver_in_catalog(
                    &node_id,
                    &endpoint,
                    &failure_domain,
                    allocator_shard_index,
                    allocator_shard_count,
                    min_capacity_free_bytes,
                    min_reclaimable_bytes,
                    now_ms,
                    &catalog_source,
                )?;
            }
            29 => {
                let node_id = cursor.read_string()?;
                let allocator_shard_index = cursor.read_u64()?;
                let allocator_shard_count = cursor.read_u64()?;
                let min_capacity_free_bytes = cursor.read_u64()?;
                let min_reclaimable_bytes = cursor.read_u64()?;
                let now_ms = cursor.read_u64()?;
                let catalog_source = cursor.read_string()?;
                let online = cursor.read_bool()?;
                self.heartbeat_local_pageserver_in_catalog(
                    &node_id,
                    allocator_shard_index,
                    allocator_shard_count,
                    min_capacity_free_bytes,
                    min_reclaimable_bytes,
                    now_ms,
                    &catalog_source,
                    online,
                )?;
            }
            30 => {
                let node_id = cursor.read_string()?;
                let allocator_shard_index = cursor.read_u64()?;
                let allocator_shard_count = cursor.read_u64()?;
                let min_capacity_free_bytes = cursor.read_u64()?;
                let min_reclaimable_bytes = cursor.read_u64()?;
                let now_ms = cursor.read_u64()?;
                let catalog_source = cursor.read_string()?;
                self.offline_local_pageserver_in_catalog(
                    &node_id,
                    allocator_shard_index,
                    allocator_shard_count,
                    min_capacity_free_bytes,
                    min_reclaimable_bytes,
                    now_ms,
                    &catalog_source,
                )?;
            }
            31 => {
                let node_id = cursor.read_string()?;
                let endpoint = cursor.read_string()?;
                let failure_domain = cursor.read_string()?;
                let replica_index =
                    cursor.read_count("persistent journal chunkserver replica index")?;
                let min_capacity_free_bytes = cursor.read_u64()?;
                let min_reclaimable_bytes = cursor.read_u64()?;
                let now_ms = cursor.read_u64()?;
                let catalog_source = cursor.read_string()?;
                self.register_local_replicaserver_in_catalog(
                    &node_id,
                    &endpoint,
                    &failure_domain,
                    replica_index,
                    min_capacity_free_bytes,
                    min_reclaimable_bytes,
                    now_ms,
                    &catalog_source,
                )?;
            }
            32 => {
                let node_id = cursor.read_string()?;
                let replica_index =
                    cursor.read_count("persistent journal chunkserver heartbeat replica index")?;
                let min_capacity_free_bytes = cursor.read_u64()?;
                let min_reclaimable_bytes = cursor.read_u64()?;
                let now_ms = cursor.read_u64()?;
                let catalog_source = cursor.read_string()?;
                let online = cursor.read_bool()?;
                self.heartbeat_local_replicaserver_in_catalog(
                    &node_id,
                    replica_index,
                    min_capacity_free_bytes,
                    min_reclaimable_bytes,
                    now_ms,
                    &catalog_source,
                    online,
                )?;
            }
            33 => {
                let node_id = cursor.read_string()?;
                let replica_index =
                    cursor.read_count("persistent journal chunkserver offline replica index")?;
                let min_capacity_free_bytes = cursor.read_u64()?;
                let min_reclaimable_bytes = cursor.read_u64()?;
                let now_ms = cursor.read_u64()?;
                let catalog_source = cursor.read_string()?;
                self.offline_local_replicaserver_in_catalog(
                    &node_id,
                    replica_index,
                    min_capacity_free_bytes,
                    min_reclaimable_bytes,
                    now_ms,
                    &catalog_source,
                )?;
            }
            34 => {
                let metaserver_node_id = cursor.read_string()?;
                let embedded_root_node_id = match cursor.read_u64()? {
                    0 => None,
                    1 => Some(cursor.read_string()?),
                    _ => return Err(ObjectError::Corruption(
                        "persistent journal embedded control-plane root presence flag is invalid"
                            .to_string(),
                    )),
                };
                let failure_domain = cursor.read_string()?;
                let metaserver_endpoint = cursor.read_string()?;
                let now_ms = cursor.read_u64()?;
                let root_lease_ttl_ms = cursor.read_u64()?;
                self.bootstrap_embedded_control_plane(
                    &metaserver_node_id,
                    embedded_root_node_id.as_deref(),
                    &failure_domain,
                    &metaserver_endpoint,
                    now_ms,
                    root_lease_ttl_ms,
                )?;
            }
            35 => {
                let mode = cursor.read_string()?;
                self.options.control_plane_mode = ControlPlaneMode::parse(&mode)?;
            }
            36 => {
                let endpoints = cursor.read_string()?;
                self.configure_metaserver_catalog_endpoints(&endpoints)?;
            }
            37 => {
                let endpoint = cursor.read_string()?;
                self.configure_rootserver_fallback_endpoint(&endpoint)?;
            }
            38 => {
                let config = read_metadata_coordinator_config(&mut cursor)?;
                self.configure_metadata_coordinator_config(config)?;
            }
            39 => {
                let config = read_metadata_coordinator_config(&mut cursor)?;
                self.configure_root_coordinator_config(config)?;
            }
            _ => return Ok(false),
        }
        Ok(true)
    }
}
