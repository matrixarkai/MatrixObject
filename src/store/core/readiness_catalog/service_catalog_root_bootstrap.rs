// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn register_root_node(&mut self, node: RootNode) -> Result<RootNode, ObjectError> {
        self.meta_service.register_node(node)
    }

    pub fn bootstrap_embedded_control_plane(
        &mut self,
        metaserver_node_id: &str,
        embedded_root_node_id: Option<&str>,
        failure_domain: &str,
        metaserver_endpoint: &str,
        now_ms: u64,
        root_lease_ttl_ms: u64,
    ) -> Result<EmbeddedControlPlaneBootstrap, ObjectError> {
        validate_node_id(metaserver_node_id)?;
        if let Some(node_id) = embedded_root_node_id {
            validate_node_id(node_id)?;
        }
        if failure_domain.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "embedded control-plane failure domain must be set".to_string(),
            ));
        }
        if metaserver_endpoint.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "embedded control-plane metaserver endpoint must be set".to_string(),
            ));
        }
        if root_lease_ttl_ms == 0 {
            return Err(ObjectError::InvalidArgument(
                "embedded control-plane root lease ttl must be positive".to_string(),
            ));
        }

        let root_node_id = embedded_root_node_id
            .map(str::to_string)
            .unwrap_or_else(|| format!("{metaserver_node_id}-root"));
        if root_node_id == metaserver_node_id {
            return Err(ObjectError::InvalidArgument(
                "embedded root node id must differ from metaserver node id".to_string(),
            ));
        }
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let embedded_root_node = RootNode {
            node_id: root_node_id.clone(),
            endpoint: metaserver_endpoint.to_string(),
            service_kind: "rootserver".to_string(),
            failure_domain: failure_domain.to_string(),
            metadata_eligible: true,
            data_eligible: false,
            gateway_eligible: false,
            metadata_value_bytes: 0,
            capacity_bytes: 0,
            used_bytes: 0,
            committed_bytes: 0,
            draining: false,
            online: true,
            last_heartbeat_ms: now_ms,
            page_allocator_shard_index: 0,
            page_allocator_shard_count: 1,
            meta_health: RootMetaHealth::default(),
            page_health: RootPageHealth::default(),
            chunk_health: RootChunkHealth::default(),
        };
        let embedded_root_node = self.register_root_node(embedded_root_node)?;
        let root_leader_lease =
            self.campaign_root_leader(&root_node_id, root_lease_ttl_ms, now_ms)?;

        let metadata_control_proof = self.metadata_control_proof_at(now_ms);
        let metadata_route_cache = self.metadata_route_cache_status_at(now_ms);
        let metaserver_node = RootNode {
            node_id: metaserver_node_id.to_string(),
            endpoint: metaserver_endpoint.to_string(),
            service_kind: "metaserver".to_string(),
            failure_domain: failure_domain.to_string(),
            metadata_eligible: true,
            data_eligible: false,
            gateway_eligible: false,
            metadata_value_bytes: metadata_control_proof.state_bytes,
            capacity_bytes: 0,
            used_bytes: 0,
            committed_bytes: 0,
            draining: false,
            online: true,
            last_heartbeat_ms: now_ms,
            page_allocator_shard_index: 0,
            page_allocator_shard_count: 1,
            meta_health: RootMetaHealth {
                control_proof: metadata_control_proof.clone(),
                route_cache: metadata_route_cache,
                ..RootMetaHealth::default()
            },
            page_health: RootPageHealth::default(),
            chunk_health: RootChunkHealth::default(),
        };
        let metaserver_node = self.register_root_node(metaserver_node)?;
        if self.options.persistent_snapshot_flush_on_commit {
            if let Err(err) = self.flush_persistent_snapshot() {
                if let Some(rollback) = rollback {
                    self.restore_object_mutation_rollback(rollback);
                }
                return Err(err);
            }
        }
        if let Err(err) = self.append_embedded_control_plane_bootstrap_journal_record(
            metaserver_node_id,
            embedded_root_node_id,
            failure_domain,
            metaserver_endpoint,
            now_ms,
            root_lease_ttl_ms,
        ) {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }

        Ok(EmbeddedControlPlaneBootstrap {
            metaserver_node,
            embedded_root_node,
            root_leader_lease,
            root_state_fingerprint: self.meta_service.root_state_fingerprint(),
            metadata_control_proof,
        })
    }

    pub fn heartbeat_root_node(
        &mut self,
        node_id: &str,
        used_bytes: u64,
        capacity_bytes: u64,
        draining: bool,
        online: bool,
        now_ms: u64,
    ) -> Result<RootNode, ObjectError> {
        self.meta_service.heartbeat_node(
            node_id,
            used_bytes,
            capacity_bytes,
            draining,
            online,
            now_ms,
        )
    }

    pub fn heartbeat_root_page_health(
        &mut self,
        node_id: &str,
        health: RootPageHealth,
        now_ms: u64,
    ) -> Result<RootNode, ObjectError> {
        self.meta_service
            .heartbeat_page_health(node_id, health, now_ms)
    }

    pub fn heartbeat_root_chunk_health(
        &mut self,
        node_id: &str,
        health: RootChunkHealth,
        now_ms: u64,
    ) -> Result<RootNode, ObjectError> {
        self.meta_service
            .heartbeat_chunk_health(node_id, health, now_ms)
    }

}
