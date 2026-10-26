// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn register_placement_node(&mut self, node: PlacementNode) -> Result<PlacementNode, ObjectError> {
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

        // THE EMBEDDED ROOT NODE IS NOT CREATED ANY MORE. This store has one control plane
        // and it is the metaserver; a second synthetic node of kind "rootserver" existed only
        // because the service whose placement state was folded into the metaserver used to
        // need one. Nothing reads it: it was never a placement candidate
        // (`is_data_service_node` admits only chunkserver and blockserver), the metadata
        // coverage walk skips any node that is not a metaserver, and the leader lease moved to
        // the metaserver node in #230.
        //
        // The id is still validated, and still journalled, so that a journal written before
        // this change replays unchanged -- record 34 carries this field and an old journal
        // must still load. It names a node this build does not create.
        if let Some(root_node_id) = embedded_root_node_id {
            if root_node_id == metaserver_node_id {
                return Err(ObjectError::InvalidArgument(
                    "embedded root node id must differ from metaserver node id".to_string(),
                ));
            }
        }
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };

        let metadata_control_proof = self.metadata_control_proof_at(now_ms);
        let metadata_route_cache = self.metadata_route_cache_status_at(now_ms);
        let metaserver_node = PlacementNode {
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
            meta_health: NodeMetaHealth {
                control_proof: metadata_control_proof.clone(),
                route_cache: metadata_route_cache,
                ..NodeMetaHealth::default()
            },
            page_health: NodePageHealth::default(),
            chunk_health: NodeChunkHealth::default(),
        };
        let metaserver_node = self.register_placement_node(metaserver_node)?;
        // THE LEASE IS CAMPAIGNED FOR THE METASERVER NODE, and it is campaigned here rather
        // than earlier because `campaign_placement_leader` refuses a candidate that is not in the
        // roster yet -- so the node has to be registered first. It used to be campaigned for
        // the embedded root node, two statements above its own registration.
        //
        // This store has one control plane and it is the metaserver, so the metaserver node
        // is where the lease belongs. A store reopened from a snapshot written before this
        // change still carries a lease held by the root node, and still loads: see
        // `is_control_plane_leader_candidate`, which accepts the old kind for exactly that
        // reason.
        let root_leader_lease =
            self.campaign_placement_leader(metaserver_node_id, root_lease_ttl_ms, now_ms)?;
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
            root_leader_lease,
            placement_state_fingerprint: self.meta_service.placement_state_fingerprint(),
            metadata_control_proof,
        })
    }

    pub fn heartbeat_placement_node(
        &mut self,
        node_id: &str,
        used_bytes: u64,
        capacity_bytes: u64,
        draining: bool,
        online: bool,
        now_ms: u64,
    ) -> Result<PlacementNode, ObjectError> {
        self.meta_service.heartbeat_node(
            node_id,
            used_bytes,
            capacity_bytes,
            draining,
            online,
            now_ms,
        )
    }

    pub fn heartbeat_placement_page_health(
        &mut self,
        node_id: &str,
        health: NodePageHealth,
        now_ms: u64,
    ) -> Result<PlacementNode, ObjectError> {
        self.meta_service
            .heartbeat_page_health(node_id, health, now_ms)
    }

    pub fn heartbeat_placement_chunk_health(
        &mut self,
        node_id: &str,
        health: NodeChunkHealth,
        now_ms: u64,
    ) -> Result<PlacementNode, ObjectError> {
        self.meta_service
            .heartbeat_chunk_health(node_id, health, now_ms)
    }

}
