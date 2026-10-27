// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixMetaService {
    pub(crate) fn invalidate_snapshot_export_cache(&self) {
        *self.placement_snapshot_export_cache.borrow_mut() = None;
        *self.root_state_fingerprint_cache.borrow_mut() = None;
    }

    fn advance_commit_index(&mut self) {
        self.invalidate_snapshot_export_cache();
        self.placement_commit_index = self.placement_commit_index.saturating_add(1);
    }

    pub fn register_node(&mut self, mut node: PlacementNode) -> Result<PlacementNode, ObjectError> {
        validate_node_id(&node.node_id)?;
        if !node.endpoint.is_empty()
            && (node.endpoint.len() > 512
                || node.endpoint.contains('\0')
                || node.endpoint.contains('\n')
                || node.endpoint.chars().any(char::is_whitespace))
        {
            return Err(ObjectError::InvalidArgument(
                "root node endpoint must be empty or 1..512 bytes without whitespace/control characters"
                    .to_string(),
            ));
        }
        validate_service_kind(&node.service_kind)?;
        Self::validate_page_allocator_shard(&node)?;
        Self::validate_node_metadata_coordinator_backend(&node)?;
        self.validate_active_page_allocator_conflict(&node)?;
        if node.used_bytes > node.capacity_bytes {
            return Err(ObjectError::InvalidArgument(
                "root node used bytes exceeds capacity".to_string(),
            ));
        }
        let existing = self.nodes.get(&node.node_id);
        if let Some(existing) = existing {
            Self::reject_stale_node_heartbeat(existing, node.last_heartbeat_ms, "registration")?;
        }
        let committed = existing.map_or(node.committed_bytes, |existing| existing.committed_bytes);
        if node.capacity_bytes < committed || node.used_bytes < committed {
            return Err(ObjectError::PreconditionFailed(
                "root node heartbeat is below committed placement bytes".to_string(),
            ));
        }
        node.committed_bytes = committed;
        self.nodes.insert(node.node_id.clone(), node.clone());
        self.advance_commit_index();
        Ok(node)
    }

    pub fn heartbeat_node(
        &mut self,
        node_id: &str,
        used_bytes: u64,
        capacity_bytes: u64,
        draining: bool,
        online: bool,
        now_ms: u64,
    ) -> Result<PlacementNode, ObjectError> {
        let existing = self
            .nodes
            .get(node_id)
            .ok_or_else(|| ObjectError::NotFound("root node missing".to_string()))?;
        Self::reject_stale_node_heartbeat(existing, now_ms, "node heartbeat")?;
        if used_bytes > capacity_bytes || capacity_bytes < existing.committed_bytes {
            return Err(ObjectError::PreconditionFailed(
                "root node heartbeat violates committed placement capacity".to_string(),
            ));
        }
        let mut updated = existing.clone();
        updated.used_bytes = used_bytes;
        updated.capacity_bytes = capacity_bytes;
        updated.draining = draining;
        updated.online = online;
        updated.last_heartbeat_ms = now_ms;
        self.validate_active_page_allocator_conflict(&updated)?;
        let node = self
            .nodes
            .get_mut(node_id)
            .ok_or_else(|| ObjectError::NotFound("root node missing".to_string()))?;
        node.used_bytes = used_bytes;
        node.capacity_bytes = capacity_bytes;
        node.draining = draining;
        node.online = online;
        node.last_heartbeat_ms = now_ms;
        let updated_node = node.clone();
        self.advance_commit_index();
        Ok(updated_node)
    }

    pub fn heartbeat_page_health(
        &mut self,
        node_id: &str,
        health: NodePageHealth,
        now_ms: u64,
    ) -> Result<PlacementNode, ObjectError> {
        validate_node_id(node_id)?;
        let node = self
            .nodes
            .get_mut(node_id)
            .ok_or_else(|| ObjectError::NotFound("root blockserver node missing".to_string()))?;
        if !node.serves_page_tier() {
            return Err(ObjectError::PreconditionFailed(
                "root page health heartbeat requires blockserver node".to_string(),
            ));
        }
        Self::reject_stale_node_heartbeat(node, now_ms, "block health heartbeat")?;
        node.page_health = health;
        node.last_heartbeat_ms = now_ms;
        let updated_node = node.clone();
        self.advance_commit_index();
        Ok(updated_node)
    }

    pub fn heartbeat_chunk_health(
        &mut self,
        node_id: &str,
        health: NodeChunkHealth,
        now_ms: u64,
    ) -> Result<PlacementNode, ObjectError> {
        validate_node_id(node_id)?;
        let node = self
            .nodes
            .get_mut(node_id)
            .ok_or_else(|| ObjectError::NotFound("root chunkserver node missing".to_string()))?;
        if !node.serves_chunk_tier() {
            return Err(ObjectError::PreconditionFailed(
                "root chunk health heartbeat requires chunkserver node".to_string(),
            ));
        }
        Self::reject_stale_node_heartbeat(node, now_ms, "chunk health heartbeat")?;
        node.chunk_health = health;
        node.last_heartbeat_ms = now_ms;
        let updated_node = node.clone();
        self.advance_commit_index();
        Ok(updated_node)
    }

    fn reject_stale_node_heartbeat(
        existing: &PlacementNode,
        now_ms: u64,
        context: &str,
    ) -> Result<(), ObjectError> {
        if now_ms < existing.last_heartbeat_ms {
            return Err(ObjectError::PreconditionFailed(format!(
                "root {context} is stale for {}: {} < {}",
                existing.node_id, now_ms, existing.last_heartbeat_ms
            )));
        }
        Ok(())
    }

    pub fn expire_node_heartbeats(&mut self, ttl_ms: u64, now_ms: u64) -> Result<u64, ObjectError> {
        if ttl_ms == 0 {
            return Err(ObjectError::InvalidArgument(
                "root heartbeat expiry requires ttl_ms".to_string(),
            ));
        }
        let expired_node_ids: Vec<String> = self
            .nodes
            .values()
            .filter(|node| {
                node.online
                    && node.last_heartbeat_ms != 0
                    && now_ms > node.last_heartbeat_ms
                    && now_ms.saturating_sub(node.last_heartbeat_ms) > ttl_ms
            })
            .map(|node| node.node_id.clone())
            .collect();
        if expired_node_ids.is_empty() {
            return Ok(0);
        }
        for node_id in &expired_node_ids {
            if let Some(node) = self.nodes.get_mut(node_id) {
                node.online = false;
            }
        }
        self.advance_commit_index();
        Ok(expired_node_ids.len() as u64)
    }

    pub fn remove_node(
        &mut self,
        node_id: &str,
        force: bool,
    ) -> Result<PlacementNodeRemoveReport, ObjectError> {
        validate_node_id(node_id)?;
        if !self.nodes.contains_key(node_id) {
            return Err(ObjectError::NotFound("root node missing".to_string()));
        }
        let mut report = PlacementNodeRemoveReport {
            node_id: node_id.to_string(),
            force,
            node_count: self.nodes.len() as u64,
            ..PlacementNodeRemoveReport::default()
        };
        for placement in self.placements.values() {
            if placement.node_ids.iter().any(|placed| placed == node_id) {
                report.referencing_object_count = report.referencing_object_count.saturating_add(1);
                report.referencing_bytes =
                    report.referencing_bytes.saturating_add(placement.length);
            }
        }
        if report.referencing_object_count != 0 && !force {
            return Err(ObjectError::PreconditionFailed(format!(
                "root node remove has committed object placements: {node_id}"
            )));
        }
        self.nodes.remove(node_id);
        report.removed = true;
        report.node_count = self.nodes.len() as u64;
        self.advance_commit_index();
        Ok(report)
    }
}
