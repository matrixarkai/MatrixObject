// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixMetaService {
    fn release_committed_bytes(&mut self, placement: &ObjectPlacement) {
        for node_id in &placement.node_ids {
            if let Some(node) = self.nodes.get_mut(node_id) {
                node.committed_bytes = node.committed_bytes.saturating_sub(placement.length);
            }
        }
    }

    pub(crate) fn validate_page_allocator_shard(node: &PlacementNode) -> Result<(), ObjectError> {
        if node.service_kind == "blockserver"
            && (node.page_allocator_shard_count == 0
                || node.page_allocator_shard_index >= node.page_allocator_shard_count)
        {
            return Err(ObjectError::InvalidArgument(
                "blockserver allocator shard is invalid".to_string(),
            ));
        }
        Ok(())
    }

    fn validate_node_metadata_coordinator_backend(node: &PlacementNode) -> Result<(), ObjectError> {
        if node.service_kind != "metaserver"
            || node
                .meta_health
                .control_proof
                .coordinator_backend
                .is_empty()
        {
            return Ok(());
        }
        let backend =
            normalize_coordinator_backend(&node.meta_health.control_proof.coordinator_backend);
        if !is_supported_coordinator_backend(&backend) {
            return Err(ObjectError::InvalidArgument(
                "metaserver metadata coordinator backend must be embedded-raft/raft/matrix-raft, embedded, etcd, or zookeeper/zk"
                    .to_string(),
            ));
        }
        Ok(())
    }

    fn validate_active_page_allocator_conflict(&self, node: &PlacementNode) -> Result<(), ObjectError> {
        if node.service_kind != "blockserver"
            || !node.data_eligible
            || !node.online
            || node.draining
        {
            return Ok(());
        }
        Self::validate_page_allocator_shard(node)?;
        for existing in self.nodes.values() {
            if existing.node_id == node.node_id
                || existing.service_kind != "blockserver"
                || !existing.data_eligible
                || !existing.online
                || existing.draining
            {
                continue;
            }
            if existing.page_allocator_shard_count != node.page_allocator_shard_count {
                return Err(ObjectError::InvalidArgument(
                    "active blockserver allocator shard count mismatch".to_string(),
                ));
            }
            if existing.page_allocator_shard_index == node.page_allocator_shard_index {
                return Err(ObjectError::InvalidArgument(
                    "active blockserver allocator shard duplicate".to_string(),
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn is_data_service_node(node: &PlacementNode) -> bool {
        node.data_eligible
            && (node.service_kind == "chunkserver" || node.service_kind == "blockserver")
    }

    fn replica_service_pages_new_placement(node: &PlacementNode) -> bool {
        node.service_kind == "chunkserver"
            && (node.chunk_health.frozen
                || !node.chunk_health.accepting_writes
                || !node.chunk_health.raft_writable
                || node.chunk_health.service_mode == "read_only"
                || node.chunk_health.service_mode == "draining")
    }

    pub(crate) fn node_accepts_new_placement(node: &PlacementNode) -> bool {
        Self::is_data_service_node(node)
            && node.online
            && !node.draining
            && !Self::replica_service_pages_new_placement(node)
    }

    pub(crate) fn node_effective_free_bytes(node: &PlacementNode) -> u64 {
        if node.service_kind == "chunkserver" && node.chunk_health.capacity_free_bytes > 0 {
            node.chunk_health.capacity_free_bytes
        } else {
            node.capacity_bytes.saturating_sub(node.committed_bytes)
        }
    }

    pub(crate) fn node_effective_capacity_bytes(node: &PlacementNode) -> u64 {
        node.committed_bytes
            .saturating_add(Self::node_effective_free_bytes(node))
    }

    fn node_reclaim_pressure_bytes(node: &PlacementNode) -> u64 {
        node.chunk_health
            .reclaimable_bytes
            .saturating_add(node.chunk_health.trailing_trim_bytes)
            .saturating_add(node.chunk_health.fragmented_discarded_bytes)
            .saturating_add(node.page_health.total_reclaimable_bytes)
            .saturating_add(node.page_health.total_trailing_trim_bytes)
            .saturating_add(node.page_health.total_fragmented_discarded_bytes)
            .saturating_add(node.page_health.chunk_physical_discard_bytes)
    }

    fn node_fragmentation_pressure_units(node: &PlacementNode) -> u64 {
        node.chunk_health
            .fragmented_chunk_count
            .saturating_add(node.chunk_health.discarded_range_count)
            .saturating_add(node.page_health.total_fragmented_chunk_count)
            .saturating_add(node.page_health.chunk_physical_discard_range_count)
    }

    fn node_reclaim_pressure_score(node: &PlacementNode) -> u64 {
        let byte_pressure = Self::node_reclaim_pressure_bytes(node);
        let fragment_pressure =
            Self::node_fragmentation_pressure_units(node).saturating_mul(1024 * 1024);
        byte_pressure.saturating_add(fragment_pressure)
    }

    fn compare_placement_target_nodes(left: &PlacementNode, right: &PlacementNode) -> Ordering {
        Self::node_reclaim_pressure_score(left)
            .cmp(&Self::node_reclaim_pressure_score(right))
            .then_with(|| {
                Self::node_effective_free_bytes(right).cmp(&Self::node_effective_free_bytes(left))
            })
            .then_with(|| {
                Self::node_reclaim_pressure_bytes(left)
                    .cmp(&Self::node_reclaim_pressure_bytes(right))
            })
            .then_with(|| {
                Self::node_fragmentation_pressure_units(left)
                    .cmp(&Self::node_fragmentation_pressure_units(right))
            })
            .then_with(|| left.failure_domain.cmp(&right.failure_domain))
            .then_with(|| left.node_id.cmp(&right.node_id))
    }
}
