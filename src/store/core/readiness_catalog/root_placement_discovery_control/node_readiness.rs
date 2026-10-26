// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn local_chunk_storage_readiness(
        &self,
        replica_index: usize,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        now_ms: u64,
    ) -> Result<ReplicaStorageReadiness, ObjectError> {
        let replica = self
            .chunk_service
            .replicas
            .get(replica_index)
            .ok_or_else(|| {
                ObjectError::InvalidArgument(format!(
                    "chunk replica index {replica_index} is out of range"
                ))
            })?;
        Ok(replica.storage_readiness(min_capacity_free_bytes, min_reclaimable_bytes, now_ms))
    }


    pub fn expire_placement_node_heartbeats(
        &mut self,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<u64, ObjectError> {
        self.meta_service.expire_node_heartbeats(ttl_ms, now_ms)
    }


    pub fn remove_placement_node(
        &mut self,
        node_id: &str,
        force: bool,
    ) -> Result<PlacementNodeRemoveReport, ObjectError> {
        self.meta_service.remove_node(node_id, force)
    }
}
