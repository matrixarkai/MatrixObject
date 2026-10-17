impl MatrixObjectStore {
    pub fn local_chunk_storage_readiness(
        &self,
        replica_index: usize,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        now_ms: u64,
    ) -> Result<ChunkStorageReadiness, ObjectError> {
        let replica = self
            .page_service
            .replicas
            .get(replica_index)
            .ok_or_else(|| {
                ObjectError::InvalidArgument(format!(
                    "chunk replica index {replica_index} is out of range"
                ))
            })?;
        Ok(replica.storage_readiness(min_capacity_free_bytes, min_reclaimable_bytes, now_ms))
    }

    pub fn expire_root_node_heartbeats(
        &mut self,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<u64, ObjectError> {
        self.root_service.expire_node_heartbeats(ttl_ms, now_ms)
    }

    pub fn remove_root_node(
        &mut self,
        node_id: &str,
        force: bool,
    ) -> Result<RootNodeRemoveReport, ObjectError> {
        self.root_service.remove_node(node_id, force)
    }

    pub fn plan_object_placement(
        &self,
        bucket: &str,
        key: &str,
        length: u64,
        replica_count: usize,
    ) -> Result<PlacementPlan, ObjectError> {
        validate_bucket(bucket)?;
        validate_key(key)?;
        self.root_service
            .plan_object_placement(ObjectId::new(bucket, key), length, replica_count)
    }

    pub fn commit_object_placement(
        &mut self,
        placement: ObjectPlacement,
        expected_plan_checksum: &str,
    ) -> Result<ObjectPlacement, ObjectError> {
        self.root_service
            .commit_object_placement(placement, expected_plan_checksum)
    }

    pub fn release_object_placement(
        &mut self,
        bucket: &str,
        key: &str,
    ) -> Result<ObjectPlacement, ObjectError> {
        self.root_service
            .release_object_placement(&ObjectId::new(bucket, key))
    }

}
