impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn list_root_object_placements_linearizable(
        &self,
        marker: Option<&ObjectId>,
        limit: usize,
        node_id_filter: Option<&str>,
    ) -> Result<RootPlacementPage, ObjectError> {
        self.ensure_root_read_index("list root object placements")?;
        self.store
            .list_root_object_placements(marker, limit, node_id_filter)
    }

    pub fn list_root_object_placements_with_read_proof_linearizable(
        &self,
        marker: Option<&ObjectId>,
        limit: usize,
        node_id_filter: Option<&str>,
    ) -> Result<ProvenRootRead<RootPlacementPage>, ObjectError> {
        let proof = self.ensure_root_read_index("list root object placements")?;
        let value = self
            .store
            .list_root_object_placements(marker, limit, node_id_filter)?;
        Ok(ProvenRootRead { value, proof })
    }

    pub fn object_placement_linearizable(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<ObjectPlacement, ObjectError> {
        self.ensure_root_read_index("object placement")?;
        self.store.object_placement(bucket, key)
    }

    pub fn object_placement_with_read_proof_linearizable(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<ProvenRootRead<ObjectPlacement>, ObjectError> {
        let proof = self.ensure_root_read_index("object placement")?;
        let value = self.store.object_placement(bucket, key)?;
        Ok(ProvenRootRead { value, proof })
    }

    pub fn audit_root_placement_health_linearizable(
        &self,
        marker: Option<&ObjectId>,
        limit: usize,
        expected_replica_count: usize,
        min_free_bytes: u64,
        degraded_only: bool,
        max_payload_bytes: u64,
    ) -> Result<RootPlacementHealthPage, ObjectError> {
        self.ensure_root_read_index("audit root placement health")?;
        self.store.audit_root_placement_health(
            marker,
            limit,
            expected_replica_count,
            min_free_bytes,
            degraded_only,
            max_payload_bytes,
        )
    }

    pub fn audit_root_placement_health_with_read_proof_linearizable(
        &self,
        marker: Option<&ObjectId>,
        limit: usize,
        expected_replica_count: usize,
        min_free_bytes: u64,
        degraded_only: bool,
        max_payload_bytes: u64,
    ) -> Result<ProvenRootRead<RootPlacementHealthPage>, ObjectError> {
        let proof = self.ensure_root_read_index("audit root placement health")?;
        let value = self.store.audit_root_placement_health(
            marker,
            limit,
            expected_replica_count,
            min_free_bytes,
            degraded_only,
            max_payload_bytes,
        )?;
        Ok(ProvenRootRead { value, proof })
    }
}
