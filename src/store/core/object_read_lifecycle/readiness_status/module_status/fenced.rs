impl MatrixObjectStore {
    pub fn module_status_fenced(
        &self,
        min_read_index: u64,
        now_ms: u64,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        expected_replica_count: usize,
        max_root_health_items: usize,
        max_root_health_payload_bytes: u64,
        expected_control_checksum: Option<&str>,
        expected_control_checksum_format: Option<&str>,
    ) -> Result<ObjectStoreModuleStatus, ObjectError> {
        self.module_status_fenced_with_readiness_checksum(
            min_read_index,
            now_ms,
            min_capacity_free_bytes,
            min_reclaimable_bytes,
            expected_replica_count,
            max_root_health_items,
            max_root_health_payload_bytes,
            expected_control_checksum,
            expected_control_checksum_format,
            None,
        )
    }

    pub fn module_status_fenced_with_readiness_checksum(
        &self,
        min_read_index: u64,
        now_ms: u64,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        expected_replica_count: usize,
        max_root_health_items: usize,
        max_root_health_payload_bytes: u64,
        expected_control_checksum: Option<&str>,
        expected_control_checksum_format: Option<&str>,
        expected_distributed_readiness_checksum: Option<&str>,
    ) -> Result<ObjectStoreModuleStatus, ObjectError> {
        let status = self.module_status(
            min_read_index,
            now_ms,
            min_capacity_free_bytes,
            min_reclaimable_bytes,
            expected_replica_count,
            max_root_health_items,
            max_root_health_payload_bytes,
        )?;
        verify_objectstore_module_status_checksum_with_readiness(
            &status,
            expected_control_checksum,
            expected_control_checksum_format,
            expected_distributed_readiness_checksum,
        )?;
        Ok(status)
    }
}
