// SPDX-License-Identifier: Apache-2.0
impl MatrixObjectStore {
    pub fn distributed_readiness_fenced(
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
    ) -> Result<ObjectStoreDistributedReadiness, ObjectError> {
        let readiness = self.distributed_readiness(
            min_read_index,
            now_ms,
            min_capacity_free_bytes,
            min_reclaimable_bytes,
            expected_replica_count,
            max_root_health_items,
            max_root_health_payload_bytes,
        )?;
        verify_objectstore_distributed_readiness_checksum(
            &readiness,
            expected_control_checksum,
            expected_control_checksum_format,
            expected_distributed_readiness_checksum,
        )?;
        Ok(readiness)
    }
}
