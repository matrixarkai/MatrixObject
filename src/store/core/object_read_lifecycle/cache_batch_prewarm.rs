impl MatrixObjectStore {
    pub fn batch_prewarm_object_range_cache(
        &mut self,
        requests: Vec<ObjectCacheRangeRequest>,
        max_items: usize,
        max_prewarm_bytes: u64,
    ) -> Result<ObjectBatchCacheControlReport, ObjectError> {
        self.batch_object_cache_control(requests, max_items, max_prewarm_bytes, true, None)
    }

    pub fn batch_prewarm_object_range_cache_routed(
        &mut self,
        requests: Vec<ObjectCacheRangeRequest>,
        max_items: usize,
        max_prewarm_bytes: u64,
        target_replica_index: Option<usize>,
        include_disabled_replicas: bool,
    ) -> Result<ObjectBatchCacheControlReport, ObjectError> {
        self.batch_object_cache_control_routed(
            requests,
            max_items,
            max_prewarm_bytes,
            true,
            None,
            target_replica_index,
            include_disabled_replicas,
        )
    }

    pub fn batch_prewarm_object_range_cache_fenced(
        &mut self,
        requests: Vec<ObjectCacheRangeRequest>,
        max_items: usize,
        max_prewarm_bytes: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectBatchCacheControlReport, ObjectError> {
        self.chunk_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        let mut report =
            self.batch_object_cache_control(requests, max_items, max_prewarm_bytes, true, None)?;
        report.expected_route_control_checksum = expected_route_control_checksum
            .unwrap_or_default()
            .to_string();
        Ok(report)
    }

    pub fn batch_prewarm_object_range_cache_routed_fenced(
        &mut self,
        requests: Vec<ObjectCacheRangeRequest>,
        max_items: usize,
        max_prewarm_bytes: u64,
        target_replica_index: Option<usize>,
        include_disabled_replicas: bool,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectBatchCacheControlReport, ObjectError> {
        self.chunk_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        let mut report = self.batch_object_cache_control_routed(
            requests,
            max_items,
            max_prewarm_bytes,
            true,
            None,
            target_replica_index,
            include_disabled_replicas,
        )?;
        report.expected_route_control_checksum = expected_route_control_checksum
            .unwrap_or_default()
            .to_string();
        Ok(report)
    }

    pub fn batch_prewarm_object_range_cache_with_metadata_read(
        &mut self,
        requests: Vec<ObjectCacheRangeRequest>,
        max_items: usize,
        max_prewarm_bytes: u64,
        options: MetadataReadOptions,
    ) -> Result<ObjectBatchCacheControlReport, ObjectError> {
        self.batch_object_cache_control(requests, max_items, max_prewarm_bytes, true, Some(options))
    }

    pub fn batch_prewarm_object_range_cache_routed_with_metadata_read(
        &mut self,
        requests: Vec<ObjectCacheRangeRequest>,
        max_items: usize,
        max_prewarm_bytes: u64,
        target_replica_index: Option<usize>,
        include_disabled_replicas: bool,
        options: MetadataReadOptions,
    ) -> Result<ObjectBatchCacheControlReport, ObjectError> {
        self.batch_object_cache_control_routed(
            requests,
            max_items,
            max_prewarm_bytes,
            true,
            Some(options),
            target_replica_index,
            include_disabled_replicas,
        )
    }

}
