// SPDX-License-Identifier: Apache-2.0
impl MatrixObjectStore {
    pub fn invalidate_object_range_cache(
        &mut self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
    ) -> Result<CacheControlStats, ObjectError> {
        self.invalidate_object_range_cache_bounded(bucket, key, offset, length, 0)
    }

    pub fn invalidate_object_range_cache_bounded(
        &mut self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        max_bytes: u64,
    ) -> Result<CacheControlStats, ObjectError> {
        let metadata = self.head_object(bucket, key)?;
        self.invalidate_object_metadata_range_cache(&metadata, offset, length, max_bytes)
    }

    pub fn invalidate_object_range_cache_routed(
        &mut self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        max_bytes: u64,
        target_replica_index: Option<usize>,
        include_disabled_replicas: bool,
    ) -> Result<CacheControlStats, ObjectError> {
        let metadata = self.head_object(bucket, key)?;
        self.invalidate_object_metadata_range_cache_routed(
            &metadata,
            offset,
            length,
            max_bytes,
            target_replica_index,
            include_disabled_replicas,
        )
    }

    pub fn invalidate_object_range_cache_with_metadata_read(
        &mut self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        max_bytes: u64,
        options: MetadataReadOptions,
    ) -> Result<(CacheControlStats, MetadataRaftReadiness), ObjectError> {
        let (metadata, readiness) = self.head_object_with_metadata_read(bucket, key, options)?;
        let stats =
            self.invalidate_object_metadata_range_cache(&metadata, offset, length, max_bytes)?;
        Ok((stats, readiness))
    }

    pub fn invalidate_object_range_cache_routed_with_metadata_read(
        &mut self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        max_bytes: u64,
        target_replica_index: Option<usize>,
        include_disabled_replicas: bool,
        options: MetadataReadOptions,
    ) -> Result<(CacheControlStats, MetadataRaftReadiness), ObjectError> {
        let (metadata, readiness) = self.head_object_with_metadata_read(bucket, key, options)?;
        let stats = self.invalidate_object_metadata_range_cache_routed(
            &metadata,
            offset,
            length,
            max_bytes,
            target_replica_index,
            include_disabled_replicas,
        )?;
        Ok((stats, readiness))
    }

    pub fn invalidate_object_version_range_cache(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
        offset: u64,
        length: u64,
        max_bytes: u64,
    ) -> Result<CacheControlStats, ObjectError> {
        let metadata = self.head_object_version(bucket, key, version)?;
        check_payload_read_allowed(&metadata, now_millis())?;
        self.invalidate_object_metadata_range_cache(&metadata, offset, length, max_bytes)
    }

    pub fn invalidate_object_version_range_cache_routed(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
        offset: u64,
        length: u64,
        max_bytes: u64,
        target_replica_index: Option<usize>,
        include_disabled_replicas: bool,
    ) -> Result<CacheControlStats, ObjectError> {
        let metadata = self.head_object_version(bucket, key, version)?;
        check_payload_read_allowed(&metadata, now_millis())?;
        self.invalidate_object_metadata_range_cache_routed(
            &metadata,
            offset,
            length,
            max_bytes,
            target_replica_index,
            include_disabled_replicas,
        )
    }

    pub fn invalidate_object_version_range_cache_with_metadata_read(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
        offset: u64,
        length: u64,
        max_bytes: u64,
        options: MetadataReadOptions,
    ) -> Result<(CacheControlStats, MetadataRaftReadiness), ObjectError> {
        let (metadata, readiness) =
            self.head_object_version_with_metadata_read(bucket, key, version, options)?;
        check_payload_read_allowed(&metadata, now_millis())?;
        let stats =
            self.invalidate_object_metadata_range_cache(&metadata, offset, length, max_bytes)?;
        Ok((stats, readiness))
    }

    pub fn invalidate_object_version_range_cache_routed_with_metadata_read(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
        offset: u64,
        length: u64,
        max_bytes: u64,
        target_replica_index: Option<usize>,
        include_disabled_replicas: bool,
        options: MetadataReadOptions,
    ) -> Result<(CacheControlStats, MetadataRaftReadiness), ObjectError> {
        let (metadata, readiness) =
            self.head_object_version_with_metadata_read(bucket, key, version, options)?;
        check_payload_read_allowed(&metadata, now_millis())?;
        let stats = self.invalidate_object_metadata_range_cache_routed(
            &metadata,
            offset,
            length,
            max_bytes,
            target_replica_index,
            include_disabled_replicas,
        )?;
        Ok((stats, readiness))
    }

    fn invalidate_object_metadata_range_cache(
        &mut self,
        metadata: &ObjectMetadata,
        offset: u64,
        length: u64,
        max_bytes: u64,
    ) -> Result<CacheControlStats, ObjectError> {
        self.invalidate_object_metadata_range_cache_routed(
            metadata, offset, length, max_bytes, None, false,
        )
    }

    fn invalidate_object_metadata_range_cache_routed(
        &mut self,
        metadata: &ObjectMetadata,
        offset: u64,
        length: u64,
        max_bytes: u64,
        target_replica_index: Option<usize>,
        include_disabled_replicas: bool,
    ) -> Result<CacheControlStats, ObjectError> {
        let range_end = bounded_object_range_end(offset, length, metadata.length)?;
        let mut aggregate = CacheControlStats::default();
        let mut object_cursor = 0u64;
        let mut budget_left = max_bytes;
        for extent in &metadata.extents {
            let extent_start = object_cursor;
            let extent_end = object_cursor.checked_add(extent.length).ok_or_else(|| {
                ObjectError::InvalidArgument("object extent overflows".to_string())
            })?;
            object_cursor = extent_end;
            if extent_end <= offset {
                continue;
            }
            if extent_start >= range_end {
                break;
            }
            let read_start = offset.max(extent_start);
            let read_end = range_end.min(extent_end);
            let local_offset = read_start - extent_start;
            let local_len = read_end - read_start;
            let stats = self.chunk_service.invalidate_chunk_range_cache_routed(
                &extent.chunk_id,
                extent.offset + local_offset,
                local_len,
                budget_left,
                target_replica_index,
                include_disabled_replicas,
            )?;
            if max_bytes > 0 {
                budget_left = budget_left.saturating_sub(stats.selected_bytes);
            }
            merge_cache_stats(&mut aggregate, stats);
            if max_bytes > 0 && budget_left == 0 {
                aggregate.budget_limited = true;
                break;
            }
        }
        Ok(aggregate)
    }

}
