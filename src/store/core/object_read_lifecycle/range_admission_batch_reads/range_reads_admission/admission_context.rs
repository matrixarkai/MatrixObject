impl MatrixObjectStore {
    fn not_modified_object_read_admission(
        &self,
        metadata: &ObjectMetadata,
        max_payload_bytes: u64,
    ) -> ObjectRangeAdmission {
        let mut report = ObjectRangeAdmission {
            bucket: metadata.object_id.bucket.to_string(),
            key: metadata.object_id.key.to_string(),
            can_accept: true,
            object_length: metadata.length,
            requested_offset: 0,
            requested_length: metadata.length,
            selected_bytes: 0,
            payload_bytes: 0,
            max_payload_bytes,
            min_ready_replica_count: 0,
            partial_status: "not_modified".to_string(),
            storage_admission_reason: "not_modified".to_string(),
            route_control_proof: self.page_service.route_control_proof(),
            ..ObjectRangeAdmission::default()
        };
        self.attach_storage_admission_context(&mut report, now_millis());
        stamp_object_range_admission_proof("CanReadObject", &mut report);
        report
    }

    fn not_modified_object_range_read_admission(
        &self,
        metadata: &ObjectMetadata,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
    ) -> ObjectRangeAdmission {
        let mut report = ObjectRangeAdmission {
            bucket: metadata.object_id.bucket.to_string(),
            key: metadata.object_id.key.to_string(),
            can_accept: true,
            object_length: metadata.length,
            requested_offset: offset,
            requested_length: length,
            selected_bytes: 0,
            payload_bytes: 0,
            max_payload_bytes,
            min_ready_replica_count: 0,
            partial_status: "not_modified".to_string(),
            storage_admission_reason: "not_modified".to_string(),
            route_control_proof: self.page_service.route_control_proof(),
            ..ObjectRangeAdmission::default()
        };
        self.attach_storage_admission_context(&mut report, now_millis());
        stamp_object_range_admission_proof("CanReadObjectRange", &mut report);
        report
    }

    fn attach_storage_admission_context(&self, report: &mut ObjectRangeAdmission, now_ms: u64) {
        let storage = self.page_storage_readiness(0, 0, now_ms);
        report.storage_active_replicas = storage.active_replicas;
        report.storage_readable_replicas = storage.readable_replicas;
        report.storage_writable_replicas = storage.writable_replicas;
        report.storage_quarantined_replicas = storage.quarantined_replicas;
        report.storage_frozen_replicas = storage.frozen_replicas;
        report.storage_draining_replicas = storage.draining_replicas;
        report.storage_capacity_pressure_replicas = storage.capacity_pressure_replicas;
        report.storage_trim_flow_limited_replicas = storage.trim_flow_limited_replicas;
        report.storage_total_capacity_bytes = storage.total_capacity_bytes;
        report.storage_total_capacity_free_bytes = storage.total_capacity_free_bytes;
        report.storage_total_reclaimable_bytes = storage.total_reclaimable_bytes;
        report.storage_total_fragmented_chunk_count = storage.total_fragmented_chunk_count;
        report.storage_total_fragmented_discarded_bytes = storage.total_fragmented_discarded_bytes;
        report.storage_total_trailing_trim_bytes = storage.total_trailing_trim_bytes;
        report.storage_reclaim_pressure = storage.reclaimable_replicas != 0;
        let read_cache = self.read_cache_stats();
        report.storage_read_cache_bytes = read_cache.bytes;
        report.storage_read_cache_entries = read_cache.entries;
        report.storage_read_cache_max_bytes = read_cache.max_bytes;
        report.storage_read_cache_max_cacheable_read_bytes = read_cache.max_cacheable_read_bytes;
        report.storage_read_cache_hits = read_cache.hits;
        report.storage_read_cache_misses = read_cache.misses;
        report.storage_read_cache_evictions = read_cache.evictions;
        report.storage_read_cache_pressure =
            read_cache.max_bytes > 0 && read_cache.bytes >= read_cache.max_bytes;
    }

    pub fn can_accept_object_range_read_with_metadata_read(
        &self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        options: MetadataReadOptions,
    ) -> Result<(ObjectRangeAdmission, MetadataRaftReadiness), ObjectError> {
        let (_, readiness) = self.head_object_with_metadata_read(bucket, key, options)?;
        let admission =
            self.can_accept_object_range_read(bucket, key, offset, length, max_payload_bytes)?;
        Ok((admission, readiness))
    }

    pub fn can_accept_object_range_read_bounded_with_metadata_read(
        &self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        options: MetadataReadOptions,
    ) -> Result<(ObjectRangeAdmission, MetadataRaftReadiness), ObjectError> {
        let (_, readiness) = self.head_object_with_metadata_read(bucket, key, options)?;
        let admission = self.can_accept_object_range_read_bounded(
            bucket,
            key,
            offset,
            length,
            max_payload_bytes,
            max_chunk_read_count,
        )?;
        Ok((admission, readiness))
    }
}
