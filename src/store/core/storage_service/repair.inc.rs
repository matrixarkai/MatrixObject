impl MatrixObjectStore {
    pub fn scrub_page_replicas(&mut self, repair: bool) -> Result<ChunkScrubReport, ObjectError> {
        self.page_service.scrub_chunk_replicas(repair)
    }

    pub fn plan_repair_page_chunk(
        &self,
        chunk_id: &PageId,
    ) -> Result<ChunkRepairReport, ObjectError> {
        self.page_service.plan_repair_chunk_replicas(chunk_id)
    }

    pub fn repair_page_chunk(
        &mut self,
        chunk_id: &PageId,
    ) -> Result<ChunkRepairReport, ObjectError> {
        self.page_service.repair_chunk_replicas(chunk_id)
    }

    pub fn repair_page_chunk_fenced(
        &mut self,
        chunk_id: &PageId,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ChunkRepairReport, ObjectError> {
        self.page_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        let mut report = self.page_service.repair_chunk_replicas(chunk_id)?;
        report.expected_route_control_checksum = expected_route_control_checksum
            .unwrap_or_default()
            .to_string();
        Ok(report)
    }

    pub fn audit_page_replicas(
        &self,
        after_chunk_id: Option<&PageId>,
        max_chunks: usize,
        max_payload_bytes: u64,
    ) -> Result<PageReplicaAuditPage, ObjectError> {
        self.page_service
            .audit_chunk_replicas(after_chunk_id, max_chunks, max_payload_bytes)
    }

    pub fn repair_audited_page_replicas(
        &mut self,
        after_chunk_id: Option<&PageId>,
        max_chunks: usize,
        max_payload_bytes: u64,
    ) -> Result<PageReplicaAuditRepairReport, ObjectError> {
        self.page_service.repair_audited_chunk_replicas(
            after_chunk_id,
            max_chunks,
            max_payload_bytes,
        )
    }

    pub fn repair_audited_page_replicas_fenced(
        &mut self,
        after_chunk_id: Option<&PageId>,
        max_chunks: usize,
        max_payload_bytes: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PageReplicaAuditRepairReport, ObjectError> {
        self.page_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        let mut report = self.page_service.repair_audited_chunk_replicas(
            after_chunk_id,
            max_chunks,
            max_payload_bytes,
        )?;
        report.expected_route_control_checksum = expected_route_control_checksum
            .unwrap_or_default()
            .to_string();
        Ok(report)
    }

    pub fn reclaim_chunk_replicas(
        &mut self,
        max_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
    ) -> ChunkPartialReclaimReport {
        self.page_service.partial_reclaim_chunk_replicas(
            max_chunks,
            max_rewrite_bytes,
            min_discarded_bytes,
        )
    }

    pub fn plan_chunk_replica_reclaim(
        &self,
        marker: Option<&PageId>,
        max_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
        min_discard_ratio_ppm: u64,
        max_discard_ranges: usize,
        max_trim_bytes: u64,
    ) -> Result<PageReplicaReclaimPlan, ObjectError> {
        self.page_service.plan_chunk_replica_reclaim(
            marker,
            max_chunks,
            max_rewrite_bytes,
            min_discarded_bytes,
            min_discard_ratio_ppm,
            max_discard_ranges,
            max_trim_bytes,
        )
    }

    pub fn reclaim_chunk_replicas_with_limits(
        &mut self,
        max_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
        min_discard_ratio_ppm: u64,
        max_discard_ranges: usize,
        max_trim_bytes: u64,
    ) -> ChunkPartialReclaimReport {
        self.page_service.reclaim_chunk_replicas_with_limits(
            max_chunks,
            max_rewrite_bytes,
            min_discarded_bytes,
            min_discard_ratio_ppm,
            max_discard_ranges,
            max_trim_bytes,
        )
    }

    pub fn reclaim_chunk_replicas_with_limits_at(
        &mut self,
        max_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
        min_discard_ratio_ppm: u64,
        max_discard_ranges: usize,
        max_trim_bytes: u64,
        now_ms: u64,
    ) -> ChunkPartialReclaimReport {
        self.page_service.reclaim_chunk_replicas_with_limits_at(
            max_chunks,
            max_rewrite_bytes,
            min_discarded_bytes,
            min_discard_ratio_ppm,
            max_discard_ranges,
            max_trim_bytes,
            now_ms,
        )
    }

    pub fn get_object_repairing(
        &mut self,
        bucket: &str,
        key: &str,
    ) -> Result<(ObjectData, PageReadStats), ObjectError> {
        let metadata = self.head_object(bucket, key)?;
        self.get_object_range_repairing(bucket, key, 0, metadata.length)
    }

    pub fn get_object_repair_report(
        &mut self,
        bucket: &str,
        key: &str,
    ) -> Result<ObjectReadRepairReport, ObjectError> {
        let metadata = self.head_object(bucket, key)?;
        self.get_object_range_repair_report(bucket, key, 0, metadata.length)
    }

    pub fn get_object_range_repairing(
        &mut self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
    ) -> Result<(ObjectData, PageReadStats), ObjectError> {
        let metadata = self.head_object(bucket, key)?;
        check_payload_read_allowed(&metadata, now_millis())?;
        let range_end = bounded_object_range_end(offset, length, metadata.length)?;
        let range_length = usize::try_from(range_end.saturating_sub(offset)).map_err(|_| {
            ObjectError::InvalidArgument("object repair read range exceeds memory size".to_string())
        })?;
        let mut data = Vec::with_capacity(range_length);
        let mut stats = PageReadStats::default();
        let mut object_cursor = 0u64;
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
            let (payload, read_stats) = self.page_service.read_slab_range_repairing(
                &extent.slab_id,
                extent.offset + local_offset,
                local_len,
            )?;
            merge_page_read_stats(&mut stats, read_stats);
            data.extend_from_slice(&payload);
        }
        Ok((ObjectData { metadata, data }, stats))
    }

    pub fn get_object_range_repair_report(
        &mut self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
    ) -> Result<ObjectReadRepairReport, ObjectError> {
        let route_control_proof = self.page_service.route_control_proof();
        let (object, stats) = self.get_object_range_repairing(bucket, key, offset, length)?;
        Ok(ObjectReadRepairReport {
            requested_offset: offset,
            requested_length: length,
            payload_checksum: etag(&object.data),
            repaired_replicas: stats.replica_read_repairs,
            failed_repairs: stats.replica_read_repair_failures,
            degraded_chunks: stats.quorum_read_degraded_chunks,
            route_control_proof,
            expected_route_control_checksum: String::new(),
            object,
            stats,
        })
    }

    pub fn get_object_range_repair_report_fenced(
        &mut self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectReadRepairReport, ObjectError> {
        self.page_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        let mut report = self.get_object_range_repair_report(bucket, key, offset, length)?;
        report.expected_route_control_checksum = expected_route_control_checksum
            .unwrap_or_default()
            .to_string();
        Ok(report)
    }

}
