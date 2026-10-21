// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    fn batch_object_cache_control(
        &mut self,
        requests: Vec<ObjectCacheRangeRequest>,
        max_items: usize,
        max_bytes: u64,
        prewarm: bool,
        metadata_read_options: Option<MetadataReadOptions>,
    ) -> Result<ObjectBatchCacheControlReport, ObjectError> {
        self.batch_object_cache_control_routed(
            requests,
            max_items,
            max_bytes,
            prewarm,
            metadata_read_options,
            None,
            false,
        )
    }

    fn batch_object_cache_control_routed(
        &mut self,
        requests: Vec<ObjectCacheRangeRequest>,
        max_items: usize,
        max_bytes: u64,
        prewarm: bool,
        metadata_read_options: Option<MetadataReadOptions>,
        target_replica_index: Option<usize>,
        include_disabled_replicas: bool,
    ) -> Result<ObjectBatchCacheControlReport, ObjectError> {
        if max_items == 0 {
            return Err(ObjectError::InvalidArgument(
                "batch object cache control max_items must be positive".to_string(),
            ));
        }
        if requests.len() > max_items {
            return Err(ObjectError::InvalidArgument(
                "batch object cache control item count exceeds max_items".to_string(),
            ));
        }
        let mut report = ObjectBatchCacheControlReport {
            requested_objects: requests.len() as u64,
            route_control_proof: self.chunk_service.route_control_proof(),
            ..ObjectBatchCacheControlReport::default()
        };
        if let Some(options) = metadata_read_options.as_ref() {
            let readiness = self.metadata_raft_readiness(options.min_read_index, options.now_ms);
            report.read_proof = object_read_proof_from_readiness(&readiness);
        }
        let mut selected_prefix_len = 0usize;
        let mut selection_closed = false;
        for request in &requests {
            report.requested_bytes = report.requested_bytes.saturating_add(request.length);
            if selection_closed
                || (max_bytes > 0
                    && report.selected_bytes.saturating_add(request.length) > max_bytes)
            {
                selection_closed = true;
                report.skipped_bytes = report.skipped_bytes.saturating_add(request.length);
                report.skipped_objects = report.skipped_objects.saturating_add(1);
                report.budget_limited = true;
                continue;
            }
            report.selected_bytes = report.selected_bytes.saturating_add(request.length);
            report.selected_objects = report.selected_objects.saturating_add(1);
            selected_prefix_len += 1;
        }
        for (index, request) in requests.into_iter().enumerate() {
            if index >= selected_prefix_len {
                let mut stats = CacheControlStats::default();
                stats.requested_ranges = 1;
                stats.requested_bytes = request.length;
                stats.skipped_bytes = request.length;
                stats.budget_limited = true;
                report.items.push(ObjectCacheRangeItem {
                    request,
                    metadata: None,
                    stats,
                    skipped: true,
                    error_code: None,
                    error_message: None,
                });
                continue;
            }
            let metadata_result = match (request.version, metadata_read_options.as_ref()) {
                (Some(version), Some(options)) => self
                    .head_object_version_with_metadata_read(
                        &request.bucket,
                        &request.key,
                        version,
                        options.clone(),
                    )
                    .map(|(metadata, _)| metadata),
                (Some(version), None) => {
                    self.head_object_version(&request.bucket, &request.key, version)
                }
                (None, Some(options)) => self
                    .head_object_with_metadata_read(&request.bucket, &request.key, options.clone())
                    .map(|(metadata, _)| metadata),
                (None, None) => self.head_object(&request.bucket, &request.key),
            };
            let result = metadata_result.and_then(|metadata| {
                if prewarm || request.version.is_some() {
                    check_payload_read_allowed(&metadata, now_millis())?;
                }
                let stats = if prewarm {
                    self.prewarm_object_metadata_range_cache_routed(
                        &metadata,
                        request.offset,
                        request.length,
                        max_bytes,
                        target_replica_index,
                        include_disabled_replicas,
                    )?
                } else {
                    self.invalidate_object_metadata_range_cache_routed(
                        &metadata,
                        request.offset,
                        request.length,
                        max_bytes,
                        target_replica_index,
                        include_disabled_replicas,
                    )?
                };
                Ok((stats, metadata))
            });
            match result {
                Ok((stats, metadata)) => {
                    merge_cache_stats(&mut report.stats, stats.clone());
                    report.items.push(ObjectCacheRangeItem {
                        request,
                        metadata: Some(metadata),
                        stats,
                        skipped: false,
                        error_code: None,
                        error_message: None,
                    });
                }
                Err(err) => {
                    report.failed_objects = report.failed_objects.saturating_add(1);
                    report.items.push(ObjectCacheRangeItem {
                        request,
                        metadata: None,
                        stats: CacheControlStats::default(),
                        skipped: false,
                        error_code: Some(object_error_code(&err).to_string()),
                        error_message: Some(err.to_string()),
                    });
                }
            }
        }
        report.stats.requested_ranges = report.requested_objects;
        report.stats.requested_bytes = report.requested_bytes;
        report.stats.selected_ranges = report.selected_objects;
        report.stats.selected_bytes = report.selected_bytes;
        report.stats.skipped_bytes = report.skipped_bytes;
        report.stats.budget_limited = report.budget_limited;
        stamp_object_batch_cache_control_proof(
            if prewarm {
                "BatchPrewarmObjectRange"
            } else {
                "BatchInvalidateObjectRangeCache"
            },
            &mut report,
        );
        Ok(report)
    }

}
