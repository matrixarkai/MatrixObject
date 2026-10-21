// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    fn batch_get_objects_internal(
        &mut self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        repair: bool,
        metadata_read_options: Option<MetadataReadOptions>,
    ) -> Result<ObjectBatchReadReport, ObjectError> {
        if max_items == 0 {
            return Err(ObjectError::InvalidArgument(
                "batch object read max_items must be positive".to_string(),
            ));
        }
        if requests.len() > max_items {
            return Err(ObjectError::InvalidArgument(
                "batch object read item count exceeds max_items".to_string(),
            ));
        }
        let mut report = ObjectBatchReadReport {
            requested_objects: requests.len() as u64,
            route_control_proof: self.chunk_service.route_control_proof(),
            ..ObjectBatchReadReport::default()
        };
        if let Some(options) = metadata_read_options.as_ref() {
            let readiness = self.metadata_raft_readiness(options.min_read_index, options.now_ms);
            report.read_proof = object_read_proof_from_readiness(&readiness);
        }
        for request in requests {
            let remaining_payload_budget = if max_payload_bytes == 0 {
                0
            } else {
                max_payload_bytes.saturating_sub(report.payload_bytes)
            };
            if max_payload_bytes > 0 && remaining_payload_budget == 0 {
                report.failed_objects = report.failed_objects.saturating_add(1);
                report.items.push(ObjectReadItem {
                    request,
                    object: None,
                    stats: ChunkReadStats::default(),
                    error_code: Some("PreconditionFailed".to_string()),
                    error_message: Some("batch object read payload budget exceeded".to_string()),
                });
                continue;
            }
            let read = match &request.range {
                Some(range) => {
                    if let Some(version) = request.version {
                        range.end().and_then(|_| {
                            if let Some(options) = metadata_read_options.as_ref() {
                                let (object, _) = self
                                    .get_object_version_range_with_metadata_read(
                                        &request.bucket,
                                        &request.key,
                                        version,
                                        range.offset,
                                        range.length,
                                        remaining_payload_budget,
                                        options.clone(),
                                    )?;
                                Ok((object, ChunkReadStats::default()))
                            } else {
                                let metadata = self.head_object_version(
                                    &request.bucket,
                                    &request.key,
                                    version,
                                )?;
                                check_payload_read_allowed(&metadata, now_millis())?;
                                if range.offset > metadata.length {
                                    return Err(ObjectError::InvalidArgument(
                                        "range offset exceeds object length".to_string(),
                                    ));
                                }
                                let available = metadata.length - range.offset;
                                let bounded = range.length.min(available);
                                if remaining_payload_budget > 0
                                    && bounded > remaining_payload_budget
                                {
                                    return Err(ObjectError::PreconditionFailed(
                                        "object range read payload budget exceeded".to_string(),
                                    ));
                                }
                                let (data, stats) = self.read_extents_with_stats_bounded(
                                    &metadata.extents,
                                    range.offset,
                                    bounded,
                                    remaining_payload_budget,
                                )?;
                                Ok((ObjectData { metadata, data }, stats))
                            }
                        })
                    } else if repair {
                        range.end().and_then(|_| {
                            self.get_object_range_repairing(
                                &request.bucket,
                                &request.key,
                                range.offset,
                                range.length,
                            )
                        })
                    } else if let Some(options) = metadata_read_options.as_ref() {
                        range.end().and_then(|_| {
                            let (admission, _) = self
                                .can_accept_object_range_read_with_metadata_read(
                                    &request.bucket,
                                    &request.key,
                                    range.offset,
                                    range.length,
                                    remaining_payload_budget,
                                    options.clone(),
                                )?;
                            if !admission.can_accept {
                                return Err(ObjectError::PreconditionFailed(
                                    admission.partial_status,
                                ));
                            }
                            let (metadata, _) = self.head_object_with_metadata_read(
                                &request.bucket,
                                &request.key,
                                options.clone(),
                            )?;
                            check_payload_read_allowed(&metadata, now_millis())?;
                            if range.offset > metadata.length {
                                return Err(ObjectError::InvalidArgument(
                                    "range offset exceeds object length".to_string(),
                                ));
                            }
                            let available = metadata.length - range.offset;
                            let bounded = range.length.min(available);
                            if max_payload_bytes > 0 && bounded > max_payload_bytes {
                                return Err(ObjectError::PreconditionFailed(
                                    "object range read payload budget exceeded".to_string(),
                                ));
                            }
                            let (data, stats) = self.read_extents_with_stats_bounded(
                                &metadata.extents,
                                range.offset,
                                bounded,
                                max_payload_bytes,
                            )?;
                            Ok((ObjectData { metadata, data }, stats))
                        })
                    } else {
                        range.end().and_then(|_| {
                            let admission = self.can_accept_object_range_read(
                                &request.bucket,
                                &request.key,
                                range.offset,
                                range.length,
                                remaining_payload_budget,
                            )?;
                            if !admission.can_accept {
                                return Err(ObjectError::PreconditionFailed(
                                    admission.partial_status,
                                ));
                            }
                            self.get_object_range_with_read_stats(
                                &request.bucket,
                                &request.key,
                                range.offset,
                                range.length,
                            )
                        })
                    }
                }
                None => {
                    if let Some(version) = request.version {
                        if let Some(options) = metadata_read_options.as_ref() {
                            (|| {
                                let (metadata, _) = self.head_object_version_with_metadata_read(
                                    &request.bucket,
                                    &request.key,
                                    version,
                                    options.clone(),
                                )?;
                                check_payload_read_allowed(&metadata, now_millis())?;
                                if remaining_payload_budget > 0
                                    && metadata.length > remaining_payload_budget
                                {
                                    return Err(ObjectError::PreconditionFailed(
                                        "object read payload budget exceeded".to_string(),
                                    ));
                                }
                                let (data, stats) = self.read_extents_with_stats_bounded(
                                    &metadata.extents,
                                    0,
                                    metadata.length,
                                    remaining_payload_budget,
                                )?;
                                Ok((ObjectData { metadata, data }, stats))
                            })()
                        } else {
                            (|| {
                                let metadata = self.head_object_version(
                                    &request.bucket,
                                    &request.key,
                                    version,
                                )?;
                                check_payload_read_allowed(&metadata, now_millis())?;
                                if remaining_payload_budget > 0
                                    && metadata.length > remaining_payload_budget
                                {
                                    return Err(ObjectError::PreconditionFailed(
                                        "object read payload budget exceeded".to_string(),
                                    ));
                                }
                                let (data, stats) = self.read_extents_with_stats_bounded(
                                    &metadata.extents,
                                    0,
                                    metadata.length,
                                    remaining_payload_budget,
                                )?;
                                Ok((ObjectData { metadata, data }, stats))
                            })()
                        }
                    } else if repair {
                        self.get_object_repairing(&request.bucket, &request.key)
                    } else if let Some(options) = metadata_read_options.as_ref() {
                        (|| {
                            let (admission, _) = self
                                .can_accept_object_range_read_with_metadata_read(
                                    &request.bucket,
                                    &request.key,
                                    0,
                                    u64::MAX,
                                    remaining_payload_budget,
                                    options.clone(),
                                )?;
                            if !admission.can_accept {
                                return Err(ObjectError::PreconditionFailed(
                                    admission.partial_status,
                                ));
                            }
                            let (metadata, _) = self.head_object_with_metadata_read(
                                &request.bucket,
                                &request.key,
                                options.clone(),
                            )?;
                            check_payload_read_allowed(&metadata, now_millis())?;
                            if max_payload_bytes > 0 && metadata.length > max_payload_bytes {
                                Err(ObjectError::PreconditionFailed(
                                    "object read payload budget exceeded".to_string(),
                                ))
                            } else {
                                let (data, stats) = self.read_extents_with_stats_bounded(
                                    &metadata.extents,
                                    0,
                                    metadata.length,
                                    max_payload_bytes,
                                )?;
                                Ok((ObjectData { metadata, data }, stats))
                            }
                        })()
                    } else {
                        (|| {
                            let admission = self.can_accept_object_range_read(
                                &request.bucket,
                                &request.key,
                                0,
                                u64::MAX,
                                remaining_payload_budget,
                            )?;
                            if !admission.can_accept {
                                return Err(ObjectError::PreconditionFailed(
                                    admission.partial_status,
                                ));
                            }
                            self.get_object_with_read_stats(&request.bucket, &request.key)
                        })()
                    }
                }
            };
            match read {
                Ok((object, stats)) => {
                    let next_payload = report
                        .payload_bytes
                        .saturating_add(object.data.len() as u64);
                    if max_payload_bytes > 0 && next_payload > max_payload_bytes {
                        report.failed_objects = report.failed_objects.saturating_add(1);
                        report.items.push(ObjectReadItem {
                            request,
                            object: None,
                            stats: ChunkReadStats::default(),
                            error_code: Some("PreconditionFailed".to_string()),
                            error_message: Some(
                                "batch object read payload budget exceeded".to_string(),
                            ),
                        });
                        continue;
                    }
                    report.payload_bytes = next_payload;
                    report.successful_objects = report.successful_objects.saturating_add(1);
                    merge_page_read_stats(&mut report.read_stats, stats.clone());
                    report.items.push(ObjectReadItem {
                        request,
                        object: Some(object),
                        stats,
                        error_code: None,
                        error_message: None,
                    });
                }
                Err(err) => {
                    report.failed_objects = report.failed_objects.saturating_add(1);
                    report.items.push(ObjectReadItem {
                        request,
                        object: None,
                        stats: ChunkReadStats::default(),
                        error_code: Some(object_error_code(&err).to_string()),
                        error_message: Some(err.to_string()),
                    });
                }
            }
        }
        stamp_object_batch_read_proof("BatchGetObjects", &mut report);
        Ok(report)
    }
}
