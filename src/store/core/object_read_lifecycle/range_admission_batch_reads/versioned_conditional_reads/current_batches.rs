impl MatrixObjectStore {
    fn batch_get_objects_conditional_internal(
        &mut self,
        requests: Vec<ConditionalObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        metadata_read_options: Option<MetadataReadOptions>,
    ) -> Result<ConditionalObjectBatchReadReport, ObjectError> {
        if max_items == 0 {
            return Err(ObjectError::InvalidArgument(
                "conditional batch object read max_items must be positive".to_string(),
            ));
        }
        if requests.len() > max_items {
            return Err(ObjectError::InvalidArgument(
                "conditional batch object read item count exceeds max_items".to_string(),
            ));
        }
        let mut report = ConditionalObjectBatchReadReport {
            requested_objects: requests.len() as u64,
            route_control_proof: self.chunk_service.route_control_proof(),
            ..ConditionalObjectBatchReadReport::default()
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
                report.items.push(ConditionalObjectReadItem {
                    request,
                    object: None,
                    stats: ChunkReadStats::default(),
                    error_code: Some("PreconditionFailed".to_string()),
                    error_message: Some(
                        "conditional batch object read payload budget exceeded".to_string(),
                    ),
                });
                continue;
            }

            let read = match &request.range {
                Some(range) => range.end().and_then(|_| {
                    if let Some(options) = metadata_read_options.as_ref() {
                        let (object, _) = self.get_object_range_conditional_with_metadata_read(
                            &request.bucket,
                            &request.key,
                            range.offset,
                            range.length,
                            remaining_payload_budget,
                            &request.preconditions,
                            options.clone(),
                        )?;
                        Ok((object, ChunkReadStats::default()))
                    } else {
                        let object = self.get_object_range_conditional(
                            &request.bucket,
                            &request.key,
                            range.offset,
                            range.length,
                            remaining_payload_budget,
                            &request.preconditions,
                        )?;
                        Ok((object, ChunkReadStats::default()))
                    }
                }),
                None => {
                    if let Some(options) = metadata_read_options.as_ref() {
                        (|| {
                            let (metadata, _) = self.head_object_with_metadata_read(
                                &request.bucket,
                                &request.key,
                                options.clone(),
                            )?;
                            let not_modified = evaluate_object_read_preconditions(
                                &metadata,
                                &request.preconditions,
                            )?;
                            if not_modified {
                                return Ok((
                                    ConditionalObjectData {
                                        metadata,
                                        data: None,
                                        not_modified: true,
                                    },
                                    ChunkReadStats::default(),
                                ));
                            }
                            check_payload_read_allowed(&metadata, now_millis())?;
                            if max_payload_bytes > 0 && metadata.length > remaining_payload_budget {
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
                            Ok((
                                ConditionalObjectData {
                                    metadata,
                                    data: Some(data),
                                    not_modified: false,
                                },
                                stats,
                            ))
                        })()
                    } else {
                        (|| {
                            let metadata = self.head_object(&request.bucket, &request.key)?;
                            let not_modified = evaluate_object_read_preconditions(
                                &metadata,
                                &request.preconditions,
                            )?;
                            if not_modified {
                                return Ok((
                                    ConditionalObjectData {
                                        metadata,
                                        data: None,
                                        not_modified: true,
                                    },
                                    ChunkReadStats::default(),
                                ));
                            }
                            check_payload_read_allowed(&metadata, now_millis())?;
                            if max_payload_bytes > 0 && metadata.length > remaining_payload_budget {
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
                            Ok((
                                ConditionalObjectData {
                                    metadata,
                                    data: Some(data),
                                    not_modified: false,
                                },
                                stats,
                            ))
                        })()
                    }
                }
            };

            match read {
                Ok((object, stats)) => {
                    if object.not_modified {
                        report.not_modified_objects = report.not_modified_objects.saturating_add(1);
                        report.items.push(ConditionalObjectReadItem {
                            request,
                            object: Some(object),
                            stats,
                            error_code: None,
                            error_message: None,
                        });
                        continue;
                    }
                    let data_len = object.data.as_ref().map_or(0, Vec::len) as u64;
                    if object.data.is_none() {
                        report.failed_objects = report.failed_objects.saturating_add(1);
                        report.items.push(ConditionalObjectReadItem {
                            request,
                            object: None,
                            stats: ChunkReadStats::default(),
                            error_code: Some("Corruption".to_string()),
                            error_message: Some(
                                "conditional object read returned no payload".to_string(),
                            ),
                        });
                        continue;
                    }
                    let next_payload = report.payload_bytes.saturating_add(data_len);
                    if max_payload_bytes > 0 && next_payload > max_payload_bytes {
                        report.failed_objects = report.failed_objects.saturating_add(1);
                        report.items.push(ConditionalObjectReadItem {
                            request,
                            object: None,
                            stats: ChunkReadStats::default(),
                            error_code: Some("PreconditionFailed".to_string()),
                            error_message: Some(
                                "conditional batch object read payload budget exceeded".to_string(),
                            ),
                        });
                        continue;
                    }
                    report.payload_bytes = next_payload;
                    report.successful_objects = report.successful_objects.saturating_add(1);
                    merge_page_read_stats(&mut report.read_stats, stats.clone());
                    report.items.push(ConditionalObjectReadItem {
                        request,
                        object: Some(object),
                        stats,
                        error_code: None,
                        error_message: None,
                    });
                }
                Err(err) => {
                    report.failed_objects = report.failed_objects.saturating_add(1);
                    report.items.push(ConditionalObjectReadItem {
                        request,
                        object: None,
                        stats: ChunkReadStats::default(),
                        error_code: Some(object_error_code(&err).to_string()),
                        error_message: Some(err.to_string()),
                    });
                }
            }
        }
        stamp_conditional_object_batch_read_proof("ConditionalBatchReadObjects", &mut report);
        Ok(report)
    }
}
