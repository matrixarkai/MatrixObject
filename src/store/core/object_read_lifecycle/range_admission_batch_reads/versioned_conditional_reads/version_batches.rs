impl MatrixObjectStore {
    fn batch_get_object_versions_conditional_internal(
        &self,
        requests: Vec<VersionedConditionalObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        metadata_read_options: Option<MetadataReadOptions>,
    ) -> Result<VersionedConditionalObjectBatchReadReport, ObjectError> {
        if max_items == 0 {
            return Err(ObjectError::InvalidArgument(
                "versioned conditional batch object read max_items must be positive".to_string(),
            ));
        }
        if requests.len() > max_items {
            return Err(ObjectError::InvalidArgument(
                "versioned conditional batch object read item count exceeds max_items".to_string(),
            ));
        }
        let mut report = VersionedConditionalObjectBatchReadReport {
            requested_objects: requests.len() as u64,
            route_control_proof: self.chunk_service.route_control_proof(),
            ..VersionedConditionalObjectBatchReadReport::default()
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
                report.items.push(VersionedConditionalObjectReadItem {
                    request,
                    object: None,
                    stats: ChunkReadStats::default(),
                    error_code: Some("PreconditionFailed".to_string()),
                    error_message: Some(
                        "versioned conditional batch object read payload budget exceeded"
                            .to_string(),
                    ),
                });
                continue;
            }
            let read = self.read_versioned_conditional_object_for_batch(
                &request,
                remaining_payload_budget,
                metadata_read_options.as_ref(),
            );
            match read {
                Ok(object) => {
                    let data_len = object.data.as_ref().map(|data| data.len()).unwrap_or(0);
                    let next_payload = report.payload_bytes.saturating_add(data_len as u64);
                    if max_payload_bytes > 0 && next_payload > max_payload_bytes {
                        report.failed_objects = report.failed_objects.saturating_add(1);
                        report.items.push(VersionedConditionalObjectReadItem {
                            request,
                            object: None,
                            stats: ChunkReadStats::default(),
                            error_code: Some("PreconditionFailed".to_string()),
                            error_message: Some(
                                "versioned conditional batch object read payload budget exceeded"
                                    .to_string(),
                            ),
                        });
                        continue;
                    }
                    report.payload_bytes = next_payload;
                    if object.not_modified {
                        report.not_modified_objects = report.not_modified_objects.saturating_add(1);
                    } else {
                        report.successful_objects = report.successful_objects.saturating_add(1);
                    }
                    report.items.push(VersionedConditionalObjectReadItem {
                        request,
                        object: Some(object),
                        stats: ChunkReadStats::default(),
                        error_code: None,
                        error_message: None,
                    });
                }
                Err(err) => {
                    report.failed_objects = report.failed_objects.saturating_add(1);
                    report.items.push(VersionedConditionalObjectReadItem {
                        request,
                        object: None,
                        stats: ChunkReadStats::default(),
                        error_code: Some(object_error_code(&err).to_string()),
                        error_message: Some(err.to_string()),
                    });
                }
            }
        }
        stamp_versioned_conditional_object_batch_read_proof(
            "VersionedConditionalBatchReadObjects",
            &mut report,
        );
        Ok(report)
    }

    fn read_versioned_conditional_object_for_batch(
        &self,
        request: &VersionedConditionalObjectReadRequest,
        max_payload_bytes: u64,
        metadata_read_options: Option<&MetadataReadOptions>,
    ) -> Result<ConditionalObjectData, ObjectError> {
        if request.version == 0 {
            return Err(ObjectError::InvalidArgument(
                "versioned conditional batch object read version must be positive".to_string(),
            ));
        }
        match &request.range {
            Some(range) => {
                range.end()?;
                if let Some(options) = metadata_read_options {
                    self.get_object_version_range_conditional_with_metadata_read(
                        &request.bucket,
                        &request.key,
                        request.version,
                        range.offset,
                        range.length,
                        max_payload_bytes,
                        &request.preconditions,
                        options.clone(),
                    )
                    .map(|(object, _)| object)
                } else {
                    self.get_object_version_range_conditional(
                        &request.bucket,
                        &request.key,
                        request.version,
                        range.offset,
                        range.length,
                        max_payload_bytes,
                        &request.preconditions,
                    )
                }
            }
            None => {
                if let Some(options) = metadata_read_options {
                    self.get_object_version_conditional_with_metadata_read(
                        &request.bucket,
                        &request.key,
                        request.version,
                        &request.preconditions,
                        options.clone(),
                    )
                    .map(|(object, _)| object)
                } else {
                    self.get_object_version_conditional(
                        &request.bucket,
                        &request.key,
                        request.version,
                        &request.preconditions,
                    )
                }
            }
        }
    }

}
