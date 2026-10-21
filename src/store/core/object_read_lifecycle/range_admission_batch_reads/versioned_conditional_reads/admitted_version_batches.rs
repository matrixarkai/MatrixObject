// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    fn batch_get_object_versions_conditional_with_read_admission_internal(
        &self,
        requests: Vec<VersionedConditionalObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        metadata_read_options: Option<MetadataReadOptions>,
    ) -> Result<AdmittedVersionedConditionalObjectBatchReadReport, ObjectError> {
        if max_items == 0 {
            return Err(ObjectError::InvalidArgument(
                "versioned admitted conditional batch object read max_items must be positive"
                    .to_string(),
            ));
        }
        if requests.len() > max_items {
            return Err(ObjectError::InvalidArgument(
                "versioned admitted conditional batch object read item count exceeds max_items"
                    .to_string(),
            ));
        }
        let mut report = AdmittedVersionedConditionalObjectBatchReadReport {
            requested_objects: requests.len() as u64,
            max_payload_bytes,
            max_chunk_read_count,
            route_control_proof: self.chunk_service.route_control_proof(),
            ..AdmittedVersionedConditionalObjectBatchReadReport::default()
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
                report.admission_rejected_objects =
                    report.admission_rejected_objects.saturating_add(1);
                report.budget_limited = true;
                report
                    .items
                    .push(AdmittedVersionedConditionalObjectReadItem {
                    request,
                    object: None,
                    error_code: Some("PreconditionFailed".to_string()),
                    error_message: Some(
                        "versioned admitted conditional batch object read payload budget exceeded"
                            .to_string(),
                    ),
                });
                continue;
            }
            let admitted = self.read_admitted_versioned_conditional_object_for_batch(
                &request,
                remaining_payload_budget,
                max_chunk_read_count.saturating_sub(report.reserved_chunk_read_count),
                metadata_read_options.as_ref(),
            );
            match admitted {
                Ok(object) => {
                    if object.admission.can_accept {
                        let admitted_chunk_reads = object
                            .admission
                            .readable_chunk_count
                            .max(object.admission.mapped_chunk_ranges);
                        if max_chunk_read_count > 0
                            && report
                                .reserved_chunk_read_count
                                .saturating_add(admitted_chunk_reads)
                                > max_chunk_read_count
                        {
                            report.admission_rejected_objects =
                                report.admission_rejected_objects.saturating_add(1);
                            report.budget_limited = true;
                            report
                                .items
                                .push(AdmittedVersionedConditionalObjectReadItem {
                                    request,
                                    object: None,
                                    error_code: Some("PreconditionFailed".to_string()),
                                    error_message: Some(
                                        "versioned admitted conditional batch object read chunk fanout exceeded"
                                            .to_string(),
                                    ),
                                });
                            continue;
                        }
                        report.reserved_chunk_read_count = report
                            .reserved_chunk_read_count
                            .saturating_add(admitted_chunk_reads);
                        if let Some(data) = object
                            .object
                            .as_ref()
                            .and_then(|conditional| conditional.data.as_ref())
                        {
                            report.payload_bytes =
                                report.payload_bytes.saturating_add(data.len() as u64);
                            report.successful_objects = report.successful_objects.saturating_add(1);
                        } else if object
                            .object
                            .as_ref()
                            .map(|conditional| conditional.not_modified)
                            .unwrap_or(false)
                        {
                            report.not_modified_objects =
                                report.not_modified_objects.saturating_add(1);
                        } else {
                            report.successful_objects = report.successful_objects.saturating_add(1);
                        }
                    } else {
                        report.admission_rejected_objects =
                            report.admission_rejected_objects.saturating_add(1);
                        report.budget_limited |= object.admission.budget_limited;
                    }
                    report
                        .items
                        .push(AdmittedVersionedConditionalObjectReadItem {
                            request,
                            object: Some(object),
                            error_code: None,
                            error_message: None,
                        });
                }
                Err(err) => {
                    report.failed_objects = report.failed_objects.saturating_add(1);
                    report
                        .items
                        .push(AdmittedVersionedConditionalObjectReadItem {
                            request,
                            object: None,
                            error_code: Some(object_error_code(&err).to_string()),
                            error_message: Some(err.to_string()),
                        });
                }
            }
        }
        stamp_admitted_versioned_conditional_object_batch_read_proof(
            "VersionedConditionalBatchReadObjectsWithAdmission",
            &mut report,
        );
        Ok(report)
    }

    fn read_admitted_versioned_conditional_object_for_batch(
        &self,
        request: &VersionedConditionalObjectReadRequest,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        metadata_read_options: Option<&MetadataReadOptions>,
    ) -> Result<AdmittedConditionalObjectData, ObjectError> {
        if request.version == 0 {
            return Err(ObjectError::InvalidArgument(
                "versioned admitted conditional batch object read version must be positive"
                    .to_string(),
            ));
        }
        if let Some(range) = &request.range {
            range.end()?;
        }
        let metadata = if let Some(options) = metadata_read_options {
            self.head_object_version_with_metadata_read(
                &request.bucket,
                &request.key,
                request.version,
                options.clone(),
            )
            .map(|(metadata, _)| metadata)?
        } else {
            self.head_object_version(&request.bucket, &request.key, request.version)?
        };
        let (offset, length) = request
            .range
            .as_ref()
            .map(|range| (range.offset, range.length))
            .unwrap_or((0, metadata.length));
        self.get_object_metadata_range_conditional_with_bounded_read_admission(
            metadata,
            offset,
            length,
            max_payload_bytes,
            max_chunk_read_count,
            &request.preconditions,
        )
    }

}
