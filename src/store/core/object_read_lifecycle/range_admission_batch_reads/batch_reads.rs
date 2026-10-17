impl MatrixObjectStore {
    pub fn batch_get_objects(
        &mut self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
    ) -> Result<ObjectBatchReadReport, ObjectError> {
        self.batch_get_objects_internal(requests, max_items, max_payload_bytes, false, None)
    }

    pub fn batch_get_objects_repairing(
        &mut self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
    ) -> Result<ObjectBatchReadReport, ObjectError> {
        self.batch_get_objects_internal(requests, max_items, max_payload_bytes, true, None)
    }

    pub fn batch_get_objects_repairing_fenced(
        &mut self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectBatchReadReport, ObjectError> {
        self.page_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        let mut report =
            self.batch_get_objects_internal(requests, max_items, max_payload_bytes, true, None)?;
        report.expected_route_control_checksum = expected_route_control_checksum
            .unwrap_or_default()
            .to_string();
        Ok(report)
    }

    pub fn batch_get_objects_with_metadata_read(
        &mut self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        options: MetadataReadOptions,
    ) -> Result<ObjectBatchReadReport, ObjectError> {
        self.batch_get_objects_internal(
            requests,
            max_items,
            max_payload_bytes,
            false,
            Some(options),
        )
    }

    pub fn batch_get_objects_with_read_admission(
        &self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
    ) -> Result<AdmittedObjectBatchReadReport, ObjectError> {
        self.batch_get_objects_with_read_admission_internal(
            requests,
            max_items,
            max_payload_bytes,
            0,
            None,
        )
    }

    pub fn batch_get_objects_with_bounded_read_admission(
        &self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
    ) -> Result<AdmittedObjectBatchReadReport, ObjectError> {
        self.batch_get_objects_with_read_admission_internal(
            requests,
            max_items,
            max_payload_bytes,
            max_chunk_read_count,
            None,
        )
    }

    pub fn batch_get_objects_with_read_admission_and_metadata_read(
        &self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        options: MetadataReadOptions,
    ) -> Result<AdmittedObjectBatchReadReport, ObjectError> {
        self.batch_get_objects_with_read_admission_internal(
            requests,
            max_items,
            max_payload_bytes,
            0,
            Some(options),
        )
    }

    pub fn batch_get_objects_with_bounded_read_admission_and_metadata_read(
        &self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        options: MetadataReadOptions,
    ) -> Result<AdmittedObjectBatchReadReport, ObjectError> {
        self.batch_get_objects_with_read_admission_internal(
            requests,
            max_items,
            max_payload_bytes,
            max_chunk_read_count,
            Some(options),
        )
    }

    fn batch_get_objects_with_read_admission_internal(
        &self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        metadata_read_options: Option<MetadataReadOptions>,
    ) -> Result<AdmittedObjectBatchReadReport, ObjectError> {
        if max_items == 0 {
            return Err(ObjectError::InvalidArgument(
                "batch admitted object read max_items must be positive".to_string(),
            ));
        }
        if requests.len() > max_items {
            return Err(ObjectError::InvalidArgument(
                "batch admitted object read item count exceeds max_items".to_string(),
            ));
        }
        let mut report = AdmittedObjectBatchReadReport {
            requested_objects: requests.len() as u64,
            max_payload_bytes,
            max_chunk_read_count,
            route_control_proof: self.page_service.route_control_proof(),
            ..AdmittedObjectBatchReadReport::default()
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
                report.items.push(AdmittedObjectReadItem {
                    request,
                    object: None,
                    error_code: Some("PreconditionFailed".to_string()),
                    error_message: Some(
                        "batch admitted object read payload budget exceeded".to_string(),
                    ),
                });
                continue;
            }
            let admitted = self.read_admitted_object_for_batch(
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
                            report.items.push(AdmittedObjectReadItem {
                                request,
                                object: None,
                                error_code: Some("PreconditionFailed".to_string()),
                                error_message: Some(
                                    "batch admitted object read chunk fanout exceeded".to_string(),
                                ),
                            });
                            continue;
                        }
                        report.reserved_chunk_read_count = report
                            .reserved_chunk_read_count
                            .saturating_add(admitted_chunk_reads);
                        if let Some(data) = object.object.as_ref() {
                            report.payload_bytes =
                                report.payload_bytes.saturating_add(data.data.len() as u64);
                        }
                        report.successful_objects = report.successful_objects.saturating_add(1);
                    } else {
                        report.admission_rejected_objects =
                            report.admission_rejected_objects.saturating_add(1);
                        report.budget_limited |= object.admission.budget_limited;
                    }
                    report.items.push(AdmittedObjectReadItem {
                        request,
                        object: Some(object),
                        error_code: None,
                        error_message: None,
                    });
                }
                Err(err) => {
                    report.failed_objects = report.failed_objects.saturating_add(1);
                    report.items.push(AdmittedObjectReadItem {
                        request,
                        object: None,
                        error_code: Some(object_error_code(&err).to_string()),
                        error_message: Some(err.to_string()),
                    });
                }
            }
        }
        stamp_admitted_object_batch_read_proof("BatchReadObjectsWithAdmission", &mut report);
        Ok(report)
    }

    fn read_admitted_object_for_batch(
        &self,
        request: &ObjectReadRequest,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        metadata_read_options: Option<&MetadataReadOptions>,
    ) -> Result<AdmittedObjectData, ObjectError> {
        if let Some(range) = &request.range {
            range.end()?;
        }
        let metadata = match (request.version, metadata_read_options) {
            (Some(version), Some(options)) => self
                .head_object_version_with_metadata_read(
                    &request.bucket,
                    &request.key,
                    version,
                    options.clone(),
                )
                .map(|(metadata, _)| metadata)?,
            (Some(version), None) => {
                self.head_object_version(&request.bucket, &request.key, version)?
            }
            (None, Some(options)) => self
                .head_object_with_metadata_read(&request.bucket, &request.key, options.clone())
                .map(|(metadata, _)| metadata)?,
            (None, None) => self.head_object(&request.bucket, &request.key)?,
        };
        let (offset, length) = request
            .range
            .as_ref()
            .map(|range| (range.offset, range.length))
            .unwrap_or((0, metadata.length));
        self.get_object_metadata_range_with_bounded_read_admission(
            metadata,
            offset,
            length,
            max_payload_bytes,
            max_chunk_read_count,
        )
    }
}
