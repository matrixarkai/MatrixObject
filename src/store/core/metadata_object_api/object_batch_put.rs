impl MatrixObjectStore {
    pub fn batch_put_objects(
        &mut self,
        requests: Vec<ObjectPutRequest>,
        max_items: usize,
        max_payload_bytes: u64,
    ) -> Result<ObjectBatchPutReport, ObjectError> {
        self.batch_put_objects_fenced(requests, max_items, max_payload_bytes, now_millis(), None)
    }

    pub fn batch_put_objects_fenced(
        &mut self,
        requests: Vec<ObjectPutRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        update_time_ms: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectBatchPutReport, ObjectError> {
        self.ensure_put_requests_read_fence("batch object put", &requests)?;
        self.page_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        if self.persistent_journal_path.is_empty() {
            return self.batch_put_objects_at(
                requests,
                max_items,
                max_payload_bytes,
                update_time_ms,
                expected_route_control_checksum,
            );
        }
        let rollback = self.capture_object_mutation_rollback();
        let (report, committed) = self.batch_put_objects_at_with_committed(
            requests,
            max_items,
            max_payload_bytes,
            update_time_ms,
            expected_route_control_checksum,
            true,
        )?;
        if !committed.is_empty() {
            if let Err(err) = self.append_batch_put_journal_record(
                &committed,
                max_items,
                max_payload_bytes,
                update_time_ms,
                None,
            ) {
                self.restore_object_mutation_rollback(rollback);
                return Err(err);
            }
        }
        Ok(report)
    }

    pub fn batch_put_objects_fenced_with_put_proof(
        &mut self,
        requests: Vec<ObjectPutRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        update_time_ms: u64,
        expected_route_control_checksum: Option<&str>,
        expected_put_batch_proof_checksum: Option<&str>,
    ) -> Result<ObjectBatchPutReport, ObjectError> {
        let mut report = self.batch_put_objects_fenced(
            requests,
            max_items,
            max_payload_bytes,
            update_time_ms,
            expected_route_control_checksum,
        )?;
        check_expected_object_batch_put_proof_checksum(
            "BatchPutObjects",
            &report.put_batch_proof_checksum,
            expected_put_batch_proof_checksum,
        )?;
        report.expected_put_batch_proof_checksum = expected_put_batch_proof_checksum
            .unwrap_or_default()
            .to_string();
        Ok(report)
    }

    pub fn batch_put_objects_with_root_placement(
        &mut self,
        requests: Vec<ObjectPutRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        replica_count: usize,
    ) -> Result<ObjectBatchPutReport, ObjectError> {
        self.batch_put_objects_with_root_placement_fenced(
            requests,
            max_items,
            max_payload_bytes,
            replica_count,
            now_millis(),
            None,
        )
    }

    pub fn batch_put_objects_with_root_placement_fenced(
        &mut self,
        requests: Vec<ObjectPutRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        replica_count: usize,
        update_time_ms: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectBatchPutReport, ObjectError> {
        self.ensure_put_requests_read_fence("batch object put with root placement", &requests)?;
        self.page_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        if self.persistent_journal_path.is_empty() {
            return self.batch_put_objects_with_root_placement_at(
                requests,
                max_items,
                max_payload_bytes,
                update_time_ms,
                replica_count,
                expected_route_control_checksum,
            );
        }
        let rollback = self.capture_object_mutation_rollback();
        let (report, committed) = self.batch_put_objects_with_root_placement_at_with_committed(
            requests,
            max_items,
            max_payload_bytes,
            update_time_ms,
            replica_count,
            expected_route_control_checksum,
            true,
        )?;
        if !committed.is_empty() {
            if let Err(err) = self.append_batch_put_journal_record(
                &committed,
                max_items,
                max_payload_bytes,
                update_time_ms,
                Some(replica_count),
            ) {
                self.restore_object_mutation_rollback(rollback);
                return Err(err);
            }
        }
        Ok(report)
    }

    pub fn batch_put_objects_with_root_placement_fenced_with_put_proof(
        &mut self,
        requests: Vec<ObjectPutRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        replica_count: usize,
        update_time_ms: u64,
        expected_route_control_checksum: Option<&str>,
        expected_put_batch_proof_checksum: Option<&str>,
    ) -> Result<ObjectBatchPutReport, ObjectError> {
        let mut report = self.batch_put_objects_with_root_placement_fenced(
            requests,
            max_items,
            max_payload_bytes,
            replica_count,
            update_time_ms,
            expected_route_control_checksum,
        )?;
        check_expected_object_batch_put_proof_checksum(
            "BatchPutObjectsWithRootPlacement",
            &report.put_batch_proof_checksum,
            expected_put_batch_proof_checksum,
        )?;
        report.expected_put_batch_proof_checksum = expected_put_batch_proof_checksum
            .unwrap_or_default()
            .to_string();
        Ok(report)
    }

    fn batch_put_objects_at(
        &mut self,
        requests: Vec<ObjectPutRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        update_time_ms: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectBatchPutReport, ObjectError> {
        self.batch_put_objects_at_with_committed(
            requests,
            max_items,
            max_payload_bytes,
            update_time_ms,
            expected_route_control_checksum,
            false,
        )
        .map(|(report, _)| report)
    }

    fn batch_put_objects_at_with_committed(
        &mut self,
        requests: Vec<ObjectPutRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        update_time_ms: u64,
        expected_route_control_checksum: Option<&str>,
        collect_committed: bool,
    ) -> Result<(ObjectBatchPutReport, Vec<ObjectPutRequest>), ObjectError> {
        if max_items == 0 {
            return Err(ObjectError::InvalidArgument(
                "batch object put max_items must be positive".to_string(),
            ));
        }
        if requests.len() > max_items {
            return Err(ObjectError::InvalidArgument(
                "batch object put request count exceeds max_items".to_string(),
            ));
        }
        let mut requested_payload_bytes = 0_u64;
        for request in &requests {
            requested_payload_bytes = requested_payload_bytes
                .checked_add(request.data.len() as u64)
                .ok_or_else(|| {
                    ObjectError::InvalidArgument(
                        "batch object put payload bytes overflow".to_string(),
                    )
                })?;
        }
        if max_payload_bytes > 0 && requested_payload_bytes > max_payload_bytes {
            return Err(ObjectError::InvalidArgument(
                "batch object put payload exceeds max_payload_bytes".to_string(),
            ));
        }

        let mut report = ObjectBatchPutReport {
            requested_objects: requests.len() as u64,
            payload_bytes: requested_payload_bytes,
            max_payload_bytes,
            route_control_proof: self.page_service.route_control_proof(),
            expected_route_control_checksum: expected_route_control_checksum
                .unwrap_or_default()
                .to_string(),
            ..ObjectBatchPutReport::default()
        };
        report.items.reserve(requests.len());
        let mut committed_requests = Vec::new();
        for request in requests {
            let journal_request = if collect_committed {
                Some(request.clone())
            } else {
                None
            };
            let ObjectPutRequest {
                bucket,
                key,
                data,
                content_type,
                options,
            } = request;
            let payload_bytes = data.len() as u64;
            let metadata = self.apply_put_storage_batched(
                bucket.clone(),
                key.clone(),
                data,
                content_type,
                update_time_ms,
                options,
                "batch_put_object",
            );
            match metadata {
                Ok(metadata) => {
                    report.put_objects = report.put_objects.saturating_add(1);
                    if let Some(journal_request) = journal_request {
                        committed_requests.push(journal_request);
                    }
                    report.items.push(ObjectPutItem {
                        bucket,
                        key,
                        metadata: Some(metadata),
                        payload_bytes,
                        error_code: None,
                        error_message: None,
                    });
                }
                Err(err) => {
                    report.failed_objects = report.failed_objects.saturating_add(1);
                    report.items.push(ObjectPutItem {
                        bucket,
                        key,
                        metadata: None,
                        payload_bytes,
                        error_code: Some(object_error_code(&err).to_string()),
                        error_message: Some(err.to_string()),
                    });
                }
            }
        }
        let readiness = self.metadata_raft_readiness(0, update_time_ms);
        report.read_proof = object_read_proof_from_readiness(&readiness);
        stamp_object_batch_put_proof("BatchPutObjects", &mut report);
        Ok((report, committed_requests))
    }

    fn batch_put_objects_with_root_placement_at(
        &mut self,
        requests: Vec<ObjectPutRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        update_time_ms: u64,
        replica_count: usize,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectBatchPutReport, ObjectError> {
        self.batch_put_objects_with_root_placement_at_with_committed(
            requests,
            max_items,
            max_payload_bytes,
            update_time_ms,
            replica_count,
            expected_route_control_checksum,
            false,
        )
        .map(|(report, _)| report)
    }

    fn batch_put_objects_with_root_placement_at_with_committed(
        &mut self,
        requests: Vec<ObjectPutRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        update_time_ms: u64,
        replica_count: usize,
        expected_route_control_checksum: Option<&str>,
        collect_committed: bool,
    ) -> Result<(ObjectBatchPutReport, Vec<ObjectPutRequest>), ObjectError> {
        if max_items == 0 {
            return Err(ObjectError::InvalidArgument(
                "batch object put max_items must be positive".to_string(),
            ));
        }
        if requests.len() > max_items {
            return Err(ObjectError::InvalidArgument(
                "batch object put request count exceeds max_items".to_string(),
            ));
        }
        let mut requested_payload_bytes = 0_u64;
        for request in &requests {
            requested_payload_bytes = requested_payload_bytes
                .checked_add(request.data.len() as u64)
                .ok_or_else(|| {
                    ObjectError::InvalidArgument(
                        "batch object put payload bytes overflow".to_string(),
                    )
                })?;
        }
        if max_payload_bytes > 0 && requested_payload_bytes > max_payload_bytes {
            return Err(ObjectError::InvalidArgument(
                "batch object put payload exceeds max_payload_bytes".to_string(),
            ));
        }

        let mut report = ObjectBatchPutReport {
            requested_objects: requests.len() as u64,
            payload_bytes: requested_payload_bytes,
            max_payload_bytes,
            route_control_proof: self.page_service.route_control_proof(),
            expected_route_control_checksum: expected_route_control_checksum
                .unwrap_or_default()
                .to_string(),
            ..ObjectBatchPutReport::default()
        };
        report.items.reserve(requests.len());
        let mut committed_requests = Vec::new();
        for request in requests {
            let journal_request = if collect_committed {
                Some(request.clone())
            } else {
                None
            };
            let ObjectPutRequest {
                bucket,
                key,
                data,
                content_type,
                options,
            } = request;
            let payload_bytes = data.len() as u64;
            let metadata = self.apply_put_with_root_placement(
                bucket.clone(),
                key.clone(),
                data,
                content_type,
                update_time_ms,
                options,
                replica_count,
            );
            match metadata {
                Ok(metadata) => {
                    report.put_objects = report.put_objects.saturating_add(1);
                    if let Some(journal_request) = journal_request {
                        committed_requests.push(journal_request);
                    }
                    report.items.push(ObjectPutItem {
                        bucket,
                        key,
                        metadata: Some(metadata),
                        payload_bytes,
                        error_code: None,
                        error_message: None,
                    });
                }
                Err(err) => {
                    report.failed_objects = report.failed_objects.saturating_add(1);
                    report.items.push(ObjectPutItem {
                        bucket,
                        key,
                        metadata: None,
                        payload_bytes,
                        error_code: Some(object_error_code(&err).to_string()),
                        error_message: Some(err.to_string()),
                    });
                }
            }
        }
        let readiness = self.metadata_raft_readiness(0, update_time_ms);
        report.read_proof = object_read_proof_from_readiness(&readiness);
        stamp_object_batch_put_proof("BatchPutObjectsWithRootPlacement", &mut report);
        Ok((report, committed_requests))
    }

}
