impl MatrixObjectStore {
    pub fn batch_copy_objects_with_root_placement(
        &mut self,
        requests: Vec<ObjectCopyRequest>,
        max_items: usize,
        max_copied_bytes: u64,
        replica_count: usize,
        update_time_ms: u64,
    ) -> Result<ObjectBatchCopyReport, ObjectError> {
        self.batch_copy_objects_with_root_placement_fenced(
            requests,
            max_items,
            max_copied_bytes,
            replica_count,
            update_time_ms,
            None,
        )
    }

    pub fn batch_copy_objects_with_root_placement_fenced(
        &mut self,
        requests: Vec<ObjectCopyRequest>,
        max_items: usize,
        max_copied_bytes: u64,
        replica_count: usize,
        update_time_ms: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectBatchCopyReport, ObjectError> {
        self.batch_copy_objects_with_root_placement_fenced_internal(
            requests,
            max_items,
            max_copied_bytes,
            replica_count,
            update_time_ms,
            expected_route_control_checksum,
            None,
            None,
        )
    }

    pub fn batch_copy_objects_with_root_placement_fenced_with_copy_proof(
        &mut self,
        requests: Vec<ObjectCopyRequest>,
        max_items: usize,
        max_copied_bytes: u64,
        replica_count: usize,
        update_time_ms: u64,
        expected_route_control_checksum: Option<&str>,
        expected_copy_batch_proof_checksum: Option<&str>,
    ) -> Result<ObjectBatchCopyReport, ObjectError> {
        self.batch_copy_objects_with_root_placement_fenced_internal(
            requests,
            max_items,
            max_copied_bytes,
            replica_count,
            update_time_ms,
            expected_route_control_checksum,
            expected_copy_batch_proof_checksum,
            None,
        )
    }

    pub fn batch_copy_objects_with_root_placement_metadata_read(
        &mut self,
        requests: Vec<ObjectCopyRequest>,
        max_items: usize,
        max_copied_bytes: u64,
        replica_count: usize,
        update_time_ms: u64,
        read_options: MetadataReadOptions,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectBatchCopyReport, ObjectError> {
        let readiness =
            self.metadata_raft_readiness(read_options.min_read_index, read_options.now_ms);
        if !readiness.readable {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata read index is not satisfied: reason={} applied={} required={}",
                readiness.reason, readiness.applied_commit_index, readiness.min_read_index
            )));
        }
        for request in &requests {
            let source_id = ObjectId::new(&request.source_bucket, &request.source_key);
            self.prepare_metadata_read(&meta_object_key(&source_id), &read_options)?;
            let destination_id =
                ObjectId::new(&request.destination_bucket, &request.destination_key);
            self.prepare_metadata_read(&meta_object_key(&destination_id), &read_options)?;
        }
        let mut report = self.batch_copy_objects_with_root_placement_fenced_internal(
            requests,
            max_items,
            max_copied_bytes,
            replica_count,
            update_time_ms,
            expected_route_control_checksum,
            None,
            Some(&read_options),
        )?;
        report.read_proof = object_read_proof_from_readiness(&readiness);
        stamp_object_batch_copy_proof("BatchCopyObjectsWithRootPlacementMetadataRead", &mut report);
        Ok(report)
    }

    fn batch_copy_objects_with_root_placement_fenced_internal(
        &mut self,
        requests: Vec<ObjectCopyRequest>,
        max_items: usize,
        max_copied_bytes: u64,
        replica_count: usize,
        update_time_ms: u64,
        expected_route_control_checksum: Option<&str>,
        expected_copy_batch_proof_checksum: Option<&str>,
        read_options: Option<&MetadataReadOptions>,
    ) -> Result<ObjectBatchCopyReport, ObjectError> {
        self.chunk_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        if max_items == 0 {
            return Err(ObjectError::InvalidArgument(
                "batch object copy max_items must be positive".to_string(),
            ));
        }
        if requests.len() > max_items {
            return Err(ObjectError::InvalidArgument(
                "batch object copy item count exceeds max_items".to_string(),
            ));
        }
        let mut report = ObjectBatchCopyReport {
            requested_objects: requests.len() as u64,
            route_control_proof: self.chunk_service.route_control_proof(),
            expected_route_control_checksum: expected_route_control_checksum
                .unwrap_or_default()
                .to_string(),
            ..ObjectBatchCopyReport::default()
        };
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        for request in requests {
            let source_length = self.batch_copy_source_length(&request, read_options)?;
            let next_copied = report.copied_bytes.saturating_add(source_length);
            if max_copied_bytes > 0 && next_copied > max_copied_bytes {
                report.failed_objects = report.failed_objects.saturating_add(1);
                report.items.push(ObjectCopyItem {
                    request,
                    metadata: None,
                    error_code: Some("PreconditionFailed".to_string()),
                    error_message: Some("batch object copy byte budget exceeded".to_string()),
                });
                continue;
            }
            let copy = self.apply_copy_with_root_placement(
                request.source_bucket.clone(),
                request.source_key.clone(),
                request.source_version,
                request.destination_bucket.clone(),
                request.destination_key.clone(),
                request.content_type.clone(),
                update_time_ms,
                request.options.clone(),
                replica_count,
            );
            match copy {
                Ok(metadata) => {
                    report.copied_objects = report.copied_objects.saturating_add(1);
                    report.copied_bytes = report.copied_bytes.saturating_add(metadata.length);
                    report.items.push(ObjectCopyItem {
                        request,
                        metadata: Some(metadata),
                        error_code: None,
                        error_message: None,
                    });
                }
                Err(err) => {
                    report.failed_objects = report.failed_objects.saturating_add(1);
                    report.items.push(ObjectCopyItem {
                        request,
                        metadata: None,
                        error_code: Some(object_error_code(&err).to_string()),
                        error_message: Some(err.to_string()),
                    });
                }
            }
        }
        if rollback.is_some() {
            let committed = committed_batch_copy_request_refs(&report);
            if !committed.is_empty() {
                if let Err(err) = self.append_batch_copy_journal_record(
                    &committed,
                    max_items,
                    max_copied_bytes,
                    update_time_ms,
                    Some(replica_count),
                ) {
                    if let Some(rollback) = rollback {
                        self.restore_object_mutation_rollback(rollback);
                    }
                    return Err(err);
                }
            }
        }
        stamp_object_batch_copy_proof("BatchCopyObjectsWithRootPlacement", &mut report);
        report.expected_copy_batch_proof_checksum = expected_copy_batch_proof_checksum
            .unwrap_or_default()
            .to_string();
        check_expected_object_batch_copy_proof_checksum(
            "BatchCopyObjectsWithRootPlacement",
            &report.copy_batch_proof_checksum,
            expected_copy_batch_proof_checksum,
        )?;
        Ok(report)
    }
}
