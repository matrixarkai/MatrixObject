// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn batch_delete_objects(
        &mut self,
        requests: Vec<ObjectDeleteRequest>,
        max_items: usize,
        release_root_placements: bool,
        update_time_ms: u64,
    ) -> Result<ObjectBatchDeleteReport, ObjectError> {
        self.batch_delete_objects_fenced(
            requests,
            max_items,
            release_root_placements,
            update_time_ms,
            None,
        )
    }

    pub fn batch_delete_objects_fenced(
        &mut self,
        requests: Vec<ObjectDeleteRequest>,
        max_items: usize,
        release_root_placements: bool,
        update_time_ms: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectBatchDeleteReport, ObjectError> {
        self.chunk_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        if max_items == 0 {
            return Err(ObjectError::InvalidArgument(
                "batch object delete max_items must be positive".to_string(),
            ));
        }
        if requests.len() > max_items {
            return Err(ObjectError::InvalidArgument(
                "batch object delete item count exceeds max_items".to_string(),
            ));
        }
        let mut seen_objects = BTreeSet::new();
        for request in &requests {
            seen_objects.insert(ObjectId::new(request.bucket.clone(), request.key.clone()));
        }
        let mut report = ObjectBatchDeleteReport {
            requested_objects: requests.len() as u64,
            unique_objects: seen_objects.len() as u64,
            duplicate_objects: requests.len().saturating_sub(seen_objects.len()) as u64,
            route_control_proof: self.chunk_service.route_control_proof(),
            expected_route_control_checksum: expected_route_control_checksum
                .unwrap_or_default()
                .to_string(),
            ..ObjectBatchDeleteReport::default()
        };
        report.items.reserve(requests.len());
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        for request in requests {
            let previous_length = match request.version {
                Some(version) => self
                    .head_object_version(&request.bucket, &request.key, version)
                    .map(|metadata| metadata.length)
                    .unwrap_or(0),
                None => self
                    .head_object(&request.bucket, &request.key)
                    .map(|metadata| metadata.length)
                    .unwrap_or(0),
            };
            let delete = if let Some(version) = request.version {
                self.apply_delete_object_version(
                    request.bucket.clone(),
                    request.key.clone(),
                    version,
                    update_time_ms,
                )
            } else {
                match self.can_accept_object_delete_discard_fenced(
                    &request.bucket,
                    &request.key,
                    0,
                    expected_route_control_checksum,
                ) {
                    Ok(admission) if !admission.can_accept => {
                        Err(ObjectError::PreconditionFailed(admission.partial_status))
                    }
                    Ok(_) if release_root_placements => self.apply_delete_with_placement_release(
                        request.bucket.clone(),
                        request.key.clone(),
                        update_time_ms,
                        request.if_match_etag.clone(),
                    ),
                    Ok(_) => self.apply_delete(
                        request.bucket.clone(),
                        request.key.clone(),
                        update_time_ms,
                        request.if_match_etag.clone(),
                    ),
                    Err(err) => Err(err),
                }
            };
            match delete {
                Ok(metadata) => {
                    report.deleted_objects = report.deleted_objects.saturating_add(1);
                    report.deleted_bytes = report.deleted_bytes.saturating_add(previous_length);
                    report.items.push(ObjectDeleteItem {
                        request,
                        metadata: Some(metadata),
                        error_code: None,
                        error_message: None,
                    });
                }
                Err(err) => {
                    report.failed_objects = report.failed_objects.saturating_add(1);
                    report.items.push(ObjectDeleteItem {
                        request,
                        metadata: None,
                        error_code: Some(object_error_code(&err).to_string()),
                        error_message: Some(err.to_string()),
                    });
                }
            }
        }
        stamp_object_batch_delete_proof("BatchDeleteObjects", &mut report);
        if rollback.is_some() {
            let committed = committed_batch_delete_request_refs(&report);
            if !committed.is_empty() {
                if let Err(err) = self.append_batch_delete_journal_record(
                    &committed,
                    max_items,
                    release_root_placements,
                    update_time_ms,
                ) {
                    if let Some(rollback) = rollback {
                        self.restore_object_mutation_rollback(rollback);
                    }
                    return Err(err);
                }
            }
        }
        stamp_object_batch_delete_proof("BatchDeleteObjects", &mut report);
        Ok(report)
    }

    pub fn batch_delete_objects_fenced_with_delete_proof(
        &mut self,
        requests: Vec<ObjectDeleteRequest>,
        max_items: usize,
        release_root_placements: bool,
        update_time_ms: u64,
        expected_route_control_checksum: Option<&str>,
        expected_delete_batch_proof_checksum: Option<&str>,
    ) -> Result<ObjectBatchDeleteReport, ObjectError> {
        let mut report = self.batch_delete_objects_fenced(
            requests,
            max_items,
            release_root_placements,
            update_time_ms,
            expected_route_control_checksum,
        )?;
        check_expected_object_batch_delete_proof_checksum(
            "BatchDeleteObjects",
            &report.delete_batch_proof_checksum,
            expected_delete_batch_proof_checksum,
        )?;
        report.expected_delete_batch_proof_checksum = expected_delete_batch_proof_checksum
            .unwrap_or_default()
            .to_string();
        Ok(report)
    }

    pub fn batch_delete_objects_with_metadata_read(
        &mut self,
        requests: Vec<ObjectDeleteRequest>,
        max_items: usize,
        release_root_placements: bool,
        update_time_ms: u64,
        read_options: MetadataReadOptions,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectBatchDeleteReport, ObjectError> {
        let readiness =
            self.metadata_raft_readiness(read_options.min_read_index, read_options.now_ms);
        if !readiness.readable {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata read index is not satisfied: reason={} applied={} required={}",
                readiness.reason, readiness.applied_commit_index, readiness.min_read_index
            )));
        }
        if read_options.require_shard_owner_serving {
            for request in &requests {
                let object_id = ObjectId::new(&request.bucket, &request.key);
                self.prepare_metadata_read(&meta_object_key(&object_id), &read_options)?;
            }
        }
        let mut report = self.batch_delete_objects_fenced(
            requests,
            max_items,
            release_root_placements,
            update_time_ms,
            expected_route_control_checksum,
        )?;
        report.read_proof = object_read_proof_from_readiness(&readiness);
        stamp_object_batch_delete_proof("BatchDeleteObjects", &mut report);
        Ok(report)
    }

    pub fn batch_delete_objects_with_metadata_read_delete_proof_fenced(
        &mut self,
        requests: Vec<ObjectDeleteRequest>,
        max_items: usize,
        release_root_placements: bool,
        update_time_ms: u64,
        read_options: MetadataReadOptions,
        expected_route_control_checksum: Option<&str>,
        expected_delete_batch_proof_checksum: Option<&str>,
    ) -> Result<ObjectBatchDeleteReport, ObjectError> {
        let mut report = self.batch_delete_objects_with_metadata_read(
            requests,
            max_items,
            release_root_placements,
            update_time_ms,
            read_options,
            expected_route_control_checksum,
        )?;
        check_expected_object_batch_delete_proof_checksum(
            "BatchDeleteObjects",
            &report.delete_batch_proof_checksum,
            expected_delete_batch_proof_checksum,
        )?;
        report.expected_delete_batch_proof_checksum = expected_delete_batch_proof_checksum
            .unwrap_or_default()
            .to_string();
        Ok(report)
    }

}
