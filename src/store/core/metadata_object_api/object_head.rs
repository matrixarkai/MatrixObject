// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn head_object(&self, bucket: &str, key: &str) -> Result<ObjectMetadata, ObjectError> {
        let object_id = ObjectId::new(bucket, key);
        self.objects
            .get(&object_id)
            .filter(|metadata| !metadata.deleted)
            .cloned()
            .ok_or_else(|| ObjectError::NotFound(format!("{}/{}", bucket, key)))
    }

    pub fn head_object_with_metadata_read(
        &self,
        bucket: &str,
        key: &str,
        options: MetadataReadOptions,
    ) -> Result<(ObjectMetadata, MetadataRaftReadiness), ObjectError> {
        validate_bucket(bucket)?;
        validate_key(key)?;
        let object_id = ObjectId::new(bucket, key);
        let readiness = self.prepare_metadata_read(&meta_object_key(&object_id), &options)?;
        let metadata = self.head_object(bucket, key)?;
        Ok((metadata, readiness))
    }

    pub fn head_objects_batch(
        &self,
        objects: Vec<ObjectId>,
        max_items: usize,
        max_payload_bytes: u64,
    ) -> Result<ObjectMetadataBatchRead, ObjectError> {
        let (batch, _) =
            self.head_objects_batch_internal(objects, max_items, max_payload_bytes, None)?;
        Ok(batch)
    }

    pub fn head_objects_batch_with_metadata_read(
        &self,
        objects: Vec<ObjectId>,
        max_items: usize,
        max_payload_bytes: u64,
        options: MetadataReadOptions,
    ) -> Result<(ObjectMetadataBatchRead, MetadataRaftReadiness), ObjectError> {
        let (batch, readiness) =
            self.head_objects_batch_internal(objects, max_items, max_payload_bytes, Some(options))?;
        Ok((batch, readiness.unwrap_or_default()))
    }

    pub fn head_objects_batch_with_metadata_read_route_fenced(
        &self,
        objects: Vec<ObjectId>,
        max_items: usize,
        max_payload_bytes: u64,
        options: MetadataReadOptions,
        expected_shard_topology_epoch: Option<u64>,
        expected_shard_topology_checksum: Option<&str>,
    ) -> Result<(ObjectMetadataBatchRead, MetadataRaftReadiness), ObjectError> {
        check_expected_metadata_shard_topology_fence(
            "HeadObjectsBatch",
            &self.meta_service,
            expected_shard_topology_epoch,
            expected_shard_topology_checksum,
        )?;
        let (mut batch, readiness) =
            self.head_objects_batch_internal(objects, max_items, max_payload_bytes, Some(options))?;
        stamp_object_metadata_batch_read_route_fence(
            &mut batch,
            expected_shard_topology_epoch,
            expected_shard_topology_checksum,
        );
        let proof = readiness
            .as_ref()
            .map(object_read_proof_from_readiness)
            .unwrap_or_default();
        stamp_object_metadata_batch_read_proof("HeadObjectsBatch", &mut batch, &proof);
        Ok((batch, readiness.unwrap_or_default()))
    }

    pub fn head_objects_conditional_batch(
        &self,
        requests: Vec<ConditionalObjectMetadataRequest>,
        max_items: usize,
        max_payload_bytes: u64,
    ) -> Result<ConditionalObjectMetadataBatchRead, ObjectError> {
        let (batch, _) = self.head_objects_conditional_batch_internal(
            requests,
            max_items,
            max_payload_bytes,
            None,
        )?;
        Ok(batch)
    }

    pub fn head_objects_conditional_batch_with_metadata_read(
        &self,
        requests: Vec<ConditionalObjectMetadataRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        options: MetadataReadOptions,
    ) -> Result<(ConditionalObjectMetadataBatchRead, MetadataRaftReadiness), ObjectError> {
        let (batch, readiness) = self.head_objects_conditional_batch_internal(
            requests,
            max_items,
            max_payload_bytes,
            Some(options),
        )?;
        Ok((batch, readiness.unwrap_or_default()))
    }

    fn head_objects_batch_internal(
        &self,
        objects: Vec<ObjectId>,
        max_items: usize,
        max_payload_bytes: u64,
        metadata_read_options: Option<MetadataReadOptions>,
    ) -> Result<(ObjectMetadataBatchRead, Option<MetadataRaftReadiness>), ObjectError> {
        if max_items == 0 {
            return Err(ObjectError::InvalidArgument(
                "object metadata batch max_items must be positive".to_string(),
            ));
        }
        if objects.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "object metadata batch must contain at least one object".to_string(),
            ));
        }
        if objects.len() > max_items {
            return Err(ObjectError::InvalidArgument(
                "object metadata batch item count exceeds max_items".to_string(),
            ));
        }
        let mut seen = BTreeSet::new();
        let mut readiness = None;
        for object_id in &objects {
            validate_bucket(&object_id.bucket)?;
            validate_key(&object_id.key)?;
            if !seen.insert(object_id.clone()) {
                return Err(ObjectError::InvalidArgument(
                    "object metadata batch contains duplicate object keys".to_string(),
                ));
            }
            if let Some(options) = metadata_read_options.as_ref() {
                readiness = Some(self.prepare_metadata_read(&meta_object_key(object_id), options)?);
            }
        }

        let mut batch = ObjectMetadataBatchRead {
            requested_objects: objects.len() as u64,
            ..ObjectMetadataBatchRead::default()
        };
        for object_id in objects {
            match self.head_object(&object_id.bucket, &object_id.key) {
                Ok(metadata) => {
                    let payload_bytes = object_metadata_payload_bytes(&metadata);
                    if max_payload_bytes > 0
                        && batch.payload_bytes.saturating_add(payload_bytes) > max_payload_bytes
                    {
                        return Err(ObjectError::InvalidArgument(
                            "object metadata batch exceeds max_payload_bytes".to_string(),
                        ));
                    }
                    batch.payload_bytes = batch.payload_bytes.saturating_add(payload_bytes);
                    batch.found_objects = batch.found_objects.saturating_add(1);
                    batch.objects.push(metadata);
                }
                Err(ObjectError::NotFound(_)) => {
                    batch.missing_objects = batch.missing_objects.saturating_add(1);
                    batch.missing.push(object_id);
                }
                Err(err) => return Err(err),
            }
        }
        let proof = readiness
            .as_ref()
            .map(object_read_proof_from_readiness)
            .unwrap_or_default();
        stamp_object_metadata_batch_read_proof("HeadObjectsBatch", &mut batch, &proof);
        Ok((batch, readiness))
    }

    fn head_objects_conditional_batch_internal(
        &self,
        requests: Vec<ConditionalObjectMetadataRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        metadata_read_options: Option<MetadataReadOptions>,
    ) -> Result<
        (
            ConditionalObjectMetadataBatchRead,
            Option<MetadataRaftReadiness>,
        ),
        ObjectError,
    > {
        if max_items == 0 {
            return Err(ObjectError::InvalidArgument(
                "conditional object metadata batch max_items must be positive".to_string(),
            ));
        }
        if requests.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "conditional object metadata batch must contain at least one object".to_string(),
            ));
        }
        if requests.len() > max_items {
            return Err(ObjectError::InvalidArgument(
                "conditional object metadata batch item count exceeds max_items".to_string(),
            ));
        }
        let mut seen = BTreeSet::new();
        let mut readiness = None;
        for request in &requests {
            validate_bucket(&request.bucket)?;
            validate_key(&request.key)?;
            if matches!(request.version, Some(0)) {
                return Err(ObjectError::InvalidArgument(
                    "conditional object metadata batch version must be positive".to_string(),
                ));
            }
            let object_id = ObjectId::new(&request.bucket, &request.key);
            let dedupe_key = (
                request.bucket.clone(),
                request.key.clone(),
                request.version.unwrap_or_default(),
            );
            if !seen.insert(dedupe_key) {
                return Err(ObjectError::InvalidArgument(
                    "conditional object metadata batch contains duplicate object keys".to_string(),
                ));
            }
            if let Some(options) = metadata_read_options.as_ref() {
                readiness =
                    Some(self.prepare_metadata_read(&meta_object_key(&object_id), options)?);
            }
        }

        let mut batch = ConditionalObjectMetadataBatchRead {
            requested_objects: requests.len() as u64,
            ..ConditionalObjectMetadataBatchRead::default()
        };
        for request in requests {
            let result = match request.version {
                Some(version) => self.head_object_version_conditional(
                    &request.bucket,
                    &request.key,
                    version,
                    &request.preconditions,
                ),
                None => self.head_object_conditional(
                    &request.bucket,
                    &request.key,
                    &request.preconditions,
                ),
            };
            match result {
                Ok(metadata) => {
                    let payload_bytes = object_metadata_payload_bytes(&metadata.metadata);
                    if max_payload_bytes > 0
                        && batch.payload_bytes.saturating_add(payload_bytes) > max_payload_bytes
                    {
                        return Err(ObjectError::InvalidArgument(
                            "conditional object metadata batch exceeds max_payload_bytes"
                                .to_string(),
                        ));
                    }
                    batch.payload_bytes = batch.payload_bytes.saturating_add(payload_bytes);
                    batch.found_objects = batch.found_objects.saturating_add(1);
                    if metadata.not_modified {
                        batch.not_modified_objects = batch.not_modified_objects.saturating_add(1);
                    }
                    batch.items.push(ConditionalObjectMetadataItem {
                        request,
                        metadata: Some(metadata),
                        error_code: None,
                        error_message: None,
                    });
                }
                Err(ObjectError::NotFound(message)) => {
                    batch.missing_objects = batch.missing_objects.saturating_add(1);
                    batch.items.push(ConditionalObjectMetadataItem {
                        request,
                        metadata: None,
                        error_code: Some("NotFound".to_string()),
                        error_message: Some(message),
                    });
                }
                Err(err) => {
                    batch.failed_objects = batch.failed_objects.saturating_add(1);
                    batch.items.push(ConditionalObjectMetadataItem {
                        request,
                        metadata: None,
                        error_code: Some(object_error_code(&err).to_string()),
                        error_message: Some(err.to_string()),
                    });
                }
            }
        }
        let proof = readiness
            .as_ref()
            .map(object_read_proof_from_readiness)
            .unwrap_or_default();
        stamp_conditional_object_metadata_batch_read_proof(
            "HeadObjectsConditionalBatch",
            &mut batch,
            &proof,
        );
        Ok((batch, readiness))
    }

}
