impl MatrixObjectStore {
    fn apply_append(
        &mut self,
        bucket: String,
        key: String,
        data: Vec<u8>,
        content_type: String,
        update_time_ms: u64,
        options: PutOptions,
    ) -> Result<ObjectMetadata, ObjectError> {
        let rollback = self.capture_object_mutation_rollback();
        let batch_material = format!(
            "append_object:{bucket}/{key}:{update_time_ms}:{}",
            data.len()
        );
        let batch_id = format!("object-append-{}", etag(batch_material.as_bytes()));
        if let Err(err) = self.page_service.begin_mutation_batch(batch_id.clone()) {
            self.restore_object_mutation_rollback(rollback);
            return Err(err);
        }
        let result =
            self.apply_append_inner(bucket, key, data, content_type, update_time_ms, options);
        match result {
            Ok(metadata) => match self.page_service.end_mutation_batch(&batch_id) {
                Ok(_) => {
                    if self.options.persistent_snapshot_flush_on_commit {
                        self.flush_persistent_snapshot()?;
                        self.flush_object_checkpoint_indexes();
                    }
                    Ok(metadata)
                }
                Err(err) => {
                    let _ = self.page_service.abort_mutation_batch(&batch_id);
                    self.restore_object_mutation_rollback(rollback);
                    Err(err)
                }
            },
            Err(err) => {
                let _ = self.page_service.abort_mutation_batch(&batch_id);
                self.restore_object_mutation_rollback(rollback);
                Err(err)
            }
        }
    }

    fn apply_append_inner(
        &mut self,
        bucket: String,
        key: String,
        data: Vec<u8>,
        content_type: String,
        update_time_ms: u64,
        options: PutOptions,
    ) -> Result<ObjectMetadata, ObjectError> {
        validate_bucket(&bucket)?;
        validate_key(&key)?;
        validate_tags(&options.tags)?;
        let storage_class = options
            .storage_class
            .clone()
            .unwrap_or_else(|| k_standard_storage_class().to_string());
        validate_storage_class(&storage_class)?;
        self.buckets.insert(bucket.clone().into());
        let object_id = ObjectId::new(bucket, key);
        let meta_key = meta_object_key(&object_id);
        self.meta_service.check_shard_fence(&meta_key)?;
        self.meta_service.check_lease_fence(
            &meta_key,
            options.lease_owner.as_deref(),
            options.lease_epoch,
            update_time_ms,
        )?;

        let previous = self
            .objects
            .get(&object_id)
            .filter(|metadata| !metadata.deleted)
            .cloned();
        if let Some(previous) = &previous {
            if options.if_none_match {
                return Err(ObjectError::PreconditionFailed(
                    "if-none-match rejected existing object".to_string(),
                ));
            }
            if put_options_have_if_match(&options)
                && !put_options_if_match_satisfied(&options, &previous.etag)
            {
                return Err(ObjectError::PreconditionFailed(
                    "if-match etag does not match current object".to_string(),
                ));
            }
            self.check_retention_for_mutation(previous, "append", update_time_ms)?;
        } else if put_options_have_if_match(&options) {
            return Err(ObjectError::PreconditionFailed(
                "if-match requires an existing object".to_string(),
            ));
        }

        let previous_length = previous.as_ref().map_or(0, |metadata| metadata.length);
        let length = previous_length
            .checked_add(data.len() as u64)
            .ok_or_else(|| {
                ObjectError::InvalidArgument("object append length overflow".to_string())
            })?;
        self.check_bucket_quota_for_put(&object_id, length)?;
        let appended_extents = self.allocate_extents(&data)?;
        let mut extents = previous
            .as_ref()
            .map_or_else(Vec::new, |metadata| metadata.extents.to_vec());
        extents.extend(appended_extents);

        let mut etag_data = previous
            .as_ref()
            .map(|metadata| self.read_extents(&metadata.extents, 0, metadata.length))
            .transpose()?
            .unwrap_or_default();
        etag_data.extend_from_slice(&data);
        verify_expected_object_checksum(options.expected_checksum.as_deref(), &etag_data)?;
        let version = self.next_object_version(&object_id);
        let mut metadata = ObjectMetadata {
            object_id: object_id.clone(),
            version,
            length,
            etag: etag(&etag_data),
            content_type: previous
                .as_ref()
                .map_or(content_type, |metadata| metadata.content_type.clone()),
            user_metadata: previous.as_ref().map_or_else(
                || options.user_metadata.clone(),
                |metadata| metadata.user_metadata.clone().into(),
            ).into(),
            storage_class: previous
                .as_ref()
                .map_or(storage_class.into(), |metadata| metadata.storage_class.clone()),
            restore_until_ms: 0,
            tags: previous
                .as_ref()
                .map_or_else(|| options.tags.clone(), |metadata| metadata.tags.clone().into()).into(),
            legal_hold: previous
                .as_ref()
                .map_or(false, |metadata| metadata.legal_hold),
            legal_hold_update_time_ms: previous
                .as_ref()
                .map_or(0, |metadata| metadata.legal_hold_update_time_ms),
            retention_until_ms: previous
                .as_ref()
                .map_or(0, |metadata| metadata.retention_until_ms),
            update_time_ms,
            extents: extents.into(),
            deleted: false,
        };
        if self.bucket_versioning.contains(object_id.bucket.as_str()) {
            if let Some(previous) = previous {
                self.versions
                    .entry(object_id.clone())
                    .or_default()
                    .push(previous);
            }
        }
        self.logical_written_bytes = saturating_add(self.logical_written_bytes, data.len() as u64);
        self.file_object(object_id, &mut metadata);
        self.commit_meta_projection(
            &metadata.object_id,
            &metadata_projection_value(&metadata),
            options.lease_owner.as_deref(),
            options.lease_epoch,
            update_time_ms,
        )?;
        self.append_object_change("append_object", &metadata);
        Ok(metadata)
    }

    fn apply_append_with_root_placement(
        &mut self,
        bucket: String,
        key: String,
        data: Vec<u8>,
        content_type: String,
        update_time_ms: u64,
        options: PutOptions,
        replica_count: usize,
    ) -> Result<ObjectMetadata, ObjectError> {
        let rollback = self.capture_object_mutation_rollback();
        validate_bucket(&bucket)?;
        validate_key(&key)?;
        let object_id = ObjectId::new(bucket.clone(), key.clone());
        let previous_length = self
            .objects
            .get(&object_id)
            .filter(|metadata| !metadata.deleted)
            .map_or(0, |metadata| metadata.length);
        let planned_length = previous_length
            .checked_add(data.len() as u64)
            .ok_or_else(|| {
                ObjectError::InvalidArgument("object append length overflow".to_string())
            })?;
        let plan = self.meta_service.plan_object_placement(
            object_id.clone(),
            planned_length,
            replica_count,
        )?;
        let metadata =
            match self.apply_append(bucket, key, data, content_type, update_time_ms, options) {
                Ok(metadata) => metadata,
                Err(err) => {
                    self.restore_object_mutation_rollback(rollback);
                    return Err(err);
                }
            };
        let placement = ObjectPlacement {
            object_id: metadata.object_id.clone(),
            version: metadata.version,
            length: metadata.length,
            node_ids: plan.node_ids,
            update_time_ms,
        };
        match self
            .meta_service
            .commit_object_placement(placement, &plan.plan_checksum)
        {
            Ok(_) => Ok(metadata),
            Err(err) => {
                self.restore_object_mutation_rollback(rollback);
                Err(err)
            }
        }
    }

}
