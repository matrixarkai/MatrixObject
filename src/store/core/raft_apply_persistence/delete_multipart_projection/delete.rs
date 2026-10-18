impl MatrixObjectStore {
    fn apply_delete(
        &mut self,
        bucket: String,
        key: String,
        update_time_ms: u64,
        if_match_etag: Option<String>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let batch_material = format!("delete_object:{bucket}/{key}:{update_time_ms}");
        let batch_id = format!("object-delete-{}", etag(batch_material.as_bytes()));
        let rollback = self.capture_targeted_mutation_rollback(&bucket, &key);
        if let Err(err) = self.page_service.begin_mutation_batch(batch_id.clone()) {
            self.restore_object_mutation_rollback(rollback);
            return Err(err);
        }
        let result = self.apply_delete_inner(bucket, key, update_time_ms, if_match_etag);
        match result {
            Ok(metadata) => match self.page_service.end_mutation_batch(&batch_id) {
                Ok(_) => Ok(metadata),
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

    fn apply_delete_inner(
        &mut self,
        bucket: String,
        key: String,
        update_time_ms: u64,
        if_match_etag: Option<String>,
    ) -> Result<ObjectMetadata, ObjectError> {
        validate_bucket(&bucket)?;
        validate_key(&key)?;
        let object_id = ObjectId::new(bucket, key);
        let meta_key = meta_object_key(&object_id);
        self.meta_service.check_shard_fence(&meta_key)?;
        self.meta_service
            .check_lease_fence(&meta_key, None, None, update_time_ms)?;
        let previous = self.objects.get(&object_id).cloned().ok_or_else(|| {
            ObjectError::NotFound(format!("{}/{}", object_id.bucket, object_id.key))
        })?;
        if previous.deleted {
            return Err(ObjectError::NotFound(format!(
                "{}/{}",
                object_id.bucket, object_id.key
            )));
        }
        if let Some(expected) = &if_match_etag {
            if &previous.etag != expected {
                return Err(ObjectError::PreconditionFailed(
                    "delete if-match etag does not match current object".to_string(),
                ));
            }
        }
        self.check_retention_for_mutation(&previous, "delete", update_time_ms)?;
        if self.bucket_versioning.contains(object_id.bucket.as_str()) {
            self.versions
                .entry(object_id.clone())
                .or_default()
                .push(previous);
        } else {
            self.mark_extents_stale(&previous.extents)?;
        }
        let mut delete_marker = ObjectMetadata {
            object_id: object_id.clone(),
            version: self.next_object_version(&object_id),
            length: 0,
            etag: String::new(),
            content_type: String::new(),
            user_metadata: BTreeMap::new(),
            storage_class: k_standard_storage_class().to_string(),
            restore_until_ms: 0,
            tags: BTreeMap::new(),
            legal_hold: false,
            legal_hold_update_time_ms: 0,
            retention_until_ms: 0,
            update_time_ms,
            extents: Vec::new(),
            deleted: true,
        };
        self.file_object(object_id, &mut delete_marker);
        self.commit_meta_projection(
            &delete_marker.object_id,
            &metadata_projection_value(&delete_marker),
            None,
            None,
            update_time_ms,
        )?;
        self.append_object_change("delete_object", &delete_marker);
        Ok(delete_marker)
    }

    fn apply_delete_with_root_release(
        &mut self,
        bucket: String,
        key: String,
        update_time_ms: u64,
        if_match_etag: Option<String>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let batch_material =
            format!("delete_object_with_root_release:{bucket}/{key}:{update_time_ms}");
        let batch_id = format!("object-delete-root-{}", etag(batch_material.as_bytes()));
        let rollback = self.capture_object_mutation_rollback();
        if let Err(err) = self.page_service.begin_mutation_batch(batch_id.clone()) {
            self.restore_object_mutation_rollback(rollback);
            return Err(err);
        }
        let result =
            self.apply_delete_with_root_release_inner(bucket, key, update_time_ms, if_match_etag);
        match result {
            Ok(metadata) => match self.page_service.end_mutation_batch(&batch_id) {
                Ok(_) => Ok(metadata),
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

    fn apply_delete_with_root_release_inner(
        &mut self,
        bucket: String,
        key: String,
        update_time_ms: u64,
        if_match_etag: Option<String>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let previous = self.head_object(&bucket, &key)?;
        let metadata = self.apply_delete_inner(bucket, key, update_time_ms, if_match_etag)?;
        if !self.bucket_versioning.contains(previous.object_id.bucket.as_str()) {
            self.release_root_placement_if_present(&previous.object_id, None);
        }
        Ok(metadata)
    }

    fn apply_delete_object_version(
        &mut self,
        bucket: String,
        key: String,
        version: u64,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        let batch_material =
            format!("delete_object_version:{bucket}/{key}:{version}:{update_time_ms}");
        let batch_id = format!("object-delete-version-{}", etag(batch_material.as_bytes()));
        let rollback = self.capture_object_mutation_rollback();
        if let Err(err) = self.page_service.begin_mutation_batch(batch_id.clone()) {
            self.restore_object_mutation_rollback(rollback);
            return Err(err);
        }
        let result = self.apply_delete_object_version_inner(bucket, key, version, update_time_ms);
        match result {
            Ok(metadata) => match self.page_service.end_mutation_batch(&batch_id) {
                Ok(_) => Ok(metadata),
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

    fn apply_delete_object_version_inner(
        &mut self,
        bucket: String,
        key: String,
        version: u64,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        validate_bucket(&bucket)?;
        validate_key(&key)?;
        if version == 0 {
            return Err(ObjectError::InvalidArgument(
                "version must be positive".to_string(),
            ));
        }
        let object_id = ObjectId::new(bucket, key);
        self.meta_service
            .check_shard_fence(&meta_object_key(&object_id))?;
        self.meta_service.check_lease_fence(
            &meta_object_key(&object_id),
            None,
            None,
            update_time_ms,
        )?;
        if self
            .objects
            .get(&object_id)
            .map_or(false, |metadata| metadata.version == version)
        {
            return self.apply_delete_inner(object_id.bucket.to_string(), object_id.key.to_string(), update_time_ms, None);
        }
        let index = self
            .versions
            .get(&object_id)
            .ok_or_else(|| {
                ObjectError::NotFound(format!(
                    "{}/{}#{}",
                    object_id.bucket, object_id.key, version
                ))
            })?
            .iter()
            .position(|metadata| metadata.version == version)
            .ok_or_else(|| {
                ObjectError::NotFound(format!(
                    "{}/{}#{}",
                    object_id.bucket, object_id.key, version
                ))
            })?;
        let candidate = self
            .versions
            .get(&object_id)
            .and_then(|versions| versions.get(index))
            .cloned()
            .ok_or_else(|| {
                ObjectError::NotFound(format!(
                    "{}/{}#{}",
                    object_id.bucket, object_id.key, version
                ))
            })?;
        self.check_retention_for_mutation(&candidate, "delete-version", update_time_ms)?;
        let versions = self.versions.get_mut(&object_id).ok_or_else(|| {
            ObjectError::NotFound(format!(
                "{}/{}#{}",
                object_id.bucket, object_id.key, version
            ))
        })?;
        let removed = versions.remove(index);
        if versions.is_empty() {
            self.versions.remove(&object_id);
        }
        self.mark_extents_stale(&removed.extents)?;
        self.append_object_change("delete_object_version", &removed);
        Ok(removed)
    }

}
