// SPDX-License-Identifier: Apache-2.0
impl MatrixObjectStore {
    fn apply_restore_object(
        &mut self,
        bucket: String,
        key: String,
        restore_until_ms: u64,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        validate_bucket(&bucket)?;
        validate_key(&key)?;
        if restore_until_ms < update_time_ms {
            return Err(ObjectError::InvalidArgument(
                "restore window expires before update time".to_string(),
            ));
        }
        let object_id = ObjectId::new(bucket, key);
        let meta_key = meta_object_key(&object_id);
        self.meta_service.check_shard_fence(&meta_key)?;
        self.meta_service
            .check_lease_fence(&meta_key, None, None, update_time_ms)?;
        let mut metadata = self
            .objects
            .get(&object_id)
            .filter(|metadata| !metadata.deleted)
            .cloned()
            .ok_or_else(|| {
                ObjectError::NotFound(format!("{}/{}", object_id.bucket, object_id.key))
            })?;
        if !is_cold_storage_class(&metadata.storage_class) {
            return Err(ObjectError::PreconditionFailed(
                "restore applies only to cold storage classes".to_string(),
            ));
        }
        metadata.lock.restore_until_ms = restore_until_ms;
        metadata.update_time_ms = update_time_ms;
        self.file_object(object_id, &mut metadata);
        self.commit_meta_projection(
            &metadata.object_id,
            RecordValue::of_object(&metadata),
            None,
            None,
            update_time_ms,
        )?;
        self.append_object_change("restore_object", &metadata);
        Ok(metadata)
    }

    fn apply_restore_object_version(
        &mut self,
        bucket: String,
        key: String,
        version: u64,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        let (object_id, source) =
            self.restore_object_version_source(bucket, key, version, update_time_ms)?;
        let data = self.read_extents(&source.extents, 0, source.length)?;
        let mut options = PutOptions::default();
        options.user_metadata = source.user_metadata.clone().into();
        options.tags = source.tags.clone().into();
        options.storage_class = Some(source.storage_class.to_string());
        self.apply_put_storage_batched(
            object_id.bucket.to_string(),
            object_id.key.to_string(),
            data,
            source.content_type.to_string(),
            update_time_ms,
            options,
            "restore_object_version",
        )
    }

    fn apply_restore_object_version_with_root_placement(
        &mut self,
        bucket: String,
        key: String,
        version: u64,
        update_time_ms: u64,
        replica_count: usize,
    ) -> Result<ObjectMetadata, ObjectError> {
        let (object_id, source) =
            self.restore_object_version_source(bucket, key, version, update_time_ms)?;
        let data = self.read_extents(&source.extents, 0, source.length)?;
        let mut options = PutOptions::default();
        options.user_metadata = source.user_metadata.clone().into();
        options.tags = source.tags.clone().into();
        options.storage_class = Some(source.storage_class.to_string());
        self.apply_put_with_root_placement(
            object_id.bucket.to_string(),
            object_id.key.to_string(),
            data,
            source.content_type.to_string(),
            update_time_ms,
            options,
            replica_count,
        )
    }

}
