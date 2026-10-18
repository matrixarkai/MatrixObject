impl MatrixObjectStore {
    fn apply_copy_storage_batched_inner(
        &mut self,
        source_bucket: String,
        source_key: String,
        source_version: Option<u64>,
        destination_bucket: String,
        destination_key: String,
        content_type: Option<String>,
        update_time_ms: u64,
        options: PutOptions,
    ) -> Result<ObjectMetadata, ObjectError> {
        validate_bucket(&source_bucket)?;
        validate_key(&source_key)?;
        validate_bucket(&destination_bucket)?;
        validate_key(&destination_key)?;
        let source_id = ObjectId::new(source_bucket, source_key);
        let source = if let Some(version) = source_version {
            self.head_object_version(&source_id.bucket, &source_id.key, version)?
        } else {
            self.objects
                .get(&source_id)
                .filter(|metadata| !metadata.deleted)
                .cloned()
                .ok_or_else(|| {
                    ObjectError::NotFound(format!("{}/{}", source_id.bucket, source_id.key))
                })?
        };
        let data = self.read_extents(&source.extents, 0, source.length)?;
        let destination_type = content_type.unwrap_or_else(|| source.content_type.to_string());
        let mut copy_options = options;
        if copy_options.user_metadata.is_empty() {
            copy_options.user_metadata = source.user_metadata.clone().into();
        }
        if copy_options.tags.is_empty() {
            copy_options.tags = source.tags.clone().into();
        }
        if copy_options.storage_class.is_none() {
            copy_options.storage_class = Some(source.storage_class.to_string());
        }
        let mut metadata = self.apply_put_with_change(
            destination_bucket,
            destination_key,
            data,
            destination_type,
            update_time_ms,
            copy_options,
            "copy_object",
        )?;
        metadata.lock.legal_hold = source.lock.legal_hold;
        metadata.lock.legal_hold_update_time_ms = source.lock.legal_hold_update_time_ms;
        metadata.lock.retention_until_ms = source.lock.retention_until_ms;
        metadata.lock.restore_until_ms = source.lock.restore_until_ms;
        self.objects
            .insert(metadata.object_id.clone(), metadata.clone());
        self.commit_meta_projection(
            &metadata.object_id,
            &metadata_projection_value(&metadata),
            None,
            None,
            update_time_ms,
        )?;
        self.replace_last_object_change_metadata("copy_object", &metadata);
        Ok(metadata)
    }
}
