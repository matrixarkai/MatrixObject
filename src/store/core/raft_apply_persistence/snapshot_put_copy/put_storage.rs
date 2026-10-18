impl MatrixObjectStore {
    /// The allocation the object map already names this object through.
    ///
    /// A mutation arrives with its own `ObjectId`, and `BTreeMap::insert`
    /// keeps the key it already holds. Without adopting, an overwrite leaves
    /// the map key and everything the mutation builds in two allocations of
    /// the same characters.
    pub(crate) fn filed_object_key(&self, object_id: ObjectId) -> ObjectId {
        if let Some((existing, _)) = self.objects.get_key_value(&object_id) {
            return existing.clone();
        }
        // A new object: its key is new, but its bucket is not. Every object
        // named its bucket through an allocation of its own, so one bucket name
        // was resident once per object -- characters and header each time.
        let mut object_id = object_id;
        if let Some(bucket) = self.buckets.get(object_id.bucket.as_str()) {
            object_id.bucket = bucket.clone();
        }
        object_id
    }

    /// File an object, making the stored value name it through the key the map
    /// keeps rather than through the mutation's own copy.
    pub(crate) fn file_object(&mut self, object_id: ObjectId, metadata: &mut ObjectMetadata) {
        let object_id = self.filed_object_key(object_id);
        // Equal characters by construction; the assignment only collapses the
        // two into one allocation.
        if metadata.object_id == object_id {
            metadata.object_id = object_id.clone();
        }
        // Whichever path built this metadata, its storage class is one of four
        // names, and this is where every mutation's object becomes resident.
        metadata.storage_class = interned_storage_class(&metadata.storage_class);
        self.objects.insert(object_id, metadata.clone());
    }

    fn apply_put_storage_batched(
        &mut self,
        bucket: String,
        key: String,
        data: Vec<u8>,
        content_type: String,
        update_time_ms: u64,
        options: PutOptions,
        operation: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        let rollback = self.capture_targeted_mutation_rollback(&bucket, &key);
        let batch_material = format!("{operation}:{bucket}/{key}:{update_time_ms}:{}", data.len());
        let batch_id = format!("object-put-{}", etag(batch_material.as_bytes()));
        if let Err(err) = self.page_service.begin_mutation_batch(batch_id.clone()) {
            self.restore_object_mutation_rollback(rollback);
            return Err(err);
        }
        match self.apply_put_with_change(
            bucket,
            key,
            data,
            content_type,
            update_time_ms,
            options,
            operation,
        ) {
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

    fn apply_put_with_change(
        &mut self,
        bucket: String,
        key: String,
        data: Vec<u8>,
        content_type: String,
        update_time_ms: u64,
        options: PutOptions,
        operation: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        validate_bucket(&bucket)?;
        validate_key(&key)?;
        validate_user_metadata(&options.user_metadata)?;
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
        if let Some(previous) = self
            .objects
            .get(&object_id)
            .filter(|metadata| !metadata.deleted)
            .cloned()
        {
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
            self.check_retention_for_mutation(&previous, "overwrite", update_time_ms)?;
            if self.bucket_versioning.contains(object_id.bucket.as_str()) {
                let versions_key = self.filed_object_key(object_id.clone());
                self.versions.entry(versions_key).or_default().push(previous);
            } else {
                self.mark_extents_stale(&previous.extents)?;
            }
        } else if put_options_have_if_match(&options) {
            return Err(ObjectError::PreconditionFailed(
                "if-match requires an existing object".to_string(),
            ));
        }
        self.check_bucket_quota_for_put(&object_id, data.len() as u64)?;
        verify_expected_object_checksum(options.expected_checksum.as_deref(), &data)?;
        let length = data.len() as u64;
        let object_etag = etag(&data);
        let extents = self.allocate_extents_owned(data)?;
        let version = self.next_object_version(&object_id);
        let mut metadata = ObjectMetadata {
            object_id: object_id.clone(),
            version,
            length,
            etag: object_etag.into(),
            content_type: content_type.into(),
            user_metadata: options.user_metadata.clone().into(),
            storage_class: storage_class.into(),
            restore_until_ms: 0,
            tags: options.tags.clone().into(),
            legal_hold: false,
            legal_hold_update_time_ms: 0,
            retention_until_ms: 0,
            update_time_ms,
            extents: extents.into(),
            deleted: false,
        };
        self.logical_written_bytes = saturating_add(self.logical_written_bytes, length);
        self.file_object(object_id, &mut metadata);
        self.commit_meta_projection(
            &metadata.object_id,
            &metadata_projection_value(&metadata),
            options.lease_owner.as_deref(),
            options.lease_epoch,
            update_time_ms,
        )?;
        self.append_object_change(operation, &metadata);
        Ok(metadata)
    }

}
