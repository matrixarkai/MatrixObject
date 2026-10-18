impl MatrixObjectStore {
    fn apply_copy(
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
        let rollback = self.capture_object_mutation_rollback();
        let batch_material = format!(
            "copy_object:{source_bucket}/{source_key}->{destination_bucket}/{destination_key}:{update_time_ms}"
        );
        let batch_id = format!("object-copy-{}", etag(batch_material.as_bytes()));
        if let Err(err) = self.page_service.begin_mutation_batch(batch_id.clone()) {
            self.restore_object_mutation_rollback(rollback);
            return Err(err);
        }
        let result = self.apply_copy_storage_batched_inner(
            source_bucket,
            source_key,
            source_version,
            destination_bucket,
            destination_key,
            content_type,
            update_time_ms,
            options,
        );
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

    fn apply_copy_with_root_placement(
        &mut self,
        source_bucket: String,
        source_key: String,
        source_version: Option<u64>,
        destination_bucket: String,
        destination_key: String,
        content_type: Option<String>,
        update_time_ms: u64,
        options: PutOptions,
        replica_count: usize,
    ) -> Result<ObjectMetadata, ObjectError> {
        validate_bucket(&source_bucket)?;
        validate_key(&source_key)?;
        validate_bucket(&destination_bucket)?;
        validate_key(&destination_key)?;
        let source_id = ObjectId::new(source_bucket.clone(), source_key.clone());
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
        let destination_id = ObjectId::new(destination_bucket.clone(), destination_key.clone());
        let plan = self.meta_service.plan_object_placement(
            destination_id.clone(),
            source.length,
            replica_count,
        )?;
        let previous = self
            .objects
            .get(&destination_id)
            .filter(|metadata| !metadata.deleted)
            .cloned();
        // Captured here, not on entry: the four validations above, the source and
        // destination lookups and `plan_object_placement` all only read, so a copy
        // rejected for a bad name or a missing source was copying every map in the
        // store first.
        let rollback = self.capture_object_mutation_rollback();
        let metadata = match self.apply_copy(
            source_bucket,
            source_key,
            source_version,
            destination_bucket,
            destination_key,
            content_type,
            update_time_ms,
            options,
        ) {
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
        let committed = match self
            .meta_service
            .commit_object_placement(placement, &plan.plan_checksum)
        {
            Ok(committed) => committed,
            Err(err) => {
                self.restore_object_mutation_rollback(rollback);
                return Err(err);
            }
        };
        if previous.as_ref().map_or(false, |old| {
            !self.bucket_versioning.contains(old.object_id.bucket.as_str())
        }) {
            self.release_root_placement_if_present(&metadata.object_id, Some(&committed));
        }
        Ok(metadata)
    }

}
