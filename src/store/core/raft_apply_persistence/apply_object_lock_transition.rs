impl MatrixObjectStore {
    fn apply_set_object_legal_hold(
        &mut self,
        bucket: String,
        key: String,
        enabled: bool,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        validate_bucket(&bucket)?;
        validate_key(&key)?;
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
        metadata.legal_hold = enabled;
        metadata.legal_hold_update_time_ms = update_time_ms;
        metadata.update_time_ms = update_time_ms;
        self.file_object(object_id, &mut metadata);
        self.commit_meta_projection(
            &metadata.object_id,
            &metadata_projection_value(&metadata),
            None,
            None,
            update_time_ms,
        )?;
        self.append_object_change("set_object_legal_hold", &metadata);
        Ok(metadata)
    }

    fn apply_set_object_retention(
        &mut self,
        bucket: String,
        key: String,
        retention_until_ms: u64,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        validate_bucket(&bucket)?;
        validate_key(&key)?;
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
        if metadata.retention_until_ms > update_time_ms
            && retention_until_ms < metadata.retention_until_ms
        {
            return Err(ObjectError::PreconditionFailed(
                "cannot shorten active object retention".to_string(),
            ));
        }
        metadata.retention_until_ms = retention_until_ms;
        metadata.update_time_ms = update_time_ms;
        self.file_object(object_id, &mut metadata);
        self.commit_meta_projection(
            &metadata.object_id,
            &metadata_projection_value(&metadata),
            None,
            None,
            update_time_ms,
        )?;
        self.append_object_change("set_object_retention", &metadata);
        Ok(metadata)
    }

    fn apply_transition_object(
        &mut self,
        bucket: String,
        key: String,
        storage_class: String,
        update_time_ms: u64,
        if_match_etag: Option<String>,
    ) -> Result<ObjectMetadata, ObjectError> {
        validate_bucket(&bucket)?;
        validate_key(&key)?;
        validate_storage_class(&storage_class)?;
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
        if if_match_etag
            .as_ref()
            .is_some_and(|expected| expected != &metadata.etag)
        {
            return Err(ObjectError::PreconditionFailed(
                "transition if-match etag does not match current object".to_string(),
            ));
        }
        metadata.storage_class = storage_class.into();
        if !is_cold_storage_class(&metadata.storage_class) {
            metadata.restore_until_ms = 0;
        }
        metadata.update_time_ms = update_time_ms;
        self.file_object(object_id, &mut metadata);
        self.commit_meta_projection(
            &metadata.object_id,
            &metadata_projection_value(&metadata),
            None,
            None,
            update_time_ms,
        )?;
        self.append_object_change("transition_object", &metadata);
        Ok(metadata)
    }

    fn apply_transition_object_with_root_placement(
        &mut self,
        bucket: String,
        key: String,
        storage_class: String,
        update_time_ms: u64,
        replica_count: usize,
        if_match_etag: Option<String>,
    ) -> Result<ObjectMetadata, ObjectError> {
        validate_bucket(&bucket)?;
        validate_key(&key)?;
        validate_storage_class(&storage_class)?;
        let object_id = ObjectId::new(bucket, key);
        let meta_key = meta_object_key(&object_id);
        self.meta_service.check_shard_fence(&meta_key)?;
        self.meta_service
            .check_lease_fence(&meta_key, None, None, update_time_ms)?;
        let current = self
            .objects
            .get(&object_id)
            .filter(|metadata| !metadata.deleted)
            .cloned()
            .ok_or_else(|| {
                ObjectError::NotFound(format!("{}/{}", object_id.bucket, object_id.key))
            })?;
        if if_match_etag
            .as_ref()
            .is_some_and(|expected| expected != &current.etag)
        {
            return Err(ObjectError::PreconditionFailed(
                "transition if-match etag does not match current object".to_string(),
            ));
        }
        if current.storage_class == storage_class {
            return Ok(current);
        }
        let plan = self.meta_service.plan_object_placement(
            object_id.clone(),
            current.length,
            replica_count,
        )?;
        let batch_material = format!(
            "transition_object_with_root_placement:{}/{}:{update_time_ms}:{}",
            object_id.bucket, object_id.key, current.length
        );
        let batch_id = format!("object-transition-{}", etag(batch_material.as_bytes()));
        // Captured here rather than on entry: everything above this line only reads --
        // the `validate_*` calls, both fence checks and `plan_object_placement` all take
        // `&self` -- and one of those paths is a SUCCESSFUL no-op return when the storage
        // class already matches. Capturing on entry made a rejected or no-op transition
        // copy every map in the store and throw the copy away.
        let rollback = self.capture_object_mutation_rollback();
        if let Err(err) = self.page_service.begin_mutation_batch(batch_id.clone()) {
            self.restore_object_mutation_rollback(rollback);
            return Err(err);
        }

        let data = match self.read_extents(&current.extents, 0, current.length) {
            Ok(data) => data,
            Err(err) => {
                let _ = self.page_service.abort_mutation_batch(&batch_id);
                self.restore_object_mutation_rollback(rollback);
                return Err(err);
            }
        };
        let new_extents = match self.allocate_extents(&data) {
            Ok(extents) => extents,
            Err(err) => {
                let _ = self.page_service.abort_mutation_batch(&batch_id);
                self.restore_object_mutation_rollback(rollback);
                return Err(err);
            }
        };
        if let Err(err) = self.mark_extents_stale(&current.extents) {
            let _ = self.page_service.abort_mutation_batch(&batch_id);
            self.restore_object_mutation_rollback(rollback);
            return Err(err);
        }

        let mut metadata = current.clone();
        metadata.storage_class = storage_class.into();
        metadata.restore_until_ms = 0;
        metadata.update_time_ms = update_time_ms;
        metadata.extents = new_extents.into();
        self.logical_written_bytes = saturating_add(self.logical_written_bytes, metadata.length);
        self.file_object(object_id, &mut metadata);
        if let Err(err) = self.commit_meta_projection(
            &metadata.object_id,
            &metadata_projection_value(&metadata),
            None,
            None,
            update_time_ms,
        ) {
            let _ = self.page_service.abort_mutation_batch(&batch_id);
            self.restore_object_mutation_rollback(rollback);
            return Err(err);
        }
        self.append_object_change("transition_object_with_root_placement", &metadata);
        if let Err(err) = self.page_service.end_mutation_batch(&batch_id) {
            let _ = self.page_service.abort_mutation_batch(&batch_id);
            self.restore_object_mutation_rollback(rollback);
            return Err(err);
        }

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
        self.release_root_placement_if_present(&metadata.object_id, Some(&committed));
        Ok(metadata)
    }

}
