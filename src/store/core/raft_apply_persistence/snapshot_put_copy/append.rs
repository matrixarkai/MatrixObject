// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
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
        // The narrowed undo, on the same terms as the put path: a page-service
        // mutation batch is opened below and aborted before every restore, so
        // the page service owns its own undo, and an append writes one object
        // key, one metadata record and one change record. What it allocates is
        // identified from the chunk records the undo carries rather than by
        // copying the payload index.
        let rollback = self.capture_targeted_mutation_rollback(&bucket, &key);
        let batch_material = format!(
            "append_object:{bucket}/{key}:{update_time_ms}:{}",
            data.len()
        );
        let batch_id = format!("object-append-{}", etag(batch_material.as_bytes()));
        if let Err(err) = self.chunk_service.begin_mutation_batch(batch_id.clone()) {
            self.restore_object_mutation_rollback(rollback);
            return Err(err);
        }
        let result =
            self.apply_append_inner(bucket, key, data, content_type, update_time_ms, options);
        match result {
            Ok(metadata) => match self.chunk_service.end_mutation_batch(&batch_id) {
                Ok(_) => {
                    if self.options.persistent_snapshot_flush_on_commit {
                        self.flush_persistent_snapshot()?;
                        self.flush_object_checkpoint_indexes();
                    }
                    Ok(metadata)
                }
                Err(err) => {
                    let _ = self.chunk_service.abort_mutation_batch(&batch_id);
                    self.restore_object_mutation_rollback(rollback);
                    Err(err)
                }
            },
            Err(err) => {
                let _ = self.chunk_service.abort_mutation_batch(&batch_id);
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
            .object(&object_id)
            .filter(|metadata| !metadata.deleted)
            .cloned();
        if let Some(previous) = &previous {
            if options.if_none_match {
                return Err(ObjectError::PreconditionFailed(
                    "if-none-match rejected existing object".to_string(),
                ));
            }
            if put_options_have_if_match(&options)
                && !put_options_if_match_satisfied(&options, &previous.etag())
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

        // ---- THE DIGEST IS CARRIED FORWARD, NOT RECOMPUTED.
        //
        // This used to read the whole object back through its extents and hash the
        // concatenation, so an append to an N-byte object did N bytes of reads and
        // N bytes of hashing -- O(N) work per append and O(N-squared) to build an
        // append-only log. Measured on a five-node shared-store sweep (one fenced
        // writer, 256-byte entries): this one line was 42.65% of the whole program's
        // instructions, and per-entry append cost doubled every time the corpus did.
        //
        // It is unnecessary. `etag_digest_bits` is an FNV-1a digest, and FNV-1a keeps
        // no state besides the 64 bits it returns, so the previous object's digest IS
        // the hash state after its bytes: resuming it over the appended bytes gives
        // bit-for-bit what hashing the concatenation gives. `fnv1a64_resumed` carries
        // the identity and `appending_resumes_the_previous_digest` asserts it.
        //
        // TWO CASES STILL HAVE TO READ. An expected checksum is a claim about the whole
        // object's bytes, and only the bytes can answer it; and an object carrying no
        // etag has no state to resume from. Both fall through to the original path
        // rather than being refused or approximated.
        // Spelled as a match on both conditions rather than a chain of combinators: the
        // first draft of this used `Option::filter` and sent the
        // expected-checksum-with-no-previous-object case down the fast arm, which would
        // have accepted a mismatched checksum silently. There is no arm here that skips
        // the verification.
        let expected_checksum = options.expected_checksum.as_deref();
        let etag_digest_bits = match (expected_checksum, previous.as_ref()) {
            // Nothing claimed about the whole object, and nothing before these bytes.
            (None, None) => fnv1a64(&data),
            // Nothing claimed, and a digest to carry forward.
            (None, Some(previous)) if previous.has_etag => {
                fnv1a64_resumed(previous.etag_digest_bits, &data)
            }
            // An expected checksum to answer, or an object with no digest to resume
            // from: read the bytes back, exactly as this always did.
            _ => {
                let mut etag_data = previous
                    .as_ref()
                    .map(|metadata| self.read_extents(&metadata.extents, 0, metadata.length))
                    .transpose()?
                    .unwrap_or_default();
                etag_data.extend_from_slice(&data);
                verify_expected_object_checksum(expected_checksum, &etag_data)?;
                fnv1a64(&etag_data)
            }
        };
        let version = self.next_object_version(&object_id);
        let mut metadata = ObjectMetadata {
            object_id: object_id.clone(),
            version,
            length,
            etag_digest_bits,
            has_etag: true,
            content_type: previous
                .as_ref()
                .map_or(content_type.into(), |metadata| metadata.content_type.clone()),
            user_metadata: previous.as_ref().map_or_else(
                || options.user_metadata.clone(),
                |metadata| metadata.user_metadata.clone().into(),
            ).into(),
            // An append keeps the object's class, and only parses the request's when
            // there is no object yet -- written as a match because the parse can fail and
            // `map_or` would have to run it either way.
            storage_class: match previous.as_ref() {
                Some(metadata) => metadata.storage_class,
                None => StorageClass::parse(&storage_class)?,
            },
            tags: previous
                .as_ref()
                .map_or_else(|| options.tags.clone(), |metadata| metadata.tags.clone().into()).into(),
            // An append inherits whatever hold or retention the object was
            // already under, and never puts it under a new one.
            lock: previous
                .as_ref()
                .map_or_else(ObjectLock::default, |metadata| metadata.lock.clone()),
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
            RecordValue::of_object(&metadata),
            options.lease_owner.as_deref(),
            options.lease_epoch,
            update_time_ms,
        )?;
        self.append_object_change("append_object", &metadata);
        Ok(metadata)
    }

    fn apply_append_with_placement(
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
            .object(&object_id)
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
