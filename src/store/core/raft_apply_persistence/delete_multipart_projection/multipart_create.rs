// SPDX-License-Identifier: Apache-2.0
impl MatrixObjectStore {
    fn apply_create_multipart_upload(
        &mut self,
        upload_id: String,
        bucket: String,
        key: String,
        content_type: String,
        options: PutOptions,
        create_time_ms: u64,
    ) -> Result<MultipartUpload, ObjectError> {
        self.apply_create_multipart_upload_with_idempotency(
            upload_id,
            bucket,
            key,
            content_type,
            options,
            create_time_ms,
            None,
        )
    }

    fn apply_create_multipart_upload_idempotent(
        &mut self,
        upload_id: String,
        bucket: String,
        key: String,
        content_type: String,
        options: PutOptions,
        create_time_ms: u64,
        idempotency_key: String,
    ) -> Result<MultipartUpload, ObjectError> {
        validate_idempotency_key(&idempotency_key)?;
        self.apply_create_multipart_upload_with_idempotency(
            upload_id,
            bucket,
            key,
            content_type,
            options,
            create_time_ms,
            Some(idempotency_key),
        )
    }

    fn apply_create_multipart_upload_with_idempotency(
        &mut self,
        upload_id: String,
        bucket: String,
        key: String,
        content_type: String,
        options: PutOptions,
        create_time_ms: u64,
        create_idempotency_key: Option<String>,
    ) -> Result<MultipartUpload, ObjectError> {
        validate_upload_id(&upload_id)?;
        validate_bucket(&bucket)?;
        validate_key(&key)?;
        self.buckets.insert(bucket.clone().into());
        let object_id = ObjectId::new(bucket.clone(), key.clone());
        let meta_key = meta_object_key(&object_id);
        self.meta_service.check_shard_fence(&meta_key)?;
        self.meta_service.check_lease_fence(
            &meta_key,
            options.lease_owner.as_deref(),
            options.lease_epoch,
            create_time_ms,
        )?;
        if let Some(existing) = self.multipart_uploads.get(&upload_id) {
            if existing.bucket == bucket
                && existing.key == key
                && existing.content_type == content_type
                && existing.options == options
                && existing.create_idempotency_key == create_idempotency_key
            {
                return Ok(existing.clone());
            }
            return Err(ObjectError::PreconditionFailed(
                "multipart upload id already exists".to_string(),
            ));
        }
        let upload = MultipartUpload {
            upload_id: upload_id.clone(),
            bucket,
            key,
            content_type,
            create_idempotency_key,
            options,
            create_time_ms,
            update_time_ms: create_time_ms,
            next_part_number: 1,
            total_bytes: 0,
            parts: BTreeMap::new(),
            part_payloads: BTreeMap::new(),
        };
        self.multipart_uploads.insert(upload_id, upload.clone());
        self.advance_upload_sequence_from_id(&upload.upload_id);
        Ok(upload)
    }

}
