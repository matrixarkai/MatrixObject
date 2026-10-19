// SPDX-License-Identifier: Apache-2.0
impl MatrixObjectStore {
    fn apply_complete_multipart_upload(
        &mut self,
        upload_id: String,
        expected_size: Option<u64>,
        expected_etag: Option<String>,
        expected_checksum: Option<String>,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        validate_upload_id(&upload_id)?;
        let upload = self
            .multipart_uploads
            .get(&upload_id)
            .cloned()
            .ok_or_else(|| ObjectError::NotFound("multipart upload missing".to_string()))?;
        if upload.parts.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "multipart upload has no parts".to_string(),
            ));
        }
        let mut data = Vec::with_capacity(upload.total_bytes as usize);
        let mut expected_part = 1u32;
        for (part_number, part) in &upload.parts {
            if *part_number != expected_part {
                return Err(ObjectError::PreconditionFailed(
                    "multipart upload parts must be contiguous from 1".to_string(),
                ));
            }
            let payload = upload.part_payloads.get(part_number).ok_or_else(|| {
                ObjectError::Corruption("multipart part payload missing".to_string())
            })?;
            if payload.len() as u64 != part.length || !etag_matches(payload, &part.etag) {
                return Err(ObjectError::Corruption(
                    "multipart part payload checksum mismatch".to_string(),
                ));
            }
            data.extend_from_slice(payload);
            expected_part = expected_part.saturating_add(1);
        }
        if expected_size.map_or(false, |expected| expected != data.len() as u64) {
            return Err(ObjectError::PreconditionFailed(
                "multipart expected size mismatch".to_string(),
            ));
        }
        let assembled_etag = etag(&data);
        if expected_etag
            .as_deref()
            .map_or(false, |expected| expected != assembled_etag)
        {
            return Err(ObjectError::PreconditionFailed(
                "multipart expected checksum mismatch".to_string(),
            ));
        }
        verify_expected_object_checksum(expected_checksum.as_deref(), &data)?;
        let metadata = self.apply_put_storage_batched(
            upload.bucket.clone(),
            upload.key.clone(),
            data,
            upload.content_type.clone(),
            update_time_ms,
            upload.options.clone(),
            "complete_multipart_put",
        )?;
        self.multipart_uploads.remove(&upload_id);
        Ok(metadata)
    }

    fn apply_complete_multipart_upload_idempotent(
        &mut self,
        upload_id: String,
        expected_size: Option<u64>,
        expected_etag: Option<String>,
        expected_checksum: Option<String>,
        update_time_ms: u64,
        idempotency_key: String,
        commit_index: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        validate_idempotency_key(&idempotency_key)?;
        let request_checksum = multipart_complete_request_checksum(
            &upload_id,
            expected_size,
            expected_etag.as_deref(),
            expected_checksum.as_deref(),
        )?;
        if let Some(existing) = self.multipart_complete_receipts.get(&idempotency_key) {
            if existing.upload_id == upload_id && existing.request_checksum == request_checksum {
                return Ok(existing.metadata.clone());
            }
            return Err(ObjectError::PreconditionFailed(
                "multipart complete idempotency key reused with different request".to_string(),
            ));
        }
        let metadata = self.apply_complete_multipart_upload(
            upload_id.clone(),
            expected_size,
            expected_etag,
            expected_checksum,
            update_time_ms,
        )?;
        self.multipart_complete_receipts.insert(
            idempotency_key.clone(),
            MultipartCompleteReceipt {
                idempotency_key,
                upload_id,
                request_checksum,
                metadata: metadata.clone(),
                commit_index,
                update_time_ms,
            },
        );
        Ok(metadata)
    }

    fn apply_complete_multipart_upload_with_root_placement(
        &mut self,
        upload_id: String,
        expected_size: Option<u64>,
        expected_etag: Option<String>,
        expected_checksum: Option<String>,
        replica_count: usize,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        validate_upload_id(&upload_id)?;
        let rollback = self.capture_object_mutation_rollback();
        let upload = self
            .multipart_uploads
            .get(&upload_id)
            .cloned()
            .ok_or_else(|| ObjectError::NotFound("multipart upload missing".to_string()))?;
        validate_bucket(&upload.bucket)?;
        validate_key(&upload.key)?;
        let object_id = ObjectId::new(upload.bucket.clone(), upload.key.clone());
        let plan = self.meta_service.plan_object_placement(
            object_id.clone(),
            upload.total_bytes,
            replica_count,
        )?;
        let previous = self
            .objects
            .get(&object_id)
            .filter(|metadata| !metadata.deleted)
            .cloned();
        let metadata = match self.apply_complete_multipart_upload(
            upload_id,
            expected_size,
            expected_etag,
            expected_checksum,
            update_time_ms,
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
