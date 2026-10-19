// SPDX-License-Identifier: Apache-2.0
impl MatrixObjectStore {
    fn apply_set_object_tags(
        &mut self,
        bucket: String,
        key: String,
        tags: BTreeMap<String, String>,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        validate_bucket(&bucket)?;
        validate_key(&key)?;
        validate_tags(&tags)?;
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
        *metadata.tags = tags.into();
        metadata.update_time_ms = update_time_ms;
        self.file_object(object_id, &mut metadata);
        self.commit_meta_projection(
            &metadata.object_id,
            &metadata_projection_value(&metadata),
            None,
            None,
            update_time_ms,
        )?;
        self.append_object_change("set_object_tags", &metadata);
        Ok(metadata)
    }

    fn apply_configure_bucket_retention(
        &mut self,
        bucket: String,
        retain_for_ms: u64,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        validate_bucket(&bucket)?;
        self.buckets.insert(bucket.clone().into());
        if retain_for_ms == 0 {
            self.bucket_retention.remove(&bucket);
        } else {
            self.bucket_retention.insert(bucket.clone(), retain_for_ms);
        }
        let object_id = ObjectId::new(bucket.clone(), "");
        self.commit_meta_projection(
            &object_id,
            &format!("bucket_retention_ms={retain_for_ms}"),
            None,
            None,
            update_time_ms,
        )?;
        Ok(ObjectMetadata {
            object_id,
            version: 0,
            length: retain_for_ms,
            etag: String::new().into(),
            content_type: "bucket/retention".to_string().into(),
            user_metadata: BTreeMap::new().into(),
            storage_class: k_standard_storage_class().to_string().into(),
            lock: ObjectLock::default(),
            tags: BTreeMap::new().into(),
            update_time_ms,
            extents: Vec::new().into(),
            deleted: retain_for_ms == 0,
        })
    }

    fn apply_configure_bucket_quota(
        &mut self,
        bucket: String,
        quota: BucketQuota,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        validate_bucket(&bucket)?;
        self.buckets.insert(bucket.clone().into());
        if quota.disabled() {
            self.bucket_quotas.remove(&bucket);
        } else {
            self.bucket_quotas.insert(bucket.clone(), quota.clone());
        }
        let object_id = ObjectId::new(bucket.clone(), "");
        self.commit_meta_projection(
            &object_id,
            &format!(
                "bucket_quota_live_bytes={} bucket_quota_live_objects={}",
                quota.max_live_bytes, quota.max_live_objects
            ),
            None,
            None,
            update_time_ms,
        )?;
        Ok(ObjectMetadata {
            object_id,
            version: 0,
            length: quota.max_live_bytes,
            etag: String::new().into(),
            content_type: "bucket/quota".to_string().into(),
            user_metadata: BTreeMap::new().into(),
            storage_class: k_standard_storage_class().to_string().into(),
            lock: ObjectLock::default(),
            tags: BTreeMap::new().into(),
            update_time_ms,
            extents: Vec::new().into(),
            deleted: quota.disabled(),
        })
    }

    fn apply_configure_bucket_retention_idempotent(
        &mut self,
        bucket: String,
        retain_for_ms: u64,
        update_time_ms: u64,
        idempotency_key: String,
        commit_index: u64,
    ) -> Result<BucketPolicyMutationReceipt, ObjectError> {
        let method = "ConfigureBucketRetention";
        let request_checksum = bucket_retention_request_checksum(&bucket, retain_for_ms)?;
        if let Some(existing) = self.bucket_policy_receipts.get(&idempotency_key) {
            return checked_bucket_policy_receipt_retry(
                existing,
                method,
                &bucket,
                &request_checksum,
            );
        }
        validate_idempotency_key(&idempotency_key)?;
        let metadata =
            self.apply_configure_bucket_retention(bucket.clone(), retain_for_ms, update_time_ms)?;
        let receipt = BucketPolicyMutationReceipt {
            idempotency_key: idempotency_key.clone(),
            method: method.to_string(),
            bucket,
            request_checksum,
            metadata,
            commit_index,
            update_time_ms,
        };
        self.bucket_policy_receipts
            .insert(idempotency_key, receipt.clone());
        Ok(receipt)
    }

    fn apply_configure_bucket_quota_idempotent(
        &mut self,
        bucket: String,
        quota: BucketQuota,
        update_time_ms: u64,
        idempotency_key: String,
        commit_index: u64,
    ) -> Result<BucketPolicyMutationReceipt, ObjectError> {
        let method = "ConfigureBucketQuota";
        let request_checksum = bucket_quota_request_checksum(&bucket, &quota)?;
        if let Some(existing) = self.bucket_policy_receipts.get(&idempotency_key) {
            return checked_bucket_policy_receipt_retry(
                existing,
                method,
                &bucket,
                &request_checksum,
            );
        }
        validate_idempotency_key(&idempotency_key)?;
        let metadata = self.apply_configure_bucket_quota(bucket.clone(), quota, update_time_ms)?;
        let receipt = BucketPolicyMutationReceipt {
            idempotency_key: idempotency_key.clone(),
            method: method.to_string(),
            bucket,
            request_checksum,
            metadata,
            commit_index,
            update_time_ms,
        };
        self.bucket_policy_receipts
            .insert(idempotency_key, receipt.clone());
        Ok(receipt)
    }

}
