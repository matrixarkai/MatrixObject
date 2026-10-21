// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    fn apply_set_bucket_versioning(
        &mut self,
        bucket: String,
        enabled: bool,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        validate_bucket(&bucket)?;
        self.buckets.insert(bucket.clone().into());
        if enabled {
            self.bucket_versioning.insert(bucket.clone());
        } else {
            self.bucket_versioning.remove(&bucket);
        }
        let object_id = ObjectId::new(bucket.clone(), "");
        self.commit_meta_projection(
            &object_id,
            &format!("bucket_versioning={enabled}"),
            None,
            None,
            update_time_ms,
        )?;
        Ok(ObjectMetadata {
            object_id,
            version: 0,
            length: 0,
            etag_digest_bits: 0,
            has_etag: false,
            content_type: "bucket/versioning".to_string().into(),
            user_metadata: BTreeMap::new().into(),
            storage_class: StorageClass::Standard,
            lock: ObjectLock::default(),
            tags: BTreeMap::new().into(),
            update_time_ms,
            extents: Vec::new().into(),
            deleted: false,
        })
    }

    fn apply_set_bucket_versioning_idempotent(
        &mut self,
        bucket: String,
        enabled: bool,
        update_time_ms: u64,
        idempotency_key: String,
        commit_index: u64,
    ) -> Result<BucketPolicyMutationReceipt, ObjectError> {
        let method = "ConfigureBucketVersioning";
        let request_checksum = bucket_versioning_request_checksum(&bucket, enabled)?;
        if let Some(existing) = self.bucket_policy_receipts.get(&idempotency_key) {
            return checked_bucket_policy_receipt_retry(
                existing,
                method,
                &bucket,
                &request_checksum,
            );
        }
        validate_idempotency_key(&idempotency_key)?;
        let metadata = self.apply_set_bucket_versioning(bucket.clone(), enabled, update_time_ms)?;
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

    fn apply_configure_bucket_lifecycle(
        &mut self,
        bucket: String,
        policy: BucketLifecyclePolicy,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        validate_bucket(&bucket)?;
        validate_lifecycle_policy(&policy)?;
        self.buckets.insert(bucket.clone().into());
        if policy.disabled() {
            self.bucket_lifecycle.remove(&bucket);
        } else {
            self.bucket_lifecycle.insert(bucket.clone(), policy.clone());
        }
        let object_id = ObjectId::new(bucket.clone(), "");
        self.commit_meta_projection(
            &object_id,
            &format!(
                "lifecycle_keep={} lifecycle_noncurrent_older_ms={} lifecycle_transition_after_ms={} lifecycle_transition_storage_class={} lifecycle_expire_current_after_ms={} lifecycle_expire_delete_markers={} lifecycle_abort_incomplete_multipart_after_ms={}",
                policy.noncurrent_versions_to_keep,
                policy.noncurrent_older_than_ms,
                policy.transition_after_ms,
                policy.transition_storage_class.as_deref().unwrap_or(""),
                policy.expire_current_after_ms,
                policy.expire_delete_markers,
                policy.abort_incomplete_multipart_after_ms
            ),
            None,
            None,
            update_time_ms,
        )?;
        Ok(ObjectMetadata {
            object_id,
            version: 0,
            length: policy.noncurrent_versions_to_keep as u64,
            etag_digest_bits: 0,
            has_etag: false,
            content_type: "bucket/lifecycle".to_string().into(),
            user_metadata: BTreeMap::new().into(),
            storage_class: StorageClass::Standard,
            lock: ObjectLock::default(),
            tags: BTreeMap::new().into(),
            update_time_ms,
            extents: Vec::new().into(),
            deleted: policy.disabled(),
        })
    }

    fn apply_configure_bucket_lifecycle_idempotent(
        &mut self,
        bucket: String,
        policy: BucketLifecyclePolicy,
        update_time_ms: u64,
        idempotency_key: String,
        commit_index: u64,
    ) -> Result<BucketPolicyMutationReceipt, ObjectError> {
        let method = "ConfigureBucketLifecycle";
        let request_checksum = bucket_lifecycle_request_checksum(&bucket, &policy)?;
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
            self.apply_configure_bucket_lifecycle(bucket.clone(), policy, update_time_ms)?;
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
