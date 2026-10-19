// SPDX-License-Identifier: Apache-2.0
impl MatrixObjectStore {
    fn apply_configure_bucket_policy(
        &mut self,
        bucket: String,
        policy: BucketPolicyDocument,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        validate_bucket(&bucket)?;
        validate_bucket_policy_document(&policy)?;
        self.buckets.insert(bucket.clone().into());
        if policy.disabled() {
            self.bucket_policy_documents.remove(&bucket);
        } else {
            self.bucket_policy_documents
                .insert(bucket.clone(), policy.clone());
        }
        let object_id = ObjectId::new(bucket.clone(), "");
        self.commit_meta_projection(
            &object_id,
            &format!("policy_document_bytes={}", policy.document.len()),
            None,
            None,
            update_time_ms,
        )?;
        Ok(ObjectMetadata {
            object_id,
            version: 0,
            length: policy.document.len() as u64,
            etag: etag(policy.document.as_bytes()).into(),
            content_type: "bucket/policy".to_string().into(),
            user_metadata: BTreeMap::new().into(),
            storage_class: k_standard_storage_class().to_string().into(),
            lock: ObjectLock::default(),
            tags: BTreeMap::new().into(),
            update_time_ms,
            extents: Vec::new().into(),
            deleted: policy.disabled(),
        })
    }

    fn apply_configure_bucket_policy_idempotent(
        &mut self,
        bucket: String,
        policy: BucketPolicyDocument,
        update_time_ms: u64,
        idempotency_key: String,
        commit_index: u64,
    ) -> Result<BucketPolicyMutationReceipt, ObjectError> {
        validate_idempotency_key(&idempotency_key)?;
        validate_bucket_policy_document(&policy)?;
        let request_checksum = bucket_policy_document_request_checksum(&bucket, &policy)?;
        if let Some(existing) = self.bucket_policy_receipts.get(&idempotency_key) {
            return checked_bucket_policy_receipt_retry(
                existing,
                "ConfigureBucketPolicy",
                &bucket,
                &request_checksum,
            );
        }
        let metadata =
            self.apply_configure_bucket_policy(bucket.clone(), policy, update_time_ms)?;
        let receipt = BucketPolicyMutationReceipt {
            idempotency_key: idempotency_key.clone(),
            method: "ConfigureBucketPolicy".to_string(),
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

    fn apply_configure_bucket_tagging(
        &mut self,
        bucket: String,
        tag_set: BucketTagSet,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        validate_bucket(&bucket)?;
        validate_tags(&tag_set.tags)?;
        self.buckets.insert(bucket.clone().into());
        if tag_set.disabled() {
            self.bucket_tags.remove(&bucket);
        } else {
            self.bucket_tags.insert(bucket.clone(), tag_set.clone());
        }
        let object_id = ObjectId::new(bucket.clone(), "");
        self.commit_meta_projection(
            &object_id,
            &format!("bucket_tag_count={}", tag_set.tags.len()),
            None,
            None,
            update_time_ms,
        )?;
        Ok(ObjectMetadata {
            object_id,
            version: 0,
            length: tag_set.tags.len() as u64,
            etag: etag(&bucket_tagging_checksum_material(&bucket, &tag_set)?).into(),
            content_type: "bucket/tagging".to_string().into(),
            user_metadata: BTreeMap::new().into(),
            storage_class: k_standard_storage_class().to_string().into(),
            lock: ObjectLock::default(),
            tags: tag_set.tags.into(),
            update_time_ms,
            extents: Vec::new().into(),
            deleted: false,
        })
    }

    fn apply_configure_bucket_tagging_idempotent(
        &mut self,
        bucket: String,
        tag_set: BucketTagSet,
        update_time_ms: u64,
        idempotency_key: String,
        commit_index: u64,
    ) -> Result<BucketPolicyMutationReceipt, ObjectError> {
        validate_idempotency_key(&idempotency_key)?;
        validate_tags(&tag_set.tags)?;
        let request_checksum = bucket_tagging_request_checksum(&bucket, &tag_set)?;
        if let Some(existing) = self.bucket_policy_receipts.get(&idempotency_key) {
            return checked_bucket_policy_receipt_retry(
                existing,
                "ConfigureBucketTagging",
                &bucket,
                &request_checksum,
            );
        }
        let metadata =
            self.apply_configure_bucket_tagging(bucket.clone(), tag_set, update_time_ms)?;
        let receipt = BucketPolicyMutationReceipt {
            idempotency_key: idempotency_key.clone(),
            method: "ConfigureBucketTagging".to_string(),
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

    fn apply_configure_bucket_cors(
        &mut self,
        bucket: String,
        mut policy: BucketCorsPolicy,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        validate_bucket(&bucket)?;
        policy.canonicalize();
        validate_cors_policy(&policy)?;
        self.buckets.insert(bucket.clone().into());
        if policy.disabled() {
            self.bucket_cors.remove(&bucket);
        } else {
            self.bucket_cors.insert(bucket.clone(), policy.clone());
        }
        let object_id = ObjectId::new(bucket.clone(), "");
        self.commit_meta_projection(
            &object_id,
            &format!(
                "cors_allowed_origins={} cors_allowed_methods={} cors_allowed_headers={} cors_expose_headers={} cors_max_age_seconds={}",
                policy.allowed_origins.join(","),
                policy.allowed_methods.join(","),
                policy.allowed_headers.join(","),
                policy.expose_headers.join(","),
                policy.max_age_seconds
            ),
            None,
            None,
            update_time_ms,
        )?;
        Ok(ObjectMetadata {
            object_id,
            version: 0,
            length: policy.allowed_origins.len() as u64,
            etag: String::new().into(),
            content_type: "bucket/cors".to_string().into(),
            user_metadata: BTreeMap::new().into(),
            storage_class: k_standard_storage_class().to_string().into(),
            lock: ObjectLock::default(),
            tags: BTreeMap::new().into(),
            update_time_ms,
            extents: Vec::new().into(),
            deleted: policy.disabled(),
        })
    }

    fn apply_configure_bucket_cors_idempotent(
        &mut self,
        bucket: String,
        mut policy: BucketCorsPolicy,
        update_time_ms: u64,
        idempotency_key: String,
        commit_index: u64,
    ) -> Result<BucketCorsMutationReceipt, ObjectError> {
        validate_idempotency_key(&idempotency_key)?;
        policy.canonicalize();
        validate_cors_policy(&policy)?;
        let request_checksum = bucket_cors_request_checksum(&bucket, &policy)?;
        if let Some(existing) = self.bucket_cors_receipts.get(&idempotency_key) {
            return checked_bucket_cors_receipt_retry(existing, &bucket, &request_checksum);
        }
        let metadata = self.apply_configure_bucket_cors(bucket.clone(), policy, update_time_ms)?;
        let policy = self.bucket_cors(&bucket)?;
        let report = BucketCorsMutationReport { metadata, policy };
        let receipt = BucketCorsMutationReceipt {
            idempotency_key: idempotency_key.clone(),
            bucket,
            request_checksum,
            report,
            commit_index,
            update_time_ms,
        };
        self.bucket_cors_receipts
            .insert(idempotency_key, receipt.clone());
        Ok(receipt)
    }

}
