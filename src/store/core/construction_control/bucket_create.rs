// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn create_bucket(&mut self, bucket: &str) -> Result<(), ObjectError> {
        self.create_bucket_fenced(bucket, 0, 0)
    }

    pub fn create_bucket_fenced(
        &mut self,
        bucket: &str,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<(), ObjectError> {
        self.ensure_metadata_mutation_fence("create bucket", min_read_index, min_commit_index)?;
        self.apply_create_bucket(bucket, now_millis()).map(|_| ())
    }

    pub fn create_bucket_idempotent(
        &mut self,
        bucket: &str,
        idempotency_key: &str,
    ) -> Result<BucketPolicyMutationReceipt, ObjectError> {
        self.create_bucket_idempotent_fenced(bucket, idempotency_key, 0, 0)
    }

    pub fn create_bucket_idempotent_fenced(
        &mut self,
        bucket: &str,
        idempotency_key: &str,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<BucketPolicyMutationReceipt, ObjectError> {
        self.ensure_metadata_mutation_fence(
            "create bucket idempotent",
            min_read_index,
            min_commit_index,
        )?;
        let command = ObjectCommand::CreateBucketIdempotent {
            bucket: bucket.to_string(),
            update_time_ms: now_millis(),
            idempotency_key: idempotency_key.to_string(),
        };
        self.apply_command(command)?;
        self.bucket_policy_receipts
            .get(idempotency_key)
            .cloned()
            .ok_or_else(|| {
                ObjectError::Corruption(
                    "create bucket idempotent command did not retain receipt".to_string(),
                )
            })
    }

    fn apply_create_bucket(
        &mut self,
        bucket: &str,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        validate_bucket(bucket)?;
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        self.buckets.insert(bucket.to_string().into());
        let key = meta_bucket_key(bucket);
        let expected_version = self
            .meta_service
            .record(key.as_str())
            .map_or(0, |record| record.version);
        self.meta_service.commit_record(
            &key,
            "created".to_string(),
            expected_version,
            None,
            None,
            now_millis(),
        )?;
        if self.options.persistent_snapshot_flush_on_commit {
            self.flush_persistent_snapshot()?;
            let checkpoint_index = self.last_metadata_commit_index();
            self.last_object_manifest_checkpoint_index = checkpoint_index;
            self.last_object_metadata_checkpoint_index = checkpoint_index;
        }
        if let Err(err) = self.append_create_bucket_journal_record(bucket) {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(bucket_membership_metadata(
            bucket,
            "bucket/create",
            update_time_ms,
            false,
        ))
    }

    fn apply_create_bucket_idempotent(
        &mut self,
        bucket: String,
        update_time_ms: u64,
        idempotency_key: String,
        commit_index: u64,
    ) -> Result<BucketPolicyMutationReceipt, ObjectError> {
        let method = "CreateBucket";
        let request_checksum = bucket_create_request_checksum(&bucket)?;
        if let Some(existing) = self.bucket_policy_receipts.get(&idempotency_key) {
            return checked_bucket_policy_receipt_retry(
                existing,
                method,
                &bucket,
                &request_checksum,
            );
        }
        validate_idempotency_key(&idempotency_key)?;
        let metadata = self.apply_create_bucket(&bucket, update_time_ms)?;
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
