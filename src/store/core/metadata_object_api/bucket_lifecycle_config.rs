// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn configure_bucket_lifecycle(
        &mut self,
        bucket: &str,
        policy: BucketLifecyclePolicy,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.configure_bucket_lifecycle_fenced(bucket, policy, 0, 0)
    }

    pub fn configure_bucket_lifecycle_idempotent(
        &mut self,
        bucket: &str,
        policy: BucketLifecyclePolicy,
        idempotency_key: &str,
    ) -> Result<BucketPolicyMutationReceipt, ObjectError> {
        self.configure_bucket_lifecycle_idempotent_fenced(bucket, policy, idempotency_key, 0, 0)
    }

    pub fn configure_bucket_lifecycle_idempotent_fenced(
        &mut self,
        bucket: &str,
        policy: BucketLifecyclePolicy,
        idempotency_key: &str,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<BucketPolicyMutationReceipt, ObjectError> {
        self.ensure_metadata_mutation_fence(
            "configure bucket lifecycle idempotent",
            min_read_index,
            min_commit_index,
        )?;
        let command = ObjectCommand::ConfigureBucketLifecycleIdempotent {
            bucket: bucket.to_string(),
            policy,
            update_time_ms: now_millis(),
            idempotency_key: idempotency_key.to_string(),
        };
        self.apply_command(command)?;
        self.bucket_policy_receipts
            .get(idempotency_key)
            .cloned()
            .ok_or_else(|| {
                ObjectError::Corruption(
                    "bucket lifecycle idempotent command did not retain receipt".to_string(),
                )
            })
    }

    pub fn configure_bucket_lifecycle_fenced(
        &mut self,
        bucket: &str,
        policy: BucketLifecyclePolicy,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_metadata_mutation_fence(
            "configure bucket lifecycle",
            min_read_index,
            min_commit_index,
        )?;
        let update_time_ms = now_millis();
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let command = ObjectCommand::ConfigureBucketLifecycle {
            bucket: bucket.to_string(),
            policy: policy.clone(),
            update_time_ms,
        };
        let metadata = self.apply_command(command)?;
        if let Err(err) = self.append_bucket_policy_journal_record(
            bucket,
            update_time_ms,
            None,
            None,
            Some(&policy),
            None,
        ) {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(metadata)
    }

    pub fn bucket_lifecycle(&self, bucket: &str) -> Result<BucketLifecyclePolicy, ObjectError> {
        validate_bucket(bucket)?;
        Ok(self
            .bucket_lifecycle
            .get(bucket)
            .cloned()
            .unwrap_or_default())
    }

    pub fn bucket_lifecycle_with_metadata_read(
        &self,
        bucket: &str,
        options: MetadataReadOptions,
    ) -> Result<(BucketLifecyclePolicy, MetadataRaftReadiness), ObjectError> {
        let readiness = self.prepare_metadata_read(&meta_bucket_key(bucket), &options)?;
        let lifecycle = self.bucket_lifecycle(bucket)?;
        Ok((lifecycle, readiness))
    }

}
