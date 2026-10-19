// SPDX-License-Identifier: Apache-2.0
impl MatrixObjectStore {
    pub fn configure_bucket_quota(
        &mut self,
        bucket: &str,
        max_live_bytes: u64,
        max_live_objects: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.configure_bucket_quota_fenced(bucket, max_live_bytes, max_live_objects, 0, 0)
    }

    pub fn configure_bucket_quota_idempotent(
        &mut self,
        bucket: &str,
        max_live_bytes: u64,
        max_live_objects: u64,
        idempotency_key: &str,
    ) -> Result<BucketPolicyMutationReceipt, ObjectError> {
        self.configure_bucket_quota_idempotent_fenced(
            bucket,
            max_live_bytes,
            max_live_objects,
            idempotency_key,
            0,
            0,
        )
    }

    pub fn configure_bucket_quota_idempotent_fenced(
        &mut self,
        bucket: &str,
        max_live_bytes: u64,
        max_live_objects: u64,
        idempotency_key: &str,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<BucketPolicyMutationReceipt, ObjectError> {
        self.ensure_metadata_mutation_fence(
            "configure bucket quota idempotent",
            min_read_index,
            min_commit_index,
        )?;
        let command = ObjectCommand::ConfigureBucketQuotaIdempotent {
            bucket: bucket.to_string(),
            quota: BucketQuota {
                max_live_bytes,
                max_live_objects,
            },
            update_time_ms: now_millis(),
            idempotency_key: idempotency_key.to_string(),
        };
        self.apply_command(command)?;
        self.bucket_policy_receipts
            .get(idempotency_key)
            .cloned()
            .ok_or_else(|| {
                ObjectError::Corruption(
                    "bucket quota idempotent command did not retain receipt".to_string(),
                )
            })
    }

    pub fn configure_bucket_quota_fenced(
        &mut self,
        bucket: &str,
        max_live_bytes: u64,
        max_live_objects: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_metadata_mutation_fence(
            "configure bucket quota",
            min_read_index,
            min_commit_index,
        )?;
        let update_time_ms = now_millis();
        let quota = BucketQuota {
            max_live_bytes,
            max_live_objects,
        };
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let command = ObjectCommand::ConfigureBucketQuota {
            bucket: bucket.to_string(),
            quota: quota.clone(),
            update_time_ms,
        };
        let metadata = self.apply_command(command)?;
        if let Err(err) = self.append_bucket_policy_journal_record(
            bucket,
            update_time_ms,
            None,
            Some(&quota),
            None,
            None,
        ) {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(metadata)
    }

    pub fn bucket_quota(&self, bucket: &str) -> Result<BucketQuota, ObjectError> {
        validate_bucket(bucket)?;
        Ok(self.bucket_quotas.get(bucket).cloned().unwrap_or_default())
    }

    pub fn bucket_quota_with_metadata_read(
        &self,
        bucket: &str,
        options: MetadataReadOptions,
    ) -> Result<(BucketQuota, MetadataRaftReadiness), ObjectError> {
        let readiness = self.prepare_metadata_read(&meta_bucket_key(bucket), &options)?;
        let quota = self.bucket_quota(bucket)?;
        Ok((quota, readiness))
    }

    pub fn bucket_live_usage(&self, bucket: &str) -> Result<(u64, u64), ObjectError> {
        validate_bucket(bucket)?;
        Ok(self.live_bucket_usage(bucket))
    }

}
