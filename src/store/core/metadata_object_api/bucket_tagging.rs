// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn configure_bucket_tagging(
        &mut self,
        bucket: &str,
        tag_set: BucketTagSet,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.configure_bucket_tagging_fenced(bucket, tag_set, 0, 0)
    }

    pub fn configure_bucket_tagging_idempotent(
        &mut self,
        bucket: &str,
        tag_set: BucketTagSet,
        idempotency_key: &str,
    ) -> Result<BucketPolicyMutationReceipt, ObjectError> {
        self.configure_bucket_tagging_idempotent_fenced(bucket, tag_set, idempotency_key, 0, 0)
    }

    pub fn configure_bucket_tagging_idempotent_fenced(
        &mut self,
        bucket: &str,
        tag_set: BucketTagSet,
        idempotency_key: &str,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<BucketPolicyMutationReceipt, ObjectError> {
        self.ensure_metadata_mutation_fence(
            "configure bucket tagging idempotent",
            min_read_index,
            min_commit_index,
        )?;
        validate_idempotency_key(idempotency_key)?;
        validate_tags(&tag_set.tags)?;
        let request_checksum = bucket_tagging_request_checksum(bucket, &tag_set)?;
        if let Some(existing) = self.bucket_policy_receipts.get(idempotency_key) {
            return checked_bucket_policy_receipt_retry(
                existing,
                "ConfigureBucketTagging",
                bucket,
                &request_checksum,
            );
        }
        let command = ObjectCommand::ConfigureBucketTaggingIdempotent {
            bucket: bucket.to_string(),
            tag_set,
            update_time_ms: now_millis(),
            idempotency_key: idempotency_key.to_string(),
        };
        self.apply_command(command)?;
        self.bucket_policy_receipts
            .get(idempotency_key)
            .cloned()
            .ok_or_else(|| {
                ObjectError::Corruption(
                    "bucket tagging idempotent command did not retain receipt".to_string(),
                )
            })
    }

    pub fn configure_bucket_tagging_fenced(
        &mut self,
        bucket: &str,
        tag_set: BucketTagSet,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_metadata_mutation_fence(
            "configure bucket tagging",
            min_read_index,
            min_commit_index,
        )?;
        validate_tags(&tag_set.tags)?;
        let command = ObjectCommand::ConfigureBucketTagging {
            bucket: bucket.to_string(),
            tag_set,
            update_time_ms: now_millis(),
        };
        self.apply_command(command)
    }

    pub fn bucket_tagging(&self, bucket: &str) -> Result<BucketTagSet, ObjectError> {
        validate_bucket(bucket)?;
        Ok(self.bucket_tags.get(bucket).cloned().unwrap_or_default())
    }

    pub fn bucket_tagging_with_metadata_read(
        &self,
        bucket: &str,
        options: MetadataReadOptions,
    ) -> Result<(BucketTagSet, MetadataRaftReadiness), ObjectError> {
        let readiness = self.prepare_metadata_read(&meta_bucket_key(bucket), &options)?;
        let tag_set = self.bucket_tagging(bucket)?;
        Ok((tag_set, readiness))
    }

}
