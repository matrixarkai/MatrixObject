// SPDX-License-Identifier: Apache-2.0
impl MatrixObjectStore {
    pub fn configure_bucket_policy(
        &mut self,
        bucket: &str,
        policy: BucketPolicyDocument,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.configure_bucket_policy_fenced(bucket, policy, 0, 0)
    }

    pub fn configure_bucket_policy_idempotent(
        &mut self,
        bucket: &str,
        policy: BucketPolicyDocument,
        idempotency_key: &str,
    ) -> Result<BucketPolicyMutationReceipt, ObjectError> {
        self.configure_bucket_policy_idempotent_fenced(bucket, policy, idempotency_key, 0, 0)
    }

    pub fn configure_bucket_policy_idempotent_fenced(
        &mut self,
        bucket: &str,
        policy: BucketPolicyDocument,
        idempotency_key: &str,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<BucketPolicyMutationReceipt, ObjectError> {
        self.ensure_metadata_mutation_fence(
            "configure bucket policy idempotent",
            min_read_index,
            min_commit_index,
        )?;
        validate_idempotency_key(idempotency_key)?;
        validate_bucket_policy_document(&policy)?;
        let request_checksum = bucket_policy_document_request_checksum(bucket, &policy)?;
        if let Some(existing) = self.bucket_policy_receipts.get(idempotency_key) {
            return checked_bucket_policy_receipt_retry(
                existing,
                "ConfigureBucketPolicy",
                bucket,
                &request_checksum,
            );
        }
        let command = ObjectCommand::ConfigureBucketPolicyIdempotent {
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
                    "bucket policy idempotent command did not retain receipt".to_string(),
                )
            })
    }

    pub fn configure_bucket_policy_fenced(
        &mut self,
        bucket: &str,
        policy: BucketPolicyDocument,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_metadata_mutation_fence(
            "configure bucket policy",
            min_read_index,
            min_commit_index,
        )?;
        validate_bucket_policy_document(&policy)?;
        let update_time_ms = now_millis();
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let command = ObjectCommand::ConfigureBucketPolicy {
            bucket: bucket.to_string(),
            policy: policy.clone(),
            update_time_ms,
        };
        let metadata = self.apply_command(command)?;
        if let Err(err) =
            self.append_bucket_policy_document_journal_record(bucket, update_time_ms, &policy)
        {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(metadata)
    }

    pub fn bucket_policy(&self, bucket: &str) -> Result<BucketPolicyDocument, ObjectError> {
        validate_bucket(bucket)?;
        Ok(self
            .bucket_policy_documents
            .get(bucket)
            .cloned()
            .unwrap_or_default())
    }

    pub fn bucket_policy_with_metadata_read(
        &self,
        bucket: &str,
        options: MetadataReadOptions,
    ) -> Result<(BucketPolicyDocument, MetadataRaftReadiness), ObjectError> {
        let readiness = self.prepare_metadata_read(&meta_bucket_key(bucket), &options)?;
        let policy = self.bucket_policy(bucket)?;
        Ok((policy, readiness))
    }

}
