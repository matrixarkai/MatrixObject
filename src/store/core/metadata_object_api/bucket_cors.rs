// SPDX-License-Identifier: Apache-2.0
impl MatrixObjectStore {
    pub fn configure_bucket_cors(
        &mut self,
        bucket: &str,
        policy: BucketCorsPolicy,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.configure_bucket_cors_fenced(bucket, policy, 0, 0)
    }

    pub fn configure_bucket_cors_idempotent(
        &mut self,
        bucket: &str,
        policy: BucketCorsPolicy,
        idempotency_key: &str,
    ) -> Result<BucketCorsMutationReceipt, ObjectError> {
        self.configure_bucket_cors_idempotent_fenced(bucket, policy, idempotency_key, 0, 0)
    }

    pub fn configure_bucket_cors_idempotent_fenced(
        &mut self,
        bucket: &str,
        mut policy: BucketCorsPolicy,
        idempotency_key: &str,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<BucketCorsMutationReceipt, ObjectError> {
        self.ensure_metadata_mutation_fence(
            "configure bucket cors idempotent",
            min_read_index,
            min_commit_index,
        )?;
        validate_idempotency_key(idempotency_key)?;
        policy.canonicalize();
        validate_cors_policy(&policy)?;
        let request_checksum = bucket_cors_request_checksum(bucket, &policy)?;
        if let Some(existing) = self.bucket_cors_receipts.get(idempotency_key) {
            return checked_bucket_cors_receipt_retry(existing, bucket, &request_checksum);
        }
        let command = ObjectCommand::ConfigureBucketCorsIdempotent {
            bucket: bucket.to_string(),
            policy,
            update_time_ms: now_millis(),
            idempotency_key: idempotency_key.to_string(),
        };
        self.apply_command(command)?;
        self.bucket_cors_receipts
            .get(idempotency_key)
            .cloned()
            .ok_or_else(|| {
                ObjectError::Corruption(
                    "bucket cors idempotent command did not retain receipt".to_string(),
                )
            })
    }

    pub fn configure_bucket_cors_fenced(
        &mut self,
        bucket: &str,
        mut policy: BucketCorsPolicy,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_metadata_mutation_fence(
            "configure bucket cors",
            min_read_index,
            min_commit_index,
        )?;
        policy.canonicalize();
        validate_cors_policy(&policy)?;
        let update_time_ms = now_millis();
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let command = ObjectCommand::ConfigureBucketCors {
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
            None,
            Some(&policy),
        ) {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(metadata)
    }

    pub fn bucket_cors(&self, bucket: &str) -> Result<BucketCorsPolicy, ObjectError> {
        validate_bucket(bucket)?;
        Ok(self.bucket_cors.get(bucket).cloned().unwrap_or_default())
    }

    pub fn bucket_cors_with_metadata_read(
        &self,
        bucket: &str,
        options: MetadataReadOptions,
    ) -> Result<(BucketCorsPolicy, MetadataRaftReadiness), ObjectError> {
        let readiness = self.prepare_metadata_read(&meta_bucket_key(bucket), &options)?;
        let policy = self.bucket_cors(bucket)?;
        Ok((policy, readiness))
    }

    pub fn evaluate_bucket_cors_preflight(
        &self,
        bucket: &str,
        request: BucketCorsPreflightRequest,
    ) -> Result<BucketCorsPreflightDecision, ObjectError> {
        self.bucket_cors(bucket)?.evaluate_preflight(request)
    }

    pub fn evaluate_bucket_cors_preflight_with_metadata_read(
        &self,
        bucket: &str,
        request: BucketCorsPreflightRequest,
        options: MetadataReadOptions,
    ) -> Result<(BucketCorsPreflightDecision, MetadataRaftReadiness), ObjectError> {
        let (policy, readiness) = self.bucket_cors_with_metadata_read(bucket, options)?;
        Ok((policy.evaluate_preflight(request)?, readiness))
    }

    pub fn bucket_cors_response_allowed_origin(
        &self,
        bucket: &str,
        origin: &str,
    ) -> Result<Option<String>, ObjectError> {
        Ok(self.bucket_cors(bucket)?.response_allowed_origin(origin))
    }

    pub fn bucket_cors_response_allowed_origin_with_metadata_read(
        &self,
        bucket: &str,
        origin: &str,
        options: MetadataReadOptions,
    ) -> Result<(Option<String>, MetadataRaftReadiness), ObjectError> {
        let (policy, readiness) = self.bucket_cors_with_metadata_read(bucket, options)?;
        Ok((policy.response_allowed_origin(origin), readiness))
    }

}
