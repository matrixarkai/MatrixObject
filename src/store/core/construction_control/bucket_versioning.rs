// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn set_bucket_versioning(
        &mut self,
        bucket: &str,
        enabled: bool,
    ) -> Result<(), ObjectError> {
        self.set_bucket_versioning_fenced(bucket, enabled, 0, 0)
    }

    pub fn set_bucket_versioning_idempotent(
        &mut self,
        bucket: &str,
        enabled: bool,
        idempotency_key: &str,
    ) -> Result<BucketPolicyMutationReceipt, ObjectError> {
        self.set_bucket_versioning_idempotent_fenced(bucket, enabled, idempotency_key, 0, 0)
    }

    pub fn set_bucket_versioning_idempotent_fenced(
        &mut self,
        bucket: &str,
        enabled: bool,
        idempotency_key: &str,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<BucketPolicyMutationReceipt, ObjectError> {
        self.ensure_metadata_mutation_fence(
            "set bucket versioning idempotent",
            min_read_index,
            min_commit_index,
        )?;
        let command = ObjectCommand::ConfigureBucketVersioningIdempotent {
            bucket: bucket.to_string(),
            enabled,
            update_time_ms: now_millis(),
            idempotency_key: idempotency_key.to_string(),
        };
        self.apply_command(command)?;
        self.bucket_policy_receipts
            .get(idempotency_key)
            .cloned()
            .ok_or_else(|| {
                ObjectError::Corruption(
                    "bucket versioning idempotent command did not retain receipt".to_string(),
                )
            })
    }

    pub fn set_bucket_versioning_fenced(
        &mut self,
        bucket: &str,
        enabled: bool,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<(), ObjectError> {
        self.ensure_metadata_mutation_fence(
            "set bucket versioning",
            min_read_index,
            min_commit_index,
        )?;
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let command = ObjectCommand::SetBucketVersioning {
            bucket: bucket.to_string(),
            enabled,
        };
        self.apply_command(command)?;
        if let Err(err) = self.append_bucket_versioning_journal_record(bucket, enabled) {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(())
    }

    pub fn bucket_versioning_enabled(&self, bucket: &str) -> Result<bool, ObjectError> {
        validate_bucket(bucket)?;
        Ok(self.bucket_versioning.contains(bucket))
    }

    pub fn bucket_versioning_enabled_with_metadata_read(
        &self,
        bucket: &str,
        options: MetadataReadOptions,
    ) -> Result<(bool, MetadataRaftReadiness), ObjectError> {
        let readiness = self.prepare_metadata_read(&meta_bucket_key(bucket), &options)?;
        let enabled = self.bucket_versioning_enabled(bucket)?;
        Ok((enabled, readiness))
    }

}
