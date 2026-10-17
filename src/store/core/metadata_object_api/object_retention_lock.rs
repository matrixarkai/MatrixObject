impl MatrixObjectStore {
    pub fn configure_bucket_retention(
        &mut self,
        bucket: &str,
        retain_for_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.configure_bucket_retention_fenced(bucket, retain_for_ms, 0, 0)
    }

    pub fn configure_bucket_retention_idempotent(
        &mut self,
        bucket: &str,
        retain_for_ms: u64,
        idempotency_key: &str,
    ) -> Result<BucketPolicyMutationReceipt, ObjectError> {
        self.configure_bucket_retention_idempotent_fenced(
            bucket,
            retain_for_ms,
            idempotency_key,
            0,
            0,
        )
    }

    pub fn configure_bucket_retention_idempotent_fenced(
        &mut self,
        bucket: &str,
        retain_for_ms: u64,
        idempotency_key: &str,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<BucketPolicyMutationReceipt, ObjectError> {
        self.ensure_metadata_mutation_fence(
            "configure bucket retention idempotent",
            min_read_index,
            min_commit_index,
        )?;
        let command = ObjectCommand::ConfigureBucketRetentionIdempotent {
            bucket: bucket.to_string(),
            retain_for_ms,
            update_time_ms: now_millis(),
            idempotency_key: idempotency_key.to_string(),
        };
        self.apply_command(command)?;
        self.bucket_policy_receipts
            .get(idempotency_key)
            .cloned()
            .ok_or_else(|| {
                ObjectError::Corruption(
                    "bucket retention idempotent command did not retain receipt".to_string(),
                )
            })
    }

    pub fn configure_bucket_retention_fenced(
        &mut self,
        bucket: &str,
        retain_for_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_metadata_mutation_fence(
            "configure bucket retention",
            min_read_index,
            min_commit_index,
        )?;
        let update_time_ms = now_millis();
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let command = ObjectCommand::ConfigureBucketRetention {
            bucket: bucket.to_string(),
            retain_for_ms,
            update_time_ms,
        };
        let metadata = self.apply_command(command)?;
        if let Err(err) = self.append_bucket_policy_journal_record(
            bucket,
            update_time_ms,
            Some(retain_for_ms),
            None,
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

    pub fn bucket_retention(&self, bucket: &str) -> Result<u64, ObjectError> {
        validate_bucket(bucket)?;
        Ok(self.bucket_retention.get(bucket).copied().unwrap_or(0))
    }

    pub fn bucket_retention_with_metadata_read(
        &self,
        bucket: &str,
        options: MetadataReadOptions,
    ) -> Result<(u64, MetadataRaftReadiness), ObjectError> {
        let readiness = self.prepare_metadata_read(&meta_bucket_key(bucket), &options)?;
        let retention = self.bucket_retention(bucket)?;
        Ok((retention, readiness))
    }

    pub fn set_object_legal_hold(
        &mut self,
        bucket: &str,
        key: &str,
        enabled: bool,
    ) -> Result<ObjectMetadata, ObjectError> {
        let update_time_ms = now_millis();
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let command = ObjectCommand::SetObjectLegalHold {
            bucket: bucket.to_string(),
            key: key.to_string(),
            enabled,
            update_time_ms,
        };
        let metadata = self.apply_command(command)?;
        if let Err(err) =
            self.append_object_lock_journal_record(bucket, key, update_time_ms, Some(enabled), None)
        {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(metadata)
    }

    pub fn get_object_legal_hold(&self, bucket: &str, key: &str) -> Result<bool, ObjectError> {
        Ok(self.head_object(bucket, key)?.legal_hold)
    }

    pub fn get_object_legal_hold_with_metadata_read(
        &self,
        bucket: &str,
        key: &str,
        options: MetadataReadOptions,
    ) -> Result<(bool, MetadataRaftReadiness), ObjectError> {
        let (metadata, readiness) = self.head_object_with_metadata_read(bucket, key, options)?;
        Ok((metadata.legal_hold, readiness))
    }

    pub fn set_object_retention(
        &mut self,
        bucket: &str,
        key: &str,
        retention_until_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        let update_time_ms = now_millis();
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let command = ObjectCommand::SetObjectRetention {
            bucket: bucket.to_string(),
            key: key.to_string(),
            retention_until_ms,
            update_time_ms,
        };
        let metadata = self.apply_command(command)?;
        if let Err(err) = self.append_object_lock_journal_record(
            bucket,
            key,
            update_time_ms,
            None,
            Some(retention_until_ms),
        ) {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(metadata)
    }

    pub fn get_object_retention(&self, bucket: &str, key: &str) -> Result<u64, ObjectError> {
        Ok(self.head_object(bucket, key)?.retention_until_ms)
    }

    pub fn get_object_retention_with_metadata_read(
        &self,
        bucket: &str,
        key: &str,
        options: MetadataReadOptions,
    ) -> Result<(u64, MetadataRaftReadiness), ObjectError> {
        let (metadata, readiness) = self.head_object_with_metadata_read(bucket, key, options)?;
        Ok((metadata.retention_until_ms, readiness))
    }

}
