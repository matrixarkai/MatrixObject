impl MatrixObjectStore {
    pub fn delete_bucket(&mut self, bucket: &str) -> Result<(), ObjectError> {
        self.delete_bucket_fenced(bucket, 0, 0)
    }

    pub fn delete_bucket_fenced(
        &mut self,
        bucket: &str,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<(), ObjectError> {
        self.ensure_metadata_mutation_fence("delete bucket", min_read_index, min_commit_index)?;
        self.apply_delete_bucket(bucket, now_millis())
    }

    pub fn delete_bucket_idempotent(
        &mut self,
        bucket: &str,
        idempotency_key: &str,
    ) -> Result<BucketPolicyMutationReceipt, ObjectError> {
        self.delete_bucket_idempotent_fenced(bucket, idempotency_key, 0, 0)
    }

    pub fn delete_bucket_idempotent_fenced(
        &mut self,
        bucket: &str,
        idempotency_key: &str,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<BucketPolicyMutationReceipt, ObjectError> {
        self.ensure_metadata_mutation_fence(
            "delete bucket idempotent",
            min_read_index,
            min_commit_index,
        )?;
        let command = ObjectCommand::DeleteBucketIdempotent {
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
                    "delete bucket idempotent command did not retain receipt".to_string(),
                )
            })
    }

    fn apply_delete_bucket(
        &mut self,
        bucket: &str,
        update_time_ms: u64,
    ) -> Result<(), ObjectError> {
        validate_bucket(bucket)?;
        if !self.buckets.contains(bucket) {
            return Err(ObjectError::NotFound(format!("bucket not found: {bucket}")));
        }
        if self
            .objects
            .values()
            .any(|metadata| metadata.object_id.bucket == bucket && !metadata.deleted)
        {
            return Err(ObjectError::InvalidArgument(format!(
                "bucket is not empty: {bucket}"
            )));
        }
        if self.versions.iter().any(|(object_id, versions)| {
            object_id.bucket == bucket && versions.iter().any(|metadata| !metadata.deleted)
        }) {
            return Err(ObjectError::InvalidArgument(format!(
                "bucket has retained object versions: {bucket}"
            )));
        }
        let rollback = self.capture_object_mutation_rollback();
        self.buckets.remove(bucket);
        self.bucket_versioning.remove(bucket);
        self.bucket_retention.remove(bucket);
        self.bucket_quotas.remove(bucket);
        self.bucket_lifecycle.remove(bucket);
        self.bucket_policy_documents.remove(bucket);
        self.bucket_tags.remove(bucket);
        self.bucket_cors.remove(bucket);
        let key = meta_bucket_key(bucket);
        let expected_version = self
            .meta_service
            .records
            .get(&key)
            .map_or(0, |record| record.version);
        match self
            .meta_service
            .delete_record(&key, expected_version, None, None, update_time_ms)
        {
            Ok(record) => record,
            Err(err) => {
                self.restore_object_mutation_rollback(rollback);
                return Err(err);
            }
        };
        if self.options.persistent_snapshot_flush_on_commit {
            if let Err(err) = self.flush_persistent_snapshot() {
                self.restore_object_mutation_rollback(rollback);
                return Err(err);
            }
            let checkpoint_index = self.last_metadata_commit_index();
            self.last_object_manifest_checkpoint_index = checkpoint_index;
            self.last_object_metadata_checkpoint_index = checkpoint_index;
        }
        if let Err(err) = self.append_delete_bucket_journal_record(bucket, update_time_ms) {
            self.restore_object_mutation_rollback(rollback);
            return Err(err);
        }
        Ok(())
    }

    fn apply_delete_bucket_idempotent(
        &mut self,
        bucket: String,
        update_time_ms: u64,
        idempotency_key: String,
        commit_index: u64,
    ) -> Result<BucketPolicyMutationReceipt, ObjectError> {
        let method = "DeleteBucket";
        let request_checksum = bucket_delete_request_checksum(&bucket)?;
        if let Some(existing) = self.bucket_policy_receipts.get(&idempotency_key) {
            return checked_bucket_policy_receipt_retry(
                existing,
                method,
                &bucket,
                &request_checksum,
            );
        }
        validate_idempotency_key(&idempotency_key)?;
        self.apply_delete_bucket(&bucket, update_time_ms)?;
        let metadata = bucket_membership_metadata(&bucket, "bucket/delete", update_time_ms, true);
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
