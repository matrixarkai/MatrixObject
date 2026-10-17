impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_configure_bucket_retention(
        &mut self,
        bucket: &str,
        retain_for_ms: u64,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::ConfigureBucketRetention {
            bucket: bucket.to_string(),
            retain_for_ms,
            update_time_ms,
        })
    }

    pub fn propose_configure_bucket_retention_idempotent(
        &mut self,
        bucket: &str,
        retain_for_ms: u64,
        update_time_ms: u64,
        idempotency_key: &str,
    ) -> Result<BucketPolicyMutationReceipt, ObjectError> {
        let request_checksum = bucket_retention_request_checksum(bucket, retain_for_ms)?;
        if let Some(existing) = self.store.bucket_policy_receipts.get(idempotency_key) {
            return checked_bucket_policy_receipt_retry(
                existing,
                "ConfigureBucketRetention",
                bucket,
                &request_checksum,
            );
        }
        validate_idempotency_key(idempotency_key)?;
        self.propose(ObjectCommand::ConfigureBucketRetentionIdempotent {
            bucket: bucket.to_string(),
            retain_for_ms,
            update_time_ms,
            idempotency_key: idempotency_key.to_string(),
        })?;
        self.store
            .bucket_policy_receipts
            .get(idempotency_key)
            .cloned()
            .ok_or_else(|| {
                ObjectError::Corruption(
                    "raft bucket retention idempotent command did not retain receipt".to_string(),
                )
            })
    }

    pub fn propose_configure_bucket_retention_idempotent_with_read_proof_linearizable(
        &mut self,
        bucket: &str,
        retain_for_ms: u64,
        update_time_ms: u64,
        idempotency_key: &str,
    ) -> Result<ProvenMetadataRead<BucketRetentionMutationReport>, ObjectError> {
        self.propose_configure_bucket_retention_idempotent_with_read_proof_linearizable_committed_at(
            bucket,
            retain_for_ms,
            update_time_ms,
            idempotency_key,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_configure_bucket_retention_idempotent_with_read_proof_linearizable_committed_at(
        &mut self,
        bucket: &str,
        retain_for_ms: u64,
        update_time_ms: u64,
        idempotency_key: &str,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<BucketRetentionMutationReport>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "configure bucket retention idempotent",
            update_time_ms,
            min_read_index,
            min_commit_index,
        )?;
        let receipt = self.propose_configure_bucket_retention_idempotent(
            bucket,
            retain_for_ms,
            update_time_ms,
            idempotency_key,
        )?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "configure bucket retention idempotent post-commit",
            update_time_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        let report = BucketRetentionMutationReport {
            metadata: receipt.metadata,
            retain_for_ms: self.store.bucket_retention(bucket)?,
        };
        Ok(proven_metadata_read(report, &readiness))
    }

    pub fn propose_configure_bucket_retention_with_read_proof_linearizable(
        &mut self,
        bucket: &str,
        retain_for_ms: u64,
        update_time_ms: u64,
    ) -> Result<ProvenMetadataRead<BucketRetentionMutationReport>, ObjectError> {
        self.propose_configure_bucket_retention_with_read_proof_linearizable_committed_at(
            bucket,
            retain_for_ms,
            update_time_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_configure_bucket_retention_with_read_proof_linearizable_committed_at(
        &mut self,
        bucket: &str,
        retain_for_ms: u64,
        update_time_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<BucketRetentionMutationReport>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "configure bucket retention",
            update_time_ms,
            min_read_index,
            min_commit_index,
        )?;
        let metadata =
            self.propose_configure_bucket_retention(bucket, retain_for_ms, update_time_ms)?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "configure bucket retention post-commit",
            update_time_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        let report = BucketRetentionMutationReport {
            metadata,
            retain_for_ms: self.store.bucket_retention(bucket)?,
        };
        Ok(proven_metadata_read(report, &readiness))
    }

    pub fn propose_configure_bucket_retention_from_meta_shard(
        &mut self,
        bucket: &str,
        retain_for_ms: u64,
        update_time_ms: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.store.check_meta_shard_owner_serving(
            &meta_bucket_key(bucket),
            expected_shard_owner,
            expected_shard_epoch,
            update_time_ms,
        )?;
        self.store.ensure_metadata_mutation_fence(
            "configure bucket retention from meta shard",
            min_read_index,
            0,
        )?;
        self.propose_configure_bucket_retention(bucket, retain_for_ms, update_time_ms)
    }

    pub fn propose_configure_bucket_retention_with_read_proof_from_meta_shard(
        &mut self,
        bucket: &str,
        retain_for_ms: u64,
        update_time_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
    ) -> Result<ProvenMetadataRead<BucketRetentionMutationReport>, ObjectError> {
        self.store.check_meta_shard_owner_serving(
            &meta_bucket_key(bucket),
            expected_shard_owner,
            expected_shard_epoch,
            update_time_ms,
        )?;
        self.propose_configure_bucket_retention_with_read_proof_linearizable_committed_at(
            bucket,
            retain_for_ms,
            update_time_ms,
            min_read_index,
            min_commit_index,
        )
    }

    pub fn propose_configure_bucket_retention_idempotent_from_meta_shard(
        &mut self,
        bucket: &str,
        retain_for_ms: u64,
        update_time_ms: u64,
        idempotency_key: &str,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
    ) -> Result<BucketPolicyMutationReceipt, ObjectError> {
        self.store.check_meta_shard_owner_serving(
            &meta_bucket_key(bucket),
            expected_shard_owner,
            expected_shard_epoch,
            update_time_ms,
        )?;
        self.store.ensure_metadata_mutation_fence(
            "configure bucket retention idempotent from meta shard",
            min_read_index,
            0,
        )?;
        self.propose_configure_bucket_retention_idempotent(
            bucket,
            retain_for_ms,
            update_time_ms,
            idempotency_key,
        )
    }

    pub fn propose_configure_bucket_retention_idempotent_with_read_proof_from_meta_shard(
        &mut self,
        bucket: &str,
        retain_for_ms: u64,
        update_time_ms: u64,
        idempotency_key: &str,
        min_read_index: u64,
        min_commit_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
    ) -> Result<ProvenMetadataRead<BucketRetentionMutationReport>, ObjectError> {
        self.store.check_meta_shard_owner_serving(
            &meta_bucket_key(bucket),
            expected_shard_owner,
            expected_shard_epoch,
            update_time_ms,
        )?;
        self.propose_configure_bucket_retention_idempotent_with_read_proof_linearizable_committed_at(
            bucket,
            retain_for_ms,
            update_time_ms,
            idempotency_key,
            min_read_index,
            min_commit_index,
        )
    }

}
