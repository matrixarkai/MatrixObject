impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_configure_bucket_lifecycle(
        &mut self,
        bucket: &str,
        policy: BucketLifecyclePolicy,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::ConfigureBucketLifecycle {
            bucket: bucket.to_string(),
            policy,
            update_time_ms,
        })
    }

    pub fn propose_configure_bucket_lifecycle_idempotent(
        &mut self,
        bucket: &str,
        policy: BucketLifecyclePolicy,
        update_time_ms: u64,
        idempotency_key: &str,
    ) -> Result<BucketPolicyMutationReceipt, ObjectError> {
        let request_checksum = bucket_lifecycle_request_checksum(bucket, &policy)?;
        if let Some(existing) = self.store.bucket_policy_receipts.get(idempotency_key) {
            return checked_bucket_policy_receipt_retry(
                existing,
                "ConfigureBucketLifecycle",
                bucket,
                &request_checksum,
            );
        }
        validate_idempotency_key(idempotency_key)?;
        self.propose(ObjectCommand::ConfigureBucketLifecycleIdempotent {
            bucket: bucket.to_string(),
            policy,
            update_time_ms,
            idempotency_key: idempotency_key.to_string(),
        })?;
        self.store
            .bucket_policy_receipts
            .get(idempotency_key)
            .cloned()
            .ok_or_else(|| {
                ObjectError::Corruption(
                    "raft bucket lifecycle idempotent command did not retain receipt".to_string(),
                )
            })
    }

    pub fn propose_configure_bucket_lifecycle_idempotent_with_read_proof_linearizable(
        &mut self,
        bucket: &str,
        policy: BucketLifecyclePolicy,
        update_time_ms: u64,
        idempotency_key: &str,
    ) -> Result<ProvenMetadataRead<BucketLifecycleMutationReport>, ObjectError> {
        self.propose_configure_bucket_lifecycle_idempotent_with_read_proof_linearizable_committed_at(
            bucket,
            policy,
            update_time_ms,
            idempotency_key,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_configure_bucket_lifecycle_idempotent_with_read_proof_linearizable_committed_at(
        &mut self,
        bucket: &str,
        policy: BucketLifecyclePolicy,
        update_time_ms: u64,
        idempotency_key: &str,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<BucketLifecycleMutationReport>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "configure bucket lifecycle idempotent",
            update_time_ms,
            min_read_index,
            min_commit_index,
        )?;
        let receipt = self.propose_configure_bucket_lifecycle_idempotent(
            bucket,
            policy,
            update_time_ms,
            idempotency_key,
        )?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "configure bucket lifecycle idempotent post-commit",
            update_time_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        let report = BucketLifecycleMutationReport {
            metadata: receipt.metadata,
            policy: self.store.bucket_lifecycle(bucket)?,
        };
        Ok(proven_metadata_read(report, &readiness))
    }

    pub fn propose_configure_bucket_lifecycle_with_read_proof_linearizable(
        &mut self,
        bucket: &str,
        policy: BucketLifecyclePolicy,
        update_time_ms: u64,
    ) -> Result<ProvenMetadataRead<BucketLifecycleMutationReport>, ObjectError> {
        self.propose_configure_bucket_lifecycle_with_read_proof_linearizable_committed_at(
            bucket,
            policy,
            update_time_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_configure_bucket_lifecycle_with_read_proof_linearizable_committed_at(
        &mut self,
        bucket: &str,
        policy: BucketLifecyclePolicy,
        update_time_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<BucketLifecycleMutationReport>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "configure bucket lifecycle",
            update_time_ms,
            min_read_index,
            min_commit_index,
        )?;
        let metadata = self.propose_configure_bucket_lifecycle(bucket, policy, update_time_ms)?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "configure bucket lifecycle post-commit",
            update_time_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        let report = BucketLifecycleMutationReport {
            metadata,
            policy: self.store.bucket_lifecycle(bucket)?,
        };
        Ok(proven_metadata_read(report, &readiness))
    }

    pub fn propose_configure_bucket_lifecycle_from_meta_shard(
        &mut self,
        bucket: &str,
        policy: BucketLifecyclePolicy,
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
            "configure bucket lifecycle from meta shard",
            min_read_index,
            0,
        )?;
        self.propose_configure_bucket_lifecycle(bucket, policy, update_time_ms)
    }

    pub fn propose_configure_bucket_lifecycle_with_read_proof_from_meta_shard(
        &mut self,
        bucket: &str,
        policy: BucketLifecyclePolicy,
        update_time_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
    ) -> Result<ProvenMetadataRead<BucketLifecycleMutationReport>, ObjectError> {
        self.store.check_meta_shard_owner_serving(
            &meta_bucket_key(bucket),
            expected_shard_owner,
            expected_shard_epoch,
            update_time_ms,
        )?;
        self.propose_configure_bucket_lifecycle_with_read_proof_linearizable_committed_at(
            bucket,
            policy,
            update_time_ms,
            min_read_index,
            min_commit_index,
        )
    }

    pub fn propose_configure_bucket_lifecycle_idempotent_from_meta_shard(
        &mut self,
        bucket: &str,
        policy: BucketLifecyclePolicy,
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
            "configure bucket lifecycle idempotent from meta shard",
            min_read_index,
            0,
        )?;
        self.propose_configure_bucket_lifecycle_idempotent(
            bucket,
            policy,
            update_time_ms,
            idempotency_key,
        )
    }

    pub fn propose_configure_bucket_lifecycle_idempotent_with_read_proof_from_meta_shard(
        &mut self,
        bucket: &str,
        policy: BucketLifecyclePolicy,
        update_time_ms: u64,
        idempotency_key: &str,
        min_read_index: u64,
        min_commit_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
    ) -> Result<ProvenMetadataRead<BucketLifecycleMutationReport>, ObjectError> {
        self.store.check_meta_shard_owner_serving(
            &meta_bucket_key(bucket),
            expected_shard_owner,
            expected_shard_epoch,
            update_time_ms,
        )?;
        self.propose_configure_bucket_lifecycle_idempotent_with_read_proof_linearizable_committed_at(
            bucket,
            policy,
            update_time_ms,
            idempotency_key,
            min_read_index,
            min_commit_index,
        )
    }
}
