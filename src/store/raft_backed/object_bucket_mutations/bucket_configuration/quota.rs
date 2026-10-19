// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_configure_bucket_quota(
        &mut self,
        bucket: &str,
        max_live_bytes: u64,
        max_live_objects: u64,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::ConfigureBucketQuota {
            bucket: bucket.to_string(),
            quota: BucketQuota {
                max_live_bytes,
                max_live_objects,
            },
            update_time_ms,
        })
    }

    pub fn propose_configure_bucket_quota_idempotent(
        &mut self,
        bucket: &str,
        max_live_bytes: u64,
        max_live_objects: u64,
        update_time_ms: u64,
        idempotency_key: &str,
    ) -> Result<BucketPolicyMutationReceipt, ObjectError> {
        let quota = BucketQuota {
            max_live_bytes,
            max_live_objects,
        };
        let request_checksum = bucket_quota_request_checksum(bucket, &quota)?;
        if let Some(existing) = self.store.bucket_policy_receipts.get(idempotency_key) {
            return checked_bucket_policy_receipt_retry(
                existing,
                "ConfigureBucketQuota",
                bucket,
                &request_checksum,
            );
        }
        validate_idempotency_key(idempotency_key)?;
        self.propose(ObjectCommand::ConfigureBucketQuotaIdempotent {
            bucket: bucket.to_string(),
            quota,
            update_time_ms,
            idempotency_key: idempotency_key.to_string(),
        })?;
        self.store
            .bucket_policy_receipts
            .get(idempotency_key)
            .cloned()
            .ok_or_else(|| {
                ObjectError::Corruption(
                    "raft bucket quota idempotent command did not retain receipt".to_string(),
                )
            })
    }

    pub fn propose_configure_bucket_quota_idempotent_with_read_proof_linearizable(
        &mut self,
        bucket: &str,
        max_live_bytes: u64,
        max_live_objects: u64,
        update_time_ms: u64,
        idempotency_key: &str,
    ) -> Result<ProvenMetadataRead<BucketQuotaMutationReport>, ObjectError> {
        self.propose_configure_bucket_quota_idempotent_with_read_proof_linearizable_committed_at(
            bucket,
            max_live_bytes,
            max_live_objects,
            update_time_ms,
            idempotency_key,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_configure_bucket_quota_idempotent_with_read_proof_linearizable_committed_at(
        &mut self,
        bucket: &str,
        max_live_bytes: u64,
        max_live_objects: u64,
        update_time_ms: u64,
        idempotency_key: &str,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<BucketQuotaMutationReport>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "configure bucket quota idempotent",
            update_time_ms,
            min_read_index,
            min_commit_index,
        )?;
        let receipt = self.propose_configure_bucket_quota_idempotent(
            bucket,
            max_live_bytes,
            max_live_objects,
            update_time_ms,
            idempotency_key,
        )?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "configure bucket quota idempotent post-commit",
            update_time_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        let quota = self.store.bucket_quota(bucket)?;
        let (live_bytes, live_objects) = self.store.bucket_live_usage(bucket)?;
        let report = BucketQuotaMutationReport {
            metadata: receipt.metadata,
            quota,
            live_bytes,
            live_objects,
        };
        Ok(proven_metadata_read(report, &readiness))
    }

    pub fn propose_configure_bucket_quota_with_read_proof_linearizable(
        &mut self,
        bucket: &str,
        max_live_bytes: u64,
        max_live_objects: u64,
        update_time_ms: u64,
    ) -> Result<ProvenMetadataRead<BucketQuotaMutationReport>, ObjectError> {
        self.propose_configure_bucket_quota_with_read_proof_linearizable_committed_at(
            bucket,
            max_live_bytes,
            max_live_objects,
            update_time_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_configure_bucket_quota_with_read_proof_linearizable_committed_at(
        &mut self,
        bucket: &str,
        max_live_bytes: u64,
        max_live_objects: u64,
        update_time_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<BucketQuotaMutationReport>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "configure bucket quota",
            update_time_ms,
            min_read_index,
            min_commit_index,
        )?;
        let metadata = self.propose_configure_bucket_quota(
            bucket,
            max_live_bytes,
            max_live_objects,
            update_time_ms,
        )?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "configure bucket quota post-commit",
            update_time_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        let quota = self.store.bucket_quota(bucket)?;
        let (live_bytes, live_objects) = self.store.bucket_live_usage(bucket)?;
        let report = BucketQuotaMutationReport {
            metadata,
            quota,
            live_bytes,
            live_objects,
        };
        Ok(proven_metadata_read(report, &readiness))
    }

    pub fn propose_configure_bucket_quota_from_meta_shard(
        &mut self,
        bucket: &str,
        max_live_bytes: u64,
        max_live_objects: u64,
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
            "configure bucket quota from meta shard",
            min_read_index,
            0,
        )?;
        self.propose_configure_bucket_quota(
            bucket,
            max_live_bytes,
            max_live_objects,
            update_time_ms,
        )
    }

    pub fn propose_configure_bucket_quota_with_read_proof_from_meta_shard(
        &mut self,
        bucket: &str,
        max_live_bytes: u64,
        max_live_objects: u64,
        update_time_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
    ) -> Result<ProvenMetadataRead<BucketQuotaMutationReport>, ObjectError> {
        self.store.check_meta_shard_owner_serving(
            &meta_bucket_key(bucket),
            expected_shard_owner,
            expected_shard_epoch,
            update_time_ms,
        )?;
        self.propose_configure_bucket_quota_with_read_proof_linearizable_committed_at(
            bucket,
            max_live_bytes,
            max_live_objects,
            update_time_ms,
            min_read_index,
            min_commit_index,
        )
    }

    pub fn propose_configure_bucket_quota_idempotent_from_meta_shard(
        &mut self,
        bucket: &str,
        max_live_bytes: u64,
        max_live_objects: u64,
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
            "configure bucket quota idempotent from meta shard",
            min_read_index,
            0,
        )?;
        self.propose_configure_bucket_quota_idempotent(
            bucket,
            max_live_bytes,
            max_live_objects,
            update_time_ms,
            idempotency_key,
        )
    }

    pub fn propose_configure_bucket_quota_idempotent_with_read_proof_from_meta_shard(
        &mut self,
        bucket: &str,
        max_live_bytes: u64,
        max_live_objects: u64,
        update_time_ms: u64,
        idempotency_key: &str,
        min_read_index: u64,
        min_commit_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
    ) -> Result<ProvenMetadataRead<BucketQuotaMutationReport>, ObjectError> {
        self.store.check_meta_shard_owner_serving(
            &meta_bucket_key(bucket),
            expected_shard_owner,
            expected_shard_epoch,
            update_time_ms,
        )?;
        self.propose_configure_bucket_quota_idempotent_with_read_proof_linearizable_committed_at(
            bucket,
            max_live_bytes,
            max_live_objects,
            update_time_ms,
            idempotency_key,
            min_read_index,
            min_commit_index,
        )
    }

}
