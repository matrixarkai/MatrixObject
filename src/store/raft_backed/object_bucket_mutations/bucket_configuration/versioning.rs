// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_set_bucket_versioning(
        &mut self,
        bucket: &str,
        enabled: bool,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::SetBucketVersioning {
            bucket: bucket.to_string(),
            enabled,
        })
    }

    pub fn propose_set_bucket_versioning_idempotent(
        &mut self,
        bucket: &str,
        enabled: bool,
        update_time_ms: u64,
        idempotency_key: &str,
    ) -> Result<BucketPolicyMutationReceipt, ObjectError> {
        let request_checksum = bucket_versioning_request_checksum(bucket, enabled)?;
        if let Some(existing) = self.store.bucket_policy_receipts.get(idempotency_key) {
            return checked_bucket_policy_receipt_retry(
                existing,
                "ConfigureBucketVersioning",
                bucket,
                &request_checksum,
            );
        }
        validate_idempotency_key(idempotency_key)?;
        self.propose(ObjectCommand::ConfigureBucketVersioningIdempotent {
            bucket: bucket.to_string(),
            enabled,
            update_time_ms,
            idempotency_key: idempotency_key.to_string(),
        })?;
        self.store
            .bucket_policy_receipts
            .get(idempotency_key)
            .cloned()
            .ok_or_else(|| {
                ObjectError::Corruption(
                    "raft bucket versioning idempotent command did not retain receipt".to_string(),
                )
            })
    }

    pub fn propose_set_bucket_versioning_idempotent_with_read_proof_linearizable(
        &mut self,
        bucket: &str,
        enabled: bool,
        update_time_ms: u64,
        idempotency_key: &str,
    ) -> Result<ProvenMetadataRead<BucketVersioningMutationReport>, ObjectError> {
        self.propose_set_bucket_versioning_idempotent_with_read_proof_linearizable_committed_at(
            bucket,
            enabled,
            update_time_ms,
            idempotency_key,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_set_bucket_versioning_idempotent_with_read_proof_linearizable_committed_at(
        &mut self,
        bucket: &str,
        enabled: bool,
        update_time_ms: u64,
        idempotency_key: &str,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<BucketVersioningMutationReport>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "set bucket versioning idempotent",
            update_time_ms,
            min_read_index,
            min_commit_index,
        )?;
        let receipt = self.propose_set_bucket_versioning_idempotent(
            bucket,
            enabled,
            update_time_ms,
            idempotency_key,
        )?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "set bucket versioning idempotent post-commit",
            update_time_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        let report = BucketVersioningMutationReport {
            metadata: receipt.metadata,
            enabled: self.store.bucket_versioning_enabled(bucket)?,
        };
        Ok(proven_metadata_read(report, &readiness))
    }

    pub fn propose_set_bucket_versioning_with_read_proof_linearizable(
        &mut self,
        bucket: &str,
        enabled: bool,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<BucketVersioningMutationReport>, ObjectError> {
        self.propose_set_bucket_versioning_with_read_proof_linearizable_committed_at(
            bucket,
            enabled,
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_set_bucket_versioning_with_read_proof_linearizable_committed_at(
        &mut self,
        bucket: &str,
        enabled: bool,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<BucketVersioningMutationReport>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "set bucket versioning",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        let metadata = self.propose_set_bucket_versioning(bucket, enabled)?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "set bucket versioning post-commit",
            now_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        let report = BucketVersioningMutationReport {
            metadata,
            enabled: self.store.bucket_versioning_enabled(bucket)?,
        };
        Ok(proven_metadata_read(report, &readiness))
    }

    pub fn propose_set_bucket_versioning_from_meta_shard(
        &mut self,
        bucket: &str,
        enabled: bool,
        now_ms: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.store.check_meta_shard_owner_serving(
            &meta_bucket_key(bucket),
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        self.store.ensure_metadata_mutation_fence(
            "set bucket versioning from meta shard",
            min_read_index,
            0,
        )?;
        self.propose_set_bucket_versioning(bucket, enabled)
    }

    pub fn propose_set_bucket_versioning_with_read_proof_from_meta_shard(
        &mut self,
        bucket: &str,
        enabled: bool,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
    ) -> Result<ProvenMetadataRead<BucketVersioningMutationReport>, ObjectError> {
        self.store.check_meta_shard_owner_serving(
            &meta_bucket_key(bucket),
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        self.propose_set_bucket_versioning_with_read_proof_linearizable_committed_at(
            bucket,
            enabled,
            now_ms,
            min_read_index,
            min_commit_index,
        )
    }

    pub fn propose_set_bucket_versioning_idempotent_from_meta_shard(
        &mut self,
        bucket: &str,
        enabled: bool,
        now_ms: u64,
        idempotency_key: &str,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
    ) -> Result<BucketPolicyMutationReceipt, ObjectError> {
        self.store.check_meta_shard_owner_serving(
            &meta_bucket_key(bucket),
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        self.store.ensure_metadata_mutation_fence(
            "set bucket versioning idempotent from meta shard",
            min_read_index,
            0,
        )?;
        self.propose_set_bucket_versioning_idempotent(bucket, enabled, now_ms, idempotency_key)
    }

    pub fn propose_set_bucket_versioning_idempotent_with_read_proof_from_meta_shard(
        &mut self,
        bucket: &str,
        enabled: bool,
        now_ms: u64,
        idempotency_key: &str,
        min_read_index: u64,
        min_commit_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
    ) -> Result<ProvenMetadataRead<BucketVersioningMutationReport>, ObjectError> {
        self.store.check_meta_shard_owner_serving(
            &meta_bucket_key(bucket),
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        self.propose_set_bucket_versioning_idempotent_with_read_proof_linearizable_committed_at(
            bucket,
            enabled,
            now_ms,
            idempotency_key,
            min_read_index,
            min_commit_index,
        )
    }

}
