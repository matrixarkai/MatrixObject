// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_create_bucket(&mut self, bucket: &str) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::CreateBucket {
            bucket: bucket.to_string(),
        })
    }

    pub fn propose_create_bucket_idempotent(
        &mut self,
        bucket: &str,
        update_time_ms: u64,
        idempotency_key: &str,
    ) -> Result<BucketPolicyMutationReceipt, ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        let request_checksum = bucket_create_request_checksum(bucket)?;
        if let Some(existing) = self.store.bucket_policy_receipts.get(idempotency_key) {
            return checked_bucket_policy_receipt_retry(
                existing,
                "CreateBucket",
                bucket,
                &request_checksum,
            );
        }
        self.propose(ObjectCommand::CreateBucketIdempotent {
            bucket: bucket.to_string(),
            update_time_ms,
            idempotency_key: idempotency_key.to_string(),
        })?;
        self.store
            .bucket_policy_receipts
            .get(idempotency_key)
            .cloned()
            .ok_or_else(|| {
                ObjectError::Corruption(
                    "raft create bucket idempotent command did not retain receipt".to_string(),
                )
            })
    }

    pub fn propose_create_bucket_idempotent_with_read_proof_linearizable(
        &mut self,
        bucket: &str,
        update_time_ms: u64,
        idempotency_key: &str,
    ) -> Result<ProvenMetadataRead<BucketPolicyMutationReceipt>, ObjectError> {
        self.propose_create_bucket_idempotent_with_read_proof_linearizable_committed_at(
            bucket,
            update_time_ms,
            idempotency_key,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_create_bucket_idempotent_with_read_proof_linearizable_committed_at(
        &mut self,
        bucket: &str,
        update_time_ms: u64,
        idempotency_key: &str,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<BucketPolicyMutationReceipt>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "create bucket idempotent",
            update_time_ms,
            min_read_index,
            min_commit_index,
        )?;
        let receipt =
            self.propose_create_bucket_idempotent(bucket, update_time_ms, idempotency_key)?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "create bucket idempotent post-commit",
            update_time_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        Ok(proven_metadata_read(receipt, &readiness))
    }

    pub fn propose_create_bucket_with_read_proof_linearizable(
        &mut self,
        bucket: &str,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        self.propose_create_bucket_with_read_proof_linearizable_committed_at(
            bucket,
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_create_bucket_with_read_proof_linearizable_committed_at(
        &mut self,
        bucket: &str,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "create bucket",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        let metadata = self.propose_create_bucket(bucket)?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "create bucket post-commit",
            now_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        Ok(proven_metadata_read(metadata, &readiness))
    }

    pub fn propose_create_bucket_from_meta_shard(
        &mut self,
        bucket: &str,
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
            "create bucket from meta shard",
            min_read_index,
            0,
        )?;
        self.propose_create_bucket(bucket)
    }

    pub fn propose_create_bucket_idempotent_from_meta_shard(
        &mut self,
        bucket: &str,
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
            "create bucket idempotent from meta shard",
            min_read_index,
            0,
        )?;
        self.propose_create_bucket_idempotent(bucket, update_time_ms, idempotency_key)
    }

    pub fn propose_create_bucket_idempotent_with_read_proof_from_meta_shard(
        &mut self,
        bucket: &str,
        update_time_ms: u64,
        idempotency_key: &str,
        min_read_index: u64,
        min_commit_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
    ) -> Result<ProvenMetadataRead<BucketPolicyMutationReceipt>, ObjectError> {
        self.store.check_meta_shard_owner_serving(
            &meta_bucket_key(bucket),
            expected_shard_owner,
            expected_shard_epoch,
            update_time_ms,
        )?;
        self.propose_create_bucket_idempotent_with_read_proof_linearizable_committed_at(
            bucket,
            update_time_ms,
            idempotency_key,
            min_read_index,
            min_commit_index,
        )
    }

    pub fn propose_create_bucket_with_read_proof_from_meta_shard(
        &mut self,
        bucket: &str,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        self.store.check_meta_shard_owner_serving(
            &meta_bucket_key(bucket),
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        self.propose_create_bucket_with_read_proof_linearizable_committed_at(
            bucket,
            now_ms,
            min_read_index,
            min_commit_index,
        )
    }
}
