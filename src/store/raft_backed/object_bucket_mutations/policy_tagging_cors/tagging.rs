impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_configure_bucket_tagging(
        &mut self,
        bucket: &str,
        tag_set: BucketTagSet,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        validate_tags(&tag_set.tags)?;
        self.propose(ObjectCommand::ConfigureBucketTagging {
            bucket: bucket.to_string(),
            tag_set,
            update_time_ms,
        })
    }

    pub fn propose_configure_bucket_tagging_idempotent(
        &mut self,
        bucket: &str,
        tag_set: BucketTagSet,
        update_time_ms: u64,
        idempotency_key: &str,
    ) -> Result<BucketPolicyMutationReceipt, ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        validate_tags(&tag_set.tags)?;
        let request_checksum = bucket_tagging_request_checksum(bucket, &tag_set)?;
        if let Some(existing) = self.store.bucket_policy_receipts.get(idempotency_key) {
            return checked_bucket_policy_receipt_retry(
                existing,
                "ConfigureBucketTagging",
                bucket,
                &request_checksum,
            );
        }
        self.propose(ObjectCommand::ConfigureBucketTaggingIdempotent {
            bucket: bucket.to_string(),
            tag_set,
            update_time_ms,
            idempotency_key: idempotency_key.to_string(),
        })?;
        self.store
            .bucket_policy_receipts
            .get(idempotency_key)
            .cloned()
            .ok_or_else(|| {
                ObjectError::Corruption(
                    "raft bucket tagging idempotent command did not retain receipt".to_string(),
                )
            })
    }

    pub fn propose_configure_bucket_tagging_with_read_proof_linearizable(
        &mut self,
        bucket: &str,
        tag_set: BucketTagSet,
        update_time_ms: u64,
    ) -> Result<ProvenMetadataRead<BucketTaggingMutationReport>, ObjectError> {
        self.propose_configure_bucket_tagging_with_read_proof_linearizable_committed_at(
            bucket,
            tag_set,
            update_time_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_configure_bucket_tagging_with_read_proof_linearizable_committed_at(
        &mut self,
        bucket: &str,
        tag_set: BucketTagSet,
        update_time_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<BucketTaggingMutationReport>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "configure bucket tagging",
            update_time_ms,
            min_read_index,
            min_commit_index,
        )?;
        let metadata = self.propose_configure_bucket_tagging(bucket, tag_set, update_time_ms)?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "configure bucket tagging post-commit",
            update_time_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        let report = BucketTaggingMutationReport {
            metadata,
            tag_set: self.store.bucket_tagging(bucket)?,
        };
        Ok(proven_metadata_read(report, &readiness))
    }

    pub fn propose_configure_bucket_tagging_idempotent_with_read_proof_linearizable(
        &mut self,
        bucket: &str,
        tag_set: BucketTagSet,
        update_time_ms: u64,
        idempotency_key: &str,
    ) -> Result<ProvenMetadataRead<BucketTaggingMutationReport>, ObjectError> {
        self.propose_configure_bucket_tagging_idempotent_with_read_proof_linearizable_committed_at(
            bucket,
            tag_set,
            update_time_ms,
            idempotency_key,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_configure_bucket_tagging_idempotent_with_read_proof_linearizable_committed_at(
        &mut self,
        bucket: &str,
        tag_set: BucketTagSet,
        update_time_ms: u64,
        idempotency_key: &str,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<BucketTaggingMutationReport>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "configure bucket tagging idempotent",
            update_time_ms,
            min_read_index,
            min_commit_index,
        )?;
        let receipt = self.propose_configure_bucket_tagging_idempotent(
            bucket,
            tag_set,
            update_time_ms,
            idempotency_key,
        )?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "configure bucket tagging idempotent post-commit",
            update_time_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        let report = BucketTaggingMutationReport {
            metadata: receipt.metadata,
            tag_set: self.store.bucket_tagging(bucket)?,
        };
        Ok(proven_metadata_read(report, &readiness))
    }

    pub fn propose_configure_bucket_tagging_from_meta_shard(
        &mut self,
        bucket: &str,
        tag_set: BucketTagSet,
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
            "configure bucket tagging from meta shard",
            min_read_index,
            0,
        )?;
        self.propose_configure_bucket_tagging(bucket, tag_set, update_time_ms)
    }

    pub fn propose_configure_bucket_tagging_with_read_proof_from_meta_shard(
        &mut self,
        bucket: &str,
        tag_set: BucketTagSet,
        update_time_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
    ) -> Result<ProvenMetadataRead<BucketTaggingMutationReport>, ObjectError> {
        self.store.check_meta_shard_owner_serving(
            &meta_bucket_key(bucket),
            expected_shard_owner,
            expected_shard_epoch,
            update_time_ms,
        )?;
        self.propose_configure_bucket_tagging_with_read_proof_linearizable_committed_at(
            bucket,
            tag_set,
            update_time_ms,
            min_read_index,
            min_commit_index,
        )
    }

    pub fn propose_configure_bucket_tagging_idempotent_from_meta_shard(
        &mut self,
        bucket: &str,
        tag_set: BucketTagSet,
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
            "configure bucket tagging idempotent from meta shard",
            min_read_index,
            0,
        )?;
        self.propose_configure_bucket_tagging_idempotent(
            bucket,
            tag_set,
            update_time_ms,
            idempotency_key,
        )
    }

    pub fn propose_configure_bucket_tagging_idempotent_with_read_proof_from_meta_shard(
        &mut self,
        bucket: &str,
        tag_set: BucketTagSet,
        update_time_ms: u64,
        idempotency_key: &str,
        min_read_index: u64,
        min_commit_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
    ) -> Result<ProvenMetadataRead<BucketTaggingMutationReport>, ObjectError> {
        self.store.check_meta_shard_owner_serving(
            &meta_bucket_key(bucket),
            expected_shard_owner,
            expected_shard_epoch,
            update_time_ms,
        )?;
        self.propose_configure_bucket_tagging_idempotent_with_read_proof_linearizable_committed_at(
            bucket,
            tag_set,
            update_time_ms,
            idempotency_key,
            min_read_index,
            min_commit_index,
        )
    }
}
