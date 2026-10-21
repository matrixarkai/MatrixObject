// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn list_buckets_from_meta_shard(
        &self,
        marker: Option<&str>,
        limit: usize,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<BucketInventoryPage, ObjectError> {
        self.store.list_buckets_with_metadata_read(
            marker,
            limit,
            MetadataReadOptions {
                min_read_index,
                now_ms,
                require_shard_owner_serving: true,
                expected_shard_owner: Some(expected_shard_owner.to_string()),
                expected_shard_epoch: Some(expected_shard_epoch),
            },
        )
    }

    pub fn bucket_versioning_enabled_from_meta_shard(
        &self,
        bucket: &str,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<(bool, MetadataRaftReadiness), ObjectError> {
        self.store.bucket_versioning_enabled_with_metadata_read(
            bucket,
            MetadataReadOptions {
                min_read_index,
                now_ms,
                require_shard_owner_serving: true,
                expected_shard_owner: Some(expected_shard_owner.to_string()),
                expected_shard_epoch: Some(expected_shard_epoch),
            },
        )
    }

    pub fn bucket_versioning_enabled_with_read_proof_from_meta_shard(
        &self,
        bucket: &str,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<bool>, ObjectError> {
        let (enabled, readiness) = self.bucket_versioning_enabled_from_meta_shard(
            bucket,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        Ok(proven_metadata_read(enabled, &readiness))
    }

    pub fn bucket_retention_from_meta_shard(
        &self,
        bucket: &str,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<(u64, MetadataRaftReadiness), ObjectError> {
        self.store.bucket_retention_with_metadata_read(
            bucket,
            MetadataReadOptions {
                min_read_index,
                now_ms,
                require_shard_owner_serving: true,
                expected_shard_owner: Some(expected_shard_owner.to_string()),
                expected_shard_epoch: Some(expected_shard_epoch),
            },
        )
    }

    pub fn bucket_retention_with_read_proof_from_meta_shard(
        &self,
        bucket: &str,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<u64>, ObjectError> {
        let (retention, readiness) = self.bucket_retention_from_meta_shard(
            bucket,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        Ok(proven_metadata_read(retention, &readiness))
    }

    pub fn bucket_quota_from_meta_shard(
        &self,
        bucket: &str,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<(BucketQuota, MetadataRaftReadiness), ObjectError> {
        self.store.bucket_quota_with_metadata_read(
            bucket,
            MetadataReadOptions {
                min_read_index,
                now_ms,
                require_shard_owner_serving: true,
                expected_shard_owner: Some(expected_shard_owner.to_string()),
                expected_shard_epoch: Some(expected_shard_epoch),
            },
        )
    }

    pub fn bucket_quota_with_read_proof_from_meta_shard(
        &self,
        bucket: &str,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<BucketQuota>, ObjectError> {
        let (quota, readiness) = self.bucket_quota_from_meta_shard(
            bucket,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        Ok(proven_metadata_read(quota, &readiness))
    }

    pub fn bucket_lifecycle_from_meta_shard(
        &self,
        bucket: &str,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<(BucketLifecyclePolicy, MetadataRaftReadiness), ObjectError> {
        self.store.bucket_lifecycle_with_metadata_read(
            bucket,
            MetadataReadOptions {
                min_read_index,
                now_ms,
                require_shard_owner_serving: true,
                expected_shard_owner: Some(expected_shard_owner.to_string()),
                expected_shard_epoch: Some(expected_shard_epoch),
            },
        )
    }

    pub fn bucket_lifecycle_with_read_proof_from_meta_shard(
        &self,
        bucket: &str,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<BucketLifecyclePolicy>, ObjectError> {
        let (policy, readiness) = self.bucket_lifecycle_from_meta_shard(
            bucket,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        Ok(proven_metadata_read(policy, &readiness))
    }

    pub fn bucket_tagging_from_meta_shard(
        &self,
        bucket: &str,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<(BucketTagSet, MetadataRaftReadiness), ObjectError> {
        self.store.bucket_tagging_with_metadata_read(
            bucket,
            MetadataReadOptions {
                min_read_index,
                now_ms,
                require_shard_owner_serving: true,
                expected_shard_owner: Some(expected_shard_owner.to_string()),
                expected_shard_epoch: Some(expected_shard_epoch),
            },
        )
    }

    pub fn bucket_tagging_with_read_proof_from_meta_shard(
        &self,
        bucket: &str,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<BucketTagSet>, ObjectError> {
        let (tag_set, readiness) = self.bucket_tagging_from_meta_shard(
            bucket,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        Ok(proven_metadata_read(tag_set, &readiness))
    }

    pub fn bucket_cors_from_meta_shard(
        &self,
        bucket: &str,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<(BucketCorsPolicy, MetadataRaftReadiness), ObjectError> {
        self.store.bucket_cors_with_metadata_read(
            bucket,
            MetadataReadOptions {
                min_read_index,
                now_ms,
                require_shard_owner_serving: true,
                expected_shard_owner: Some(expected_shard_owner.to_string()),
                expected_shard_epoch: Some(expected_shard_epoch),
            },
        )
    }

    pub fn bucket_cors_with_read_proof_from_meta_shard(
        &self,
        bucket: &str,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<BucketCorsPolicy>, ObjectError> {
        let (policy, readiness) = self.bucket_cors_from_meta_shard(
            bucket,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        Ok(proven_metadata_read(policy, &readiness))
    }

    pub fn evaluate_bucket_cors_preflight_from_meta_shard(
        &self,
        bucket: &str,
        request: BucketCorsPreflightRequest,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<(BucketCorsPreflightDecision, MetadataRaftReadiness), ObjectError> {
        self.store
            .evaluate_bucket_cors_preflight_with_metadata_read(
                bucket,
                request,
                MetadataReadOptions {
                    min_read_index,
                    now_ms,
                    require_shard_owner_serving: true,
                    expected_shard_owner: Some(expected_shard_owner.to_string()),
                    expected_shard_epoch: Some(expected_shard_epoch),
                },
            )
    }

    pub fn evaluate_bucket_cors_preflight_with_read_proof_from_meta_shard(
        &self,
        bucket: &str,
        request: BucketCorsPreflightRequest,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<BucketCorsPreflightDecision>, ObjectError> {
        let (decision, readiness) = self.evaluate_bucket_cors_preflight_from_meta_shard(
            bucket,
            request,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        Ok(proven_metadata_read(decision, &readiness))
    }

    pub fn bucket_cors_response_allowed_origin_from_meta_shard(
        &self,
        bucket: &str,
        origin: &str,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<(Option<String>, MetadataRaftReadiness), ObjectError> {
        self.store
            .bucket_cors_response_allowed_origin_with_metadata_read(
                bucket,
                origin,
                MetadataReadOptions {
                    min_read_index,
                    now_ms,
                    require_shard_owner_serving: true,
                    expected_shard_owner: Some(expected_shard_owner.to_string()),
                    expected_shard_epoch: Some(expected_shard_epoch),
                },
            )
    }

    pub fn bucket_cors_response_allowed_origin_with_read_proof_from_meta_shard(
        &self,
        bucket: &str,
        origin: &str,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<Option<String>>, ObjectError> {
        let (allowed_origin, readiness) = self
            .bucket_cors_response_allowed_origin_from_meta_shard(
                bucket,
                origin,
                min_read_index,
                expected_shard_owner,
                expected_shard_epoch,
                now_ms,
            )?;
        Ok(proven_metadata_read(allowed_origin, &readiness))
    }

}
