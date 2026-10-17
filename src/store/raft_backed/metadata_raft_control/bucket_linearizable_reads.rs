impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn store(&self) -> &MatrixObjectStore {
        &self.store
    }

    pub fn raft_commit_index(&self) -> u64 {
        self.raft.commit_index()
    }

    pub fn metadata_raft_readiness(
        &self,
        min_read_index: u64,
        now_ms: u64,
    ) -> MetadataRaftReadiness {
        self.store
            .metadata_raft_readiness(min_read_index, now_ms)
            .with_raft_position(self.raft.term(), self.raft.commit_index())
    }

    pub fn plan_metadata_integrity_linearizable(
        &self,
        now_ms: u64,
        tombstone_prefix: &str,
        max_sample_count: usize,
    ) -> Result<MetadataIntegrityPlan, ObjectError> {
        self.store.plan_metadata_integrity(
            now_ms,
            tombstone_prefix,
            max_sample_count,
            self.raft.commit_index(),
        )
    }

    pub fn list_buckets_linearizable(
        &self,
        marker: Option<&str>,
        limit: usize,
        now_ms: u64,
    ) -> Result<BucketInventoryPage, ObjectError> {
        self.store.list_buckets_with_metadata_read(
            marker,
            limit,
            MetadataReadOptions {
                min_read_index: self.raft.commit_index(),
                now_ms,
                ..MetadataReadOptions::default()
            },
        )
    }

    pub fn bucket_versioning_enabled_linearizable(
        &self,
        bucket: &str,
        now_ms: u64,
    ) -> Result<(bool, MetadataRaftReadiness), ObjectError> {
        self.store.bucket_versioning_enabled_with_metadata_read(
            bucket,
            MetadataReadOptions {
                min_read_index: self.raft.commit_index(),
                now_ms,
                ..MetadataReadOptions::default()
            },
        )
    }

    pub fn bucket_versioning_enabled_with_read_proof_linearizable(
        &self,
        bucket: &str,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<bool>, ObjectError> {
        let (enabled, readiness) = self.bucket_versioning_enabled_linearizable(bucket, now_ms)?;
        Ok(proven_metadata_read(enabled, &readiness))
    }

    pub fn bucket_retention_linearizable(
        &self,
        bucket: &str,
        now_ms: u64,
    ) -> Result<(u64, MetadataRaftReadiness), ObjectError> {
        self.store.bucket_retention_with_metadata_read(
            bucket,
            MetadataReadOptions {
                min_read_index: self.raft.commit_index(),
                now_ms,
                ..MetadataReadOptions::default()
            },
        )
    }

    pub fn bucket_retention_with_read_proof_linearizable(
        &self,
        bucket: &str,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<u64>, ObjectError> {
        let (retention, readiness) = self.bucket_retention_linearizable(bucket, now_ms)?;
        Ok(proven_metadata_read(retention, &readiness))
    }

    pub fn audit_object_metadata_linearizable(
        &self,
        bucket: &str,
        prefix: &str,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<ObjectMetadataAudit, ObjectError> {
        self.store.audit_object_metadata(
            bucket,
            prefix,
            marker,
            limit,
            max_payload_bytes,
            MetadataReadOptions {
                min_read_index: self.raft.commit_index(),
                now_ms,
                ..MetadataReadOptions::default()
            },
        )
    }

    pub fn audit_object_metadata_from_meta_shard_route_fenced(
        &self,
        bucket: &str,
        prefix: &str,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        expected_shard_topology_checksum: &str,
        now_ms: u64,
    ) -> Result<ObjectMetadataAudit, ObjectError> {
        self.store.audit_object_metadata_route_fenced(
            bucket,
            prefix,
            marker,
            limit,
            max_payload_bytes,
            MetadataReadOptions {
                min_read_index,
                now_ms,
                require_shard_owner_serving: true,
                expected_shard_owner: Some(expected_shard_owner.to_string()),
                expected_shard_epoch: Some(expected_shard_epoch),
            },
            Some(expected_shard_epoch),
            Some(expected_shard_topology_checksum),
        )
    }

    pub fn bucket_quota_linearizable(
        &self,
        bucket: &str,
        now_ms: u64,
    ) -> Result<(BucketQuota, MetadataRaftReadiness), ObjectError> {
        self.store.bucket_quota_with_metadata_read(
            bucket,
            MetadataReadOptions {
                min_read_index: self.raft.commit_index(),
                now_ms,
                ..MetadataReadOptions::default()
            },
        )
    }

    pub fn bucket_quota_with_read_proof_linearizable(
        &self,
        bucket: &str,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<BucketQuota>, ObjectError> {
        let (quota, readiness) = self.bucket_quota_linearizable(bucket, now_ms)?;
        Ok(proven_metadata_read(quota, &readiness))
    }

    pub fn bucket_lifecycle_linearizable(
        &self,
        bucket: &str,
        now_ms: u64,
    ) -> Result<(BucketLifecyclePolicy, MetadataRaftReadiness), ObjectError> {
        self.store.bucket_lifecycle_with_metadata_read(
            bucket,
            MetadataReadOptions {
                min_read_index: self.raft.commit_index(),
                now_ms,
                ..MetadataReadOptions::default()
            },
        )
    }

    pub fn bucket_lifecycle_with_read_proof_linearizable(
        &self,
        bucket: &str,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<BucketLifecyclePolicy>, ObjectError> {
        let (policy, readiness) = self.bucket_lifecycle_linearizable(bucket, now_ms)?;
        Ok(proven_metadata_read(policy, &readiness))
    }

    pub fn apply_bucket_lifecycle_policies_linearizable(
        &mut self,
        max_actions: usize,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<LifecycleApplyResult>, ObjectError> {
        self.propose_apply_bucket_lifecycle_policies_report_with_read_proof_linearizable(
            max_actions,
            now_ms,
        )
    }

    pub fn apply_bucket_lifecycle_transitions_linearizable(
        &mut self,
        max_actions: usize,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<LifecycleApplyResult>, ObjectError> {
        self.propose_apply_bucket_lifecycle_transitions_report_with_read_proof_linearizable(
            max_actions,
            now_ms,
        )
    }

    pub fn apply_legacy_bucket_lifecycle_linearizable(
        &mut self,
        bucket: &str,
        noncurrent_versions_to_keep: usize,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<u64>, ObjectError> {
        self.propose_apply_legacy_bucket_lifecycle_report_with_read_proof_linearizable(
            bucket,
            noncurrent_versions_to_keep,
            now_ms,
        )
    }

    pub fn bucket_cors_linearizable(
        &self,
        bucket: &str,
        now_ms: u64,
    ) -> Result<(BucketCorsPolicy, MetadataRaftReadiness), ObjectError> {
        self.store.bucket_cors_with_metadata_read(
            bucket,
            MetadataReadOptions {
                min_read_index: self.raft.commit_index(),
                now_ms,
                ..MetadataReadOptions::default()
            },
        )
    }

    pub fn bucket_cors_with_read_proof_linearizable(
        &self,
        bucket: &str,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<BucketCorsPolicy>, ObjectError> {
        let (policy, readiness) = self.bucket_cors_linearizable(bucket, now_ms)?;
        Ok(proven_metadata_read(policy, &readiness))
    }

    pub fn bucket_policy_linearizable(
        &self,
        bucket: &str,
        now_ms: u64,
    ) -> Result<(BucketPolicyDocument, MetadataRaftReadiness), ObjectError> {
        self.store.bucket_policy_with_metadata_read(
            bucket,
            MetadataReadOptions {
                min_read_index: self.raft.commit_index(),
                now_ms,
                ..MetadataReadOptions::default()
            },
        )
    }

    pub fn bucket_policy_with_read_proof_linearizable(
        &self,
        bucket: &str,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<BucketPolicyDocument>, ObjectError> {
        let (policy, readiness) = self.bucket_policy_linearizable(bucket, now_ms)?;
        Ok(proven_metadata_read(policy, &readiness))
    }

    pub fn bucket_tagging_linearizable(
        &self,
        bucket: &str,
        now_ms: u64,
    ) -> Result<(BucketTagSet, MetadataRaftReadiness), ObjectError> {
        self.store.bucket_tagging_with_metadata_read(
            bucket,
            MetadataReadOptions {
                min_read_index: self.raft.commit_index(),
                now_ms,
                ..MetadataReadOptions::default()
            },
        )
    }

    pub fn bucket_tagging_with_read_proof_linearizable(
        &self,
        bucket: &str,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<BucketTagSet>, ObjectError> {
        let (tag_set, readiness) = self.bucket_tagging_linearizable(bucket, now_ms)?;
        Ok(proven_metadata_read(tag_set, &readiness))
    }

    pub fn evaluate_bucket_cors_preflight_linearizable(
        &self,
        bucket: &str,
        request: BucketCorsPreflightRequest,
        now_ms: u64,
    ) -> Result<(BucketCorsPreflightDecision, MetadataRaftReadiness), ObjectError> {
        self.store
            .evaluate_bucket_cors_preflight_with_metadata_read(
                bucket,
                request,
                MetadataReadOptions {
                    min_read_index: self.raft.commit_index(),
                    now_ms,
                    ..MetadataReadOptions::default()
                },
            )
    }

    pub fn evaluate_bucket_cors_preflight_with_read_proof_linearizable(
        &self,
        bucket: &str,
        request: BucketCorsPreflightRequest,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<BucketCorsPreflightDecision>, ObjectError> {
        let (decision, readiness) =
            self.evaluate_bucket_cors_preflight_linearizable(bucket, request, now_ms)?;
        Ok(proven_metadata_read(decision, &readiness))
    }

    pub fn bucket_cors_response_allowed_origin_linearizable(
        &self,
        bucket: &str,
        origin: &str,
        now_ms: u64,
    ) -> Result<(Option<String>, MetadataRaftReadiness), ObjectError> {
        self.store
            .bucket_cors_response_allowed_origin_with_metadata_read(
                bucket,
                origin,
                MetadataReadOptions {
                    min_read_index: self.raft.commit_index(),
                    now_ms,
                    ..MetadataReadOptions::default()
                },
            )
    }

    pub fn bucket_cors_response_allowed_origin_with_read_proof_linearizable(
        &self,
        bucket: &str,
        origin: &str,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<Option<String>>, ObjectError> {
        let (allowed_origin, readiness) =
            self.bucket_cors_response_allowed_origin_linearizable(bucket, origin, now_ms)?;
        Ok(proven_metadata_read(allowed_origin, &readiness))
    }

}
