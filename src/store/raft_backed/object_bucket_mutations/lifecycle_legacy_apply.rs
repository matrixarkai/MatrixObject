// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_apply_legacy_bucket_lifecycle(
        &mut self,
        bucket: &str,
        noncurrent_versions_to_keep: usize,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::ApplyLegacyBucketLifecycle {
            bucket: bucket.to_string(),
            noncurrent_versions_to_keep,
            now_ms,
        })
    }

    pub fn propose_apply_legacy_bucket_lifecycle_report(
        &mut self,
        bucket: &str,
        noncurrent_versions_to_keep: usize,
        now_ms: u64,
    ) -> Result<u64, ObjectError> {
        let mut projected = self.store.clone();
        let expired = projected.apply_lifecycle_at(bucket, noncurrent_versions_to_keep, now_ms)?;
        self.propose_apply_legacy_bucket_lifecycle(bucket, noncurrent_versions_to_keep, now_ms)?;
        Ok(expired)
    }

    pub fn propose_apply_legacy_bucket_lifecycle_report_with_read_proof_linearizable(
        &mut self,
        bucket: &str,
        noncurrent_versions_to_keep: usize,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<u64>, ObjectError> {
        self.propose_apply_legacy_bucket_lifecycle_report_with_read_proof_linearizable_committed_at(
            bucket,
            noncurrent_versions_to_keep,
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_apply_legacy_bucket_lifecycle_report_with_read_proof_linearizable_committed_at(
        &mut self,
        bucket: &str,
        noncurrent_versions_to_keep: usize,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<u64>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "legacy bucket lifecycle apply report",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        let expired = self.propose_apply_legacy_bucket_lifecycle_report(
            bucket,
            noncurrent_versions_to_keep,
            now_ms,
        )?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "legacy bucket lifecycle apply report post-commit",
            now_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        Ok(proven_metadata_read(expired, &readiness))
    }

    pub fn propose_apply_legacy_bucket_lifecycle_report_from_meta_shard(
        &mut self,
        bucket: &str,
        noncurrent_versions_to_keep: usize,
        now_ms: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
    ) -> Result<u64, ObjectError> {
        self.store.check_meta_shard_owner_serving(
            &meta_bucket_key(bucket),
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        self.propose_apply_legacy_bucket_lifecycle_report(
            bucket,
            noncurrent_versions_to_keep,
            now_ms,
        )
    }

    pub fn propose_apply_legacy_bucket_lifecycle_report_with_read_proof_from_meta_shard(
        &mut self,
        bucket: &str,
        noncurrent_versions_to_keep: usize,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
    ) -> Result<ProvenMetadataRead<u64>, ObjectError> {
        self.store.check_meta_shard_owner_serving(
            &meta_bucket_key(bucket),
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        self.propose_apply_legacy_bucket_lifecycle_report_with_read_proof_linearizable_committed_at(
            bucket,
            noncurrent_versions_to_keep,
            now_ms,
            min_read_index,
            min_commit_index,
        )
    }
}
