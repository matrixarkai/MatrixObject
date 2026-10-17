impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_apply_bucket_lifecycle_transitions(
        &mut self,
        max_actions: usize,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::ApplyBucketLifecycleTransitions {
            max_actions,
            now_ms,
        })
    }

    pub fn propose_apply_bucket_lifecycle_transitions_report(
        &mut self,
        max_actions: usize,
        now_ms: u64,
    ) -> Result<LifecycleApplyResult, ObjectError> {
        let mut projected = self.store.clone();
        let report = projected.apply_bucket_lifecycle_transitions_at(max_actions, now_ms)?;
        self.propose_apply_bucket_lifecycle_transitions(max_actions, now_ms)?;
        Ok(report)
    }

    pub fn propose_apply_bucket_lifecycle_transitions_report_with_read_proof_linearizable(
        &mut self,
        max_actions: usize,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<LifecycleApplyResult>, ObjectError> {
        self.propose_apply_bucket_lifecycle_transitions_report_with_read_proof_linearizable_committed_at(
            max_actions,
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_apply_bucket_lifecycle_transitions_report_with_read_proof_linearizable_committed_at(
        &mut self,
        max_actions: usize,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<LifecycleApplyResult>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "bucket lifecycle transition report",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        let report = self.propose_apply_bucket_lifecycle_transitions_report(max_actions, now_ms)?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "bucket lifecycle transition report post-commit",
            now_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        Ok(proven_metadata_read(report, &readiness))
    }

    pub fn propose_apply_bucket_lifecycle_transitions_report_from_meta_shard(
        &mut self,
        max_actions: usize,
        now_ms: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
    ) -> Result<LifecycleApplyResult, ObjectError> {
        self.store.check_meta_shard_owner_serving(
            meta_lifecycle_transition_key(),
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        self.propose_apply_bucket_lifecycle_transitions_report(max_actions, now_ms)
    }

    pub fn propose_apply_bucket_lifecycle_transitions_report_with_read_proof_from_meta_shard(
        &mut self,
        max_actions: usize,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
    ) -> Result<ProvenMetadataRead<LifecycleApplyResult>, ObjectError> {
        self.store.check_meta_shard_owner_serving(
            meta_lifecycle_transition_key(),
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        self.propose_apply_bucket_lifecycle_transitions_report_with_read_proof_linearizable_committed_at(
            max_actions,
            now_ms,
            min_read_index,
            min_commit_index,
        )
    }
}
