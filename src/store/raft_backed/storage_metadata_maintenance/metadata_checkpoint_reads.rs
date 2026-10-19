// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn plan_meta_change_checkpoint_lag_linearizable(
        &self,
        prefix: &str,
        marker: Option<&str>,
        max_checkpoints: usize,
        target_change_sequence: Option<u64>,
        target_commit_index: Option<u64>,
        min_change_lag: u64,
        min_commit_lag: u64,
        include_not_behind: bool,
        only_compacted: bool,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<MetadataChangeCheckpointLagPlan, ObjectError> {
        self.plan_meta_change_checkpoint_lag_linearizable_committed_at(
            prefix,
            marker,
            max_checkpoints,
            target_change_sequence,
            target_commit_index,
            min_change_lag,
            min_commit_lag,
            include_not_behind,
            only_compacted,
            max_payload_bytes,
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn plan_meta_change_checkpoint_lag_linearizable_committed_at(
        &self,
        prefix: &str,
        marker: Option<&str>,
        max_checkpoints: usize,
        target_change_sequence: Option<u64>,
        target_commit_index: Option<u64>,
        min_change_lag: u64,
        min_commit_lag: u64,
        include_not_behind: bool,
        only_compacted: bool,
        max_payload_bytes: u64,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<MetadataChangeCheckpointLagPlan, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "metadata change checkpoint lag plan",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        self.store.plan_metadata_change_checkpoint_lag(
            prefix,
            marker,
            max_checkpoints,
            target_change_sequence,
            target_commit_index,
            min_change_lag,
            min_commit_lag,
            include_not_behind,
            only_compacted,
            max_payload_bytes,
        )
    }

    pub fn plan_meta_change_checkpoint_lag_with_read_proof_linearizable(
        &self,
        prefix: &str,
        marker: Option<&str>,
        max_checkpoints: usize,
        target_change_sequence: Option<u64>,
        target_commit_index: Option<u64>,
        min_change_lag: u64,
        min_commit_lag: u64,
        include_not_behind: bool,
        only_compacted: bool,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<MetadataChangeCheckpointLagPlan>, ObjectError> {
        self.plan_meta_change_checkpoint_lag_with_read_proof_linearizable_committed_at(
            prefix,
            marker,
            max_checkpoints,
            target_change_sequence,
            target_commit_index,
            min_change_lag,
            min_commit_lag,
            include_not_behind,
            only_compacted,
            max_payload_bytes,
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn plan_meta_change_checkpoint_lag_with_read_proof_linearizable_committed_at(
        &self,
        prefix: &str,
        marker: Option<&str>,
        max_checkpoints: usize,
        target_change_sequence: Option<u64>,
        target_commit_index: Option<u64>,
        min_change_lag: u64,
        min_commit_lag: u64,
        include_not_behind: bool,
        only_compacted: bool,
        max_payload_bytes: u64,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<MetadataChangeCheckpointLagPlan>, ObjectError> {
        let readiness = self.ensure_metadata_read_index_committed_at(
            "metadata change checkpoint lag plan",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        let plan = self.store.plan_metadata_change_checkpoint_lag(
            prefix,
            marker,
            max_checkpoints,
            target_change_sequence,
            target_commit_index,
            min_change_lag,
            min_commit_lag,
            include_not_behind,
            only_compacted,
            max_payload_bytes,
        )?;
        Ok(proven_metadata_read(plan, &readiness))
    }

    pub fn metadata_change_checkpoint_linearizable(
        &self,
        consumer_id: &str,
        prefix: &str,
        now_ms: u64,
    ) -> Result<(MetadataChangeCheckpoint, MetadataRaftReadiness), ObjectError> {
        self.metadata_change_checkpoint_linearizable_committed_at(
            consumer_id,
            prefix,
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn metadata_change_checkpoint_linearizable_committed_at(
        &self,
        consumer_id: &str,
        prefix: &str,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<(MetadataChangeCheckpoint, MetadataRaftReadiness), ObjectError> {
        let readiness = self.ensure_metadata_read_index_committed_at(
            "metadata change checkpoint",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        let checkpoint = self.store.metadata_change_checkpoint(consumer_id, prefix)?;
        Ok((checkpoint, readiness))
    }

    pub fn metadata_change_checkpoint_with_read_proof_linearizable(
        &self,
        consumer_id: &str,
        prefix: &str,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<MetadataChangeCheckpoint>, ObjectError> {
        self.metadata_change_checkpoint_with_read_proof_linearizable_committed_at(
            consumer_id,
            prefix,
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn metadata_change_checkpoint_with_read_proof_linearizable_committed_at(
        &self,
        consumer_id: &str,
        prefix: &str,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<MetadataChangeCheckpoint>, ObjectError> {
        let (checkpoint, readiness) = self.metadata_change_checkpoint_linearizable_committed_at(
            consumer_id,
            prefix,
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        Ok(proven_metadata_read(checkpoint, &readiness))
    }

    pub fn page_metadata_change_checkpoints_linearizable(
        &self,
        prefix: &str,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<(MetadataChangeCheckpointPage, MetadataRaftReadiness), ObjectError> {
        self.page_metadata_change_checkpoints_linearizable_committed_at(
            prefix,
            marker,
            limit,
            max_payload_bytes,
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn page_metadata_change_checkpoints_linearizable_committed_at(
        &self,
        prefix: &str,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<(MetadataChangeCheckpointPage, MetadataRaftReadiness), ObjectError> {
        let readiness = self.ensure_metadata_read_index_committed_at(
            "metadata change checkpoint list",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        let page = self.store.page_metadata_change_checkpoints(
            prefix,
            marker,
            limit,
            max_payload_bytes,
        )?;
        Ok((page, readiness))
    }

    pub fn page_metadata_change_checkpoints_with_read_proof_linearizable(
        &self,
        prefix: &str,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<MetadataChangeCheckpointPage>, ObjectError> {
        self.page_metadata_change_checkpoints_with_read_proof_linearizable_committed_at(
            prefix,
            marker,
            limit,
            max_payload_bytes,
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn page_metadata_change_checkpoints_with_read_proof_linearizable_committed_at(
        &self,
        prefix: &str,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<MetadataChangeCheckpointPage>, ObjectError> {
        let (page, readiness) = self.page_metadata_change_checkpoints_linearizable_committed_at(
            prefix,
            marker,
            limit,
            max_payload_bytes,
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        Ok(proven_metadata_read(page, &readiness))
    }

}
