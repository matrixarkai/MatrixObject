// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn plan_failover_meta_shards_with_read_proof_linearizable(
        &self,
        failed_owner_node_id: &str,
        replacement_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<MetadataShardMigrationReport>, ObjectError> {
        self.plan_failover_meta_shards_with_read_proof_linearizable_committed_at(
            failed_owner_node_id,
            replacement_owner_node_id,
            max_shards,
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn plan_failover_meta_shards_with_read_proof_linearizable_committed_at(
        &self,
        failed_owner_node_id: &str,
        replacement_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<MetadataShardMigrationReport>, ObjectError> {
        let readiness = self.ensure_metadata_read_index_committed_at(
            "metadata shard failover plan",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        let report = self.store.plan_metadata_shard_failover(
            failed_owner_node_id,
            replacement_owner_node_id,
            max_shards,
            now_ms,
        )?;
        Ok(proven_metadata_read(report, &readiness))
    }

    pub fn propose_proven_failover_meta_shards_fenced(
        &mut self,
        proven_plan: ProvenMetadataRead<MetadataShardMigrationReport>,
        failed_owner_node_id: &str,
        replacement_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
        expected_plan_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_plan_checksum = expected_plan_checksum.as_ref();
        if !expected_plan_checksum.is_empty()
            && proven_plan.value.plan_checksum != expected_plan_checksum
        {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata shard failover proven plan checksum mismatch: expected {} actual {}",
                expected_plan_checksum, proven_plan.value.plan_checksum
            )));
        }
        if self.store.last_metadata_commit_index() < proven_plan.proof.metadata_commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven metadata shard failover proof is not applied: applied={} required={}",
                self.store.last_metadata_commit_index(),
                proven_plan.proof.metadata_commit_index
            )));
        }
        self.propose_failover_meta_shards_fenced(
            failed_owner_node_id,
            replacement_owner_node_id,
            max_shards,
            now_ms,
            &proven_plan.value.plan_checksum,
            Some(proven_plan.value.metadata_shard_topology_epoch),
            Some(&proven_plan.value.metadata_shard_topology_checksum),
            Some(&proven_plan.value.metadata_coordinator_session_checksum),
        )
    }

    pub fn propose_failover_meta_shards_linearizable_fenced(
        &mut self,
        failed_owner_node_id: &str,
        replacement_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
        expected_plan_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let proven_plan = self.plan_failover_meta_shards_with_read_proof_linearizable(
            failed_owner_node_id,
            replacement_owner_node_id,
            max_shards,
            now_ms,
        )?;
        self.propose_proven_failover_meta_shards_fenced(
            proven_plan,
            failed_owner_node_id,
            replacement_owner_node_id,
            max_shards,
            now_ms,
            expected_plan_checksum,
        )
    }

    pub fn plan_expired_meta_shard_owner_failover_report_with_read_proof_linearizable(
        &self,
        expired_owner_node_id: &str,
        replacement_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<MetadataShardMigrationReport>, ObjectError> {
        self.plan_expired_meta_shard_owner_failover_report_with_read_proof_linearizable_committed_at(
            expired_owner_node_id,
            replacement_owner_node_id,
            max_shards,
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn plan_expired_meta_shard_owner_failover_report_with_read_proof_linearizable_committed_at(
        &self,
        expired_owner_node_id: &str,
        replacement_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<MetadataShardMigrationReport>, ObjectError> {
        let readiness = self.ensure_metadata_read_index_committed_at(
            "expired metadata shard owner failover plan",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        let report = self.store.plan_expired_metadata_shard_owner_failover(
            expired_owner_node_id,
            replacement_owner_node_id,
            max_shards,
            now_ms,
        )?;
        Ok(proven_metadata_read(report, &readiness))
    }

    pub fn propose_proven_expired_meta_shard_owner_failover_fenced(
        &mut self,
        proven_plan: ProvenMetadataRead<MetadataShardMigrationReport>,
        expired_owner_node_id: &str,
        replacement_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
        expected_plan_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_plan_checksum = expected_plan_checksum.as_ref();
        if !expected_plan_checksum.is_empty()
            && proven_plan.value.plan_checksum != expected_plan_checksum
        {
            return Err(ObjectError::PreconditionFailed(format!(
                "expired metadata shard owner failover proven plan checksum mismatch: expected {} actual {}",
                expected_plan_checksum, proven_plan.value.plan_checksum
            )));
        }
        if self.store.last_metadata_commit_index() < proven_plan.proof.metadata_commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven expired metadata shard owner failover proof is not applied: applied={} required={}",
                self.store.last_metadata_commit_index(),
                proven_plan.proof.metadata_commit_index
            )));
        }
        self.propose_expired_meta_shard_owner_failover_fenced(
            expired_owner_node_id,
            replacement_owner_node_id,
            max_shards,
            now_ms,
            &proven_plan.value.plan_checksum,
            Some(proven_plan.value.metadata_shard_topology_epoch),
            Some(&proven_plan.value.metadata_shard_topology_checksum),
            Some(&proven_plan.value.metadata_coordinator_session_checksum),
        )
    }

    pub fn propose_expired_meta_shard_owner_failover_linearizable_fenced(
        &mut self,
        expired_owner_node_id: &str,
        replacement_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
        expected_plan_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let proven_plan = self
            .plan_expired_meta_shard_owner_failover_report_with_read_proof_linearizable(
                expired_owner_node_id,
                replacement_owner_node_id,
                max_shards,
                now_ms,
            )?;
        self.propose_proven_expired_meta_shard_owner_failover_fenced(
            proven_plan,
            expired_owner_node_id,
            replacement_owner_node_id,
            max_shards,
            now_ms,
            expected_plan_checksum,
        )
    }

    pub fn plan_rebalance_meta_shards_with_read_proof_linearizable(
        &self,
        source_owner_node_id: &str,
        target_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<MetadataShardMigrationReport>, ObjectError> {
        self.plan_rebalance_meta_shards_with_read_proof_linearizable_committed_at(
            source_owner_node_id,
            target_owner_node_id,
            max_shards,
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn plan_rebalance_meta_shards_with_read_proof_linearizable_committed_at(
        &self,
        source_owner_node_id: &str,
        target_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<MetadataShardMigrationReport>, ObjectError> {
        let readiness = self.ensure_metadata_read_index_committed_at(
            "metadata shard rebalance plan",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        let report = self.store.plan_metadata_shard_rebalance(
            source_owner_node_id,
            target_owner_node_id,
            max_shards,
            now_ms,
        )?;
        Ok(proven_metadata_read(report, &readiness))
    }

    pub fn propose_proven_rebalance_meta_shards_fenced(
        &mut self,
        proven_plan: ProvenMetadataRead<MetadataShardMigrationReport>,
        source_owner_node_id: &str,
        target_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
        expected_plan_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_plan_checksum = expected_plan_checksum.as_ref();
        if !expected_plan_checksum.is_empty()
            && proven_plan.value.plan_checksum != expected_plan_checksum
        {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata shard rebalance proven plan checksum mismatch: expected {} actual {}",
                expected_plan_checksum, proven_plan.value.plan_checksum
            )));
        }
        if self.store.last_metadata_commit_index() < proven_plan.proof.metadata_commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven metadata shard rebalance proof is not applied: applied={} required={}",
                self.store.last_metadata_commit_index(),
                proven_plan.proof.metadata_commit_index
            )));
        }
        self.propose_rebalance_meta_shards_fenced(
            source_owner_node_id,
            target_owner_node_id,
            max_shards,
            now_ms,
            &proven_plan.value.plan_checksum,
            Some(proven_plan.value.metadata_shard_topology_epoch),
            Some(&proven_plan.value.metadata_shard_topology_checksum),
            Some(&proven_plan.value.metadata_coordinator_session_checksum),
        )
    }

    pub fn propose_rebalance_meta_shards_linearizable_fenced(
        &mut self,
        source_owner_node_id: &str,
        target_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
        expected_plan_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let proven_plan = self.plan_rebalance_meta_shards_with_read_proof_linearizable(
            source_owner_node_id,
            target_owner_node_id,
            max_shards,
            now_ms,
        )?;
        self.propose_proven_rebalance_meta_shards_fenced(
            proven_plan,
            source_owner_node_id,
            target_owner_node_id,
            max_shards,
            now_ms,
            expected_plan_checksum,
        )
    }

}
